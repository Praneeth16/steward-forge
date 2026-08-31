"""Deterministic revalidation and execution of worker tool requests."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from threading import RLock
from typing import Any, Literal

from pydantic import BaseModel, ValidationError

from broker.contracts import (
    ArtifactWriteArgs,
    MutationReceipt,
    MutationRequest,
    SandboxWriteArgs,
    TaskRecordArgs,
    WorkerContract,
)
from broker.zero_ops import HealthSnapshot, PreActDenied, ZeroOpsPreAct


class BrokerDenied(ValueError):
    """A worker request failed deterministic broker validation."""


class IdempotencyConflict(BrokerDenied):
    """An idempotency key was reused for different request content."""


@dataclass(frozen=True, slots=True)
class ToolSpec:
    arguments_model: type[BaseModel]
    category: Literal["mutation", "evidence"]
    executor: Callable[[Any], dict[str, Any]]


@dataclass(frozen=True, slots=True)
class BrokerEvent:
    worker_id: str
    tool_id: str
    idempotency_key: str
    outcome: Literal["allowed", "denied", "replayed"]
    reason: str


class ArtifactPolicy:
    """Reject dangerous content even when it matches the argument schema."""

    denied_fragments = (
        "../",
        ".github/",
        "disable policy",
        "drop table",
        "export credential",
        "reveal secret",
    )

    def validate(self, value: object) -> None:
        for text in self._strings(value):
            normalized = text.casefold()
            if any(fragment in normalized for fragment in self.denied_fragments):
                raise BrokerDenied("harmful content was rejected by deterministic policy")

    def _strings(self, value: object) -> list[str]:
        if isinstance(value, str):
            return [value]
        if isinstance(value, dict):
            return [text for item in value.values() for text in self._strings(item)]
        if isinstance(value, list | tuple | set):
            return [text for item in value for text in self._strings(item)]
        return []


class CapabilityBroker:
    """The only execution path for state-changing worker tools."""

    def __init__(
        self,
        *,
        contracts: list[WorkerContract],
        tools: dict[str, ToolSpec],
        pre_act: ZeroOpsPreAct,
        artifact_policy: ArtifactPolicy,
    ) -> None:
        self._contracts = {
            (contract.contract_id, contract.contract_version): contract
            for contract in contracts
        }
        self._tools = tools
        self._pre_act = pre_act
        self._artifact_policy = artifact_policy
        self._receipts: dict[str, tuple[str, MutationReceipt]] = {}
        self._lock = RLock()
        self.events: list[BrokerEvent] = []

    def execute(self, request: MutationRequest) -> MutationReceipt:
        with self._lock:
            try:
                contract, tool, parsed, request_hash = self._validate(request)
                replay = self._receipts.get(request.idempotency_key)
                if replay is not None:
                    previous_hash, receipt = replay
                    if previous_hash != request_hash:
                        raise IdempotencyConflict(
                            "idempotency key is bound to a different request"
                        )
                    self._event(request, "replayed", "first receipt returned")
                    return receipt

                if tool.category != "evidence":
                    self._artifact_policy.validate(request.arguments)
                self._pre_act.authorize(tool.category)
                result = tool.executor(parsed)
                receipt = MutationReceipt(
                    receipt_id=hashlib.sha256(
                        f"receipt:{request_hash}".encode()
                    ).hexdigest()[:24],
                    request_hash=request_hash,
                    worker_id=contract.worker_id,
                    tool_id=request.tool_id,
                    result=result,
                )
                self._receipts[request.idempotency_key] = (request_hash, receipt)
                self._event(request, "allowed", "request executed")
                return receipt
            except (BrokerDenied, PreActDenied, ValidationError) as error:
                self._event(request, "denied", str(error))
                if isinstance(error, BrokerDenied):
                    raise
                raise BrokerDenied(str(error)) from error

    def _validate(
        self, request: MutationRequest
    ) -> tuple[WorkerContract, ToolSpec, BaseModel, str]:
        contract = self._contracts.get((request.contract_id, request.contract_version))
        if contract is None or contract.worker_id != request.worker_id:
            raise BrokerDenied("worker contract is not registered")
        if request.tool_id not in contract.allowed_tools:
            raise BrokerDenied("tool is not allowed by worker contract")
        tool = self._tools.get(request.tool_id)
        if tool is None:
            raise BrokerDenied("tool is not registered")
        parsed = tool.arguments_model.model_validate(request.arguments)
        if isinstance(parsed, SandboxWriteArgs) and (
            parsed.catalog != contract.sandbox_catalog
            or parsed.schema_name != contract.sandbox_schema
        ):
            raise BrokerDenied("requested resource is outside the contract sandbox")
        if isinstance(parsed, TaskRecordArgs):
            if parsed.task.worker_id != contract.worker_id:
                raise BrokerDenied("task worker does not match the worker contract")
            self._require_artifact_scope(contract, parsed.task.expected_output)
        if isinstance(parsed, ArtifactWriteArgs):
            self._require_artifact_scope(contract, parsed.artifact.path)
        request_hash = hashlib.sha256(
            json.dumps(
                request.model_dump(mode="json"),
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        return contract, tool, parsed, request_hash

    def _event(
        self,
        request: MutationRequest,
        outcome: Literal["allowed", "denied", "replayed"],
        reason: str,
    ) -> None:
        self.events.append(
            BrokerEvent(
                worker_id=request.worker_id,
                tool_id=request.tool_id,
                idempotency_key=request.idempotency_key,
                outcome=outcome,
                reason=reason,
            )
        )

    @staticmethod
    def _require_artifact_scope(contract: WorkerContract, path: str) -> None:
        prefixes = (prefix.rstrip("/") for prefix in contract.allowed_artifact_prefixes)
        if not any(path == prefix or path.startswith(f"{prefix}/") for prefix in prefixes):
            raise BrokerDenied("requested path is outside the contract artifact scope")


def create_tracer_broker(
    health_probe: Callable[[], HealthSnapshot] | None = None,
) -> CapabilityBroker:
    """Build the deterministic tracer broker around an injectable health probe.

    The default snapshot keeps the local tracer reproducible. A deployment must
    inject live platform probes before treating these checks as observed health.
    """

    def record_task(arguments: TaskRecordArgs) -> dict[str, Any]:
        return arguments.model_dump(mode="json")

    def accept_candidate(arguments: ArtifactWriteArgs) -> dict[str, Any]:
        return arguments.model_dump(mode="json")

    probe = health_probe or _deterministic_tracer_health
    return CapabilityBroker(
        contracts=[
            WorkerContract(
                contract_id="scrum-master-tracer",
                contract_version=1,
                worker_id="scrum-master",
                allowed_tools={"workflow.record-task", "artifact.accept-candidate"},
                allowed_artifact_prefixes={"generated/tracer"},
            )
        ],
        tools={
            "workflow.record-task": ToolSpec(
                arguments_model=TaskRecordArgs,
                category="mutation",
                executor=record_task,
            ),
            "artifact.accept-candidate": ToolSpec(
                arguments_model=ArtifactWriteArgs,
                category="mutation",
                executor=accept_candidate,
            ),
        },
        pre_act=ZeroOpsPreAct(probe),
        artifact_policy=ArtifactPolicy(),
    )


def _deterministic_tracer_health() -> HealthSnapshot:
    return HealthSnapshot(
        lakebase_available=True,
        lakebase_fresh=True,
        pipeline_fresh=True,
        unity_catalog_fresh=True,
    )
