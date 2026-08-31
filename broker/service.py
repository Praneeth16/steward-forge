"""Deterministic revalidation and execution of worker tool requests."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass
from threading import RLock
from typing import Any, Literal

from pydantic import BaseModel, ValidationError

from broker.contracts import (
    ArtifactCommitArgs,
    ArtifactWriteArgs,
    MutationReceipt,
    MutationRequest,
    SandboxWriteArgs,
    SyntheticTableWriteArgs,
    TaskRecordArgs,
    WorkerContract,
)
from broker.security import contains_secret
from broker.zero_ops import HealthSnapshot, PreActDenied, ZeroOpsPreAct

LeaseFence = Callable[[MutationRequest], AbstractContextManager[None]]


def mutation_request_hash(request: MutationRequest) -> str:
    """Return the canonical request digest used to bind broker receipts."""

    return hashlib.sha256(
        json.dumps(
            request.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()


def mutation_receipt_id(request_hash: str) -> str:
    """Derive the deterministic receipt identifier for a request digest."""

    return hashlib.sha256(f"receipt:{request_hash}".encode()).hexdigest()[:24]


class BrokerDenied(ValueError):
    """A worker request failed deterministic broker validation."""


class IdempotencyConflict(BrokerDenied):
    """An idempotency key was reused for different request content."""


@dataclass(frozen=True, slots=True)
class ToolSpec:
    arguments_model: type[BaseModel]
    category: Literal["mutation", "evidence"]
    executor: Callable[[Any], dict[str, Any]]
    scan_artifact_content: bool = True


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
    denied_path_parts = frozenset(
        {".github", "infrastructure", "platform", "secret", "secrets", "resources"}
    )
    denied_file_names = frozenset(
        {".env", "app.yaml", "app.yml", "databricks.yml", "terraform.tf"}
    )

    def validate(self, value: object) -> None:
        for text in self._strings(value):
            normalized = text.casefold()
            if any(fragment in normalized for fragment in self.denied_fragments):
                raise BrokerDenied("harmful content was rejected by deterministic policy")
            if contains_secret(text):
                raise BrokerDenied("secret-like content was rejected by deterministic policy")

    def validate_artifact_path(self, path: str) -> None:
        parts = path.split("/")
        normalized_parts = {part.casefold() for part in parts}
        normalized_stems = {part.casefold().rsplit(".", maxsplit=1)[0] for part in parts}
        if (
            path.startswith("/")
            or any(part in {"", ".", ".."} for part in parts)
            or normalized_parts & self.denied_path_parts
            or normalized_stems & self.denied_path_parts
            or parts[-1].casefold() in self.denied_file_names
        ):
            raise BrokerDenied("privileged or unsafe artifact path was rejected")

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
        lease_fence: LeaseFence | None = None,
    ) -> None:
        self._contracts = {
            (contract.contract_id, contract.contract_version): contract
            for contract in contracts
        }
        self._tools = tools
        self._pre_act = pre_act
        self._artifact_policy = artifact_policy
        self._lease_fence = lease_fence
        self._receipts: dict[str, tuple[str, MutationReceipt]] = {}
        self._lock = RLock()
        self.events: list[BrokerEvent] = []

    def execute(self, request: MutationRequest) -> MutationReceipt:
        with self._lock:
            try:
                contract, tool, parsed, request_hash = self._validate(request)
                fence = (
                    self._lease_fence(request)
                    if self._lease_fence is not None
                    else nullcontext()
                )
                with fence:
                    replay = self._receipts.get(request.idempotency_key)
                    if replay is not None:
                        previous_hash, receipt = replay
                        if previous_hash != request_hash:
                            raise IdempotencyConflict(
                                "idempotency key is bound to a different request"
                            )
                        self._event(request, "replayed", "first receipt returned")
                        return receipt

                    if tool.category != "evidence" and tool.scan_artifact_content:
                        self._artifact_policy.validate(request.arguments)
                    self._pre_act.authorize(tool.category)
                    result = tool.executor(parsed)
                    receipt = MutationReceipt(
                        receipt_id=mutation_receipt_id(request_hash),
                        request_hash=request_hash,
                        worker_id=contract.worker_id,
                        workflow_id=request.workflow_id,
                        lease_owner=request.lease_owner,
                        lease_epoch=request.lease_epoch,
                        tool_id=request.tool_id,
                        result=result,
                    )
                    self._receipts[request.idempotency_key] = (
                        request_hash,
                        receipt,
                    )
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
        if (request.lease_owner is None) != (request.lease_epoch is None):
            raise BrokerDenied("lease owner and epoch must be supplied together")
        if request.lease_owner is not None and (
            request.workflow_id is None or self._lease_fence is None
        ):
            raise BrokerDenied("lease-bound requests require a configured durable fence")
        contract = self._contracts.get((request.contract_id, request.contract_version))
        if contract is None or contract.worker_id != request.worker_id:
            raise BrokerDenied("worker contract is not registered")
        if request.tool_id not in contract.allowed_tools:
            raise BrokerDenied("tool is not allowed by worker contract")
        tool = self._tools.get(request.tool_id)
        if tool is None:
            raise BrokerDenied("tool is not registered")
        parsed = tool.arguments_model.model_validate(request.arguments)
        if isinstance(parsed, SandboxWriteArgs | SyntheticTableWriteArgs) and (
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
        if isinstance(parsed, ArtifactCommitArgs):
            if parsed.branch != contract.artifact_branch:
                raise BrokerDenied("candidate branch is outside the worker contract")
            for artifact in parsed.artifacts:
                self._require_artifact_scope(contract, artifact.path)
                if artifact.path.rstrip("/") in contract.allowed_artifact_prefixes:
                    raise BrokerDenied("artifact path must name a file below the generated prefix")
                self._artifact_policy.validate_artifact_path(artifact.path)
        request_hash = mutation_request_hash(request)
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


def create_data_engineer_broker(
    *,
    sandbox_catalog: str,
    sandbox_schema: str,
    table_writer: Callable[[SyntheticTableWriteArgs], dict[str, Any]],
    health_probe: Callable[[], HealthSnapshot] | None = None,
    lease_fence: LeaseFence | None = None,
) -> CapabilityBroker:
    """Build the versioned, sandbox-only broker for the Data Engineer worker."""

    return CapabilityBroker(
        contracts=[
            WorkerContract(
                contract_id="data-engineer-synthetic-pipeline",
                contract_version=1,
                worker_id="data-engineer",
                allowed_tools={"sandbox.write-synthetic-table"},
                sandbox_catalog=sandbox_catalog,
                sandbox_schema=sandbox_schema,
                allowed_artifact_prefixes={"generated/data-engineer"},
            )
        ],
        tools={
            "sandbox.write-synthetic-table": ToolSpec(
                arguments_model=SyntheticTableWriteArgs,
                category="mutation",
                executor=table_writer,
                # Row text is governed data, not an executable instruction channel.
                # Its exact canary placement is checked by the deterministic data gate.
                scan_artifact_content=False,
            )
        },
        pre_act=ZeroOpsPreAct(health_probe or _deterministic_tracer_health),
        artifact_policy=ArtifactPolicy(),
        lease_fence=lease_fence,
    )


def create_software_engineer_broker(
    *,
    generated_prefix: str,
    artifact_branch: str,
    commit_executor: Callable[[ArtifactCommitArgs], dict[str, Any]],
    health_probe: Callable[[], HealthSnapshot] | None = None,
    lease_fence: LeaseFence | None = None,
) -> CapabilityBroker:
    """Build the candidate-branch broker for the Software Engineer worker."""

    return CapabilityBroker(
        contracts=[
            WorkerContract(
                contract_id="software-engineer-artifact-writer",
                contract_version=1,
                worker_id="software-engineer",
                allowed_tools={"artifact.commit-candidate"},
                allowed_artifact_prefixes={generated_prefix},
                artifact_branch=artifact_branch,
            )
        ],
        tools={
            "artifact.commit-candidate": ToolSpec(
                arguments_model=ArtifactCommitArgs,
                category="mutation",
                executor=commit_executor,
            )
        },
        pre_act=ZeroOpsPreAct(health_probe or _deterministic_tracer_health),
        artifact_policy=ArtifactPolicy(),
        lease_fence=lease_fence,
    )


def _deterministic_tracer_health() -> HealthSnapshot:
    return HealthSnapshot(
        lakebase_available=True,
        lakebase_fresh=True,
        pipeline_fresh=True,
        unity_catalog_fresh=True,
    )
