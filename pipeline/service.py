"""Narrow task-to-receipt orchestration for the Data Engineer worker."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, replace
from typing import Protocol

from broker.contracts import SyntheticTableWriteArgs
from broker.service import CapabilityBroker, LeaseFence, create_data_engineer_broker
from gates.data import DataCandidateGate
from workers.de.models import (
    DataEngineerReceipt,
    DataEngineerTask,
    ProgressEvent,
)
from workers.de.worker import (
    DataEngineerCandidate,
    DataEngineerExecution,
    DataEngineerWorker,
)


class CatalogWriter(Protocol):
    def write(self, arguments: SyntheticTableWriteArgs) -> dict[str, object]: ...


@dataclass(frozen=True, slots=True)
class DataEngineerRunResult:
    task: DataEngineerTask
    execution: DataEngineerExecution
    gate_results: dict[str, str]
    progress: tuple[ProgressEvent, ...]
    receipt: DataEngineerReceipt


@dataclass(frozen=True, slots=True)
class DataPublishSession:
    """Task-scoped broker state retained across orchestrator retries."""

    task: DataEngineerTask
    broker: CapabilityBroker


class DataEngineeringPipeline:
    """Deterministic coordinator; it owns the gate and final receipt."""

    def __init__(
        self,
        catalog: CatalogWriter,
        *,
        gate: DataCandidateGate | None = None,
    ) -> None:
        self._catalog = catalog
        self._worker = DataEngineerWorker()
        self._gate = gate or DataCandidateGate()

    @staticmethod
    def plan_task(
        *,
        brief_id: str,
        run_id: str,
        seed: int,
        sandbox_catalog: str,
        sandbox_schema: str,
    ) -> DataEngineerTask:
        task_id = hashlib.sha256(
            f"{brief_id}:{run_id}:data-engineer:v1".encode()
        ).hexdigest()[:24]
        return DataEngineerTask(
            task_id=task_id,
            brief_id=brief_id,
            run_id=run_id,
            seed=seed,
            sandbox_catalog=sandbox_catalog,
            sandbox_schema=sandbox_schema,
        )

    def run(self, task: DataEngineerTask) -> DataEngineerRunResult:
        candidate = self.prepare(task)
        return self.publish(task, candidate)

    def prepare(self, task: DataEngineerTask) -> DataEngineerCandidate:
        """Perform read-only generation, repair, and artifact preparation."""

        return self._worker.prepare(task)

    def publish(
        self,
        task: DataEngineerTask,
        candidate: DataEngineerCandidate,
        *,
        session: DataPublishSession | None = None,
        lease_owner: str | None = None,
        lease_epoch: int | None = None,
        lease_fence: LeaseFence | None = None,
    ) -> DataEngineerRunResult:
        """Gate and publish a prepared candidate through the mutation broker.

        A lease-bound publish without a supplied session creates a fenced broker
        session from ``lease_fence``. Callers that retain a session across retries
        must supply its fence when creating that session, not here.
        """

        if (lease_owner is None) != (lease_epoch is None):
            raise ValueError("lease_owner and lease_epoch must be supplied together")
        if session is not None and lease_fence is not None:
            raise ValueError("session and lease_fence cannot both be supplied")
        if lease_owner is not None and session is None and lease_fence is None:
            raise ValueError("lease-bound publish requires lease_fence or a fenced session")

        active_session = session or self.begin_publish(task, lease_fence=lease_fence)
        if active_session.task != task:
            raise ValueError("data publish session is bound to a different task")
        broker = active_session.broker
        gate_results = self._gate.evaluate(task, candidate)
        gated_progress = candidate.progress + (
            ProgressEvent(
                sequence=len(candidate.progress) + 1,
                stage="gate.passed",
                detail="All deterministic Data Engineer gates passed.",
            ),
        )
        candidate = replace(candidate, progress=gated_progress)
        execution = self._worker.publish(
            task,
            candidate,
            broker,
            lease_owner=lease_owner,
            lease_epoch=lease_epoch,
        )
        progress = execution.progress
        receipt = _build_receipt(task, execution, gate_results)
        progress += (
            ProgressEvent(
                sequence=len(progress) + 1,
                stage="receipt.emitted",
                detail=f"Emitted receipt {receipt.receipt_id}.",
            ),
        )
        return DataEngineerRunResult(
            task=task,
            execution=execution,
            gate_results=gate_results,
            progress=progress,
            receipt=receipt,
        )

    def begin_publish(
        self,
        task: DataEngineerTask,
        *,
        lease_fence: LeaseFence | None = None,
    ) -> DataPublishSession:
        """Create the broker session whose receipts make retries replay-safe."""

        broker = create_data_engineer_broker(
            sandbox_catalog=task.sandbox_catalog,
            sandbox_schema=task.sandbox_schema,
            table_writer=self._catalog.write,
            lease_fence=lease_fence,
        )
        return DataPublishSession(task=task, broker=broker)


def _build_receipt(
    task: DataEngineerTask,
    execution: DataEngineerExecution,
    gate_results: dict[str, str],
) -> DataEngineerReceipt:
    receipt_payload = {
        "task_id": task.task_id,
        "manifest_sha": execution.manifest.sha,
        "catalog_relations": execution.lineage.targets,
        "mutation_receipt_ids": tuple(
            receipt.receipt_id for receipt in execution.mutation_receipts
        ),
        "repair_attempts": execution.repair_attempts,
        "gate_results": gate_results,
    }
    canonical = json.dumps(receipt_payload, sort_keys=True, separators=(",", ":"))
    return DataEngineerReceipt(
        receipt_id=hashlib.sha256(f"receipt:{canonical}".encode()).hexdigest()[:24],
        **receipt_payload,
    )
