"""Narrow task-to-receipt orchestration for the Data Engineer worker."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, replace
from typing import Protocol

from broker.contracts import SyntheticTableWriteArgs
from broker.service import create_data_engineer_broker
from gates.data import DataCandidateGate
from workers.de.models import (
    DataEngineerReceipt,
    DataEngineerTask,
    ProgressEvent,
)
from workers.de.worker import DataEngineerExecution, DataEngineerWorker


class CatalogWriter(Protocol):
    def write(self, arguments: SyntheticTableWriteArgs) -> dict[str, object]: ...


@dataclass(frozen=True, slots=True)
class DataEngineerRunResult:
    task: DataEngineerTask
    execution: DataEngineerExecution
    gate_results: dict[str, str]
    progress: tuple[ProgressEvent, ...]
    receipt: DataEngineerReceipt


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
        broker = create_data_engineer_broker(
            sandbox_catalog=task.sandbox_catalog,
            sandbox_schema=task.sandbox_schema,
            table_writer=self._catalog.write,
        )
        candidate = self._worker.prepare(task)
        gate_results = self._gate.evaluate(task, candidate)
        gated_progress = candidate.progress + (
            ProgressEvent(
                sequence=len(candidate.progress) + 1,
                stage="gate.passed",
                detail="All deterministic Data Engineer gates passed.",
            ),
        )
        candidate = replace(candidate, progress=gated_progress)
        execution = self._worker.publish(task, candidate, broker)
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
