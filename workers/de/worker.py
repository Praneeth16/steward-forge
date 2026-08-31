"""Deterministic Data Engineer worker constrained by a broker contract."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from broker.contracts import MutationReceipt, MutationRequest, SyntheticTableWriteArgs
from broker.service import CapabilityBroker
from data.generators import build_namespace, canonical_jsonl, generate_all
from data.generators.common import CANARY_PLACEMENTS, PRIMARY_KEYS, TABLE_SCHEMAS
from data.quality import evaluate_all
from data.repair import repair_documented_defects
from workers.de.models import (
    CatalogTableOutput,
    DataEngineerTask,
    GeneratedArtifact,
    LineageRecord,
    ProgressEvent,
)

_PIPELINE_CODE = '''"""Generated deterministic publish transform."""

EXPECTED_DATASETS = ("backlog", "pipeline_runs", "platform_costs")


def build_outputs(tables, namespace):
    if tuple(tables) != EXPECTED_DATASETS:
        raise ValueError("dataset contract mismatch")
    for rows in tables.values():
        if not rows or any(row["synthetic"] is not True for row in rows):
            raise ValueError("only synthetic rows can be published")
        if any(row["namespace"] != namespace for row in rows):
            raise ValueError("row is outside the task namespace")
    return {dataset: list(rows) for dataset, rows in tables.items()}
'''

_TEST_CODE = '''"""Generated deterministic tests for the publish transform."""


def run_generated_tests(build_outputs, tables, namespace):
    outputs = build_outputs(tables, namespace)
    assert tuple(outputs) == ("backlog", "pipeline_runs", "platform_costs")
    assert all(row["synthetic"] is True for rows in outputs.values() for row in rows)
    assert all(row["namespace"] == namespace for rows in outputs.values() for row in rows)
    return {"datasets": "passed", "namespace": "passed", "synthetic": "passed"}
'''


@dataclass(frozen=True, slots=True)
class DataEngineerCandidate:
    tables: dict[str, list[dict[str, object]]]
    initial_finding_count: int
    final_finding_count: int
    repair_attempts: int
    artifacts: tuple[GeneratedArtifact, ...]
    lineage: LineageRecord
    manifest: GeneratedArtifact
    progress: tuple[ProgressEvent, ...]


@dataclass(frozen=True, slots=True)
class DataEngineerExecution(DataEngineerCandidate):
    catalog_outputs: tuple[CatalogTableOutput, ...]
    mutation_receipts: tuple[MutationReceipt, ...]


class DataEngineerWorker:
    worker_id = "data-engineer"
    contract_id = "data-engineer-synthetic-pipeline"
    contract_version = 1

    def propose_table_write(
        self,
        task: DataEngineerTask,
        dataset: str,
        rows: list[dict[str, object]],
        *,
        catalog: str | None = None,
        schema: str | None = None,
    ) -> MutationRequest:
        namespace = build_namespace(task.brief_id, task.run_id)
        arguments = SyntheticTableWriteArgs(
            catalog=catalog or task.sandbox_catalog,
            schema=schema or task.sandbox_schema,
            namespace=namespace,
            dataset=dataset,
            rows=rows,
        )
        return MutationRequest(
            contract_id=self.contract_id,
            contract_version=self.contract_version,
            worker_id=self.worker_id,
            workflow_id=task.brief_id,
            tool_id="sandbox.write-synthetic-table",
            arguments=arguments.model_dump(mode="json", by_alias=True),
            idempotency_key=f"{task.task_id}:publish:{dataset}:v1",
        )

    def prepare(self, task: DataEngineerTask) -> DataEngineerCandidate:
        namespace = build_namespace(task.brief_id, task.run_id)
        progress: list[ProgressEvent] = []

        def record(stage: str, detail: str) -> None:
            progress.append(
                ProgressEvent(sequence=len(progress) + 1, stage=stage, detail=detail)
            )

        record("task.accepted", f"Accepted {task.task_id} under contract v1.")
        planted = generate_all(task.seed, task.brief_id, task.run_id)
        record("data.generated", "Generated three deterministic synthetic datasets.")
        canaries_before = _canary_values(planted)
        initial_findings = evaluate_all(planted)
        repaired = planted
        repair_attempts = 0
        if initial_findings:
            record("quality.failed", f"Detected {len(initial_findings)} planted defects.")
            if task.max_repair_attempts < 1:
                raise ValueError("repair budget exhausted before the deterministic repair")
            repaired, report = repair_documented_defects(
                planted, initial_findings, attempt=1
            )
            repair_attempts = report.attempt
            record("repair.applied", f"Repaired {report.repaired_count} documented defects.")
        final_findings = evaluate_all(repaired)
        if final_findings:
            raise ValueError("quality contract still fails after bounded repair")
        if _canary_values(repaired) != canaries_before:
            raise ValueError("repair changed an instruction canary")
        record("quality.passed", "All quality expectations pass after one bounded repair.")

        artifacts = _build_artifacts(namespace)
        targets = tuple(
            f"{task.sandbox_catalog}.{task.sandbox_schema}.{namespace}__{dataset}"
            for dataset in TABLE_SCHEMAS
        )
        lineage = LineageRecord(
            namespace=namespace,
            sources=tuple(f"steward-forge-generator:v1:{name}" for name in TABLE_SCHEMAS),
            targets=targets,
        )
        manifest = _build_manifest(
            task, artifacts, lineage, repaired, repair_attempts=repair_attempts
        )
        record("candidate.built", "Built code, tests, lineage, and a compact hash manifest.")
        return DataEngineerCandidate(
            tables=repaired,
            initial_finding_count=len(initial_findings),
            final_finding_count=len(final_findings),
            repair_attempts=repair_attempts,
            artifacts=artifacts,
            lineage=lineage,
            manifest=manifest,
            progress=tuple(progress),
        )

    def publish(
        self,
        task: DataEngineerTask,
        candidate: DataEngineerCandidate,
        broker: CapabilityBroker,
    ) -> DataEngineerExecution:
        receipts = tuple(
            broker.execute(
                self.propose_table_write(task, dataset, candidate.tables[dataset])
            )
            for dataset in TABLE_SCHEMAS
        )
        outputs = tuple(CatalogTableOutput.model_validate(receipt.result) for receipt in receipts)
        if tuple(output.relation for output in outputs) != candidate.lineage.targets:
            raise ValueError("catalog adapter output does not match candidate lineage")
        manifest = json.loads(candidate.manifest.content)
        if {output.dataset: output.data_sha256 for output in outputs} != manifest["table_hashes"]:
            raise ValueError("catalog adapter output does not match candidate table hashes")
        progress = candidate.progress + (
            ProgressEvent(
                sequence=len(candidate.progress) + 1,
                stage="catalog.written",
                detail="Broker accepted three sandbox table writes.",
            ),
        )
        return DataEngineerExecution(
            tables=candidate.tables,
            initial_finding_count=candidate.initial_finding_count,
            final_finding_count=candidate.final_finding_count,
            repair_attempts=candidate.repair_attempts,
            artifacts=candidate.artifacts,
            lineage=candidate.lineage,
            manifest=candidate.manifest,
            catalog_outputs=outputs,
            mutation_receipts=receipts,
            progress=progress,
        )


def _artifact(path: str, content: str) -> GeneratedArtifact:
    return GeneratedArtifact(
        path=path,
        content=content,
        sha=hashlib.sha256(content.encode()).hexdigest(),
    )


def _build_artifacts(namespace: str) -> tuple[GeneratedArtifact, ...]:
    root = f"generated/data-engineer/{namespace}"
    return (
        _artifact(f"{root}/pipeline.py", _PIPELINE_CODE),
        _artifact(f"{root}/test_pipeline.py", _TEST_CODE),
    )


def expected_artifact_hashes() -> dict[str, str]:
    """Return trusted source hashes for the version 1 deterministic candidate."""

    return {
        "pipeline.py": hashlib.sha256(_PIPELINE_CODE.encode()).hexdigest(),
        "test_pipeline.py": hashlib.sha256(_TEST_CODE.encode()).hexdigest(),
    }


def _build_manifest(
    task: DataEngineerTask,
    artifacts: tuple[GeneratedArtifact, ...],
    lineage: LineageRecord,
    tables: dict[str, list[dict[str, object]]],
    *,
    repair_attempts: int,
) -> GeneratedArtifact:
    lineage_json = json.dumps(
        lineage.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
    )
    payload = {
        "schema_id": "steward-forge.data-engineer-manifest",
        "schema_version": 1,
        "task_id": task.task_id,
        "artifact_hashes": {
            artifact.path.rsplit("/", 1)[-1]: artifact.sha for artifact in artifacts
        },
        "table_hashes": {
            dataset: hashlib.sha256(canonical_jsonl(rows)).hexdigest()
            for dataset, rows in tables.items()
        },
        "lineage_sha256": hashlib.sha256(lineage_json.encode()).hexdigest(),
        "repair_attempts": repair_attempts,
    }
    content = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return _artifact(
        f"generated/data-engineer/{lineage.namespace}/candidate-manifest.json", content
    )


def _canary_values(
    tables: dict[str, list[dict[str, object]]],
) -> dict[tuple[str, str], object]:
    values: dict[tuple[str, str], object] = {}
    for dataset, placement in CANARY_PLACEMENTS.items():
        primary_key = PRIMARY_KEYS[dataset]
        row = next(
            row for row in tables[dataset] if row[primary_key] == placement["record_id"]
        )
        values[(dataset, placement["field"])] = row[placement["field"]]
    return values
