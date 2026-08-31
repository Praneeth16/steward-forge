from __future__ import annotations

import hashlib
import json
from dataclasses import replace

import pytest

from broker.service import BrokerDenied, create_data_engineer_broker
from data.generators import generate_all
from gates.data.gate import DataCandidateGate, DataGateDenied
from pipeline import DataEngineeringPipeline
from workers.de import DataEngineerTask, DataEngineerWorker, InMemoryCatalogAdapter
from workers.de.models import GeneratedArtifact


def _task() -> DataEngineerTask:
    return DataEngineeringPipeline.plan_task(
        brief_id="brief-01",
        run_id="run-01",
        seed=2026,
        sandbox_catalog="demo_catalog",
        sandbox_schema="steward_forge_sandbox",
    )


def _broker(task: DataEngineerTask, adapter: InMemoryCatalogAdapter):
    return create_data_engineer_broker(
        sandbox_catalog=task.sandbox_catalog,
        sandbox_schema=task.sandbox_schema,
        table_writer=adapter.write,
    )


def test_de_contract_is_versioned_and_restricts_catalog_mutations() -> None:
    task = _task()
    adapter = InMemoryCatalogAdapter()
    broker = _broker(task, adapter)
    worker = DataEngineerWorker()
    tables = generate_all(task.seed, task.brief_id, task.run_id)

    allowed = worker.propose_table_write(task, "backlog", tables["backlog"])
    assert allowed.contract_id == "data-engineer-synthetic-pipeline"
    assert allowed.contract_version == 1
    assert broker.execute(allowed).result["relation"].startswith(
        "demo_catalog.steward_forge_sandbox."
    )

    outside = worker.propose_table_write(
        task,
        "backlog",
        tables["backlog"],
        catalog="other_catalog",
    )
    with pytest.raises(BrokerDenied, match="outside the contract sandbox"):
        broker.execute(outside)

    denial = broker.events[-1]
    assert denial.outcome == "denied"
    assert denial.worker_id == "data-engineer"
    assert "outside the contract sandbox" in denial.reason


def test_worker_produces_compact_code_tests_lineage_and_manifest() -> None:
    task = _task()
    adapter = InMemoryCatalogAdapter()
    broker = _broker(task, adapter)

    worker = DataEngineerWorker()
    candidate = worker.prepare(task)
    DataCandidateGate().evaluate(task, candidate)
    execution = worker.publish(task, candidate, broker)

    assert execution.initial_finding_count == 3
    assert execution.final_finding_count == 0
    assert execution.repair_attempts == 1
    assert {artifact.path.rsplit("/", 1)[-1] for artifact in execution.artifacts} == {
        "pipeline.py",
        "test_pipeline.py",
    }
    assert execution.lineage.namespace == "steward_forge_brief_01_run_01"
    assert len(execution.lineage.targets) == 3
    manifest = json.loads(execution.manifest.content)
    assert set(manifest["artifact_hashes"]) == {"pipeline.py", "test_pipeline.py"}
    assert set(manifest["table_hashes"]) == {"backlog", "pipeline_runs", "platform_costs"}
    assert "rows" not in manifest
    assert all("SF_CANARY::" not in artifact.content for artifact in execution.artifacts)


def test_integration_covers_task_execution_catalog_gate_progress_and_receipt() -> None:
    adapter = InMemoryCatalogAdapter()
    pipeline = DataEngineeringPipeline(adapter)
    task = _task()

    result = pipeline.run(task)

    assert result.task == task
    assert set(adapter.tables) == set(result.execution.lineage.targets)
    assert {output.row_count for output in result.execution.catalog_outputs} == {36, 42, 48}
    assert result.gate_results == {
        "artifact_contract": "passed",
        "generated_tests": "passed",
        "lineage": "passed",
        "quality": "passed",
        "sandbox": "passed",
    }
    assert [event.stage for event in result.progress] == [
        "task.accepted",
        "data.generated",
        "quality.failed",
        "repair.applied",
        "quality.passed",
        "candidate.built",
        "gate.passed",
        "catalog.written",
        "receipt.emitted",
    ]
    assert result.receipt.task_id == task.task_id
    assert result.receipt.manifest_sha == result.execution.manifest.sha
    assert result.receipt.catalog_relations == result.execution.lineage.targets
    assert result.receipt.repair_attempts == 1
    assert len(result.receipt.mutation_receipt_ids) == 3


def test_broker_replay_does_not_repeat_catalog_writes() -> None:
    task = _task()
    adapter = InMemoryCatalogAdapter()
    broker = _broker(task, adapter)
    worker = DataEngineerWorker()

    candidate = worker.prepare(task)
    first = worker.publish(task, candidate, broker)
    second = worker.publish(task, candidate, broker)

    assert second.catalog_outputs == first.catalog_outputs
    assert len(adapter.write_events) == 3
    assert [event.outcome for event in broker.events[-3:]] == [
        "replayed",
        "replayed",
        "replayed",
    ]


def test_worker_fails_closed_when_repair_budget_is_zero() -> None:
    task = _task().model_copy(update={"max_repair_attempts": 0})
    adapter = InMemoryCatalogAdapter()

    with pytest.raises(ValueError, match="repair budget exhausted"):
        DataEngineeringPipeline(adapter).run(task)
    assert adapter.tables == {}


def test_failed_candidate_gate_cannot_write_catalog() -> None:
    class RejectingGate(DataCandidateGate):
        def evaluate(self, task, candidate):
            raise DataGateDenied("candidate rejected for test")

    adapter = InMemoryCatalogAdapter()
    with pytest.raises(DataGateDenied, match="candidate rejected"):
        DataEngineeringPipeline(adapter, gate=RejectingGate()).run(_task())
    assert adapter.tables == {}


def test_worker_authored_python_is_not_executed_by_the_gate() -> None:
    task = _task()
    candidate = DataEngineerWorker().prepare(task)
    hostile_content = 'raise RuntimeError("candidate code executed")\n'
    hostile = GeneratedArtifact(
        path=candidate.artifacts[0].path,
        content=hostile_content,
        sha=hashlib.sha256(hostile_content.encode()).hexdigest(),
    )
    compromised = replace(candidate, artifacts=(hostile, candidate.artifacts[1]))

    with pytest.raises(DataGateDenied, match="artifact_contract"):
        DataCandidateGate().evaluate(task, compromised)


@pytest.mark.parametrize(
    "artifacts",
    [
        lambda candidate: (candidate.artifacts[0],),
        lambda candidate: (candidate.artifacts[0], candidate.artifacts[0]),
    ],
    ids=["missing", "duplicate"],
)
def test_malformed_artifact_sets_fail_as_controlled_gate_denials(artifacts) -> None:
    task = _task()
    candidate = DataEngineerWorker().prepare(task)
    malformed = replace(candidate, artifacts=artifacts(candidate))

    with pytest.raises(DataGateDenied, match="artifact_contract"):
        DataCandidateGate().evaluate(task, malformed)
