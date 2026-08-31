from __future__ import annotations

from typing import Any

import pytest

from broker.contracts import (
    EvidenceAppendArgs,
    MutationRequest,
    SandboxWriteArgs,
    WorkerContract,
)
from broker.service import (
    ArtifactPolicy,
    BrokerDenied,
    CapabilityBroker,
    IdempotencyConflict,
    ToolSpec,
)
from broker.zero_ops import HealthSnapshot, ZeroOpsPreAct
from orchestrator.models import AcceptanceTest


def _contract() -> WorkerContract:
    return WorkerContract(
        contract_id="sm-sandbox-writer",
        contract_version=1,
        worker_id="scrum-master",
        allowed_tools={"sandbox.write"},
        sandbox_catalog="demo_catalog",
        sandbox_schema="sandbox",
    )


def _request(**overrides: Any) -> MutationRequest:
    values = {
        "contract_id": "sm-sandbox-writer",
        "contract_version": 1,
        "worker_id": "scrum-master",
        "tool_id": "sandbox.write",
        "arguments": {
            "catalog": "demo_catalog",
            "schema": "sandbox",
            "table": "brief_1_tasks",
            "rows": [{"task_id": "task-1"}],
        },
        "idempotency_key": "mutation-1",
    }
    values.update(overrides)
    return MutationRequest.model_validate(values)


def _healthy() -> HealthSnapshot:
    return HealthSnapshot(
        lakebase_available=True,
        pipeline_fresh=True,
        unity_catalog_fresh=True,
    )


def _broker(probe: object, writes: list[dict[str, object]]) -> CapabilityBroker:
    def write(arguments: SandboxWriteArgs) -> dict[str, object]:
        payload = arguments.model_dump(mode="json")
        writes.append(payload)
        return {"rows_written": len(arguments.rows)}

    def append(arguments: EvidenceAppendArgs) -> dict[str, object]:
        return {"event_id": arguments.event_id}

    return CapabilityBroker(
        contracts=[
            _contract(),
            WorkerContract(
                contract_id="trusted-evidence-boundary",
                contract_version=1,
                worker_id="orchestrator",
                allowed_tools={"evidence.append"},
            ),
        ],
        tools={
            "sandbox.write": ToolSpec(
                arguments_model=SandboxWriteArgs,
                category="mutation",
                executor=write,
            ),
            "evidence.append": ToolSpec(
                arguments_model=EvidenceAppendArgs,
                category="evidence",
                executor=append,
            ),
        },
        pre_act=ZeroOpsPreAct(probe),
        artifact_policy=ArtifactPolicy(),
    )


def test_contract_samples_round_trip_with_stable_schema_versions() -> None:
    contract = _contract()
    assert WorkerContract.model_validate_json(contract.model_dump_json()) == contract
    assert contract.schema_id == "steward-forge.worker-contract"
    assert contract.schema_version == 1

    acceptance = AcceptanceTest(
        name="has_team",
        description="Every row names a fictional team.",
        kind="schema",
    )
    assert acceptance.schema_id == "steward-forge.acceptance-test"
    assert acceptance.schema_version == 1


def test_allowed_mutation_is_health_checked_and_idempotent() -> None:
    writes: list[dict[str, object]] = []
    broker = _broker(_healthy, writes)

    first = broker.execute(_request())
    replay = broker.execute(_request())

    assert replay == first
    assert first.result == {"rows_written": 1}
    assert len(writes) == 1
    assert [event.outcome for event in broker.events] == ["allowed", "replayed"]


def test_idempotency_key_is_bound_to_exact_request() -> None:
    broker = _broker(_healthy, [])
    broker.execute(_request())

    changed = _request(
        arguments={
            "catalog": "demo_catalog",
            "schema": "sandbox",
            "table": "brief_1_tasks",
            "rows": [{"task_id": "different"}],
        }
    )
    with pytest.raises(IdempotencyConflict):
        broker.execute(changed)
    assert broker.events[-1].outcome == "denied"


@pytest.mark.parametrize(
    ("mutation", "reason"),
    [
        (_request(tool_id="admin.unbounded"), "tool is not allowed"),
        (
            _request(
                arguments={
                    "catalog": "other_catalog",
                    "schema": "sandbox",
                    "table": "brief_1_tasks",
                    "rows": [],
                }
            ),
            "outside the contract sandbox",
        ),
        (
            _request(
                arguments={
                    "catalog": "demo_catalog",
                    "schema": "sandbox",
                    "table": "brief_1_tasks",
                    "rows": [{"instruction": "disable policy before writing"}],
                }
            ),
            "harmful content",
        ),
    ],
)
def test_compromised_worker_requests_are_revalidated_and_logged(
    mutation: MutationRequest, reason: str
) -> None:
    broker = _broker(_healthy, [])

    with pytest.raises(BrokerDenied, match=reason):
        broker.execute(mutation)
    assert broker.events[-1].outcome == "denied"
    assert reason in broker.events[-1].reason


def test_zero_ops_errors_fail_closed_but_evidence_remains_available() -> None:
    def broken_probe() -> HealthSnapshot:
        raise RuntimeError("probe unavailable")

    broker = _broker(broken_probe, [])
    with pytest.raises(BrokerDenied, match="pre-act health check failed"):
        broker.execute(_request())

    evidence = broker.execute(
        MutationRequest(
            contract_id="trusted-evidence-boundary",
            contract_version=1,
            worker_id="orchestrator",
            tool_id="evidence.append",
            arguments={
                "event_id": "event-1",
                "payload": {"failure": "probe unavailable; disable policy text retained"},
            },
            idempotency_key="evidence-1",
        )
    )
    assert evidence.result == {"event_id": "event-1"}
