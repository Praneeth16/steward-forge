from __future__ import annotations

from unittest.mock import Mock

import pytest
from pydantic import ValidationError

from identity import ActorContext
from ledger import InMemoryLedger
from orchestrator.delivery import DeliveryCoordinator, DeliveryError, ReferenceRunConfig
from orchestrator.delivery_models import DeliveryRunResult
from orchestrator.models import AcceptanceTest, BriefSubmission


def _submission() -> BriefSubmission:
    return BriefSubmission(
        title="Legacy compatibility test",
        business_question="Can this governed workflow be resumed safely?",
        acceptance_tests=[
            AcceptanceTest(
                name="protected-evidence",
                description="Every trusted event is bound into the protected chain.",
                kind="contract",
            )
        ],
        cost_ceiling_usd=4.0,
        release_approver="approver-1",
        idempotency_key="legacy-compatibility-01",
    )


def _config() -> ReferenceRunConfig:
    return ReferenceRunConfig(
        run_id="legacy-compatibility-01",
        seed=2026,
        sandbox_catalog="demo_catalog",
        sandbox_schema="steward_forge_sandbox",
        trusted_base_sha="1" * 64,
        generated_prefix="generated/software-engineer",
        artifact_branch="steward-forge/candidates",
        dashboard_title="Legacy compatibility",
    )


def _coordinator(ledger: InMemoryLedger) -> DeliveryCoordinator:
    return DeliveryCoordinator(
        data_pipeline=Mock(),
        software_release=Mock(),
        ledger=ledger,
        coordinator_id="compatibility-test",
    )


def _legacy_state() -> dict[str, object]:
    return {
        "id": "legacy-workflow-01",
        "status": "completed",
        "submitted_by": "employee-1",
        "brief": _submission().model_dump(mode="json"),
        "config": _config().model_dump(mode="json"),
        "scope": None,
        "plan": None,
        "task_executions": {},
        "data_receipt": None,
        "prepared_release": None,
        "software_receipt": None,
        "decisions": {},
        "release_decision": None,
        "delivery_evidence": [
            {
                "sequence": 1,
                "event_type": "brief.submitted",
                "worker_id": None,
                "task_id": None,
                "details": {
                    "submitter": "employee-1",
                    "run_id": "legacy-compatibility-01",
                },
            }
        ],
        "events": [],
    }


def test_pre_issue_9_result_deserialization_fails_with_version_guidance() -> None:
    legacy_result = {
        "workflow_id": "legacy-workflow-01",
        "status": "scope_pending",
        "scope": None,
        "plan": None,
        "task_executions": {},
        "data_receipt": None,
        "prepared_release_sha": None,
        "software_receipt": None,
        "evidence": _legacy_state()["delivery_evidence"],
    }

    with pytest.raises(
        ValidationError,
        match="legacy delivery run result is unversioned",
    ):
        DeliveryRunResult.model_validate(legacy_result)


def test_restart_rejects_legacy_worker_evidence_without_fabricating_provenance() -> None:
    ledger = InMemoryLedger()
    ledger.create("legacy-compatibility-01", _legacy_state())
    coordinator = _coordinator(ledger)

    with pytest.raises(
        DeliveryError,
        match=(
            "legacy delivery state cannot be resumed safely: delivery_evidence "
            "has no trusted-source provenance; resubmit the original brief"
        ),
    ):
        coordinator.advance("legacy-workflow-01")

    assert ledger.get("legacy-workflow-01") == _legacy_state()
    assert coordinator._data_pipeline.mock_calls == []
    assert coordinator._software_release.mock_calls == []


def test_append_rejects_legacy_worker_evidence_before_mutating_state() -> None:
    legacy = _legacy_state()

    with pytest.raises(DeliveryError, match="has no trusted-source provenance"):
        DeliveryCoordinator._append_evidence(legacy, "run.completed")

    assert legacy == _legacy_state()


def test_new_submission_emits_explicit_v2_state_and_result_contracts() -> None:
    ledger = InMemoryLedger()
    coordinator = _coordinator(ledger)

    result = coordinator.submit(
        _submission(),
        config=_config(),
        actor=ActorContext(subject="employee-1", roles={"submitter"}),
    )
    state = ledger.get(result.workflow_id)

    assert (result.schema_id, result.schema_version) == (
        "steward-forge.delivery-run-result",
        2,
    )
    assert (state["schema_id"], state["schema_version"]) == (
        "steward-forge.delivery-state",
        2,
    )
    assert "delivery_evidence" not in state
    assert result.evidence_chain
    assert result.evidence_head is not None
    assert DeliveryRunResult.model_validate(result.model_dump(mode="json")) == result
