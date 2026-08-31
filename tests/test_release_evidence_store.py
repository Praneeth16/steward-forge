from __future__ import annotations

import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from release_evidence import (
    DeploymentObservation,
    GovernedReleaseReceipt,
    InMemoryReleaseEvidenceStore,
    ReleaseEvidenceConflict,
    ReleaseEvidenceNotFound,
    ReleaseEvidencePointer,
    ReleaseIntent,
)

CHAIN_REFERENCE = "a" * 64 + ":42:" + "8" * 64


def _intent_values() -> dict[str, object]:
    return {
        "brief_id": "brief-caf\u00e9-01",
        "workflow_id": "workflow-01",
        "run_id": "run-01",
        "task_id": "swe-task-001",
        "code_sha256": "1" * 64,
        "artifact_hashes": {
            "generated/software-engineer/dashboard.html": "2" * 64,
            "generated/software-engineer/dashboard.js": "3" * 64,
        },
        "broker_receipt_id": "4" * 24,
        "data_receipt_id": "5" * 24,
        "data_manifest_sha256": "6" * 64,
        "data_relations": (
            "demo_catalog.sandbox.backlog",
            "demo_catalog.sandbox.pipeline_runs",
        ),
        "scope_approval_id": "scope-approval-01",
        "release_approval_id": "release-approval-01",
        "gate_results": {"integration": "passed", "unit": "passed"},
        "gate_report_sha256": "7" * 64,
        "cost_minor_units": 375,
        "cost_currency": "USD",
        "model_usage_status": "not_used",
        "model_id": None,
        "model_input_tokens": None,
        "model_output_tokens": None,
        "deployment_idempotency_key": "deploy:workflow-01:swe-task-001",
    }


def _intent(**overrides: object) -> ReleaseIntent:
    values = _intent_values()
    values.update(overrides)
    return ReleaseIntent.model_validate(values)


def _observation(
    intent: ReleaseIntent,
    **overrides: object,
) -> DeploymentObservation:
    values: dict[str, object] = {
        "receipt_id": intent.receipt_id,
        "request_hash": intent.request_hash,
        "observed_at": datetime(2026, 8, 31, 7, 30, tzinfo=UTC),
        "lease_owner": "orchestrator-a",
        "lease_epoch": 7,
        "status": "succeeded",
        "workspace_ids": {"dashboard": "dashboard-123"},
        "rollback_state": {"previous_dashboard": "dashboard-122"},
        "deployment_output": {"url": "https://workspace.example/dashboard-123"},
    }
    values.update(overrides)
    return DeploymentObservation.model_validate(values)


def _receipt(
    intent: ReleaseIntent | None = None,
    **observation_overrides: object,
) -> GovernedReleaseReceipt:
    release_intent = intent or _intent()
    return GovernedReleaseReceipt.from_intent(
        release_intent,
        _observation(release_intent, **observation_overrides),
        evidence_chain_reference=CHAIN_REFERENCE,
    )


def test_release_intent_uses_canonical_bytes_for_a_stable_compatible_id() -> None:
    intent = _intent()
    reversed_values = dict(reversed(tuple(_intent_values().items())))
    reordered = ReleaseIntent.model_validate(reversed_values)
    canonical_payload = {
        "schema_id": "steward-forge.release-intent",
        "schema_version": 1,
        **_intent_values(),
    }
    canonical = json.dumps(
        canonical_payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
    expected_hash = hashlib.sha256(canonical).hexdigest()
    expected_receipt_id = hashlib.sha256(f"receipt:{expected_hash}".encode("ascii")).hexdigest()[
        :24
    ]

    assert intent.request_hash == expected_hash
    assert intent.receipt_id == expected_receipt_id
    assert reordered.request_hash == intent.request_hash
    assert reordered.receipt_id == intent.receipt_id
    assert len(intent.request_hash) == 64
    assert len(intent.receipt_id) == 24


def test_dynamic_deployment_fields_cannot_change_the_predeployment_identity() -> None:
    intent = _intent()
    first = _observation(intent)
    later = _observation(
        intent,
        observed_at=datetime(2026, 9, 1, 9, 45, tzinfo=UTC),
        lease_owner="orchestrator-b",
        lease_epoch=8,
        workspace_ids={"dashboard": "dashboard-999"},
        rollback_state={"previous_dashboard": "dashboard-998"},
    )

    assert first.receipt_id == later.receipt_id == intent.receipt_id
    assert first.request_hash == later.request_hash == intent.request_hash
    for dynamic_field in (
        "observed_at",
        "lease_owner",
        "lease_epoch",
        "workspace_ids",
        "rollback_state",
    ):
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            ReleaseIntent.model_validate(
                {**_intent_values(), dynamic_field: first.model_dump()[dynamic_field]}
            )


def test_serialized_intent_round_trips_and_rejects_tampered_derived_ids() -> None:
    intent = _intent()
    serialized = intent.model_dump(mode="json")

    assert ReleaseIntent.model_validate(serialized) == intent
    with pytest.raises(ValidationError, match="request_hash does not match intent"):
        ReleaseIntent.model_validate({**serialized, "request_hash": "f" * 64})


def test_receipt_exposes_and_binds_all_release_provenance() -> None:
    intent = _intent()
    receipt = _receipt(intent)

    assert receipt.receipt_id == intent.receipt_id
    assert receipt.request_hash == intent.request_hash
    assert receipt.brief_id == intent.brief_id
    assert receipt.workflow_id == intent.workflow_id
    assert receipt.run_id == intent.run_id
    assert receipt.task_id == intent.task_id
    assert receipt.code_sha256 == intent.code_sha256
    assert receipt.artifact_hashes == intent.artifact_hashes
    assert receipt.broker_receipt_id == intent.broker_receipt_id
    assert receipt.data_receipt_id == intent.data_receipt_id
    assert receipt.data_manifest_sha256 == intent.data_manifest_sha256
    assert receipt.data_relations == intent.data_relations
    assert receipt.scope_approval_id == intent.scope_approval_id
    assert receipt.release_approval_id == intent.release_approval_id
    assert receipt.gate_results == intent.gate_results
    assert receipt.gate_report_sha256 == intent.gate_report_sha256
    assert receipt.cost_minor_units == 375
    assert isinstance(receipt.cost_minor_units, int)
    assert receipt.model_usage_status == "not_used"
    assert receipt.deployment.receipt_id == receipt.receipt_id
    assert receipt.evidence_chain_reference == CHAIN_REFERENCE


def test_identical_replay_returns_the_original_immutable_row_under_concurrency() -> None:
    store = InMemoryReleaseEvidenceStore()
    original = _receipt()
    replays = [
        GovernedReleaseReceipt.model_validate(original.model_dump(mode="json")) for _ in range(12)
    ]

    with ThreadPoolExecutor(max_workers=6) as executor:
        returned = list(executor.map(store.insert_if_absent, [original, *replays]))

    assert all(row is original for row in returned)
    assert store.get(original.receipt_id) is original
    with pytest.raises(ValidationError, match="Instance is frozen"):
        original.brief_id = "tampered"  # type: ignore[misc]


def test_nested_release_provenance_is_immutable_after_identity_is_derived() -> None:
    intent = _intent()
    receipt = _receipt(intent)
    request_hash = intent.request_hash
    receipt_id = intent.receipt_id

    with pytest.raises(TypeError):
        intent.artifact_hashes["generated/software-engineer/dashboard.html"] = "f" * 64
    with pytest.raises(TypeError):
        intent.gate_results["unit"] = "failed"
    with pytest.raises(TypeError):
        receipt.deployment.workspace_ids["dashboard"] = "dashboard-tampered"
    with pytest.raises(TypeError):
        receipt.deployment.rollback_state["nested"] = {"tampered": True}

    assert intent.request_hash == request_hash
    assert intent.receipt_id == receipt_id


def test_conflicting_same_id_bytes_fail_closed() -> None:
    store = InMemoryReleaseEvidenceStore()
    original = _receipt()
    conflict = _receipt(
        observed_at=datetime(2026, 9, 1, 9, 45, tzinfo=UTC),
        workspace_ids={"dashboard": "dashboard-conflict"},
    )
    store.insert_if_absent(original)

    with pytest.raises(ReleaseEvidenceConflict, match=original.receipt_id):
        store.insert_if_absent(conflict)

    assert store.get(original.receipt_id) is original


def test_missing_read_and_test_only_delete_are_explicit() -> None:
    store = InMemoryReleaseEvidenceStore()

    with pytest.raises(ReleaseEvidenceNotFound, match="f{24}"):
        store.get("f" * 24)

    receipt = _receipt()
    store.insert_if_absent(receipt)
    store.delete_for_test(receipt.receipt_id)
    with pytest.raises(ReleaseEvidenceNotFound):
        store.get(receipt.receipt_id)


def test_pointer_id_is_cryptographically_bound_to_the_same_request() -> None:
    receipt = _receipt()
    pointer = ReleaseEvidencePointer(
        receipt_id=receipt.receipt_id,
        request_hash=receipt.request_hash,
        receipt_location="delta://evidence.release_receipts/" + receipt.receipt_id,
        receipt_sha256="9" * 64,
        evidence_chain_reference=receipt.evidence_chain_reference,
    )

    assert pointer.receipt_id == receipt.receipt_id
    assert pointer.request_hash == receipt.request_hash
    with pytest.raises(ValidationError, match="receipt_id does not match request_hash"):
        ReleaseEvidencePointer.model_validate(
            {
                **pointer.model_dump(mode="json"),
                "receipt_id": "a" * 24,
            }
        )


@pytest.mark.parametrize(
    "model",
    [
        pytest.param(lambda: _intent(), id="intent"),
        pytest.param(lambda: _observation(_intent()), id="observation"),
        pytest.param(lambda: _receipt(), id="receipt"),
        pytest.param(
            lambda: ReleaseEvidencePointer(
                receipt_id=_intent().receipt_id,
                request_hash=_intent().request_hash,
                receipt_location="delta://evidence.release_receipts/row",
                receipt_sha256="9" * 64,
                evidence_chain_reference=CHAIN_REFERENCE,
            ),
            id="pointer",
        ),
    ],
)
def test_contracts_reject_undeclared_fields(model) -> None:
    valid = model()

    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        type(valid).model_validate({**valid.model_dump(mode="json"), "undeclared": True})
