from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta

import pytest

from release_evidence import (
    DeploymentObservation,
    GovernedReleaseReceipt,
    InMemoryReleaseEvidencePointerStore,
    InMemoryReleaseEvidenceStore,
    ReleaseEvidenceConflict,
    ReleaseEvidenceNotFound,
    ReleaseEvidencePointer,
    ReleaseEvidencePublisher,
    ReleaseIntent,
)

LOCATION = "delta://evidence.release_receipts/partition-2026-08-31/receipt-row"
CHAIN_REFERENCE = "a" * 64 + ":19:" + "7" * 64


def _receipt(*, observed_at: datetime | None = None) -> GovernedReleaseReceipt:
    intent = ReleaseIntent(
        brief_id="brief-01",
        workflow_id="workflow-01",
        run_id="run-01",
        task_id="swe-task-01",
        code_sha256="1" * 64,
        artifact_hashes={"generated/app.py": "2" * 64},
        broker_receipt_id="3" * 24,
        data_receipt_id="4" * 24,
        data_manifest_sha256="5" * 64,
        data_relations=("demo.sandbox.output",),
        scope_approval_id="scope-approval-01",
        release_approval_id="release-approval-01",
        gate_results={"release": "passed"},
        gate_report_sha256="6" * 64,
        cost_minor_units=275,
        cost_currency="USD",
        model_usage_status="not_used",
        deployment_idempotency_key="deploy:workflow-01:swe-task-01",
    )
    deployment = DeploymentObservation(
        receipt_id=intent.receipt_id,
        request_hash=intent.request_hash,
        observed_at=observed_at or datetime(2026, 8, 31, 8, 0, tzinfo=UTC),
        lease_owner="orchestrator-a",
        lease_epoch=4,
        status="succeeded",
        workspace_ids={"job": "job-123"},
        rollback_state={"previous_job": "job-122"},
        deployment_output={"run_id": "run-456"},
    )
    return GovernedReleaseReceipt.from_intent(
        intent,
        deployment,
        evidence_chain_reference=CHAIN_REFERENCE,
    )


def _publisher() -> tuple[
    ReleaseEvidencePublisher,
    InMemoryReleaseEvidenceStore,
    InMemoryReleaseEvidencePointerStore,
]:
    receipts = InMemoryReleaseEvidenceStore()
    pointers = InMemoryReleaseEvidencePointerStore()
    return ReleaseEvidencePublisher(receipts, pointers), receipts, pointers


def test_publish_binds_both_stores_to_one_id_location_and_receipt_hash() -> None:
    publisher, receipts, pointers = _publisher()
    receipt = _receipt()

    first = publisher.publish(receipt, receipt_location=LOCATION)
    replay = publisher.publish(
        GovernedReleaseReceipt.model_validate(receipt.model_dump(mode="json")),
        receipt_location=LOCATION,
    )
    canonical_receipt = json.dumps(
        receipt.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")

    assert first.receipt.receipt_id == first.pointer.receipt_id == receipt.receipt_id
    assert first.pointer.receipt_location == LOCATION
    assert first.pointer.receipt_sha256 == hashlib.sha256(canonical_receipt).hexdigest()
    assert first.pointer.request_hash == receipt.request_hash
    assert first.pointer.evidence_chain_reference == receipt.evidence_chain_reference
    assert replay.receipt is first.receipt
    assert replay.pointer is first.pointer
    assert receipts.get(receipt.receipt_id) is first.receipt
    assert pointers.get(receipt.receipt_id) is first.pointer


def test_pointer_store_replays_exact_bytes_and_rejects_same_id_conflicts() -> None:
    publisher, _, pointers = _publisher()
    receipt = _receipt()
    pointer = publisher.pointer_for(receipt, receipt_location=LOCATION)
    replay = ReleaseEvidencePointer.model_validate(pointer.model_dump(mode="json"))
    conflicting = pointer.model_copy(update={"receipt_location": LOCATION + "-other"})

    assert pointers.insert_if_absent(pointer) is pointer
    assert pointers.insert_if_absent(replay) is pointer
    with pytest.raises(ReleaseEvidenceConflict, match=receipt.receipt_id):
        pointers.insert_if_absent(conflicting)
    assert pointers.get(receipt.receipt_id) is pointer


def test_reconcile_repairs_receipt_present_pointer_missing() -> None:
    publisher, receipts, pointers = _publisher()
    receipt = _receipt()
    stored_receipt = receipts.insert_if_absent(receipt)

    repaired = publisher.reconcile(receipt, receipt_location=LOCATION)

    assert repaired.receipt is stored_receipt
    assert pointers.get(receipt.receipt_id) is repaired.pointer
    assert repaired.pointer.receipt_sha256 == publisher.receipt_sha256(receipt)


def test_reconcile_repairs_pointer_present_receipt_missing() -> None:
    publisher, receipts, pointers = _publisher()
    receipt = _receipt()
    pointer = publisher.pointer_for(receipt, receipt_location=LOCATION)
    stored_pointer = pointers.insert_if_absent(pointer)

    repaired = publisher.reconcile(receipt, receipt_location=LOCATION)

    assert repaired.pointer is stored_pointer
    assert receipts.get(receipt.receipt_id) is repaired.receipt
    assert repaired.receipt == receipt


def test_reconcile_rejects_conflicting_receipt_without_creating_a_pointer() -> None:
    publisher, receipts, pointers = _publisher()
    receipt = _receipt()
    conflict = _receipt(observed_at=receipt.deployment.observed_at + timedelta(minutes=5))
    assert conflict.receipt_id == receipt.receipt_id
    receipts.insert_if_absent(receipt)

    with pytest.raises(ReleaseEvidenceConflict, match=receipt.receipt_id):
        publisher.reconcile(conflict, receipt_location=LOCATION)
    with pytest.raises(ReleaseEvidenceNotFound):
        pointers.get(receipt.receipt_id)


def test_reconcile_rejects_conflicting_pointer_without_creating_a_receipt() -> None:
    publisher, receipts, pointers = _publisher()
    receipt = _receipt()
    conflict = publisher.pointer_for(receipt, receipt_location=LOCATION).model_copy(
        update={"receipt_sha256": "f" * 64}
    )
    pointers.insert_if_absent(conflict)

    with pytest.raises(ReleaseEvidenceConflict, match=receipt.receipt_id):
        publisher.reconcile(receipt, receipt_location=LOCATION)
    with pytest.raises(ReleaseEvidenceNotFound):
        receipts.get(receipt.receipt_id)
