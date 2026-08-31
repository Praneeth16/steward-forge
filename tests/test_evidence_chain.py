from __future__ import annotations

from dataclasses import replace

import pytest

from evidence.chain import (
    HASH_ALGORITHM,
    SERIALIZATION,
    EvidenceIntegrityError,
    EvidenceRecord,
    ProtectedHead,
    append,
    canonical_json_bytes,
    chain_id_for,
    verify,
    verify_prefix,
    verify_transition,
)

WORKFLOW_ID = "workflow-123"
PAYLOAD = {"z": "café", "a": [3, {"b": False, "a": None}]}


def _three_record_chain() -> tuple[list[EvidenceRecord], ProtectedHead]:
    first, head = append(
        None,
        workflow_id=WORKFLOW_ID,
        record_type="brief.submitted",
        payload=PAYLOAD,
        trusted_source="orchestrator",
    )
    second, head = append(
        head,
        workflow_id=WORKFLOW_ID,
        record_type="scope.approved",
        payload={"approver": "reviewer-1"},
        trusted_source="approval-gateway",
    )
    third, head = append(
        head,
        workflow_id=WORKFLOW_ID,
        record_type="release.completed",
        payload={"receipt_id": "receipt-1"},
        trusted_source="release-gateway",
    )
    return [first, second, third], head


def test_canonical_serialization_and_first_hash_match_golden_values() -> None:
    assert canonical_json_bytes(PAYLOAD) == (b'{"a":[3,{"a":null,"b":false}],"z":"caf\\u00e9"}')
    assert chain_id_for(WORKFLOW_ID) == (
        "ec72c0410a6a819a39942134a24e42c37fefdcafb310958e41b1aeb38a48c979"
    )

    record, head = append(
        None,
        workflow_id=WORKFLOW_ID,
        record_type="brief.submitted",
        payload=PAYLOAD,
        trusted_source="orchestrator",
    )

    assert record.current_hash == (
        "e04289e12d60e9c9b14445661e22d118e48605d35e0107d131b3e20ee9484700"
    )
    assert record.to_dict() == {
        "serialization": "steward-forge-json-v1",
        "hash_algorithm": "sha256",
        "chain_id": chain_id_for(WORKFLOW_ID),
        "sequence": 1,
        "previous_hash": "0" * 64,
        "current_hash": record.current_hash,
        "record_type": "brief.submitted",
        "source": "orchestrator",
        "payload": PAYLOAD,
    }
    assert head == ProtectedHead(
        serialization=SERIALIZATION,
        hash_algorithm=HASH_ALGORITHM,
        chain_id=record.chain_id,
        sequence=1,
        current_hash=record.current_hash,
    )


def test_payload_is_copied_and_deeply_immutable() -> None:
    source_payload = {"nested": {"items": [1, 2]}}
    record, _ = append(
        None,
        workflow_id=WORKFLOW_ID,
        record_type="brief.submitted",
        payload=source_payload,
        trusted_source="orchestrator",
    )

    source_payload["nested"]["items"].append(3)  # type: ignore[index,union-attr]
    assert record.to_dict()["payload"] == {"nested": {"items": [1, 2]}}
    with pytest.raises(TypeError):
        record.payload["nested"] = {}  # type: ignore[index]
    with pytest.raises(AttributeError):
        record.payload["nested"]["items"].append(3)  # type: ignore[index,union-attr]


@pytest.mark.parametrize("invalid_number", [float("nan"), float("inf"), -float("inf")])
def test_canonical_serialization_rejects_non_finite_numbers(invalid_number: float) -> None:
    with pytest.raises(ValueError, match="finite|JSON"):
        canonical_json_bytes({"value": invalid_number})


def test_append_owns_metadata_even_when_payload_contains_spoofed_fields() -> None:
    record, _ = append(
        None,
        workflow_id=WORKFLOW_ID,
        record_type="task.completed",
        payload={
            "source": "software-engineer",
            "sequence": 999,
            "previous_hash": "f" * 64,
            "current_hash": "f" * 64,
        },
        trusted_source="orchestrator",
    )

    assert record.source == "orchestrator"
    assert record.sequence == 1
    assert record.previous_hash == "0" * 64
    assert record.current_hash != "f" * 64


def test_independent_verification_accepts_an_intact_chain_and_protected_head() -> None:
    records, head = _three_record_chain()

    verify(records, head, workflow_id=WORKFLOW_ID)


@pytest.mark.parametrize("attack", ["mutation", "deletion", "reordering"])
def test_verification_detects_record_mutation_deletion_and_reordering(attack: str) -> None:
    records, head = _three_record_chain()

    if attack == "mutation":
        attacked = [replace(records[0], payload={"changed": True}), *records[1:]]
    elif attack == "deletion":
        attacked = [records[0], records[2]]
    else:
        attacked = [records[1], records[0], records[2]]

    with pytest.raises(EvidenceIntegrityError):
        verify(attacked, head, workflow_id=WORKFLOW_ID)


def test_verification_detects_a_fork_against_the_protected_head() -> None:
    records, protected_head = _three_record_chain()
    forked_second, forked_head = append(
        ProtectedHead.from_record(records[0]),
        workflow_id=WORKFLOW_ID,
        record_type="scope.rejected",
        payload={"reason": "alternate history"},
        trusted_source="approval-gateway",
    )
    assert forked_head != ProtectedHead.from_record(records[1])

    with pytest.raises(EvidenceIntegrityError, match="protected head"):
        verify([records[0], forked_second], protected_head, workflow_id=WORKFLOW_ID)


def test_verification_detects_a_wrong_protected_head() -> None:
    records, head = _three_record_chain()
    wrong_head = replace(head, current_hash="f" * 64)

    with pytest.raises(EvidenceIntegrityError, match="protected head"):
        verify(records, wrong_head, workflow_id=WORKFLOW_ID)


@pytest.mark.parametrize(
    ("field", "unsupported"),
    [("serialization", "steward-forge-json-v2"), ("hash_algorithm", "sha512")],
)
def test_verification_rejects_unsupported_serialization_and_algorithm(
    field: str, unsupported: str
) -> None:
    records, head = _three_record_chain()
    records[0] = replace(records[0], **{field: unsupported})

    with pytest.raises(EvidenceIntegrityError, match="unsupported"):
        verify(records, head, workflow_id=WORKFLOW_ID)


def test_transition_verification_checks_only_the_new_suffix_against_both_heads() -> None:
    records, head = _three_record_chain()
    previous_head = ProtectedHead.from_record(records[0])

    verify_transition(previous_head, records[1:], head, workflow_id=WORKFLOW_ID)

    with pytest.raises(EvidenceIntegrityError):
        verify_transition(previous_head, [records[2], records[1]], head, workflow_id=WORKFLOW_ID)


def test_prefix_verification_rejects_valid_rewritten_history() -> None:
    original, original_head = _three_record_chain()
    old_records = original[:2]
    old_head = ProtectedHead.from_record(old_records[-1])

    alternate_second, alternate_head = append(
        ProtectedHead.from_record(original[0]),
        workflow_id=WORKFLOW_ID,
        record_type="scope.approved",
        payload={"approver": "different-reviewer"},
        trusted_source="approval-gateway",
    )
    alternate_third, alternate_head = append(
        alternate_head,
        workflow_id=WORKFLOW_ID,
        record_type="release.completed",
        payload={"receipt_id": "receipt-1"},
        trusted_source="release-gateway",
    )
    rewritten = [original[0], alternate_second, alternate_third]

    verify(rewritten, alternate_head, workflow_id=WORKFLOW_ID)
    with pytest.raises(EvidenceIntegrityError, match="append-only prefix"):
        verify_prefix(
            old_records,
            old_head,
            rewritten,
            alternate_head,
            workflow_id=WORKFLOW_ID,
        )

    verify_prefix(
        old_records,
        old_head,
        original,
        original_head,
        workflow_id=WORKFLOW_ID,
    )


def test_prefix_verification_rejects_truncation() -> None:
    records, head = _three_record_chain()
    truncated = records[:1]

    with pytest.raises(EvidenceIntegrityError, match="append-only prefix"):
        verify_prefix(
            records,
            head,
            truncated,
            ProtectedHead.from_record(truncated[-1]),
            workflow_id=WORKFLOW_ID,
        )
