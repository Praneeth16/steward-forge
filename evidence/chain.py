"""Storage-neutral canonical evidence hash chain.

Evidence records may live in a readable append-only store. ``ProtectedHead`` is
the independent trust anchor and must be stored separately with storage ACLs
that allow only the trusted evidence service to update it. A hash chain without
that separately protected value can be rewritten and rehashed by an attacker.
"""

from __future__ import annotations

import hashlib
import hmac
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Literal

SERIALIZATION = "steward-forge-json-v1"
HASH_ALGORITHM = "sha256"
GENESIS_HASH = "0" * 64
TrustedEvidenceSource = Literal[
    "orchestrator",
    "approval-gateway",
    "capability-broker",
    "release-gateway",
]
TRUSTED_EVIDENCE_SOURCES = frozenset(
    {"orchestrator", "approval-gateway", "capability-broker", "release-gateway"}
)


class EvidenceIntegrityError(ValueError):
    """Stored evidence does not match the canonical chain contract."""


def canonical_json_bytes(value: object) -> bytes:
    """Serialize JSON as sorted, compact ASCII bytes and reject NaN/infinity."""

    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")


def chain_id_for(workflow_id: str) -> str:
    """Derive a deterministic, domain-separated chain ID from a workflow ID."""

    _require_non_empty("workflow_id", workflow_id)
    binding = {
        "serialization": SERIALIZATION,
        "workflow_id": workflow_id,
    }
    return hashlib.sha256(canonical_json_bytes(binding)).hexdigest()


def _freeze_json(value: object) -> object:
    """Copy a JSON value into recursively immutable Python containers."""

    normalized = json.loads(canonical_json_bytes(_thaw_json(value)))
    return _freeze_normalized(normalized)


def _freeze_normalized(value: object) -> object:
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze_normalized(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze_normalized(item) for item in value)
    return value


def _thaw_json(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: _thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json(item) for item in value]
    return value


def freeze_json(value: object) -> object:
    """Return a detached, recursively immutable canonical JSON value."""

    return _freeze_json(value)


def thaw_json(value: object) -> object:
    """Return a detached JSON-compatible value from immutable containers."""

    return _thaw_json(value)


@dataclass(frozen=True, slots=True)
class EvidenceRecord:
    """One immutable canonical record produced by a trusted boundary."""

    serialization: str
    hash_algorithm: str
    chain_id: str
    sequence: int
    previous_hash: str
    current_hash: str
    record_type: str
    source: str
    payload: Any

    def __post_init__(self) -> None:
        object.__setattr__(self, "payload", _freeze_json(self.payload))

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> EvidenceRecord:
        """Parse and validate one persisted record dictionary."""

        fields = {
            "serialization",
            "hash_algorithm",
            "chain_id",
            "sequence",
            "previous_hash",
            "current_hash",
            "record_type",
            "source",
            "payload",
        }
        persisted = _require_persisted_mapping(value, fields, "evidence record")
        try:
            record = cls(
                serialization=_persisted_string(persisted, "serialization"),
                hash_algorithm=_persisted_string(persisted, "hash_algorithm"),
                chain_id=_persisted_string(persisted, "chain_id"),
                sequence=_persisted_positive_integer(persisted, "sequence"),
                previous_hash=_persisted_string(persisted, "previous_hash"),
                current_hash=_persisted_string(persisted, "current_hash"),
                record_type=_persisted_string(persisted, "record_type"),
                source=_persisted_string(persisted, "source"),
                payload=persisted["payload"],
            )
        except EvidenceIntegrityError:
            raise
        except (TypeError, ValueError) as error:
            raise EvidenceIntegrityError("evidence record payload is not canonical JSON") from error
        _require_supported(record.serialization, record.hash_algorithm, "record")
        _validate_digest(record.chain_id, "record chain_id")
        _validate_digest(record.previous_hash, "record previous_hash")
        _validate_digest(record.current_hash, "record current_hash")
        _require_non_empty_integrity("record_type", record.record_type)
        _require_trusted_source(record.source, integrity_error=True)
        return record

    def hash_material(self) -> dict[str, object]:
        """Return the envelope covered by ``current_hash``."""

        return {
            "serialization": self.serialization,
            "hash_algorithm": self.hash_algorithm,
            "chain_id": self.chain_id,
            "sequence": self.sequence,
            "previous_hash": self.previous_hash,
            "record_type": self.record_type,
            "source": self.source,
            "payload": _thaw_json(self.payload),
        }

    def to_dict(self) -> dict[str, object]:
        """Return a detached JSON-compatible representation for persistence."""

        return self.hash_material() | {"current_hash": self.current_hash}

    def canonical_bytes(self) -> bytes:
        """Return the complete byte-stable stored representation."""

        return canonical_json_bytes(self.to_dict())


@dataclass(frozen=True, slots=True)
class ProtectedHead:
    """Separately stored trust anchor protected by storage ACLs.

    Records and this value must not share a writer role. Only a trusted evidence
    service should be allowed to compare-and-set the protected head.
    """

    serialization: str
    hash_algorithm: str
    chain_id: str
    sequence: int
    current_hash: str

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> ProtectedHead:
        """Parse and validate one persisted protected-head dictionary."""

        fields = {
            "serialization",
            "hash_algorithm",
            "chain_id",
            "sequence",
            "current_hash",
        }
        persisted = _require_persisted_mapping(value, fields, "evidence head")
        head = cls(
            serialization=_persisted_string(persisted, "serialization"),
            hash_algorithm=_persisted_string(persisted, "hash_algorithm"),
            chain_id=_persisted_string(persisted, "chain_id"),
            sequence=_persisted_positive_integer(persisted, "sequence"),
            current_hash=_persisted_string(persisted, "current_hash"),
        )
        _require_supported(head.serialization, head.hash_algorithm, "protected head")
        _validate_digest(head.chain_id, "protected head chain_id")
        _validate_digest(head.current_hash, "protected head current_hash")
        return head

    @classmethod
    def from_record(cls, record: EvidenceRecord) -> ProtectedHead:
        return cls(
            serialization=record.serialization,
            hash_algorithm=record.hash_algorithm,
            chain_id=record.chain_id,
            sequence=record.sequence,
            current_hash=record.current_hash,
        )

    def to_dict(self) -> dict[str, object]:
        """Return a detached JSON-compatible representation for persistence."""

        return {
            "serialization": self.serialization,
            "hash_algorithm": self.hash_algorithm,
            "chain_id": self.chain_id,
            "sequence": self.sequence,
            "current_hash": self.current_hash,
        }


def append(
    previous_head: ProtectedHead | None,
    *,
    workflow_id: str,
    record_type: str,
    payload: object,
    trusted_source: TrustedEvidenceSource,
) -> tuple[EvidenceRecord, ProtectedHead]:
    """Create one record and its next head from trusted-boundary inputs.

    Callers provide record content. This function, not a worker payload, owns
    source placement, chain identity, sequence, and both hashes.
    """

    _require_non_empty("record_type", record_type)
    _require_trusted_source(trusted_source)
    expected_chain_id = chain_id_for(workflow_id)
    if previous_head is None:
        sequence = 1
        previous_hash = GENESIS_HASH
    else:
        _validate_head(previous_head, expected_chain_id)
        sequence = previous_head.sequence + 1
        previous_hash = previous_head.current_hash

    record_without_hash = EvidenceRecord(
        serialization=SERIALIZATION,
        hash_algorithm=HASH_ALGORITHM,
        chain_id=expected_chain_id,
        sequence=sequence,
        previous_hash=previous_hash,
        current_hash="",
        record_type=record_type,
        source=trusted_source,
        payload=payload,
    )
    current_hash = _record_hash(record_without_hash)
    record = EvidenceRecord(
        serialization=record_without_hash.serialization,
        hash_algorithm=record_without_hash.hash_algorithm,
        chain_id=record_without_hash.chain_id,
        sequence=record_without_hash.sequence,
        previous_hash=record_without_hash.previous_hash,
        current_hash=current_hash,
        record_type=record_without_hash.record_type,
        source=record_without_hash.source,
        payload=record_without_hash.payload,
    )
    return record, ProtectedHead.from_record(record)


def verify(
    records: Sequence[EvidenceRecord],
    protected_head: ProtectedHead | None,
    *,
    workflow_id: str,
) -> ProtectedHead | None:
    """Independently recompute a full chain and compare its protected head."""

    expected_chain_id = chain_id_for(workflow_id)
    if protected_head is not None:
        _validate_head(protected_head, expected_chain_id)
    calculated_head = _verify_records(records, None, expected_chain_id)
    _require_matching_head(calculated_head, protected_head)
    return calculated_head


def verify_transition(
    previous_head: ProtectedHead | None,
    appended_records: Sequence[EvidenceRecord],
    current_head: ProtectedHead | None,
    *,
    workflow_id: str,
) -> ProtectedHead | None:
    """Verify a newly persisted suffix from a separately trusted prior head.

    A ledger adapter can call this after reading only records newer than the
    prior sequence, then compare-and-set ``previous_head`` to ``current_head``.
    """

    expected_chain_id = chain_id_for(workflow_id)
    if previous_head is not None:
        _validate_head(previous_head, expected_chain_id)
    if current_head is not None:
        _validate_head(current_head, expected_chain_id)
    calculated_head = _verify_records(appended_records, previous_head, expected_chain_id)
    _require_matching_head(calculated_head, current_head)
    return calculated_head


def verify_prefix(
    previous_records: Sequence[EvidenceRecord],
    previous_head: ProtectedHead | None,
    current_records: Sequence[EvidenceRecord],
    current_head: ProtectedHead | None,
    *,
    workflow_id: str,
) -> None:
    """Prove that a valid later snapshot preserves the valid earlier snapshot."""

    verify(previous_records, previous_head, workflow_id=workflow_id)
    verify(current_records, current_head, workflow_id=workflow_id)
    require_append_only_prefix(previous_records, current_records)


def require_append_only_prefix(
    previous_records: Sequence[EvidenceRecord],
    current_records: Sequence[EvidenceRecord],
) -> None:
    """Compare the prefix of two snapshots that were already verified."""

    if len(current_records) < len(previous_records):
        raise EvidenceIntegrityError("current records violate the append-only prefix")
    for sequence, (previous, current) in enumerate(
        zip(previous_records, current_records, strict=False), start=1
    ):
        if not hmac.compare_digest(previous.canonical_bytes(), current.canonical_bytes()):
            raise EvidenceIntegrityError(f"record {sequence} violates the append-only prefix")


def _verify_records(
    records: Sequence[EvidenceRecord],
    initial_head: ProtectedHead | None,
    expected_chain_id: str,
) -> ProtectedHead | None:
    calculated_head = initial_head
    expected_sequence = 1 if initial_head is None else initial_head.sequence + 1
    expected_previous_hash = GENESIS_HASH if initial_head is None else initial_head.current_hash

    for record in records:
        if not isinstance(record, EvidenceRecord):
            raise EvidenceIntegrityError("evidence chain contains an unsupported record")
        _require_supported(record.serialization, record.hash_algorithm, "record")
        if record.chain_id != expected_chain_id:
            raise EvidenceIntegrityError(
                f"record {expected_sequence} belongs to the wrong evidence chain"
            )
        if record.sequence != expected_sequence:
            raise EvidenceIntegrityError(
                f"record sequence is {record.sequence}; expected {expected_sequence}"
            )
        if record.previous_hash != expected_previous_hash:
            raise EvidenceIntegrityError(
                f"record {record.sequence} does not link to the previous hash"
            )
        _require_non_empty_integrity("record_type", record.record_type)
        _require_trusted_source(record.source, integrity_error=True)
        _validate_digest(record.current_hash, f"record {record.sequence} current_hash")
        expected_hash = _record_hash(record)
        if not hmac.compare_digest(record.current_hash, expected_hash):
            raise EvidenceIntegrityError(f"record {record.sequence} hash does not match content")

        calculated_head = ProtectedHead.from_record(record)
        expected_sequence += 1
        expected_previous_hash = record.current_hash

    return calculated_head


def _record_hash(record: EvidenceRecord) -> str:
    return hashlib.sha256(canonical_json_bytes(record.hash_material())).hexdigest()


def _validate_head(head: ProtectedHead, expected_chain_id: str) -> None:
    _require_supported(head.serialization, head.hash_algorithm, "protected head")
    if head.chain_id != expected_chain_id:
        raise EvidenceIntegrityError("protected head belongs to the wrong evidence chain")
    if not isinstance(head.sequence, int) or isinstance(head.sequence, bool) or head.sequence < 1:
        raise EvidenceIntegrityError("protected head sequence must be a positive integer")
    _validate_digest(head.current_hash, "protected head current_hash")


def _require_matching_head(
    calculated_head: ProtectedHead | None, protected_head: ProtectedHead | None
) -> None:
    if calculated_head != protected_head:
        raise EvidenceIntegrityError("evidence records do not match the protected head")


def _require_supported(serialization: str, hash_algorithm: str, subject: str) -> None:
    if serialization != SERIALIZATION:
        raise EvidenceIntegrityError(f"unsupported {subject} serialization: {serialization!r}")
    if hash_algorithm != HASH_ALGORITHM:
        raise EvidenceIntegrityError(f"unsupported {subject} hash algorithm: {hash_algorithm!r}")


def _validate_digest(value: str, field: str) -> None:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise EvidenceIntegrityError(f"{field} must be a lowercase SHA-256 digest")


def _require_non_empty(field: str, value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")


def _require_non_empty_integrity(field: str, value: str) -> None:
    try:
        _require_non_empty(field, value)
    except ValueError as error:
        raise EvidenceIntegrityError(str(error)) from error


def _require_trusted_source(
    value: str,
    *,
    integrity_error: bool = False,
) -> None:
    if value in TRUSTED_EVIDENCE_SOURCES:
        return
    message = f"unsupported trusted evidence source: {value!r}"
    if integrity_error:
        raise EvidenceIntegrityError(message)
    raise ValueError(message)


def _require_persisted_mapping(
    value: object, fields: set[str], subject: str
) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise EvidenceIntegrityError(f"{subject} must be a dictionary")
    actual_fields = set(value)
    if any(not isinstance(field, str) for field in actual_fields):
        raise EvidenceIntegrityError(f"{subject} field names must be strings")
    if actual_fields != fields:
        missing = sorted(fields - actual_fields)
        unexpected = sorted(actual_fields - fields)
        raise EvidenceIntegrityError(
            f"{subject} fields do not match the persistence contract: "
            f"missing={missing!r}, unexpected={unexpected!r}"
        )
    return value


def _persisted_string(value: Mapping[str, object], field: str) -> str:
    persisted = value[field]
    if not isinstance(persisted, str):
        raise EvidenceIntegrityError(f"{field} must be a string")
    return persisted


def _persisted_positive_integer(value: Mapping[str, object], field: str) -> int:
    persisted = value[field]
    if not isinstance(persisted, int) or isinstance(persisted, bool) or persisted < 1:
        raise EvidenceIntegrityError(f"{field} must be a positive integer")
    return persisted
