"""Replay-safe in-memory persistence for cross-store release evidence."""

from __future__ import annotations

from threading import RLock

from evidence import canonical_json_bytes
from release_evidence.models import (
    GovernedReleaseReceipt,
    ReleaseEvidencePointer,
)


class ReleaseEvidenceNotFound(KeyError):
    """A governed release-evidence row does not exist."""


class ReleaseEvidenceConflict(ValueError):
    """One receipt ID was reused for different canonical bytes."""


class _InsertOnlyStore[StoredEvidence: (GovernedReleaseReceipt, ReleaseEvidencePointer)]:
    def __init__(self, row_kind: str) -> None:
        self._row_kind = row_kind
        self._lock = RLock()
        self._rows: dict[str, StoredEvidence] = {}
        self._canonical_bytes: dict[str, bytes] = {}

    def insert_if_absent(
        self,
        receipt_id: str,
        row: StoredEvidence,
    ) -> StoredEvidence:
        row_bytes = canonical_json_bytes(row.model_dump(mode="json"))
        with self._lock:
            existing = self._rows.get(receipt_id)
            if existing is None:
                self._rows[receipt_id] = row
                self._canonical_bytes[receipt_id] = row_bytes
                return row
            if self._canonical_bytes[receipt_id] != row_bytes:
                raise ReleaseEvidenceConflict(
                    f"{self._row_kind} {receipt_id} is already bound to different bytes"
                )
            return existing

    def get(self, receipt_id: str) -> StoredEvidence:
        with self._lock:
            try:
                return self._rows[receipt_id]
            except KeyError as error:
                raise ReleaseEvidenceNotFound(receipt_id) from error

    def delete_for_test(self, receipt_id: str) -> None:
        with self._lock:
            self._rows.pop(receipt_id, None)
            self._canonical_bytes.pop(receipt_id, None)


class InMemoryReleaseEvidenceStore:
    """Thread-safe insert-if-absent storage used by tests and local execution."""

    def __init__(self) -> None:
        self._store = _InsertOnlyStore[GovernedReleaseReceipt]("receipt")

    def insert_if_absent(
        self,
        receipt: GovernedReleaseReceipt,
    ) -> GovernedReleaseReceipt:
        """Insert once, replay exact bytes, and reject same-ID conflicts."""

        return self._store.insert_if_absent(receipt.receipt_id, receipt)

    def get(self, receipt_id: str) -> GovernedReleaseReceipt:
        """Return one receipt or raise an explicit missing-row error."""

        return self._store.get(receipt_id)

    def delete_for_test(self, receipt_id: str) -> None:
        """Remove one row to simulate a torn write in reconciliation tests."""

        self._store.delete_for_test(receipt_id)


class InMemoryReleaseEvidencePointerStore:
    """Thread-safe insert-only Lakebase-style pointer storage.

    Production adapters should enforce the same receipt-ID uniqueness and byte
    comparison in one database transaction. Updates are intentionally absent:
    changing a location or receipt hash under an existing ID is a conflict.
    """

    def __init__(self) -> None:
        self._store = _InsertOnlyStore[ReleaseEvidencePointer]("pointer")

    def insert_if_absent(self, pointer: ReleaseEvidencePointer) -> ReleaseEvidencePointer:
        """Insert once, replay exact bytes, and reject same-ID conflicts."""

        return self._store.insert_if_absent(pointer.receipt_id, pointer)

    def get(self, receipt_id: str) -> ReleaseEvidencePointer:
        """Return one pointer or raise an explicit missing-row error."""

        return self._store.get(receipt_id)

    def delete_for_test(self, receipt_id: str) -> None:
        """Remove one row to simulate a torn write in reconciliation tests."""

        self._store.delete_for_test(receipt_id)
