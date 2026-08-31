"""Publish and reconcile governed receipts across independent stores."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Protocol

from evidence import canonical_json_bytes
from release_evidence.models import (
    GovernedReleaseReceipt,
    ReleaseEvidencePointer,
)
from release_evidence.store import ReleaseEvidenceNotFound


class ReleaseReceiptStore(Protocol):
    """Receipt persistence required by the publisher."""

    def insert_if_absent(self, receipt: GovernedReleaseReceipt) -> GovernedReleaseReceipt: ...

    def get(self, receipt_id: str) -> GovernedReleaseReceipt: ...


class ReleasePointerStore(Protocol):
    """Lakebase-style pointer persistence required by the publisher."""

    def insert_if_absent(self, pointer: ReleaseEvidencePointer) -> ReleaseEvidencePointer: ...

    def get(self, receipt_id: str) -> ReleaseEvidencePointer: ...


@dataclass(frozen=True, slots=True)
class PublishedReleaseEvidence:
    """The exact immutable rows stored on both sides of publication."""

    receipt: GovernedReleaseReceipt
    pointer: ReleaseEvidencePointer


class ReleaseEvidencePublisher:
    """Publish or repair one receipt and its independently stored pointer."""

    def __init__(
        self,
        receipt_store: ReleaseReceiptStore,
        pointer_store: ReleasePointerStore,
    ) -> None:
        self._receipt_store = receipt_store
        self._pointer_store = pointer_store

    @staticmethod
    def receipt_sha256(receipt: GovernedReleaseReceipt) -> str:
        """Hash the exact canonical bytes persisted in the receipt store."""

        receipt_bytes = canonical_json_bytes(receipt.model_dump(mode="json"))
        return hashlib.sha256(receipt_bytes).hexdigest()

    def pointer_for(
        self,
        receipt: GovernedReleaseReceipt,
        *,
        receipt_location: str,
    ) -> ReleaseEvidencePointer:
        """Expose the explicit durable location and canonical receipt hash binding."""

        return ReleaseEvidencePointer(
            receipt_id=receipt.receipt_id,
            request_hash=receipt.request_hash,
            receipt_location=receipt_location,
            receipt_sha256=self.receipt_sha256(receipt),
            evidence_chain_reference=receipt.evidence_chain_reference,
        )

    def publish(
        self,
        receipt: GovernedReleaseReceipt,
        *,
        receipt_location: str,
    ) -> PublishedReleaseEvidence:
        """Insert both rows and return the original stored rows on exact replay."""

        return self._persist(receipt, receipt_location=receipt_location)

    def reconcile(
        self,
        receipt: GovernedReleaseReceipt,
        *,
        receipt_location: str,
    ) -> PublishedReleaseEvidence:
        """Repair either torn-write direction from a canonical receipt candidate."""

        return self._persist(receipt, receipt_location=receipt_location)

    def _persist(
        self,
        receipt: GovernedReleaseReceipt,
        *,
        receipt_location: str,
    ) -> PublishedReleaseEvidence:
        pointer = self.pointer_for(receipt, receipt_location=receipt_location)
        existing_receipt, existing_pointer = self._preflight_existing(receipt, pointer)
        stored_receipt = existing_receipt or self._receipt_store.insert_if_absent(receipt)
        stored_pointer = existing_pointer or self._pointer_store.insert_if_absent(pointer)
        return PublishedReleaseEvidence(receipt=stored_receipt, pointer=stored_pointer)

    def _preflight_existing(
        self,
        receipt: GovernedReleaseReceipt,
        pointer: ReleaseEvidencePointer,
    ) -> tuple[GovernedReleaseReceipt | None, ReleaseEvidencePointer | None]:
        """Detect known conflicts before filling a missing opposite side."""

        try:
            existing_receipt = self._receipt_store.get(receipt.receipt_id)
        except ReleaseEvidenceNotFound:
            existing_receipt = None
        else:
            existing_receipt = self._receipt_store.insert_if_absent(receipt)

        try:
            existing_pointer = self._pointer_store.get(pointer.receipt_id)
        except ReleaseEvidenceNotFound:
            existing_pointer = None
        else:
            existing_pointer = self._pointer_store.insert_if_absent(pointer)
        return existing_receipt, existing_pointer
