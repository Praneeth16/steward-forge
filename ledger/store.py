"""Ledger boundary and in-memory tracer implementation."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator, Mapping
from contextlib import AbstractContextManager, contextmanager
from copy import deepcopy
from threading import RLock
from typing import Any, Protocol

from evidence import (
    EvidenceIntegrityError,
    EvidenceRecord,
    ProtectedHead,
    require_append_only_prefix,
    verify,
)

WorkflowState = dict[str, Any]
PersistedEvidenceRecord = dict[str, object]
PersistedEvidenceHead = dict[str, object]
EvidenceView = tuple[list[PersistedEvidenceRecord], PersistedEvidenceHead | None]


class LedgerNotFound(KeyError):
    """A requested workflow does not exist."""


class LedgerConflict(ValueError):
    """An idempotency key or version is bound to different content."""


class Ledger(Protocol):
    """Storage operations the deterministic orchestrator requires."""

    def create(
        self, idempotency_key: str, initial_state: WorkflowState
    ) -> tuple[WorkflowState, bool]: ...

    def get(self, brief_id: str) -> WorkflowState: ...

    def get_evidence(self, brief_id: str) -> EvidenceView: ...

    def transaction(self, brief_id: str) -> AbstractContextManager[WorkflowState]: ...


def parse_evidence_state(
    state: WorkflowState, *, workflow_id: str
) -> tuple[list[EvidenceRecord], ProtectedHead | None]:
    """Parse and verify the evidence snapshot embedded in workflow state."""

    has_chain = "evidence_chain" in state
    has_head = "evidence_head" in state
    if not has_chain and not has_head:
        return [], None
    if not has_chain or not has_head:
        raise EvidenceIntegrityError(
            "workflow evidence_chain and evidence_head must either both exist or both be absent"
        )

    persisted_records = state["evidence_chain"]
    persisted_head = state["evidence_head"]
    if not isinstance(persisted_records, list):
        raise EvidenceIntegrityError(
            "workflow evidence_chain must be a list of record dictionaries"
        )
    if not isinstance(persisted_head, Mapping):
        raise EvidenceIntegrityError("workflow evidence_head must be a dictionary")

    records = [EvidenceRecord.from_dict(record) for record in persisted_records]
    head = ProtectedHead.from_dict(persisted_head)
    verify(records, head, workflow_id=workflow_id)
    return records, head


def verify_evidence_state(
    state: WorkflowState,
    protected_head: ProtectedHead | None,
    *,
    workflow_id: str,
) -> tuple[list[EvidenceRecord], ProtectedHead | None]:
    """Verify embedded evidence against the ledger-owned trust anchor."""

    records, embedded_head = parse_evidence_state(state, workflow_id=workflow_id)
    if embedded_head != protected_head:
        raise EvidenceIntegrityError("workflow evidence does not match the protected head")
    return records, protected_head


def verify_evidence_transition(
    previous_state: WorkflowState,
    previous_protected_head: ProtectedHead | None,
    current_state: WorkflowState,
    *,
    workflow_id: str,
) -> ProtectedHead | None:
    """Accept only a valid suffix that preserves the protected old prefix."""

    previous_records, _ = verify_evidence_state(
        previous_state,
        previous_protected_head,
        workflow_id=workflow_id,
    )
    current_records, current_head = parse_evidence_state(current_state, workflow_id=workflow_id)
    require_append_only_prefix(previous_records, current_records)
    return current_head


def detached_evidence(
    records: list[EvidenceRecord], protected_head: ProtectedHead | None
) -> EvidenceView:
    """Return JSON-compatible copies without exposing ledger-owned objects."""

    return (
        [record.to_dict() for record in records],
        protected_head.to_dict() if protected_head is not None else None,
    )


class InMemoryLedger:
    """Thread-safe ledger used by the local workbench and integration tests.

    The transaction boundary mirrors the durable PostgreSQL adapter. Keeping
    workflow logic outside this class lets the tracer switch storage without
    changing approval or release behavior.
    """

    def __init__(self) -> None:
        self._lock = RLock()
        self._brief_ids_by_key: dict[str, str] = {}
        self._input_hashes_by_key: dict[str, str] = {}
        self._briefs: dict[str, WorkflowState] = {}
        self._evidence_heads: dict[str, ProtectedHead] = {}

    def create(
        self, idempotency_key: str, initial_state: WorkflowState
    ) -> tuple[WorkflowState, bool]:
        with self._lock:
            brief_id = str(initial_state["id"])
            _, initial_head = parse_evidence_state(initial_state, workflow_id=brief_id)
            input_hash = hashlib.sha256(
                json.dumps(
                    initial_state,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=True,
                ).encode()
            ).hexdigest()
            existing_id = self._brief_ids_by_key.get(idempotency_key)
            if existing_id:
                if self._input_hashes_by_key[idempotency_key] != input_hash:
                    raise LedgerConflict("idempotency key is already bound to a different payload")
                verify_evidence_state(
                    self._briefs[existing_id],
                    self._evidence_heads.get(existing_id),
                    workflow_id=existing_id,
                )
                return deepcopy(self._briefs[existing_id]), False

            self._brief_ids_by_key[idempotency_key] = brief_id
            self._input_hashes_by_key[idempotency_key] = input_hash
            self._briefs[brief_id] = deepcopy(initial_state)
            if initial_head is not None:
                self._evidence_heads[brief_id] = initial_head
            return deepcopy(initial_state), True

    def get(self, brief_id: str) -> WorkflowState:
        with self._lock:
            try:
                state = self._briefs[brief_id]
            except KeyError as error:
                raise LedgerNotFound(brief_id) from error
            verify_evidence_state(
                state,
                self._evidence_heads.get(brief_id),
                workflow_id=brief_id,
            )
            return deepcopy(state)

    def get_evidence(self, brief_id: str) -> EvidenceView:
        with self._lock:
            try:
                state = self._briefs[brief_id]
            except KeyError as error:
                raise LedgerNotFound(brief_id) from error
            records, protected_head = verify_evidence_state(
                state,
                self._evidence_heads.get(brief_id),
                workflow_id=brief_id,
            )
            return detached_evidence(records, protected_head)

    @contextmanager
    def transaction(self, brief_id: str) -> Iterator[WorkflowState]:
        with self._lock:
            try:
                stored = self._briefs[brief_id]
            except KeyError as error:
                raise LedgerNotFound(brief_id) from error
            protected_head = self._evidence_heads.get(brief_id)
            verify_evidence_state(stored, protected_head, workflow_id=brief_id)
            previous_state = deepcopy(stored)
            state = deepcopy(stored)
            yield state
            current_head = verify_evidence_transition(
                previous_state,
                protected_head,
                state,
                workflow_id=brief_id,
            )
            self._briefs[brief_id] = deepcopy(state)
            if current_head is not None:
                self._evidence_heads[brief_id] = current_head
