"""Ledger boundary and in-memory tracer implementation."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from copy import deepcopy
from threading import RLock
from typing import Any, Protocol

WorkflowState = dict[str, Any]


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

    def transaction(self, brief_id: str) -> AbstractContextManager[WorkflowState]: ...


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

    def create(
        self, idempotency_key: str, initial_state: WorkflowState
    ) -> tuple[WorkflowState, bool]:
        with self._lock:
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
                return deepcopy(self._briefs[existing_id]), False

            brief_id = str(initial_state["id"])
            self._brief_ids_by_key[idempotency_key] = brief_id
            self._input_hashes_by_key[idempotency_key] = input_hash
            self._briefs[brief_id] = deepcopy(initial_state)
            return deepcopy(initial_state), True

    def get(self, brief_id: str) -> WorkflowState:
        with self._lock:
            try:
                return deepcopy(self._briefs[brief_id])
            except KeyError as error:
                raise LedgerNotFound(brief_id) from error

    @contextmanager
    def transaction(self, brief_id: str) -> Iterator[WorkflowState]:
        with self._lock:
            try:
                stored = self._briefs[brief_id]
            except KeyError as error:
                raise LedgerNotFound(brief_id) from error
            state = deepcopy(stored)
            yield state
            self._briefs[brief_id] = deepcopy(state)
