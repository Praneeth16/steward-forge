"""Injectable access controls used by the reversible kill switch."""

from __future__ import annotations

from threading import RLock
from typing import Protocol


class RevocationLayer(Protocol):
    """One independently observable worker-access control."""

    def revoke(self, worker_id: str) -> None: ...

    def restore(self, worker_id: str) -> None: ...

    def is_revoked(self, worker_id: str) -> bool: ...


class InMemoryRevocationLayer:
    """Deterministic adapter for local tests; deployments inject live controls."""

    def __init__(self, name: str) -> None:
        self.name = name
        self._revoked: set[str] = set()
        self._lock = RLock()

    def revoke(self, worker_id: str) -> None:
        with self._lock:
            self._revoked.add(worker_id)

    def restore(self, worker_id: str) -> None:
        with self._lock:
            self._revoked.discard(worker_id)

    def is_revoked(self, worker_id: str) -> bool:
        with self._lock:
            return worker_id in self._revoked
