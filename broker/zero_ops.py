"""Fail-closed health gate evaluated before external mutations."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class HealthSnapshot:
    lakebase_available: bool
    pipeline_fresh: bool
    unity_catalog_fresh: bool

    @property
    def healthy(self) -> bool:
        return self.lakebase_available and self.pipeline_fresh and self.unity_catalog_fresh


class PreActDenied(RuntimeError):
    """The current system state is unsafe for a mutation."""


class ZeroOpsPreAct:
    """Checks operational health while exempting evidence capture."""

    def __init__(self, probe: Callable[[], HealthSnapshot]) -> None:
        self._probe = probe

    def authorize(self, category: str) -> None:
        if category == "evidence":
            return
        try:
            snapshot = self._probe()
        except Exception as error:
            raise PreActDenied("pre-act health check failed") from error
        if not snapshot.healthy:
            raise PreActDenied("pre-act health check failed: system is stale or unavailable")
