"""Fail-closed health gate evaluated before external mutations."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class HealthSnapshot:
    lakebase_available: bool
    lakebase_fresh: bool
    pipeline_fresh: bool
    unity_catalog_fresh: bool

    @property
    def healthy(self) -> bool:
        return not self.failures

    @property
    def failures(self) -> tuple[str, ...]:
        failures: list[str] = []
        if not self.lakebase_available:
            failures.append("Lakebase is unavailable")
        elif not self.lakebase_fresh:
            failures.append("Lakebase is stale")
        if not self.pipeline_fresh:
            failures.append("pipeline is stale")
        if not self.unity_catalog_fresh:
            failures.append("Unity Catalog is stale")
        return tuple(failures)


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
            if not isinstance(snapshot, HealthSnapshot):
                raise TypeError("health probe returned an invalid snapshot")
            failures = snapshot.failures
        except Exception as error:
            raise PreActDenied("pre-act health check failed") from error
        if failures:
            raise PreActDenied(f"pre-act health check failed: {'; '.join(failures)}")
