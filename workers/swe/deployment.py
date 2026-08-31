"""Injectable, idempotent workspace deployment boundary."""

from __future__ import annotations

import hashlib
from copy import deepcopy

from workers.swe.models import DeploymentResult


class InMemoryDeploymentAdapter:
    """Local deployment proof that records IDs and rollback state."""

    def __init__(
        self,
        *,
        previous_release_sha: str | None = None,
        previous_workspace_ids: dict[str, str] | None = None,
    ) -> None:
        self._rollback_state: dict[str, object] = {
            "release_sha": previous_release_sha,
            "workspace_ids": dict(previous_workspace_ids or {}),
        }
        self._results: dict[str, tuple[tuple[str, bool], DeploymentResult]] = {}
        self.deploy_calls = 0

    def deploy(
        self,
        *,
        commit_sha: str,
        include_genie: bool,
        idempotency_key: str,
    ) -> DeploymentResult:
        existing = self._results.get(idempotency_key)
        if existing is not None:
            existing_request, result = existing
            if existing_request != (commit_sha, include_genie):
                raise ValueError("deployment idempotency key is bound to another request")
            return result
        workspace_ids = {
            "deployment": _workspace_id("deployment", commit_sha),
            "dashboard": _workspace_id("dashboard", commit_sha),
        }
        if include_genie:
            workspace_ids["genie_space"] = _workspace_id("genie", commit_sha)
        result = DeploymentResult(
            commit_sha=commit_sha,
            workspace_ids=workspace_ids,
            rollback_state=deepcopy(self._rollback_state),
        )
        self._results[idempotency_key] = ((commit_sha, include_genie), result)
        self._rollback_state = {
            "release_sha": commit_sha,
            "workspace_ids": dict(workspace_ids),
        }
        self.deploy_calls += 1
        return result


def _workspace_id(kind: str, commit_sha: str) -> str:
    suffix = hashlib.sha256(f"{kind}:{commit_sha}".encode()).hexdigest()[:16]
    return f"{kind}-{suffix}"
