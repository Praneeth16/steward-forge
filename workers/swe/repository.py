"""Injectable repository boundary for broker-owned candidate commits."""

from __future__ import annotations

import hashlib
import json

from broker.contracts import ArtifactCommitArgs, DraftArtifact
from workers.swe.models import ArtifactCommit


class InMemoryArtifactRepository:
    """Local proof adapter; no Git remote is mutated."""

    def __init__(self, trusted_base_sha: str) -> None:
        self._parents: dict[str, str | None] = {trusted_base_sha: None}
        self._branch_heads: dict[str, str] = {}
        self._artifacts: dict[str, tuple[DraftArtifact, ...]] = {}
        self._requests: dict[str, ArtifactCommit] = {}
        self.commit_calls = 0

    def commit(self, arguments: ArtifactCommitArgs) -> dict[str, object]:
        request_hash = hashlib.sha256(
            arguments.model_dump_json().encode()
        ).hexdigest()
        existing = self._requests.get(request_hash)
        if existing is not None:
            return existing.model_dump(mode="json")
        if arguments.parent_sha not in self._parents:
            raise ValueError("candidate parent is unknown to the repository")
        current_head = self._branch_heads.get(arguments.branch)
        if current_head is not None and current_head != arguments.parent_sha:
            raise ValueError("candidate branch head changed")
        canonical = json.dumps(
            arguments.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        )
        commit_sha = hashlib.sha256(f"commit:{canonical}".encode()).hexdigest()
        commit = ArtifactCommit(
            commit_sha=commit_sha,
            parent_sha=arguments.parent_sha,
            branch=arguments.branch,
            paths=tuple(artifact.path for artifact in arguments.artifacts),
            artifact_hashes={
                artifact.path: artifact.sha for artifact in arguments.artifacts
            },
        )
        self._parents[commit_sha] = arguments.parent_sha
        self._artifacts[commit_sha] = arguments.artifacts
        self._branch_heads[arguments.branch] = commit_sha
        self._requests[request_hash] = commit
        self.commit_calls += 1
        return commit.model_dump(mode="json")

    def read(self, commit_sha: str) -> tuple[DraftArtifact, ...]:
        if commit_sha not in self._artifacts:
            raise ValueError("committed artifacts are unavailable")
        return self._artifacts[commit_sha]

    def is_descendant(self, commit_sha: str, ancestor_sha: str) -> bool:
        current: str | None = commit_sha
        visited: set[str] = set()
        while current is not None and current not in visited:
            if current == ancestor_sha:
                return True
            visited.add(current)
            current = self._parents.get(current)
        return False

    def detach_for_test(self, commit_sha: str) -> None:
        if commit_sha not in self._parents:
            raise ValueError("cannot detach an unknown commit")
        self._parents[commit_sha] = None

    def replace_artifacts_for_test(
        self, commit_sha: str, artifacts: tuple[DraftArtifact, ...]
    ) -> None:
        if commit_sha not in self._artifacts:
            raise ValueError("cannot replace artifacts for an unknown commit")
        self._artifacts[commit_sha] = artifacts
