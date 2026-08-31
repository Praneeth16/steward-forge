"""Injectable, observe-before-mutate workspace deployment boundary."""

from __future__ import annotations

import hashlib
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime
from threading import RLock

from workers.swe.models import DeploymentResult


@dataclass(frozen=True, slots=True)
class DeploymentRequest:
    commit_sha: str
    include_genie: bool
    receipt_id: str | None
    request_hash: str | None


DeploymentRecord = tuple[DeploymentRequest, DeploymentResult]


class DeploymentConflict(ValueError):
    """An idempotency key is already bound to different deployment input."""


class DeploymentAcknowledgementLost(ConnectionError):
    """The remote deployment succeeded, but its local acknowledgement was lost."""


class InMemoryDeploymentBackend:
    """Shared fake remote state that survives construction of new adapters."""

    def __init__(
        self,
        *,
        previous_release_sha: str | None = None,
        previous_workspace_ids: dict[str, str] | None = None,
    ) -> None:
        self._lock = RLock()
        self._rollback_state: dict[str, object] = {
            "release_sha": previous_release_sha,
            "workspace_ids": dict(previous_workspace_ids or {}),
        }
        self._records: dict[str, DeploymentRecord] = {}
        self.deploy_calls = 0

    def observe(
        self,
        *,
        commit_sha: str,
        include_genie: bool,
        idempotency_key: str,
        receipt_id: str | None = None,
        request_hash: str | None = None,
    ) -> DeploymentResult | None:
        """Read remote truth and fail if the key names another request."""

        with self._lock:
            record = self._records.get(idempotency_key)
            if record is None:
                return None
            self._validate_request(
                record,
                commit_sha=commit_sha,
                include_genie=include_genie,
                receipt_id=receipt_id,
                request_hash=request_hash,
            )
            return record[1]

    def ensure_deployed(
        self,
        *,
        commit_sha: str,
        include_genie: bool,
        idempotency_key: str,
        receipt_id: str | None = None,
        request_hash: str | None = None,
        lease_owner: str | None = None,
        lease_epoch: int | None = None,
    ) -> tuple[DeploymentResult, bool]:
        """Atomically return remote state or perform one new deployment."""

        with self._lock:
            record = self._records.get(idempotency_key)
            if record is not None:
                self._validate_request(
                    record,
                    commit_sha=commit_sha,
                    include_genie=include_genie,
                    receipt_id=receipt_id,
                    request_hash=request_hash,
                )
                return record[1], False

            workspace_ids = {
                "deployment": _workspace_id("deployment", commit_sha),
                "dashboard": _workspace_id("dashboard", commit_sha),
            }
            if include_genie:
                workspace_ids["genie_space"] = _workspace_id("genie", commit_sha)
            result = DeploymentResult(
                commit_sha=commit_sha,
                observed_at=datetime.now(UTC),
                receipt_id=receipt_id,
                request_hash=request_hash,
                lease_owner=lease_owner,
                lease_epoch=lease_epoch,
                workspace_ids=workspace_ids,
                rollback_state=deepcopy(self._rollback_state),
            )
            self._records[idempotency_key] = (
                DeploymentRequest(
                    commit_sha=commit_sha,
                    include_genie=include_genie,
                    receipt_id=receipt_id,
                    request_hash=request_hash,
                ),
                result,
            )
            self._rollback_state = {
                "release_sha": commit_sha,
                "workspace_ids": dict(workspace_ids),
            }
            self.deploy_calls += 1
            return result, True

    @staticmethod
    def _validate_request(
        record: DeploymentRecord,
        *,
        commit_sha: str,
        include_genie: bool,
        receipt_id: str | None,
        request_hash: str | None,
    ) -> None:
        request = DeploymentRequest(
            commit_sha=commit_sha,
            include_genie=include_genie,
            receipt_id=receipt_id,
            request_hash=request_hash,
        )
        if record[0] != request:
            raise DeploymentConflict("deployment idempotency key is bound to another request")


class InMemoryDeploymentAdapter:
    """Local adapter that observes shared remote truth before deployment."""

    def __init__(
        self,
        *,
        backend: InMemoryDeploymentBackend | None = None,
        previous_release_sha: str | None = None,
        previous_workspace_ids: dict[str, str] | None = None,
        lose_acknowledgement_once: bool = False,
    ) -> None:
        if backend is not None and (
            previous_release_sha is not None or previous_workspace_ids is not None
        ):
            raise ValueError("previous release state belongs to the shared deployment backend")
        self._backend = backend or InMemoryDeploymentBackend(
            previous_release_sha=previous_release_sha,
            previous_workspace_ids=previous_workspace_ids,
        )
        self._lose_acknowledgement_once = lose_acknowledgement_once
        self.deploy_calls = 0

    def observe(
        self,
        *,
        commit_sha: str,
        include_genie: bool,
        idempotency_key: str,
        receipt_id: str | None = None,
        request_hash: str | None = None,
    ) -> DeploymentResult | None:
        """Read a matching completed deployment without issuing a mutation."""

        return self._backend.observe(
            commit_sha=commit_sha,
            include_genie=include_genie,
            idempotency_key=idempotency_key,
            receipt_id=receipt_id,
            request_hash=request_hash,
        )

    def ensure_deployed(
        self,
        *,
        commit_sha: str,
        include_genie: bool,
        idempotency_key: str,
        receipt_id: str | None = None,
        request_hash: str | None = None,
        lease_owner: str | None = None,
        lease_epoch: int | None = None,
    ) -> DeploymentResult:
        """Observe first, then perform at most one remote deployment."""

        observed = self.observe(
            commit_sha=commit_sha,
            include_genie=include_genie,
            idempotency_key=idempotency_key,
            receipt_id=receipt_id,
            request_hash=request_hash,
        )
        if observed is not None:
            return observed

        result, deployed = self._backend.ensure_deployed(
            commit_sha=commit_sha,
            include_genie=include_genie,
            idempotency_key=idempotency_key,
            receipt_id=receipt_id,
            request_hash=request_hash,
            lease_owner=lease_owner,
            lease_epoch=lease_epoch,
        )
        if deployed:
            self.deploy_calls += 1
            if self._lose_acknowledgement_once:
                self._lose_acknowledgement_once = False
                raise DeploymentAcknowledgementLost(
                    f"deployment {idempotency_key} succeeded remotely before acknowledgement"
                )
        return result

    def deploy(
        self,
        *,
        commit_sha: str,
        include_genie: bool,
        idempotency_key: str,
        receipt_id: str | None = None,
        request_hash: str | None = None,
    ) -> DeploymentResult:
        """Compatibility wrapper for callers using the original API."""

        return self.ensure_deployed(
            commit_sha=commit_sha,
            include_genie=include_genie,
            idempotency_key=idempotency_key,
            receipt_id=receipt_id,
            request_hash=request_hash,
        )


def _workspace_id(kind: str, commit_sha: str) -> str:
    suffix = hashlib.sha256(f"{kind}:{commit_sha}".encode()).hexdigest()[:16]
    return f"{kind}-{suffix}"
