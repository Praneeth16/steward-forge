"""Fenced leases, durable checkpoints, and reversible worker recovery."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

from broker.contracts import MutationReceipt, MutationRequest
from broker.service import BrokerDenied
from ledger import Ledger
from recovery.layers import RevocationLayer
from recovery.models import (
    CheckpointRecord,
    KillResult,
    ResumeResult,
    TransitionResult,
    WorkerLease,
)


class RecoveryError(ValueError):
    """A recovery operation violated durable workflow state."""


class LeaseRejected(RecoveryError):
    """An owner or epoch no longer holds the active lease."""


class LayerVerificationError(RecoveryError):
    """A kill-switch layer did not reach its required observed state."""


def _utc_now() -> datetime:
    return datetime.now(UTC)


class RecoveryController:
    """Own recovery transitions inside each row-locked workflow record.

    Kill checkpoints are persisted synchronously before any access-layer call.
    Each record carries a five-second operator deadline; layer revocation can
    take longer without losing the checkpoint or evidence.
    """

    KILL_CHECKPOINT_BOUND_SECONDS = 5
    REQUIRED_LAYERS = frozenset({"gateway_access", "uc_grants", "credentials"})

    def __init__(
        self,
        ledger: Ledger,
        *,
        layers: Mapping[str, RevocationLayer],
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        if set(layers) != self.REQUIRED_LAYERS:
            raise RecoveryError(
                "kill switch requires exactly the three required layers: "
                "gateway_access, uc_grants, credentials"
            )
        self._ledger = ledger
        self._layers = dict(layers)
        self._clock = clock

    def claim(
        self,
        brief_id: str,
        worker_id: str,
        owner: str,
        *,
        lease_seconds: int,
    ) -> WorkerLease:
        self._require_positive_ttl(lease_seconds)
        now = self._now()
        with self._ledger.transaction(brief_id) as state:
            worker = self._worker(state, worker_id)
            existing = self._lease(worker)
            if worker["status"] == "checkpointed":
                raise LeaseRejected("worker access is revoked; restore it before claiming")
            if existing is not None and existing.expires_at > now:
                if existing.owner == owner:
                    return existing
                raise LeaseRejected("worker already has an active lease")
            lease = self._new_lease(
                worker,
                worker_id,
                owner,
                lease_seconds,
                now,
            )
            worker["status"] = "running"
            self._event(
                state,
                "worker.claimed",
                worker_id=worker_id,
                owner=owner,
                epoch=lease.epoch,
            )
            return lease

    def heartbeat(
        self,
        brief_id: str,
        worker_id: str,
        owner: str,
        epoch: int,
        *,
        lease_seconds: int,
    ) -> WorkerLease:
        self._require_positive_ttl(lease_seconds)
        now = self._now()
        with self._ledger.transaction(brief_id) as state:
            worker = self._worker(state, worker_id)
            self._assert_lease(worker, owner, epoch, now)
            lease = WorkerLease(
                worker_id=worker_id,
                owner=owner,
                heartbeat_at=now,
                expires_at=now + timedelta(seconds=lease_seconds),
                epoch=epoch,
            )
            worker["lease"] = lease.model_dump(mode="json")
            self._event(
                state,
                "worker.heartbeat",
                worker_id=worker_id,
                owner=owner,
                epoch=epoch,
            )
            return lease

    def write_worker_state(
        self,
        brief_id: str,
        worker_id: str,
        owner: str,
        epoch: int,
        updates: Mapping[str, Any],
    ) -> dict[str, Any]:
        now = self._now()
        with self._ledger.transaction(brief_id) as state:
            worker = self._worker(state, worker_id)
            self._assert_lease(worker, owner, epoch, now)
            worker["worker_state"].update(deepcopy(dict(updates)))
            self._event(
                state,
                "worker.state-written",
                worker_id=worker_id,
                epoch=epoch,
            )
            return deepcopy(worker["worker_state"])

    def checkpoint(
        self,
        brief_id: str,
        worker_id: str,
        owner: str,
        epoch: int,
        *,
        checkpoint_id: str,
        payload: Mapping[str, Any],
    ) -> CheckpointRecord:
        now = self._now()
        with self._ledger.transaction(brief_id) as state:
            worker = self._worker(state, worker_id)
            self._assert_lease(worker, owner, epoch, now)
            checkpoint = self._persist_checkpoint(
                worker,
                checkpoint_id=checkpoint_id,
                worker_id=worker_id,
                epoch=epoch,
                payload=payload,
                now=now,
                deadline=now,
                reason="worker",
            )
            self._event(
                state,
                "worker.checkpointed",
                worker_id=worker_id,
                epoch=epoch,
                checkpoint_id=checkpoint_id,
            )
            return checkpoint

    def transition(
        self,
        brief_id: str,
        worker_id: str,
        owner: str,
        epoch: int,
        *,
        transition_id: str,
        expected_step: str,
        next_step: str,
        worker_state_updates: Mapping[str, Any] | None = None,
        commit_binding: Mapping[str, Any] | None = None,
        on_commit: Callable[[dict[str, Any]], None] | None = None,
        release_lease: bool = False,
    ) -> TransitionResult:
        now = self._now()
        requested = {
            "lease_epoch": epoch,
            "expected_step": expected_step,
            "next_step": next_step,
            "worker_state_sha256": self._binding_hash(worker_state_updates),
            "commit_binding_sha256": self._binding_hash(commit_binding),
            "release_lease": release_lease,
        }
        with self._ledger.transaction(brief_id) as state:
            worker = self._worker(state, worker_id)
            self._assert_lease(worker, owner, epoch, now)
            existing = worker["transitions"].get(transition_id)
            if existing is not None:
                if existing != requested:
                    raise RecoveryError(
                        "transition ID is already bound to different content"
                    )
                return TransitionResult(
                    transition_id=transition_id,
                    committed=False,
                    step=str(worker["workflow_step"]),
                )
            if worker["workflow_step"] != expected_step:
                raise RecoveryError(
                    f"workflow step is {worker['workflow_step']!r}, not {expected_step!r}"
                )
            worker["workflow_step"] = next_step
            worker["transitions"][transition_id] = requested
            if worker_state_updates is not None:
                worker["worker_state"].update(
                    deepcopy(dict(worker_state_updates))
                )
            if on_commit is not None:
                on_commit(state)
            if release_lease:
                worker["lease"] = None
                worker["status"] = "idle"
            self._event(
                state,
                "worker.transitioned",
                worker_id=worker_id,
                epoch=epoch,
                transition_id=transition_id,
                step=next_step,
            )
            return TransitionResult(
                transition_id=transition_id,
                committed=True,
                step=next_step,
            )

    @contextmanager
    def lease_fence(self, request: MutationRequest) -> Iterator[None]:
        """Hold the durable row fence for the complete broker execution."""

        if (
            request.workflow_id is None
            or request.lease_owner is None
            or request.lease_epoch is None
        ):
            raise BrokerDenied("lease-bound broker fields are required")
        try:
            with self.worker_fence(
                request.workflow_id,
                request.worker_id,
                request.lease_owner,
                request.lease_epoch,
            ):
                yield
        except LeaseRejected as error:
            raise BrokerDenied(str(error)) from error

    @contextmanager
    def worker_fence(
        self,
        brief_id: str,
        worker_id: str,
        owner: str,
        epoch: int,
    ) -> Iterator[None]:
        """Hold the durable workflow row while one governed mutation executes."""

        with self._ledger.transaction(brief_id) as state:
            worker = self._worker(state, worker_id)
            lease = self._assert_lease(worker, owner, epoch, self._now())
            lease_seconds = int(
                (lease.expires_at - lease.heartbeat_at).total_seconds()
            )
            if lease_seconds <= 0:
                raise LeaseRejected("active lease has no renewable duration")
            yield
            renewed_at = self._now()
            current = self._lease(worker)
            if (
                worker["status"] != "running"
                or current is None
                or current.owner != owner
                or current.epoch != epoch
                or int(worker["epoch"]) != epoch
            ):
                raise LeaseRejected("stale lease owner or epoch was rejected")
            renewed = WorkerLease(
                worker_id=worker_id,
                owner=owner,
                heartbeat_at=renewed_at,
                expires_at=renewed_at + timedelta(seconds=lease_seconds),
                epoch=epoch,
            )
            worker["lease"] = renewed.model_dump(mode="json")
            self._event(
                state,
                "worker.fence-renewed",
                worker_id=worker_id,
                owner=owner,
                epoch=epoch,
            )

    def validate_receipt(self, brief_id: str, receipt: MutationReceipt) -> None:
        if (
            receipt.workflow_id != brief_id
            or receipt.lease_owner is None
            or receipt.lease_epoch is None
        ):
            raise LeaseRejected("receipt is not bound to this workflow lease")
        with self._ledger.transaction(brief_id) as state:
            worker = self._worker(state, receipt.worker_id)
            try:
                self._assert_lease(
                    worker,
                    receipt.lease_owner,
                    receipt.lease_epoch,
                    self._now(),
                )
            except LeaseRejected as error:
                raise LeaseRejected("stale receipt was rejected") from error

    def kill(
        self,
        brief_id: str,
        worker_id: str,
        *,
        operation_id: str,
        checkpoint_payload: Mapping[str, Any],
    ) -> KillResult:
        now = self._now()
        checkpoint_id = self._operation_checkpoint_id("kill", operation_id)
        with self._ledger.transaction(brief_id) as state:
            worker = self._worker(state, worker_id)
            existing = worker.get("kill")
            if existing is not None and existing["operation_id"] != operation_id:
                if worker["status"] == "checkpointed":
                    raise RecoveryError(
                        "worker is already bound to a different kill operation"
                    )
                worker["kill_history"][existing["operation_id"]] = existing
                if worker["restore"] is not None:
                    restore = worker["restore"]
                    worker["restore_history"][restore["operation_id"]] = restore
                worker["kill"] = None
                worker["restore"] = None
                existing = None
            if existing is not None and worker.get("restore") is not None:
                raise RecoveryError("kill operation cannot be replayed after restore begins")
            if existing is not None and worker["status"] != "checkpointed":
                raise RecoveryError("completed kill operation cannot be replayed after restore")
            if existing is not None:
                checkpoint = CheckpointRecord.model_validate(
                    worker["checkpoints"][existing["checkpoint_id"]]
                )
                if checkpoint.payload != dict(checkpoint_payload):
                    raise RecoveryError(
                        "kill operation is already bound to different content"
                    )
            if existing is None:
                epoch = int(worker["epoch"]) + 1
                worker["epoch"] = epoch
                worker["lease"] = None
                worker["status"] = "checkpointed"
                checkpoint = self._persist_checkpoint(
                    worker,
                    checkpoint_id=checkpoint_id,
                    worker_id=worker_id,
                    epoch=epoch,
                    payload=checkpoint_payload,
                    now=now,
                    deadline=now
                    + timedelta(seconds=self.KILL_CHECKPOINT_BOUND_SECONDS),
                    reason="kill",
                )
                worker["kill"] = {
                    "operation_id": operation_id,
                    "requested_at": now.isoformat(),
                    "checkpoint_id": checkpoint.checkpoint_id,
                    "layers": {
                        name: {"desired": "revoked", "verified": False}
                        for name in sorted(self.REQUIRED_LAYERS)
                    },
                }
                self._event(
                    state,
                    "worker.kill-requested",
                    worker_id=worker_id,
                    epoch=epoch,
                    operation_id=operation_id,
                    checkpoint_id=checkpoint.checkpoint_id,
                )

        verified = self._apply_layers(
            brief_id,
            worker_id,
            operation_kind="kill",
            operation_id=operation_id,
        )
        return KillResult(
            operation_id=operation_id,
            status="checkpointed",
            checkpoint=checkpoint,
            revoked_layers=verified,
        )

    def restore(
        self,
        brief_id: str,
        worker_id: str,
        *,
        operation_id: str,
        new_owner: str,
        lease_seconds: int,
    ) -> ResumeResult:
        self._require_positive_ttl(lease_seconds)
        with self._ledger.transaction(brief_id) as state:
            worker = self._worker(state, worker_id)
            requested = {
                "operation_id": operation_id,
                "new_owner": new_owner,
                "lease_seconds": lease_seconds,
            }
            existing_restore = worker.get("restore")
            if existing_restore is not None and existing_restore != requested:
                raise RecoveryError("restore is already bound to different content")
            prior = worker["recoveries"].get(operation_id)
            if prior is not None:
                return ResumeResult.model_validate(prior)
            if worker["status"] != "checkpointed" or worker.get("kill") is None:
                raise RecoveryError("worker is not checkpointed by a kill operation")
            if any(
                not details["verified"]
                for details in worker["kill"]["layers"].values()
            ):
                raise RecoveryError("kill layers are not fully verified")
            if existing_restore is None:
                worker["restore"] = requested
                for layer in worker["kill"]["layers"].values():
                    layer["desired"] = "active"
                    layer["verified"] = False
                self._event(
                    state,
                    "worker.restore-requested",
                    worker_id=worker_id,
                    operation_id=operation_id,
                )

        self._apply_layers(
            brief_id,
            worker_id,
            operation_kind="restore",
            operation_id=operation_id,
        )
        with self._ledger.transaction(brief_id) as state:
            worker = self._worker(state, worker_id)
            prior = worker["recoveries"].get(operation_id)
            if prior is not None:
                return ResumeResult.model_validate(prior)
            if any(
                not details["verified"]
                for details in worker["kill"]["layers"].values()
            ):
                raise LayerVerificationError("not all access layers were restored")
            restored_at = self._now()
            checkpoint = self._resume_latest_checkpoint(
                worker, operation_id, restored_at
            )
            lease = self._new_lease(
                worker,
                worker_id,
                new_owner,
                lease_seconds,
                restored_at,
            )
            result = ResumeResult(
                recovery_id=operation_id,
                lease=lease,
                checkpoint=checkpoint,
            )
            worker["status"] = "running"
            worker["recoveries"][operation_id] = result.model_dump(mode="json")
            self._event(
                state,
                "worker.restored",
                worker_id=worker_id,
                epoch=lease.epoch,
                operation_id=operation_id,
                checkpoint_id=checkpoint.checkpoint_id,
            )
            return result

    def resume_expired(
        self,
        brief_id: str,
        worker_id: str,
        *,
        recovery_id: str,
        new_owner: str,
        lease_seconds: int,
    ) -> ResumeResult:
        self._require_positive_ttl(lease_seconds)
        now = self._now()
        with self._ledger.transaction(brief_id) as state:
            worker = self._worker(state, worker_id)
            prior = worker["recoveries"].get(recovery_id)
            if prior is not None:
                previous = ResumeResult.model_validate(prior)
                previous_ttl = int(
                    (
                        previous.lease.expires_at - previous.lease.heartbeat_at
                    ).total_seconds()
                )
                if previous.lease.owner != new_owner or previous_ttl != lease_seconds:
                    raise RecoveryError(
                        "recovery ID is bound to a different owner or lease duration"
                    )
                return previous
            lease = self._lease(worker)
            if lease is not None and lease.expires_at > now:
                raise LeaseRejected("worker still has an active lease")
            if worker["status"] == "checkpointed":
                raise LeaseRejected("killed worker must use restore")
            checkpoint = self._resume_latest_checkpoint(worker, recovery_id, now)
            new_lease = self._new_lease(
                worker,
                worker_id,
                new_owner,
                lease_seconds,
                now,
            )
            result = ResumeResult(
                recovery_id=recovery_id,
                lease=new_lease,
                checkpoint=checkpoint,
            )
            worker["recoveries"][recovery_id] = result.model_dump(mode="json")
            self._event(
                state,
                "worker.recovered",
                worker_id=worker_id,
                epoch=new_lease.epoch,
                recovery_id=recovery_id,
                checkpoint_id=checkpoint.checkpoint_id,
            )
            return result

    def _apply_layers(
        self,
        brief_id: str,
        worker_id: str,
        *,
        operation_kind: Literal["kill", "restore"],
        operation_id: str,
    ) -> frozenset[str]:
        revoked = operation_kind == "kill"
        for name in sorted(self.REQUIRED_LAYERS):
            layer = self._layers[name]
            operation_error: Exception | None = None
            try:
                observed = layer.is_revoked(worker_id)
                if observed != revoked:
                    if revoked:
                        layer.revoke(worker_id)
                    else:
                        layer.restore(worker_id)
                    observed = layer.is_revoked(worker_id)
            except Exception as error:  # observed state decides a lost acknowledgement
                operation_error = error
                try:
                    observed = layer.is_revoked(worker_id)
                except Exception as verification_error:
                    raise LayerVerificationError(
                        f"could not verify {name} after {operation_kind}"
                    ) from verification_error
            if observed != revoked:
                message = f"{name} did not reach required {operation_kind} state"
                raise LayerVerificationError(message) from operation_error
            self._mark_layer_verified(
                brief_id,
                worker_id,
                operation_kind=operation_kind,
                operation_id=operation_id,
                layer_name=name,
            )
        return self.REQUIRED_LAYERS

    def _mark_layer_verified(
        self,
        brief_id: str,
        worker_id: str,
        *,
        operation_kind: Literal["kill", "restore"],
        operation_id: str,
        layer_name: str,
    ) -> None:
        with self._ledger.transaction(brief_id) as state:
            worker = self._worker(state, worker_id)
            operation = worker.get(operation_kind)
            if operation is None or operation["operation_id"] != operation_id:
                raise RecoveryError(f"{operation_kind} operation changed during execution")
            details = worker["kill"]["layers"][layer_name]
            desired = "revoked" if operation_kind == "kill" else "active"
            if details["desired"] != desired:
                raise RecoveryError("access-layer intent changed during execution")
            if details["verified"]:
                return
            details["verified"] = True
            self._event(
                state,
                f"worker.{operation_kind}-layer-verified",
                worker_id=worker_id,
                operation_id=operation_id,
                layer=layer_name,
            )

    def _resume_latest_checkpoint(
        self, worker: dict[str, Any], recovery_id: str, now: datetime
    ) -> CheckpointRecord:
        checkpoint_id = worker.get("latest_checkpoint_id")
        if checkpoint_id is None:
            raise RecoveryError("worker has no checkpoint to resume")
        checkpoint = CheckpointRecord.model_validate(
            worker["checkpoints"][checkpoint_id]
        )
        if checkpoint.resume_id is not None and checkpoint.resume_id != recovery_id:
            raise RecoveryError("checkpoint has already been resumed")
        if checkpoint.resume_id is None:
            checkpoint = checkpoint.model_copy(
                update={
                    "resumed_at": now,
                    "resume_id": recovery_id,
                    "resume_count": 1,
                }
            )
            worker["checkpoints"][checkpoint_id] = checkpoint.model_dump(mode="json")
        return checkpoint

    @staticmethod
    def _new_lease(
        worker: dict[str, Any],
        worker_id: str,
        owner: str,
        lease_seconds: int,
        now: datetime,
    ) -> WorkerLease:
        lease = WorkerLease(
            worker_id=worker_id,
            owner=owner,
            heartbeat_at=now,
            expires_at=now + timedelta(seconds=lease_seconds),
            epoch=int(worker["epoch"]) + 1,
        )
        worker["epoch"] = lease.epoch
        worker["lease"] = lease.model_dump(mode="json")
        return lease

    @staticmethod
    def _persist_checkpoint(
        worker: dict[str, Any],
        *,
        checkpoint_id: str,
        worker_id: str,
        epoch: int,
        payload: Mapping[str, Any],
        now: datetime,
        deadline: datetime,
        reason: str,
    ) -> CheckpointRecord:
        payload_copy = deepcopy(dict(payload))
        existing = worker["checkpoints"].get(checkpoint_id)
        if existing is not None:
            checkpoint = CheckpointRecord.model_validate(existing)
            existing_binding = (
                checkpoint.worker_id,
                checkpoint.lease_epoch,
                checkpoint.payload,
                checkpoint.reason,
            )
            requested_binding = (worker_id, epoch, payload_copy, reason)
            if existing_binding != requested_binding:
                raise RecoveryError(
                    "checkpoint ID is already bound to different content"
                )
            return checkpoint

        requested = CheckpointRecord(
            checkpoint_id=checkpoint_id,
            worker_id=worker_id,
            lease_epoch=epoch,
            created_at=now,
            deadline_at=deadline,
            payload=payload_copy,
            reason=reason,
        )
        worker["checkpoints"][checkpoint_id] = requested.model_dump(mode="json")
        worker["latest_checkpoint_id"] = checkpoint_id
        return requested

    @staticmethod
    def _worker(state: dict[str, Any], worker_id: str) -> dict[str, Any]:
        recovery = state.setdefault("recovery", {"workers": {}})
        workers = recovery.setdefault("workers", {})
        return workers.setdefault(
            worker_id,
            {
                "epoch": 0,
                "lease": None,
                "status": "idle",
                "workflow_step": "planned",
                "worker_state": {},
                "checkpoints": {},
                "latest_checkpoint_id": None,
                "transitions": {},
                "recoveries": {},
                "kill": None,
                "restore": None,
                "kill_history": {},
                "restore_history": {},
            },
        )

    @staticmethod
    def _lease(worker: Mapping[str, Any]) -> WorkerLease | None:
        value = worker.get("lease")
        return None if value is None else WorkerLease.model_validate(value)

    @classmethod
    def _assert_lease(
        cls,
        worker: Mapping[str, Any],
        owner: str,
        epoch: int,
        now: datetime,
    ) -> WorkerLease:
        lease = cls._lease(worker)
        if (
            worker["status"] != "running"
            or lease is None
            or lease.owner != owner
            or lease.epoch != epoch
            or int(worker["epoch"]) != epoch
            or lease.expires_at <= now
        ):
            raise LeaseRejected("stale lease owner or epoch was rejected")
        return lease

    def _now(self) -> datetime:
        now = self._clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise RecoveryError("recovery clock must return a timezone-aware value")
        return now.astimezone(UTC)

    @staticmethod
    def _require_positive_ttl(lease_seconds: int) -> None:
        if lease_seconds <= 0:
            raise RecoveryError("lease_seconds must be positive")

    @staticmethod
    def _operation_checkpoint_id(kind: str, operation_id: str) -> str:
        digest = hashlib.sha256(f"{kind}:{operation_id}".encode()).hexdigest()[:20]
        return f"{kind}-{digest}"

    @staticmethod
    def _binding_hash(value: Mapping[str, Any] | None) -> str | None:
        if value is None:
            return None
        canonical = json.dumps(
            dict(value), sort_keys=True, separators=(",", ":"), ensure_ascii=True
        )
        return hashlib.sha256(canonical.encode()).hexdigest()

    @staticmethod
    def _event(state: dict[str, Any], event_type: str, **details: Any) -> None:
        state.setdefault("events", []).append({"type": event_type, **details})
