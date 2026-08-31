from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from broker.contracts import MutationRequest, SandboxWriteArgs, WorkerContract
from broker.service import ArtifactPolicy, BrokerDenied, CapabilityBroker, ToolSpec
from broker.zero_ops import HealthSnapshot, ZeroOpsPreAct
from ledger.store import InMemoryLedger
from recovery import (
    InMemoryRevocationLayer,
    LeaseRejected,
    RecoveryController,
    RecoveryError,
)


class Clock:
    def __init__(self) -> None:
        self.now = datetime(2026, 8, 31, 9, 0, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: int) -> None:
        self.now += timedelta(seconds=seconds)


class LostAcknowledgementLayer(InMemoryRevocationLayer):
    """Apply one operation, then simulate losing its acknowledgement."""

    def __init__(self, name: str, *, lose_on: str) -> None:
        super().__init__(name)
        self._lose_on = lose_on
        self._lost = False

    def revoke(self, worker_id: str) -> None:
        super().revoke(worker_id)
        if self._lose_on == "revoke" and not self._lost:
            self._lost = True
            raise TimeoutError("revoke acknowledgement was lost")

    def restore(self, worker_id: str) -> None:
        super().restore(worker_id)
        if self._lose_on == "restore" and not self._lost:
            self._lost = True
            raise TimeoutError("restore acknowledgement was lost")


class FailOnceLayer(InMemoryRevocationLayer):
    """Fail before changing state so a restarted controller must reconcile."""

    def __init__(self, name: str) -> None:
        super().__init__(name)
        self._failed = False

    def revoke(self, worker_id: str) -> None:
        if not self._failed:
            self._failed = True
            raise TimeoutError("control plane unavailable")
        super().revoke(worker_id)


class FailOnceRestoreLayer(InMemoryRevocationLayer):
    """Fail one restore before changing state to leave durable restore intent."""

    def __init__(self, name: str) -> None:
        super().__init__(name)
        self._failed = False

    def restore(self, worker_id: str) -> None:
        if not self._failed:
            self._failed = True
            raise TimeoutError("control plane unavailable")
        super().restore(worker_id)


def _workflow() -> dict[str, Any]:
    return {
        "id": "brief-1",
        "status": "in_progress",
        "events": [{"type": "brief.submitted"}],
        "mutation_receipts": [{"receipt_id": "receipt-before-kill"}],
    }


def _layers(**overrides: InMemoryRevocationLayer) -> dict[str, InMemoryRevocationLayer]:
    layers = {
        "gateway_access": InMemoryRevocationLayer("gateway_access"),
        "uc_grants": InMemoryRevocationLayer("uc_grants"),
        "credentials": InMemoryRevocationLayer("credentials"),
    }
    layers.update(overrides)
    return layers


def _controller(
    *,
    clock: Clock | None = None,
    ledger: InMemoryLedger | None = None,
    layers: dict[str, InMemoryRevocationLayer] | None = None,
) -> tuple[RecoveryController, InMemoryLedger, Clock]:
    actual_ledger = ledger if ledger is not None else InMemoryLedger()
    try:
        actual_ledger.get("brief-1")
    except KeyError:
        actual_ledger.create("submission-1", _workflow())
    actual_clock = clock or Clock()
    return (
        RecoveryController(
            actual_ledger,
            layers=layers if layers is not None else _layers(),
            clock=actual_clock,
        ),
        actual_ledger,
        actual_clock,
    )


def _healthy() -> HealthSnapshot:
    return HealthSnapshot(
        lakebase_available=True,
        lakebase_fresh=True,
        pipeline_fresh=True,
        unity_catalog_fresh=True,
    )


def _broker(
    controller: RecoveryController, writes: list[dict[str, Any]]
) -> CapabilityBroker:
    def write(arguments: SandboxWriteArgs) -> dict[str, Any]:
        writes.append(arguments.model_dump(mode="json"))
        return {"rows_written": len(arguments.rows)}

    return CapabilityBroker(
        contracts=[
            WorkerContract(
                contract_id="worker-contract",
                contract_version=1,
                worker_id="data-engineer",
                allowed_tools={"sandbox.write"},
                sandbox_catalog="steward",
                sandbox_schema="sandbox",
            )
        ],
        tools={
            "sandbox.write": ToolSpec(
                arguments_model=SandboxWriteArgs,
                category="mutation",
                executor=write,
            )
        },
        pre_act=ZeroOpsPreAct(_healthy),
        artifact_policy=ArtifactPolicy(),
        lease_fence=controller.lease_fence,
    )


def _request(owner: str, epoch: int, *, key: str = "write-1") -> MutationRequest:
    return MutationRequest(
        contract_id="worker-contract",
        contract_version=1,
        worker_id="data-engineer",
        workflow_id="brief-1",
        lease_owner=owner,
        lease_epoch=epoch,
        tool_id="sandbox.write",
        arguments={
            "catalog": "steward",
            "schema": "sandbox",
            "table": "output",
            "rows": [{"signal": "green"}],
        },
        idempotency_key=key,
    )


def test_claims_bind_owner_expiry_heartbeat_and_monotonic_epoch() -> None:
    controller, _, clock = _controller()

    first = controller.claim("brief-1", "data-engineer", "process-a", lease_seconds=30)
    assert first.owner == "process-a"
    assert first.epoch == 1
    assert first.heartbeat_at == clock.now
    assert first.expires_at == clock.now + timedelta(seconds=30)

    clock.advance(10)
    heartbeat = controller.heartbeat(
        "brief-1", "data-engineer", "process-a", first.epoch, lease_seconds=30
    )
    assert heartbeat.heartbeat_at == clock.now
    assert heartbeat.expires_at == clock.now + timedelta(seconds=30)

    with pytest.raises(LeaseRejected, match="active lease"):
        controller.claim("brief-1", "data-engineer", "process-b", lease_seconds=30)

    clock.advance(31)
    second = controller.claim("brief-1", "data-engineer", "process-b", lease_seconds=30)
    assert second.owner == "process-b"
    assert second.epoch == first.epoch + 1


def test_in_memory_transaction_does_not_leak_committed_state_reference() -> None:
    ledger = InMemoryLedger()
    ledger.create("submission-1", _workflow())

    with ledger.transaction("brief-1") as state:
        state["status"] = "checkpointed"
    state["status"] = "tampered-after-commit"

    assert ledger.get("brief-1")["status"] == "checkpointed"


def test_reassignment_fences_stale_state_checkpoint_tool_and_receipt() -> None:
    controller, _, clock = _controller()
    stale = controller.claim("brief-1", "data-engineer", "process-a", lease_seconds=5)
    writes: list[dict[str, Any]] = []
    broker = _broker(controller, writes)
    receipt = broker.execute(_request(stale.owner, stale.epoch))
    controller.validate_receipt("brief-1", receipt)

    clock.advance(6)
    current = controller.claim("brief-1", "data-engineer", "process-b", lease_seconds=30)

    with pytest.raises(LeaseRejected, match="stale lease"):
        controller.write_worker_state(
            "brief-1", "data-engineer", stale.owner, stale.epoch, {"row": 2}
        )
    with pytest.raises(LeaseRejected, match="stale lease"):
        controller.checkpoint(
            "brief-1",
            "data-engineer",
            stale.owner,
            stale.epoch,
            checkpoint_id="stale-checkpoint",
            payload={"step": "write"},
        )
    with pytest.raises(BrokerDenied, match="stale lease"):
        broker.execute(_request(stale.owner, stale.epoch, key="stale-write"))
    with pytest.raises(LeaseRejected, match="stale receipt"):
        controller.validate_receipt("brief-1", receipt)

    assert len(writes) == 1
    assert current.epoch == 2


def test_successful_worker_fence_renews_a_lease_that_expires_during_mutation() -> None:
    controller, ledger, clock = _controller()
    lease = controller.claim(
        "brief-1", "data-engineer", "process-a", lease_seconds=5
    )

    with controller.worker_fence(
        "brief-1", "data-engineer", lease.owner, lease.epoch
    ):
        clock.advance(6)

    stored = ledger.get("brief-1")["recovery"]["workers"]["data-engineer"]
    renewed = stored["lease"]
    assert renewed["owner"] == lease.owner
    assert renewed["epoch"] == lease.epoch
    assert datetime.fromisoformat(renewed["heartbeat_at"]) == clock.now
    assert datetime.fromisoformat(renewed["expires_at"]) == (
        clock.now + timedelta(seconds=5)
    )
    controller.write_worker_state(
        "brief-1", "data-engineer", lease.owner, lease.epoch, {"committed": True}
    )


def test_kill_waits_for_an_inflight_fenced_mutation_then_advances_epoch() -> None:
    controller, _, _ = _controller()
    lease = controller.claim(
        "brief-1", "data-engineer", "process-a", lease_seconds=30
    )
    mutation_entered = threading.Event()
    release_mutation = threading.Event()
    kill_started = threading.Event()
    writes: list[dict[str, Any]] = []

    def write(arguments: SandboxWriteArgs) -> dict[str, Any]:
        mutation_entered.set()
        assert release_mutation.wait(timeout=2)
        writes.append(arguments.model_dump(mode="json"))
        return {"rows_written": len(arguments.rows)}

    broker = CapabilityBroker(
        contracts=[
            WorkerContract(
                contract_id="worker-contract",
                contract_version=1,
                worker_id="data-engineer",
                allowed_tools={"sandbox.write"},
                sandbox_catalog="steward",
                sandbox_schema="sandbox",
            )
        ],
        tools={
            "sandbox.write": ToolSpec(
                arguments_model=SandboxWriteArgs,
                category="mutation",
                executor=write,
            )
        },
        pre_act=ZeroOpsPreAct(_healthy),
        artifact_policy=ArtifactPolicy(),
        lease_fence=controller.lease_fence,
    )

    def kill_worker():
        kill_started.set()
        return controller.kill(
            "brief-1",
            "data-engineer",
            operation_id="kill-during-mutation",
            checkpoint_payload={"reason": "operator stop"},
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        mutation = executor.submit(
            broker.execute,
            _request(lease.owner, lease.epoch, key="inflight-write"),
        )
        assert mutation_entered.wait(timeout=2)
        kill = executor.submit(kill_worker)
        assert kill_started.wait(timeout=2)
        assert not kill.done()
        release_mutation.set()
        receipt = mutation.result(timeout=2)
        kill.result(timeout=2)

    assert len(writes) == 1
    assert receipt.lease_owner == lease.owner
    assert receipt.lease_epoch == lease.epoch
    with pytest.raises(LeaseRejected, match="stale receipt"):
        controller.validate_receipt("brief-1", receipt)


def test_concurrent_transitions_commit_once_and_lost_ack_replays() -> None:
    controller, ledger, _ = _controller()
    lease = controller.claim("brief-1", "data-engineer", "process-a", lease_seconds=30)

    def transition() -> bool:
        return controller.transition(
            "brief-1",
            "data-engineer",
            lease.owner,
            lease.epoch,
            transition_id="task-completed",
            expected_step="planned",
            next_step="completed",
        ).committed

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: transition(), range(2)))

    assert sorted(results) == [False, True]
    worker = ledger.get("brief-1")["recovery"]["workers"]["data-engineer"]
    assert worker["workflow_step"] == "completed"
    assert list(worker["transitions"]) == ["task-completed"]


def test_transition_replay_rejects_different_worker_or_delivery_state() -> None:
    controller, _, _ = _controller()
    lease = controller.claim(
        "brief-1", "data-engineer", "process-a", lease_seconds=30
    )
    arguments = {
        "brief_id": "brief-1",
        "worker_id": "data-engineer",
        "owner": lease.owner,
        "epoch": lease.epoch,
        "transition_id": "bounded-task-completed",
        "expected_step": "planned",
        "next_step": "succeeded",
    }

    controller.transition(
        **arguments,
        worker_state_updates={"attempt_count": 1},
        commit_binding={"evidence_sequence": 8},
    )

    with pytest.raises(RecoveryError, match="different content"):
        controller.transition(
            **arguments,
            worker_state_updates={"attempt_count": 2},
            commit_binding={"evidence_sequence": 8},
        )
    with pytest.raises(RecoveryError, match="different content"):
        controller.transition(
            **arguments,
            worker_state_updates={"attempt_count": 1},
            commit_binding={"evidence_sequence": 9},
        )


def test_checkpoint_lost_ack_replay_returns_the_original_record() -> None:
    controller, _, clock = _controller()
    lease = controller.claim("brief-1", "data-engineer", "process-a", lease_seconds=30)
    first = controller.checkpoint(
        "brief-1",
        "data-engineer",
        lease.owner,
        lease.epoch,
        checkpoint_id="checkpoint-lost-ack",
        payload={"next_action": "write"},
    )

    clock.advance(1)
    replay = controller.checkpoint(
        "brief-1",
        "data-engineer",
        lease.owner,
        lease.epoch,
        checkpoint_id="checkpoint-lost-ack",
        payload={"next_action": "write"},
    )

    assert replay == first


def test_kill_checkpoints_within_bound_and_revokes_all_layers_without_data_loss() -> None:
    layers = _layers()
    controller, ledger, clock = _controller(layers=layers)
    lease = controller.claim("brief-1", "data-engineer", "process-a", lease_seconds=30)
    controller.write_worker_state(
        "brief-1", "data-engineer", lease.owner, lease.epoch, {"row": 18}
    )

    killed = controller.kill(
        "brief-1",
        "data-engineer",
        operation_id="kill-1",
        checkpoint_payload={"next_action": "quality-check"},
    )

    assert killed.status == "checkpointed"
    assert killed.checkpoint.created_at == clock.now
    assert killed.checkpoint.deadline_at == clock.now + timedelta(
        seconds=controller.KILL_CHECKPOINT_BOUND_SECONDS
    )
    assert killed.checkpoint.created_at <= killed.checkpoint.deadline_at
    assert all(layer.is_revoked("data-engineer") for layer in layers.values())
    state = ledger.get("brief-1")
    assert state["events"][0] == {"type": "brief.submitted"}
    assert state["mutation_receipts"] == [{"receipt_id": "receipt-before-kill"}]
    assert state["recovery"]["workers"]["data-engineer"]["worker_state"] == {"row": 18}


def test_restore_verifies_layers_resumes_checkpoint_once_and_keeps_receipts() -> None:
    layers = _layers()
    controller, ledger, _ = _controller(layers=layers)
    lease = controller.claim("brief-1", "data-engineer", "process-a", lease_seconds=30)
    controller.checkpoint(
        "brief-1",
        "data-engineer",
        lease.owner,
        lease.epoch,
        checkpoint_id="before-write",
        payload={"next_action": "write"},
    )
    controller.kill(
        "brief-1",
        "data-engineer",
        operation_id="kill-1",
        checkpoint_payload={"next_action": "write"},
    )

    restored = controller.restore(
        "brief-1",
        "data-engineer",
        operation_id="restore-1",
        new_owner="process-b",
        lease_seconds=30,
    )
    replay = controller.restore(
        "brief-1",
        "data-engineer",
        operation_id="restore-1",
        new_owner="process-b",
        lease_seconds=30,
    )

    assert restored == replay
    assert restored.checkpoint.payload == {"next_action": "write"}
    assert restored.checkpoint.resume_count == 1
    assert restored.lease.epoch > lease.epoch
    assert all(not layer.is_revoked("data-engineer") for layer in layers.values())
    assert ledger.get("brief-1")["mutation_receipts"] == [
        {"receipt_id": "receipt-before-kill"}
    ]


@pytest.mark.parametrize("lost_on", ["revoke", "restore"])
def test_lost_layer_acknowledgements_are_reconciled_by_observed_state(
    lost_on: str,
) -> None:
    layers = _layers(
        gateway_access=LostAcknowledgementLayer("gateway_access", lose_on=lost_on)
    )
    controller, _, _ = _controller(layers=layers)
    controller.claim("brief-1", "data-engineer", "process-a", lease_seconds=30)

    controller.kill(
        "brief-1",
        "data-engineer",
        operation_id="kill-1",
        checkpoint_payload={"next_action": "write"},
    )
    restored = controller.restore(
        "brief-1",
        "data-engineer",
        operation_id="restore-1",
        new_owner="process-b",
        lease_seconds=30,
    )

    assert restored.checkpoint.resume_count == 1


def test_restart_during_recovery_resumes_in_flight_workflow_once() -> None:
    ledger = InMemoryLedger()
    clock = Clock()
    layers = _layers()
    controller, _, _ = _controller(ledger=ledger, clock=clock, layers=layers)
    lease = controller.claim("brief-1", "data-engineer", "process-a", lease_seconds=5)
    controller.transition(
        "brief-1",
        "data-engineer",
        lease.owner,
        lease.epoch,
        transition_id="start-write",
        expected_step="planned",
        next_step="writing",
    )
    controller.checkpoint(
        "brief-1",
        "data-engineer",
        lease.owner,
        lease.epoch,
        checkpoint_id="in-flight-write",
        payload={"next_action": "write", "receipt_ids": ["receipt-before-kill"]},
    )
    clock.advance(6)

    restarted = RecoveryController(ledger, layers=layers, clock=clock)
    resumed = restarted.resume_expired(
        "brief-1",
        "data-engineer",
        recovery_id="restart-1",
        new_owner="process-b",
        lease_seconds=30,
    )
    replay = restarted.resume_expired(
        "brief-1",
        "data-engineer",
        recovery_id="restart-1",
        new_owner="process-b",
        lease_seconds=30,
    )
    completed = restarted.transition(
        "brief-1",
        "data-engineer",
        resumed.lease.owner,
        resumed.lease.epoch,
        transition_id="finish-write",
        expected_step="writing",
        next_step="completed",
    )
    lost_ack_replay = restarted.transition(
        "brief-1",
        "data-engineer",
        resumed.lease.owner,
        resumed.lease.epoch,
        transition_id="finish-write",
        expected_step="writing",
        next_step="completed",
    )

    assert resumed == replay
    assert resumed.checkpoint.resume_count == 1
    assert completed.committed is True
    assert lost_ack_replay.committed is False
    state = ledger.get("brief-1")
    assert state["mutation_receipts"] == [{"receipt_id": "receipt-before-kill"}]
    assert list(state["recovery"]["workers"]["data-engineer"]["recoveries"]) == [
        "restart-1"
    ]
    assert list(state["recovery"]["workers"]["data-engineer"]["transitions"]) == [
        "start-write",
        "finish-write",
    ]


def test_recovery_id_is_bound_to_owner_and_lease_duration() -> None:
    controller, _, clock = _controller()
    lease = controller.claim("brief-1", "data-engineer", "process-a", lease_seconds=5)
    controller.checkpoint(
        "brief-1",
        "data-engineer",
        lease.owner,
        lease.epoch,
        checkpoint_id="in-flight-write",
        payload={"next_action": "write"},
    )
    clock.advance(6)
    controller.resume_expired(
        "brief-1",
        "data-engineer",
        recovery_id="restart-1",
        new_owner="process-b",
        lease_seconds=30,
    )

    with pytest.raises(RecoveryError, match="different owner or lease duration"):
        controller.resume_expired(
            "brief-1",
            "data-engineer",
            recovery_id="restart-1",
            new_owner="process-c",
            lease_seconds=30,
        )
    with pytest.raises(RecoveryError, match="different owner or lease duration"):
        controller.resume_expired(
            "brief-1",
            "data-engineer",
            recovery_id="restart-1",
            new_owner="process-b",
            lease_seconds=60,
        )


def test_restart_during_partial_kill_reconciles_persisted_intent() -> None:
    ledger = InMemoryLedger()
    clock = Clock()
    layers = _layers(uc_grants=FailOnceLayer("uc_grants"))
    controller, _, _ = _controller(ledger=ledger, clock=clock, layers=layers)
    controller.claim("brief-1", "data-engineer", "process-a", lease_seconds=30)

    with pytest.raises(RecoveryError, match="did not reach required kill state"):
        controller.kill(
            "brief-1",
            "data-engineer",
            operation_id="kill-restart",
            checkpoint_payload={"next_action": "write"},
        )
    interrupted = ledger.get("brief-1")["recovery"]["workers"]["data-engineer"]
    assert interrupted["status"] == "checkpointed"
    assert interrupted["lease"] is None
    assert interrupted["kill"]["operation_id"] == "kill-restart"

    restarted = RecoveryController(ledger, layers=layers, clock=clock)
    completed = restarted.kill(
        "brief-1",
        "data-engineer",
        operation_id="kill-restart",
        checkpoint_payload={"next_action": "write"},
    )

    assert completed.revoked_layers == controller.REQUIRED_LAYERS
    assert all(layer.is_revoked("data-engineer") for layer in layers.values())


def test_restore_waits_until_every_kill_layer_is_verified() -> None:
    layers = _layers(uc_grants=FailOnceLayer("uc_grants"))
    controller, _, _ = _controller(layers=layers)
    controller.claim("brief-1", "data-engineer", "process-a", lease_seconds=30)
    with pytest.raises(RecoveryError, match="did not reach required kill state"):
        controller.kill(
            "brief-1",
            "data-engineer",
            operation_id="kill-in-flight",
            checkpoint_payload={"next_action": "write"},
        )

    with pytest.raises(RecoveryError, match="kill layers are not fully verified"):
        controller.restore(
            "brief-1",
            "data-engineer",
            operation_id="restore-too-early",
            new_owner="process-b",
            lease_seconds=30,
        )


def test_stale_kill_retry_cannot_revoke_a_restored_worker() -> None:
    layers = _layers()
    controller, _, _ = _controller(layers=layers)
    controller.claim("brief-1", "data-engineer", "process-a", lease_seconds=30)
    controller.kill(
        "brief-1",
        "data-engineer",
        operation_id="kill-1",
        checkpoint_payload={"next_action": "write"},
    )
    controller.restore(
        "brief-1",
        "data-engineer",
        operation_id="restore-1",
        new_owner="process-b",
        lease_seconds=30,
    )

    with pytest.raises(RecoveryError, match="cannot be replayed after restore"):
        controller.kill(
            "brief-1",
            "data-engineer",
            operation_id="kill-1",
            checkpoint_payload={"next_action": "write"},
        )

    assert all(not layer.is_revoked("data-engineer") for layer in layers.values())


def test_kill_retry_cannot_revoke_layers_after_partial_restore_begins() -> None:
    layers = _layers(gateway_access=FailOnceRestoreLayer("gateway_access"))
    controller, _, _ = _controller(layers=layers)
    controller.claim("brief-1", "data-engineer", "process-a", lease_seconds=30)
    controller.kill(
        "brief-1",
        "data-engineer",
        operation_id="kill-1",
        checkpoint_payload={"next_action": "write"},
    )

    with pytest.raises(RecoveryError, match="did not reach required restore state"):
        controller.restore(
            "brief-1",
            "data-engineer",
            operation_id="restore-1",
            new_owner="process-b",
            lease_seconds=30,
        )
    assert not layers["credentials"].is_revoked("data-engineer")

    with pytest.raises(RecoveryError, match="cannot be replayed after restore begins"):
        controller.kill(
            "brief-1",
            "data-engineer",
            operation_id="kill-1",
            checkpoint_payload={"next_action": "write"},
        )

    assert not layers["credentials"].is_revoked("data-engineer")


def test_worker_can_be_killed_and_restored_again_without_deleting_history() -> None:
    layers = _layers()
    controller, ledger, _ = _controller(layers=layers)
    controller.claim("brief-1", "data-engineer", "process-a", lease_seconds=30)
    controller.kill(
        "brief-1",
        "data-engineer",
        operation_id="kill-1",
        checkpoint_payload={"cycle": 1},
    )
    controller.restore(
        "brief-1",
        "data-engineer",
        operation_id="restore-1",
        new_owner="process-b",
        lease_seconds=30,
    )

    second_kill = controller.kill(
        "brief-1",
        "data-engineer",
        operation_id="kill-2",
        checkpoint_payload={"cycle": 2},
    )

    worker = ledger.get("brief-1")["recovery"]["workers"]["data-engineer"]
    assert second_kill.checkpoint.payload == {"cycle": 2}
    assert worker["kill_history"]["kill-1"]["operation_id"] == "kill-1"
    assert worker["restore_history"]["restore-1"]["operation_id"] == "restore-1"
    assert len(worker["checkpoints"]) == 2


def test_kill_switch_requires_exactly_three_named_layers() -> None:
    with pytest.raises(RecoveryError, match="three required layers"):
        _controller(layers={"gateway_access": InMemoryRevocationLayer("gateway_access")})
