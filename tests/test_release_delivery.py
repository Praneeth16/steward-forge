from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from gates.swe.release import SoftwareReleaseService
from identity import ActorContext
from ledger import InMemoryLedger
from orchestrator.delivery import (
    DeliveryCoordinator,
    DeliveryError,
    ExecutionLanes,
    ReferenceRunConfig,
)
from orchestrator.models import AcceptanceTest, BriefSubmission, ReleaseDecision, ScopeDecision
from pipeline import DataEngineeringPipeline
from recovery import InMemoryRevocationLayer, RecoveryController
from release_evidence import (
    InMemoryReleaseEvidencePointerStore,
    InMemoryReleaseEvidenceStore,
    ReleaseEvidenceConflict,
    ReleaseEvidenceNotFound,
    ReleaseEvidencePublisher,
    ReleaseIntent,
)
from workers.de import InMemoryCatalogAdapter
from workers.swe import (
    InMemoryArtifactRepository,
    InMemoryDeploymentAdapter,
    InMemoryDeploymentBackend,
)

BASE_SHA = "1" * 64


class Clock:
    def __init__(self) -> None:
        self.now = datetime(2026, 8, 31, 9, 0, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: int) -> None:
        self.now += timedelta(seconds=seconds)


class CrashAfterEnabledMutation(ExecutionLanes):
    def __init__(self) -> None:
        super().__init__()
        self.enabled = False

    def mutate(self, operation):
        result = super().mutate(operation)
        if self.enabled:
            self.enabled = False
            raise KeyboardInterrupt("release process stopped after remote deployment")
        return result


class DelayedVisibilityDeploymentAdapter(InMemoryDeploymentAdapter):
    def __init__(
        self,
        *,
        backend: InMemoryDeploymentBackend,
        hidden_observations: int,
        lose_acknowledgement_once: bool = True,
    ) -> None:
        super().__init__(
            backend=backend,
            lose_acknowledgement_once=lose_acknowledgement_once,
        )
        self._shared_backend = backend
        self.hidden_observations = hidden_observations
        self.observe_calls = 0

    def observe(self, **kwargs):
        self.observe_calls += 1
        if self._shared_backend.deploy_calls and self.hidden_observations:
            self.hidden_observations -= 1
            return None
        return super().observe(**kwargs)


class FailOnceDeploymentAdapter(InMemoryDeploymentAdapter):
    def __init__(self, *, backend: InMemoryDeploymentBackend) -> None:
        super().__init__(backend=backend)
        self.ensure_calls = 0

    def ensure_deployed(self, **kwargs):
        self.ensure_calls += 1
        if self.ensure_calls == 1:
            raise ConnectionError("deployment request failed before acknowledgement")
        return super().ensure_deployed(**kwargs)


class CrashAfterOneEvidenceWrite(ReleaseEvidencePublisher):
    def __init__(self, receipt_store, pointer_store, *, first_side: str) -> None:
        super().__init__(receipt_store, pointer_store)
        self._receipt_store_for_test = receipt_store
        self._pointer_store_for_test = pointer_store
        self._first_side = first_side
        self._crash_once = True

    def reconcile(self, receipt, *, receipt_location):
        if self._crash_once:
            self._crash_once = False
            if self._first_side == "receipt":
                self._receipt_store_for_test.insert_if_absent(receipt)
            else:
                pointer = self.pointer_for(
                    receipt,
                    receipt_location=receipt_location,
                )
                self._pointer_store_for_test.insert_if_absent(pointer)
            raise KeyboardInterrupt(f"crash after {self._first_side}-only insert")
        return super().reconcile(receipt, receipt_location=receipt_location)


def _layers() -> dict[str, InMemoryRevocationLayer]:
    return {name: InMemoryRevocationLayer(name) for name in RecoveryController.REQUIRED_LAYERS}


def _submission(idempotency_key: str) -> BriefSubmission:
    return BriefSubmission(
        title="Delivery health reference brief",
        business_question="Show delivery health and the work that needs attention.",
        acceptance_tests=[
            AcceptanceTest(
                name="governed-sources",
                description="Every signal uses governed sandbox tables.",
                kind="contract",
            )
        ],
        cost_ceiling_usd=4.0,
        release_approver="approver-1",
        viewer_subjects=["auditor-1"],
        idempotency_key=idempotency_key,
    )


def _config(run_id: str) -> ReferenceRunConfig:
    return ReferenceRunConfig(
        run_id=run_id,
        seed=2026,
        sandbox_catalog="demo_catalog",
        sandbox_schema="steward_forge_sandbox",
        trusted_base_sha=BASE_SHA,
        generated_prefix="generated/software-engineer",
        artifact_branch="steward-forge/candidates",
        dashboard_title="Engineering delivery signals",
    )


def _submitter() -> ActorContext:
    return ActorContext(subject="employee-1", roles={"submitter", "viewer"})


def _approver() -> ActorContext:
    return ActorContext(subject="approver-1", roles={"approver", "viewer"})


def _publisher():
    receipts = InMemoryReleaseEvidenceStore()
    pointers = InMemoryReleaseEvidencePointerStore()
    return ReleaseEvidencePublisher(receipts, pointers), receipts, pointers


def _coordinator(
    *,
    ledger: InMemoryLedger,
    repository: InMemoryArtifactRepository,
    deployer: InMemoryDeploymentAdapter,
    publisher: ReleaseEvidencePublisher,
    recovery: RecoveryController | None = None,
    lanes: ExecutionLanes | None = None,
    coordinator_id: str = "coordinator-a",
) -> DeliveryCoordinator:
    return DeliveryCoordinator(
        ledger=ledger,
        data_pipeline=DataEngineeringPipeline(InMemoryCatalogAdapter()),
        software_release=SoftwareReleaseService(repository, deployer),
        release_evidence_publisher=publisher,
        recovery=recovery,
        lanes=lanes,
        coordinator_id=coordinator_id,
    )


def _advance_to_release(
    coordinator: DeliveryCoordinator,
    *,
    idempotency_key: str,
    run_id: str,
):
    submitted = coordinator.submit(
        _submission(idempotency_key),
        config=_config(run_id),
        actor=_submitter(),
    )
    coordinator.decide_scope(
        submitted.workflow_id,
        ScopeDecision(
            decision_id=f"scope-{run_id}",
            decision="approved",
            scope_version=1,
        ),
        _approver(),
    )
    pending = coordinator.advance(submitted.workflow_id)
    assert pending.prepared_release_sha is not None
    return pending


def _release_decision(run_id: str, commit_sha: str) -> ReleaseDecision:
    return ReleaseDecision(
        decision_id=f"release-{run_id}",
        decision="approved",
        commit_sha=commit_sha,
    )


def test_delivery_persists_intent_before_deploy_and_returns_governed_receipt() -> None:
    ledger = InMemoryLedger()
    repository = InMemoryArtifactRepository(BASE_SHA)
    backend = InMemoryDeploymentBackend(previous_release_sha="0" * 64)
    publisher, receipts, pointers = _publisher()
    coordinator = _coordinator(
        ledger=ledger,
        repository=repository,
        deployer=InMemoryDeploymentAdapter(backend=backend),
        publisher=publisher,
    )
    pending = _advance_to_release(
        coordinator,
        idempotency_key="governed-release-01",
        run_id="governed-release",
    )

    completed = coordinator.decide_release(
        pending.workflow_id,
        _release_decision("governed-release", pending.prepared_release_sha),
        _approver(),
    )

    receipt = completed.governed_release_receipt
    pointer = completed.release_evidence_pointer
    assert completed.status == "completed"
    assert receipt is not None
    assert pointer is not None
    assert completed.software_receipt is not None
    assert receipt.receipt_id == pointer.receipt_id == completed.software_receipt.receipt_id
    assert receipt.brief_id == pending.workflow_id
    assert receipt.code_sha256 == pending.prepared_release_sha
    assert receipt.data_receipt_id == completed.data_receipt.receipt_id
    assert receipt.scope_approval_id == "scope-governed-release"
    assert receipt.release_approval_id == "release-governed-release"
    assert receipt.model_usage_status == "not_used"
    assert set(receipt.gate_results.values()) == {"passed"}
    assert receipt.evidence_chain_reference == pointer.evidence_chain_reference
    assert receipts.get(receipt.receipt_id) == receipt
    assert pointers.get(receipt.receipt_id) == pointer
    assert backend.deploy_calls == 1

    state = ledger.get(pending.workflow_id)
    assert state["release_intent"]["receipt_id"] == receipt.receipt_id
    assert state["release_intent"]["request_hash"] == receipt.request_hash
    records, head = ledger.get_evidence(pending.workflow_id)
    assert head == completed.evidence_head
    assert tuple(records) == completed.evidence_chain
    assert any(record["source"] == "approval-gateway" for record in records)
    assert any(record["source"] == "capability-broker" for record in records)
    assert any(record["source"] == "release-gateway" for record in records)


def test_lost_deployment_ack_does_not_consume_a_second_worker_attempt() -> None:
    ledger = InMemoryLedger()
    backend = InMemoryDeploymentBackend(previous_release_sha="0" * 64)
    publisher, _, _ = _publisher()
    coordinator = _coordinator(
        ledger=ledger,
        repository=InMemoryArtifactRepository(BASE_SHA),
        deployer=InMemoryDeploymentAdapter(
            backend=backend,
            lose_acknowledgement_once=True,
        ),
        publisher=publisher,
    )
    pending = _advance_to_release(
        coordinator,
        idempotency_key="lost-ack-release-01",
        run_id="lost-ack-release",
    )

    completed = coordinator.decide_release(
        pending.workflow_id,
        _release_decision("lost-ack-release", pending.prepared_release_sha),
        _approver(),
    )

    assert completed.status == "completed"
    assert completed.task_executions["software-engineer"].attempt_count == 1
    assert backend.deploy_calls == 1


def test_restart_observes_deployment_and_reconciles_without_redeploy_or_retry_charge() -> None:
    ledger = InMemoryLedger()
    repository = InMemoryArtifactRepository(BASE_SHA)
    backend = InMemoryDeploymentBackend(previous_release_sha="0" * 64)
    publisher, receipts, pointers = _publisher()
    clock = Clock()
    layers = _layers()
    crash_lanes = CrashAfterEnabledMutation()
    first = _coordinator(
        ledger=ledger,
        repository=repository,
        deployer=InMemoryDeploymentAdapter(backend=backend),
        publisher=publisher,
        recovery=RecoveryController(ledger, layers=layers, clock=clock),
        lanes=crash_lanes,
        coordinator_id="coordinator-before-crash",
    )
    pending = _advance_to_release(
        first,
        idempotency_key="restart-release-01",
        run_id="restart-release",
    )
    decision = _release_decision("restart-release", pending.prepared_release_sha)
    crash_lanes.enabled = True

    with pytest.raises(KeyboardInterrupt, match="after remote deployment"):
        first.decide_release(pending.workflow_id, decision, _approver())

    interrupted = ledger.get(pending.workflow_id)
    assert interrupted["status"] == "release_in_progress"
    assert interrupted["release_intent"] is not None
    assert interrupted["governed_release_receipt"] is None
    assert backend.deploy_calls == 1

    clock.advance(DeliveryCoordinator.LEASE_SECONDS + 1)
    restarted = _coordinator(
        ledger=ledger,
        repository=repository,
        deployer=InMemoryDeploymentAdapter(backend=backend),
        publisher=ReleaseEvidencePublisher(receipts, pointers),
        recovery=RecoveryController(ledger, layers=layers, clock=clock),
        coordinator_id="coordinator-after-crash",
    )
    completed = restarted.decide_release(
        pending.workflow_id,
        decision,
        _approver(),
    )

    assert completed.status == "completed"
    assert completed.governed_release_receipt is not None
    assert completed.task_executions["software-engineer"].attempt_count == 1
    assert backend.deploy_calls == 1
    assert receipts.get(completed.governed_release_receipt.receipt_id)
    assert pointers.get(completed.governed_release_receipt.receipt_id)


def test_lost_ack_waits_for_delayed_visibility_without_another_charge() -> None:
    ledger = InMemoryLedger()
    backend = InMemoryDeploymentBackend(previous_release_sha="0" * 64)
    deployer = DelayedVisibilityDeploymentAdapter(
        backend=backend,
        hidden_observations=3,
    )
    publisher, _, _ = _publisher()
    coordinator = _coordinator(
        ledger=ledger,
        repository=InMemoryArtifactRepository(BASE_SHA),
        deployer=deployer,
        publisher=publisher,
    )
    pending = _advance_to_release(
        coordinator,
        idempotency_key="delayed-visible-release-01",
        run_id="delayed-visible-release",
    )
    decision = _release_decision("delayed-visible-release", pending.prepared_release_sha)

    waiting = coordinator.decide_release(
        pending.workflow_id,
        decision,
        _approver(),
    )
    completed = coordinator.decide_release(
        pending.workflow_id,
        decision,
        _approver(),
    )

    assert waiting.status == "release_in_progress"
    assert waiting.task_executions["software-engineer"].attempt_count == 1
    assert completed.status == "completed"
    assert completed.task_executions["software-engineer"].attempt_count == 1
    assert backend.deploy_calls == 1
    assert deployer.deploy_calls == 1


@pytest.mark.parametrize("tampered_binding", ["intent", "reference"])
def test_resume_rejects_intent_or_reference_not_bound_to_protected_event(
    tampered_binding: str,
) -> None:
    ledger = InMemoryLedger()
    backend = InMemoryDeploymentBackend(previous_release_sha="0" * 64)
    publisher, _, _ = _publisher()
    coordinator = _coordinator(
        ledger=ledger,
        repository=InMemoryArtifactRepository(BASE_SHA),
        deployer=DelayedVisibilityDeploymentAdapter(
            backend=backend,
            hidden_observations=100,
        ),
        publisher=publisher,
    )
    pending = _advance_to_release(
        coordinator,
        idempotency_key=f"tampered-release-binding-{tampered_binding}",
        run_id=f"tampered-release-{tampered_binding}",
    )
    decision = _release_decision(
        f"tampered-release-{tampered_binding}", pending.prepared_release_sha
    )
    waiting = coordinator.decide_release(
        pending.workflow_id,
        decision,
        _approver(),
    )
    assert waiting.status == "release_in_progress"

    with ledger.transaction(pending.workflow_id) as state:
        if tampered_binding == "intent":
            payload = dict(state["release_intent"])
            payload.pop("request_hash")
            payload.pop("receipt_id")
            payload["run_id"] = "hostile-sibling-state"
            state["release_intent"] = ReleaseIntent.model_validate(payload).model_dump(mode="json")
        else:
            state["release_evidence_chain_reference"] = f"{'0' * 64}:1:{'0' * 64}"

    with pytest.raises(DeliveryError, match="protected"):
        coordinator.decide_release(
            pending.workflow_id,
            decision,
            _approver(),
        )
    assert backend.deploy_calls == 1


@pytest.mark.parametrize(
    "tampered_state",
    [
        "config",
        "scope",
        "plan",
        "data_receipt",
        "prepared_release",
        "task_execution",
    ],
)
def test_release_intent_rejects_hostile_mutable_sibling_state(
    tampered_state: str,
) -> None:
    ledger = InMemoryLedger()
    backend = InMemoryDeploymentBackend(previous_release_sha="0" * 64)
    publisher, _, _ = _publisher()
    coordinator = _coordinator(
        ledger=ledger,
        repository=InMemoryArtifactRepository(BASE_SHA),
        deployer=InMemoryDeploymentAdapter(backend=backend),
        publisher=publisher,
    )
    pending = _advance_to_release(
        coordinator,
        idempotency_key=f"hostile-sibling-{tampered_state}",
        run_id=f"hostile-sibling-{tampered_state}",
    )

    with ledger.transaction(pending.workflow_id) as state:
        if tampered_state == "config":
            state["config"]["run_id"] = "hostile-run-id"
        elif tampered_state == "scope":
            state["scope"]["outcome"] = "Hostile replacement outcome"
        elif tampered_state == "plan":
            state["plan"]["tasks"][1]["attempt_cost_usd"] /= 2
        elif tampered_state == "data_receipt":
            state["data_receipt"]["manifest_sha"] = "f" * 64
        elif tampered_state == "prepared_release":
            state["prepared_release"]["gates"]["checks"][0]["detail"] = (
                "hostile replacement gate detail"
            )
        else:
            state["task_executions"]["software-engineer"]["budget_consumed_usd"] += 0.01

    with pytest.raises(DeliveryError):
        coordinator.decide_release(
            pending.workflow_id,
            _release_decision(
                f"hostile-sibling-{tampered_state}",
                pending.prepared_release_sha,
            ),
            _approver(),
        )
    assert backend.deploy_calls == 0


@pytest.mark.parametrize("missing_side", ["receipt", "pointer"])
def test_exact_completed_replay_repairs_missing_external_evidence(
    missing_side: str,
) -> None:
    ledger = InMemoryLedger()
    backend = InMemoryDeploymentBackend(previous_release_sha="0" * 64)
    publisher, receipts, pointers = _publisher()
    coordinator = _coordinator(
        ledger=ledger,
        repository=InMemoryArtifactRepository(BASE_SHA),
        deployer=InMemoryDeploymentAdapter(backend=backend),
        publisher=publisher,
    )
    pending = _advance_to_release(
        coordinator,
        idempotency_key=f"completed-repair-{missing_side}",
        run_id=f"completed-repair-{missing_side}",
    )
    decision = _release_decision(f"completed-repair-{missing_side}", pending.prepared_release_sha)
    completed = coordinator.decide_release(
        pending.workflow_id,
        decision,
        _approver(),
    )
    receipt_id = completed.governed_release_receipt.receipt_id
    missing_store = receipts if missing_side == "receipt" else pointers
    missing_store.delete_for_test(receipt_id)

    replay = coordinator.decide_release(
        pending.workflow_id,
        decision,
        _approver(),
    )

    assert replay == completed
    assert receipts.get(receipt_id) == completed.governed_release_receipt
    assert pointers.get(receipt_id) == completed.release_evidence_pointer
    assert replay.task_executions["software-engineer"].attempt_count == 1
    assert backend.deploy_calls == 1


def test_exact_completed_replay_fails_closed_on_external_evidence_conflict() -> None:
    ledger = InMemoryLedger()
    backend = InMemoryDeploymentBackend(previous_release_sha="0" * 64)
    publisher, _, pointers = _publisher()
    coordinator = _coordinator(
        ledger=ledger,
        repository=InMemoryArtifactRepository(BASE_SHA),
        deployer=InMemoryDeploymentAdapter(backend=backend),
        publisher=publisher,
    )
    pending = _advance_to_release(
        coordinator,
        idempotency_key="completed-conflict-01",
        run_id="completed-conflict",
    )
    decision = _release_decision("completed-conflict", pending.prepared_release_sha)
    completed = coordinator.decide_release(
        pending.workflow_id,
        decision,
        _approver(),
    )
    pointer = completed.release_evidence_pointer
    pointers.delete_for_test(pointer.receipt_id)
    pointers.insert_if_absent(
        pointer.model_copy(update={"receipt_location": "delta://hostile/location"})
    )

    with pytest.raises(ReleaseEvidenceConflict, match=pointer.receipt_id):
        coordinator.decide_release(
            pending.workflow_id,
            decision,
            _approver(),
        )
    assert backend.deploy_calls == 1


@pytest.mark.parametrize("first_side", ["receipt", "pointer"])
def test_restart_repairs_partial_evidence_publication_with_fresh_components(
    first_side: str,
) -> None:
    ledger = InMemoryLedger()
    repository = InMemoryArtifactRepository(BASE_SHA)
    backend = InMemoryDeploymentBackend(previous_release_sha="0" * 64)
    receipts = InMemoryReleaseEvidenceStore()
    pointers = InMemoryReleaseEvidencePointerStore()
    clock = Clock()
    layers = _layers()
    first = _coordinator(
        ledger=ledger,
        repository=repository,
        deployer=InMemoryDeploymentAdapter(backend=backend),
        publisher=CrashAfterOneEvidenceWrite(
            receipts,
            pointers,
            first_side=first_side,
        ),
        recovery=RecoveryController(ledger, layers=layers, clock=clock),
        coordinator_id=f"before-{first_side}-crash",
    )
    pending = _advance_to_release(
        first,
        idempotency_key=f"partial-publication-{first_side}",
        run_id=f"partial-publication-{first_side}",
    )
    decision = _release_decision(f"partial-publication-{first_side}", pending.prepared_release_sha)

    with pytest.raises(KeyboardInterrupt, match=f"{first_side}-only"):
        first.decide_release(pending.workflow_id, decision, _approver())

    interrupted = ledger.get(pending.workflow_id)
    receipt_id = interrupted["release_intent"]["receipt_id"]
    present_store = receipts if first_side == "receipt" else pointers
    missing_store = pointers if first_side == "receipt" else receipts
    assert present_store.get(receipt_id)
    with pytest.raises(ReleaseEvidenceNotFound):
        missing_store.get(receipt_id)
    assert backend.deploy_calls == 1

    clock.advance(DeliveryCoordinator.LEASE_SECONDS + 1)
    restarted = _coordinator(
        ledger=ledger,
        repository=repository,
        deployer=InMemoryDeploymentAdapter(backend=backend),
        publisher=ReleaseEvidencePublisher(receipts, pointers),
        recovery=RecoveryController(ledger, layers=layers, clock=clock),
        coordinator_id=f"after-{first_side}-crash",
    )
    completed = restarted.decide_release(
        pending.workflow_id,
        decision,
        _approver(),
    )

    assert completed.status == "completed"
    assert completed.task_executions["software-engineer"].attempt_count == 1
    assert receipts.get(receipt_id) == completed.governed_release_receipt
    assert pointers.get(receipt_id) == completed.release_evidence_pointer
    assert backend.deploy_calls == 1


def test_retry_receipt_reports_authorized_ceiling_without_underreporting() -> None:
    ledger = InMemoryLedger()
    backend = InMemoryDeploymentBackend(previous_release_sha="0" * 64)
    deployer = FailOnceDeploymentAdapter(backend=backend)
    publisher, _, _ = _publisher()
    coordinator = _coordinator(
        ledger=ledger,
        repository=InMemoryArtifactRepository(BASE_SHA),
        deployer=deployer,
        publisher=publisher,
    )
    pending = _advance_to_release(
        coordinator,
        idempotency_key="charged-retry-cost-01",
        run_id="charged-retry-cost",
    )

    completed = coordinator.decide_release(
        pending.workflow_id,
        _release_decision("charged-retry-cost", pending.prepared_release_sha),
        _approver(),
    )

    actual_cost_minor_units = round(
        sum(execution.budget_consumed_usd for execution in completed.task_executions.values()) * 100
    )
    receipt = completed.governed_release_receipt
    assert completed.status == "completed"
    assert completed.task_executions["software-engineer"].attempt_count == 2
    assert receipt.cost_basis == "authorized_ceiling"
    assert receipt.cost_minor_units >= actual_cost_minor_units
    assert backend.deploy_calls == 1
