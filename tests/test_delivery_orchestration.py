from __future__ import annotations

import ast
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path

import pytest

from broker.contracts import SyntheticTableWriteArgs
from gates.swe import SoftwareGateSuite
from gates.swe.release import SoftwareReleaseService
from identity import AccessDenied, ActorContext
from ledger import InMemoryLedger, LedgerConflict
from orchestrator.delivery import (
    DeliveryCoordinator,
    DeliveryError,
    ExecutionLanes,
    ReferenceRunConfig,
)
from orchestrator.delivery_models import (
    DeliveryTask,
    EscalationEvent,
    ProductScope,
    ScrumPlan,
)
from orchestrator.models import (
    AcceptanceTest,
    BriefSubmission,
    ReleaseDecision,
    ScopeDecision,
)
from pipeline import DataEngineeringPipeline
from recovery import InMemoryRevocationLayer, RecoveryController
from workers.de import InMemoryCatalogAdapter
from workers.pm import ProductManagerWorker
from workers.sm import ScrumMasterWorker
from workers.swe import InMemoryArtifactRepository, InMemoryDeploymentAdapter

BASE_SHA = "1" * 64


def _submission(**overrides: object) -> BriefSubmission:
    values = {
        "title": "Delivery health reference brief",
        "business_question": "Show delivery health and the work that needs attention.",
        "acceptance_tests": [
            AcceptanceTest(
                name="governed-sources",
                description="Every dashboard signal uses the governed sandbox tables.",
                kind="contract",
            ),
            AcceptanceTest(
                name="three-signals",
                description="The result covers backlog, reliability, and platform cost.",
                kind="quality",
            ),
        ],
        "cost_ceiling_usd": 4.0,
        "release_approver": "approver-1",
        "viewer_subjects": ["auditor-1"],
        "idempotency_key": "delivery-reference-01",
    }
    values.update(overrides)
    return BriefSubmission.model_validate(values)


def _config(**overrides: object) -> ReferenceRunConfig:
    values = {
        "run_id": "run-01",
        "seed": 2026,
        "sandbox_catalog": "demo_catalog",
        "sandbox_schema": "steward_forge_sandbox",
        "trusted_base_sha": BASE_SHA,
        "generated_prefix": "generated/software-engineer",
        "artifact_branch": "steward-forge/candidates",
        "dashboard_title": "Engineering delivery signals",
    }
    values.update(overrides)
    return ReferenceRunConfig.model_validate(values)


def _coordinator(
    *,
    pipeline: DataEngineeringPipeline | None = None,
    software: SoftwareReleaseService | None = None,
    scrum_master: ScrumMasterWorker | None = None,
) -> DeliveryCoordinator:
    data_path = pipeline or DataEngineeringPipeline(InMemoryCatalogAdapter())
    software_path = software or SoftwareReleaseService(
        InMemoryArtifactRepository(BASE_SHA),
        InMemoryDeploymentAdapter(previous_release_sha="0" * 64),
    )
    return DeliveryCoordinator(
        data_pipeline=data_path,
        software_release=software_path,
        scrum_master=scrum_master,
    )


def _submitter() -> ActorContext:
    return ActorContext(subject="employee-1", roles={"submitter", "viewer"})


def _approver(subject: str = "approver-1") -> ActorContext:
    return ActorContext(subject=subject, roles={"approver", "viewer"})


def _scope_decision(
    *,
    decision_id: str = "scope-decision-1",
    decision: str = "approved",
) -> ScopeDecision:
    return ScopeDecision(
        decision_id=decision_id,
        decision=decision,
        scope_version=1,
    )


def _release_decision(
    commit_sha: str,
    *,
    decision_id: str = "release-decision-1",
    decision: str = "approved",
) -> ReleaseDecision:
    return ReleaseDecision(
        decision_id=decision_id,
        decision=decision,
        commit_sha=commit_sha,
    )


def _execute(
    coordinator: DeliveryCoordinator,
    submission: BriefSubmission,
    *,
    config: ReferenceRunConfig | None = None,
    scope_approver: ActorContext | None = None,
    release_approver: ActorContext | None = None,
):
    return coordinator.execute(
        submission,
        config=config or _config(),
        submitter=_submitter(),
        scope_decision=_scope_decision(),
        scope_approver=scope_approver or _approver(),
        release_decision_provider=_release_decision,
        release_approver=release_approver or _approver(),
    )


def test_pm_emits_a_versioned_complete_scope_with_valid_acceptance_tests() -> None:
    proposal = ProductManagerWorker().propose("brief-01", _submission())

    assert proposal.outcome == _submission().business_question
    assert proposal.scope == (
        "Publish deterministic synthetic delivery-health data in the configured sandbox.",
        "Release a governed dashboard over backlog, reliability, and platform cost.",
    )
    assert proposal.assumptions
    assert proposal.acceptance_tests == tuple(_submission().acceptance_tests)
    assert proposal.proposed_by == "product-manager"
    assert proposal.schema_version == 1


def test_pm_cannot_approve_its_own_scope() -> None:
    coordinator = _coordinator()
    submission = _submission(release_approver="product-manager")

    with pytest.raises(AccessDenied, match="scope author cannot approve"):
        _execute(
            coordinator,
            submission,
            scope_approver=_approver("product-manager"),
            release_approver=_approver("product-manager"),
        )


def test_named_dual_role_submitter_may_approve_scope_but_not_release() -> None:
    actor = ActorContext(
        subject="dual-role-1",
        roles={"submitter", "approver", "viewer"},
    )
    coordinator = _coordinator()
    submission = _submission(
        idempotency_key="dual-role-scope-01",
        release_approver=actor.subject,
    )
    submitted = coordinator.submit(
        submission,
        config=_config(run_id="dual-role-scope"),
        actor=actor,
    )

    planned = coordinator.decide_scope(
        submitted.workflow_id,
        _scope_decision(),
        actor,
    )
    pending = coordinator.advance(submitted.workflow_id)

    assert planned.status == "planned"
    with pytest.raises(AccessDenied, match="submitter cannot approve release"):
        coordinator.decide_release(
            submitted.workflow_id,
            _release_decision(pending.prepared_release_sha),
            actor,
        )


def test_coordinator_exposes_explicit_scope_and_release_decision_phases() -> None:
    coordinator = _coordinator()
    submission = _submission(idempotency_key="explicit-phases-01")

    submitted = coordinator.submit(
        submission,
        config=_config(run_id="explicit-phases"),
        actor=_submitter(),
    )

    assert submitted.status == "scope_pending"
    assert not any(
        event.event_type == "scope.approved" for event in submitted.evidence
    )

    planned = coordinator.decide_scope(
        submitted.workflow_id,
        _scope_decision(),
        _approver(),
    )
    pending = coordinator.advance(submitted.workflow_id)

    assert planned.status == "planned"
    assert pending.status == "release_pending"
    assert pending.prepared_release_sha is not None
    assert pending.software_receipt is None

    completed = coordinator.decide_release(
        submitted.workflow_id,
        _release_decision(pending.prepared_release_sha),
        _approver(),
    )

    assert completed.status == "completed"
    assert completed.software_receipt is not None


def test_rejected_human_decisions_are_terminal_and_replay_exactly() -> None:
    scope_coordinator = _coordinator()
    scope_submission = _submission(idempotency_key="scope-rejected-01")
    submitted = scope_coordinator.submit(
        scope_submission,
        config=_config(run_id="scope-rejected"),
        actor=_submitter(),
    )
    scope_rejection = _scope_decision(decision="rejected")

    rejected = scope_coordinator.decide_scope(
        submitted.workflow_id,
        scope_rejection,
        _approver(),
    )
    replay = scope_coordinator.decide_scope(
        submitted.workflow_id,
        scope_rejection,
        _approver(),
    )

    assert replay == rejected
    assert rejected.status == "scope_rejected"
    assert [event.event_type for event in rejected.evidence].count(
        "scope.rejected"
    ) == 1

    release_coordinator = _coordinator()
    release_submission = _submission(idempotency_key="release-rejected-01")
    release_submitted = release_coordinator.submit(
        release_submission,
        config=_config(run_id="release-rejected"),
        actor=_submitter(),
    )
    release_coordinator.decide_scope(
        release_submitted.workflow_id,
        _scope_decision(),
        _approver(),
    )
    pending = release_coordinator.advance(release_submitted.workflow_id)
    assert pending.prepared_release_sha is not None
    release_rejection = _release_decision(
        pending.prepared_release_sha,
        decision="rejected",
    )

    release_rejected = release_coordinator.decide_release(
        release_submitted.workflow_id,
        release_rejection,
        _approver(),
    )
    release_replay = release_coordinator.decide_release(
        release_submitted.workflow_id,
        release_rejection,
        _approver(),
    )

    assert release_replay == release_rejected
    assert release_rejected.status == "release_rejected"
    assert (
        release_rejected.task_executions["software-engineer"].stop_reason
        == "release approval was rejected"
    )
    assert [event.event_type for event in release_rejected.evidence].count(
        "release.decision.recorded"
    ) == 1


def test_execute_resumes_scope_planned_and_release_pending_states() -> None:
    scope_coordinator = _coordinator()
    scope_submission = _submission(idempotency_key="resume-scope-01")
    scope_coordinator.submit(
        scope_submission,
        config=_config(run_id="resume-scope"),
        actor=_submitter(),
    )
    assert _execute(
        scope_coordinator,
        scope_submission,
        config=_config(run_id="resume-scope"),
    ).status == "completed"

    planned_coordinator = _coordinator()
    planned_submission = _submission(idempotency_key="resume-planned-01")
    planned = planned_coordinator.submit(
        planned_submission,
        config=_config(run_id="resume-planned"),
        actor=_submitter(),
    )
    planned_coordinator.decide_scope(
        planned.workflow_id,
        _scope_decision(),
        _approver(),
    )
    assert _execute(
        planned_coordinator,
        planned_submission,
        config=_config(run_id="resume-planned"),
    ).status == "completed"

    pending_coordinator = _coordinator()
    pending_submission = _submission(idempotency_key="resume-release-01")
    pending = pending_coordinator.submit(
        pending_submission,
        config=_config(run_id="resume-release"),
        actor=_submitter(),
    )
    pending_coordinator.decide_scope(
        pending.workflow_id,
        _scope_decision(),
        _approver(),
    )
    release_pending = pending_coordinator.advance(pending.workflow_id)
    assert release_pending.status == "release_pending"
    assert _execute(
        pending_coordinator,
        pending_submission,
        config=_config(run_id="resume-release"),
    ).status == "completed"


def test_sm_creates_bounded_tasks_and_escalations_but_has_no_retry_api() -> None:
    submission = _submission()
    scope = ProductManagerWorker().propose("brief-01", submission)
    worker = ScrumMasterWorker()

    plan = worker.plan_delivery(scope, submission.cost_ceiling_usd)
    escalation = worker.escalate(
        plan.tasks[0], attempt=1, reason="catalog service was unavailable"
    )

    assert [task.worker_id for task in plan.tasks] == [
        "data-engineer",
        "software-engineer",
    ]
    assert all(task.max_attempts == 2 for task in plan.tasks)
    assert all(task.attempt_cost_usd > 0 for task in plan.tasks)
    assert sum(task.budget_usd for task in plan.tasks) == pytest.approx(
        submission.cost_ceiling_usd
    )
    assert plan.tasks[1].depends_on == (plan.tasks[0].task_id,)
    assert plan.brief_id == scope.brief_id
    assert plan.approved_scope_sha256 == scope.fingerprint()
    assert escalation.retry_owner == "orchestrator"
    assert escalation.action == "retry-or-stop"
    assert not hasattr(worker, "retry")


def test_execution_lanes_overlap_reads_and_serialize_mutations() -> None:
    lanes = ExecutionLanes(max_read_workers=2)
    read_barrier = threading.Barrier(2)
    read_active = 0
    max_read_active = 0
    mutation_active = 0
    max_mutation_active = 0
    guard = threading.Lock()

    def read_job(label: str) -> str:
        nonlocal read_active, max_read_active
        with guard:
            read_active += 1
            max_read_active = max(max_read_active, read_active)
        read_barrier.wait(timeout=2)
        with guard:
            read_active -= 1
        return label

    assert lanes.prepare_concurrently({
        "de": lambda: read_job("de"),
        "swe": lambda: read_job("swe"),
    }) == {"de": "de", "swe": "swe"}

    mutation_barrier = threading.Barrier(2)

    def mutation_job() -> None:
        nonlocal mutation_active, max_mutation_active
        with guard:
            mutation_active += 1
            max_mutation_active = max(max_mutation_active, mutation_active)
        threading.Event().wait(0.02)
        with guard:
            mutation_active -= 1

    def enter_mutation_lane() -> None:
        mutation_barrier.wait(timeout=2)
        lanes.mutate(mutation_job)

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(enter_mutation_lane) for _ in range(2)]
        for future in futures:
            future.result()

    assert max_read_active == 2
    assert max_mutation_active == 1


class _FailOncePipeline(DataEngineeringPipeline):
    def __init__(self) -> None:
        super().__init__(InMemoryCatalogAdapter())
        self.publish_calls = 0

    def publish(
        self,
        task,
        candidate,
        *,
        session=None,
        lease_owner=None,
        lease_epoch=None,
    ):
        self.publish_calls += 1
        if self.publish_calls == 1:
            raise TimeoutError("catalog acknowledgement timed out")
        return super().publish(
            task,
            candidate,
            session=session,
            lease_owner=lease_owner,
            lease_epoch=lease_epoch,
        )


class _AlwaysFailPipeline(DataEngineeringPipeline):
    def __init__(self) -> None:
        super().__init__(InMemoryCatalogAdapter())
        self.publish_calls = 0

    def publish(self, task, candidate, **kwargs):
        self.publish_calls += 1
        raise TimeoutError("catalog service unavailable")


class _NonRetryableFailPipeline(DataEngineeringPipeline):
    def __init__(self) -> None:
        super().__init__(InMemoryCatalogAdapter())
        self.publish_calls = 0

    def publish(self, task, candidate, **kwargs):
        self.publish_calls += 1
        raise ValueError("candidate contract is invalid")


class _PreparationFailPipeline(DataEngineeringPipeline):
    def __init__(self) -> None:
        super().__init__(InMemoryCatalogAdapter())

    def prepare(self, task):
        raise ValueError("read-only candidate preparation failed")


class _BlockingPipeline(DataEngineeringPipeline):
    def __init__(self, catalog) -> None:
        super().__init__(catalog)
        self.entered = threading.Event()
        self.release = threading.Event()

    def publish(self, task, candidate, **kwargs):
        self.entered.set()
        assert self.release.wait(timeout=2)
        return super().publish(task, candidate, **kwargs)


class _CountingPreparePipeline(DataEngineeringPipeline):
    def __init__(self, catalog=None) -> None:
        super().__init__(catalog or InMemoryCatalogAdapter())
        self.prepare_calls = 0

    def prepare(self, task):
        self.prepare_calls += 1
        return super().prepare(task)


class _CountingCatalog(InMemoryCatalogAdapter):
    def __init__(self) -> None:
        super().__init__()
        self.calls = 0

    def write(self, arguments: SyntheticTableWriteArgs) -> dict[str, object]:
        self.calls += 1
        return super().write(arguments)


class _FailSecondCatalog(InMemoryCatalogAdapter):
    def __init__(self) -> None:
        super().__init__()
        self.calls: dict[str, int] = {}
        self.failed = False

    def write(self, arguments: SyntheticTableWriteArgs) -> dict[str, object]:
        self.calls[arguments.dataset] = self.calls.get(arguments.dataset, 0) + 1
        if arguments.dataset == "pipeline_runs" and not self.failed:
            self.failed = True
            raise TimeoutError("second table write timed out")
        return super().write(arguments)


class _BlockingRestoreSoftwareRelease(SoftwareReleaseService):
    def __init__(self, repository, deployer) -> None:
        super().__init__(repository, deployer)
        self.restore_entered = threading.Event()
        self.restore_release = threading.Event()

    def restore_prepared(self, prepared):
        self.restore_entered.set()
        assert self.restore_release.wait(timeout=2)
        return super().restore_prepared(prepared)


class _FailOnceCandidatePreparation(SoftwareReleaseService):
    def __init__(self) -> None:
        super().__init__(
            InMemoryArtifactRepository(BASE_SHA),
            InMemoryDeploymentAdapter(previous_release_sha="0" * 64),
        )
        self.prepare_attempts = 0

    def prepare_candidate(self, *args, **kwargs):
        self.prepare_attempts += 1
        if self.prepare_attempts == 1:
            raise TimeoutError("candidate preparation timed out")
        return super().prepare_candidate(*args, **kwargs)


class _AlwaysFailCandidatePreparation(SoftwareReleaseService):
    def __init__(self) -> None:
        super().__init__(
            InMemoryArtifactRepository(BASE_SHA),
            InMemoryDeploymentAdapter(previous_release_sha="0" * 64),
        )
        self.prepare_attempts = 0

    def prepare_candidate(self, *args, **kwargs):
        self.prepare_attempts += 1
        raise TimeoutError("candidate preparation timed out")


class _CountingSoftwareGate(SoftwareGateSuite):
    def __init__(self) -> None:
        self.evaluate_calls = 0

    def evaluate(self, task, candidate, *, committed_artifacts):
        self.evaluate_calls += 1
        return super().evaluate(
            task,
            candidate,
            committed_artifacts=committed_artifacts,
        )


class _OneAttemptBudgetScrumMaster(ScrumMasterWorker):
    def plan_delivery(self, scope, cost_ceiling_usd):
        plan = super().plan_delivery(scope, cost_ceiling_usd)
        first = plan.tasks[0].model_copy(
            update={
                "budget_usd": plan.tasks[0].attempt_cost_usd,
            }
        )
        return plan.model_copy(update={"tasks": (first, plan.tasks[1])})


class _OverBudgetScrumMaster(ScrumMasterWorker):
    def plan_delivery(self, scope, cost_ceiling_usd):
        plan = super().plan_delivery(scope, cost_ceiling_usd)
        inflated = plan.tasks[0].model_copy(
            update={"budget_usd": plan.tasks[0].budget_usd + 0.01}
        )
        return plan.model_copy(update={"tasks": (inflated, plan.tasks[1])})


class _ConflictingEscalationScrumMaster(ScrumMasterWorker):
    def escalate(self, task, *, attempt, reason):
        return EscalationEvent(
            task_id="falsified-task",
            worker_id="software-engineer",
            attempt=attempt + 1,
            reason="falsified failure",
        )


class _MalformedEscalationScrumMaster(ScrumMasterWorker):
    def escalate(self, task, *, attempt, reason):
        return {"untrusted": "malformed"}


class _StalePlanScrumMaster(ScrumMasterWorker):
    def __init__(self, field: str) -> None:
        self._field = field

    def plan_delivery(self, scope, cost_ceiling_usd):
        plan = super().plan_delivery(scope, cost_ceiling_usd)
        if self._field == "brief_id":
            return plan.model_copy(update={"brief_id": "another-brief"})
        return plan.model_copy(update={"approved_scope_sha256": "0" * 64})


class _MalformedPlanScrumMaster(ScrumMasterWorker):
    def plan_delivery(self, scope, cost_ceiling_usd):
        plan = super().plan_delivery(scope, cost_ceiling_usd)
        return ScrumPlan.model_construct(
            **(plan.__dict__ | {"plan_id": ""})
        )


class _MalformedProductManager(ProductManagerWorker):
    def __init__(self, *, as_dict: bool) -> None:
        self._as_dict = as_dict

    def propose(self, brief_id, brief):
        if self._as_dict:
            return {"brief_id": brief_id, "scope_version": 1}
        return ProductScope.model_construct(
            brief_id="",
            outcome=brief.business_question,
            scope=("unsafe",),
            assumptions=("unsafe",),
            acceptance_tests=tuple(brief.acceptance_tests),
        )


class _ConcurrentProductManager(ProductManagerWorker):
    def __init__(self) -> None:
        self.barrier = threading.Barrier(2)
        self.calls = 0
        self.guard = threading.Lock()

    def propose(self, brief_id, brief):
        with self.guard:
            self.calls += 1
        self.barrier.wait(timeout=2)
        return super().propose(brief_id, brief)


class _CrashOnceProductManager(ProductManagerWorker):
    def __init__(self) -> None:
        self.crashed = False

    def propose(self, brief_id, brief):
        if not self.crashed:
            self.crashed = True
            raise KeyboardInterrupt("proposal process stopped")
        return super().propose(brief_id, brief)


class _CrashOnceOnAtomicTerminalLedger(InMemoryLedger):
    def __init__(self) -> None:
        super().__init__()
        self.crashed = False

    @contextmanager
    def transaction(self, brief_id):
        with super().transaction(brief_id) as state:
            yield state
            if (
                not self.crashed
                and state.get("status") == "failed"
                and any(
                    event.get("record_type") == "run.failed"
                    for event in state.get("evidence_chain", [])
                )
            ):
                self.crashed = True
                raise RuntimeError("simulated crash before terminal commit")


class _CapturingPipeline(DataEngineeringPipeline):
    def __init__(self) -> None:
        super().__init__(InMemoryCatalogAdapter())
        self.result = None

    def publish(self, task, candidate, **kwargs):
        self.result = super().publish(task, candidate, **kwargs)
        return self.result


class _RecordingRecovery(RecoveryController):
    def __init__(self, ledger: InMemoryLedger) -> None:
        super().__init__(
            ledger,
            layers={
                name: InMemoryRevocationLayer(name)
                for name in RecoveryController.REQUIRED_LAYERS
            },
        )
        self.fence_depth = 0

    @contextmanager
    def worker_fence(self, brief_id, worker_id, owner, epoch):
        with super().worker_fence(brief_id, worker_id, owner, epoch):
            self.fence_depth += 1
            try:
                yield
            finally:
                self.fence_depth -= 1


class _FenceAssertingDeployment(InMemoryDeploymentAdapter):
    def __init__(self, recovery: _RecordingRecovery) -> None:
        super().__init__(previous_release_sha="0" * 64)
        self._recovery = recovery

    def deploy(self, *, commit_sha, include_genie, idempotency_key):
        assert self._recovery.fence_depth > 0
        return super().deploy(
            commit_sha=commit_sha,
            include_genie=include_genie,
            idempotency_key=idempotency_key,
        )


def test_hostile_scrum_plan_cannot_exceed_the_brief_ceiling() -> None:
    coordinator = _coordinator(scrum_master=_OverBudgetScrumMaster())
    submission = _submission(idempotency_key="over-budget-plan-01")
    submitted = coordinator.submit(
        submission,
        config=_config(run_id="over-budget-plan"),
        actor=_submitter(),
    )

    result = coordinator.decide_scope(
        submitted.workflow_id,
        _scope_decision(),
        _approver(),
    )

    assert result.status == "failed"
    rejected = [
        event for event in result.evidence if event.event_type == "plan.rejected"
    ]
    assert len(rejected) == 1
    assert "exceeds the brief cost ceiling" in rejected[0].details["failure"]


@pytest.mark.parametrize("field", ["brief_id", "approved_scope_sha256"])
def test_scrum_plan_must_bind_the_exact_approved_scope(field: str) -> None:
    coordinator = _coordinator(scrum_master=_StalePlanScrumMaster(field))
    submission = _submission(idempotency_key=f"stale-plan-{field}")
    submitted = coordinator.submit(
        submission,
        config=_config(run_id=f"stale-plan-{field}"),
        actor=_submitter(),
    )

    result = coordinator.decide_scope(
        submitted.workflow_id,
        _scope_decision(),
        _approver(),
    )

    assert result.status == "failed"
    assert [event.event_type for event in result.evidence].count("plan.rejected") == 1


def test_constructed_scrum_plan_is_revalidated_before_persistence() -> None:
    coordinator = _coordinator(scrum_master=_MalformedPlanScrumMaster())
    submission = _submission(idempotency_key="malformed-plan-01")
    submitted = coordinator.submit(
        submission,
        config=_config(run_id="malformed-plan"),
        actor=_submitter(),
    )

    result = coordinator.decide_scope(
        submitted.workflow_id,
        _scope_decision(),
        _approver(),
    )

    assert result.status == "failed"
    rejected = [
        event for event in result.evidence if event.event_type == "plan.rejected"
    ]
    assert len(rejected) == 1
    assert "plan_id" in rejected[0].details["failure"]


@pytest.mark.parametrize("as_dict", [False, True])
def test_malformed_product_scope_becomes_an_explicit_failed_workflow(
    as_dict: bool,
) -> None:
    coordinator = DeliveryCoordinator(
        data_pipeline=DataEngineeringPipeline(InMemoryCatalogAdapter()),
        software_release=SoftwareReleaseService(
            InMemoryArtifactRepository(BASE_SHA),
            InMemoryDeploymentAdapter(previous_release_sha="0" * 64),
        ),
        product_manager=_MalformedProductManager(as_dict=as_dict),
    )
    submission = _submission(idempotency_key=f"malformed-scope-{as_dict}")

    result = coordinator.submit(
        submission,
        config=_config(run_id=f"malformed-scope-{str(as_dict).lower()}"),
        actor=_submitter(),
    )

    assert result.status == "failed"
    assert result.scope is None
    assert [event.event_type for event in result.evidence] == [
        "brief.submitted",
        "scope.proposal.failed",
    ]


def test_concurrent_submit_replay_finishes_one_validated_scope_proposal() -> None:
    ledger = InMemoryLedger()
    product_manager = _ConcurrentProductManager()

    def coordinator(instance_id: str) -> DeliveryCoordinator:
        return DeliveryCoordinator(
            coordinator_id=instance_id,
            ledger=ledger,
            data_pipeline=DataEngineeringPipeline(InMemoryCatalogAdapter()),
            software_release=SoftwareReleaseService(
                InMemoryArtifactRepository(BASE_SHA),
                InMemoryDeploymentAdapter(previous_release_sha="0" * 64),
            ),
            product_manager=product_manager,
        )

    submission = _submission(idempotency_key="concurrent-submit-01")
    config = _config(run_id="concurrent-submit")
    coordinators = (coordinator("submit-a"), coordinator("submit-b"))
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(
            executor.map(
                lambda item: item.submit(
                    submission,
                    config=config,
                    actor=_submitter(),
                ),
                coordinators,
            )
        )

    assert results[0] == results[1]
    assert results[0].status == "scope_pending"
    assert results[0].scope is not None
    assert product_manager.calls == 2
    event_types = [event.event_type for event in results[0].evidence]
    assert event_types == ["brief.submitted", "scope.proposed"]


def test_submit_replay_resumes_a_crashed_initial_scope_proposal() -> None:
    product_manager = _CrashOnceProductManager()
    coordinator = DeliveryCoordinator(
        data_pipeline=DataEngineeringPipeline(InMemoryCatalogAdapter()),
        software_release=SoftwareReleaseService(
            InMemoryArtifactRepository(BASE_SHA),
            InMemoryDeploymentAdapter(previous_release_sha="0" * 64),
        ),
        product_manager=product_manager,
    )
    submission = _submission(idempotency_key="crashed-submit-01")
    config = _config(run_id="crashed-submit")

    with pytest.raises(KeyboardInterrupt, match="process stopped"):
        coordinator.submit(submission, config=config, actor=_submitter())
    resumed = coordinator.submit(submission, config=config, actor=_submitter())

    assert resumed.status == "scope_pending"
    assert resumed.scope is not None
    assert [event.event_type for event in resumed.evidence] == [
        "brief.submitted",
        "scope.proposed",
    ]


def test_mutation_receipts_bind_the_active_lease_owner_and_epoch() -> None:
    pipeline = _CapturingPipeline()

    result = _execute(
        _coordinator(pipeline=pipeline),
        _submission(idempotency_key="receipt-lease-binding-01"),
        config=_config(run_id="receipt-lease-binding"),
    )

    assert result.status == "completed"
    assert pipeline.result is not None
    mutation_receipts = pipeline.result.execution.mutation_receipts
    assert {receipt.workflow_id for receipt in mutation_receipts} == {
        result.workflow_id
    }
    assert all(receipt.lease_owner for receipt in mutation_receipts)
    assert {receipt.lease_epoch for receipt in mutation_receipts} == {1}


def test_workspace_deployment_runs_inside_the_durable_worker_fence() -> None:
    ledger = InMemoryLedger()
    recovery = _RecordingRecovery(ledger)
    deployer = _FenceAssertingDeployment(recovery)
    coordinator = DeliveryCoordinator(
        ledger=ledger,
        recovery=recovery,
        data_pipeline=DataEngineeringPipeline(InMemoryCatalogAdapter()),
        software_release=SoftwareReleaseService(
            InMemoryArtifactRepository(BASE_SHA),
            deployer,
        ),
    )

    result = _execute(
        coordinator,
        _submission(idempotency_key="deployment-fence-01"),
        config=_config(run_id="deployment-fence"),
    )

    assert result.status == "completed"
    assert deployer.deploy_calls == 1


def test_orchestrator_owns_retry_and_persists_attempt_and_checkpoint_evidence() -> None:
    pipeline = _FailOncePipeline()
    result = _execute(
        _coordinator(pipeline=pipeline),
        _submission(),
    )

    data_state = result.task_executions["data-engineer"]
    assert result.status == "completed"
    assert pipeline.publish_calls == 2
    assert data_state.state == "succeeded"
    assert data_state.attempt_count == 2
    assert data_state.failures == ("catalog acknowledgement timed out",)
    assert any(
        event.event_type == "task.escalated"
        and event.details["retry_owner"] == "orchestrator"
        for event in result.evidence
    )
    assert any(
        event.event_type == "task.checkpointed"
        and event.worker_id == "data-engineer"
        for event in result.evidence
    )


def test_conflicting_sm_escalation_is_recorded_with_canonical_context() -> None:
    result = _execute(
        _coordinator(
            pipeline=_FailOncePipeline(),
            scrum_master=_ConflictingEscalationScrumMaster(),
        ),
        _submission(idempotency_key="conflicting-escalation-01"),
        config=_config(run_id="conflicting-escalation"),
    )

    escalated = next(
        event for event in result.evidence if event.event_type == "task.escalated"
    )
    assert result.status == "completed"
    assert escalated.task_id == result.plan.tasks[0].task_id
    assert escalated.worker_id == "data-engineer"
    assert escalated.details == {
        "schema_id": "steward-forge.escalation",
        "schema_version": 1,
        "task_id": result.plan.tasks[0].task_id,
        "worker_id": "data-engineer",
        "attempt": 1,
        "reason": "catalog acknowledgement timed out",
        "retry_owner": "orchestrator",
        "action": "retry-or-stop",
        "worker_report_status": "canonicalized",
    }


def test_malformed_sm_escalation_cannot_strand_terminal_failure_handling() -> None:
    result = _execute(
        _coordinator(
            pipeline=_NonRetryableFailPipeline(),
            scrum_master=_MalformedEscalationScrumMaster(),
        ),
        _submission(idempotency_key="malformed-escalation-01"),
        config=_config(run_id="malformed-escalation"),
    )

    escalated = next(
        event for event in result.evidence if event.event_type == "task.escalated"
    )
    assert result.status == "failed"
    assert escalated.task_id == result.plan.tasks[0].task_id
    assert escalated.worker_id == "data-engineer"
    assert escalated.details["attempt"] == 1
    assert escalated.details["reason"] == "candidate contract is invalid"
    assert escalated.details["retry_owner"] == "orchestrator"
    assert escalated.details["action"] == "retry-or-stop"
    assert escalated.details["worker_report_status"] == "invalid"


def test_data_retry_reuses_broker_receipts_after_a_partial_publish() -> None:
    catalog = _FailSecondCatalog()
    result = _execute(
        _coordinator(pipeline=DataEngineeringPipeline(catalog)),
        _submission(idempotency_key="partial-data-publish-01"),
        config=_config(run_id="partial-data"),
    )

    assert result.status == "completed"
    assert result.task_executions["data-engineer"].attempt_count == 2
    assert catalog.calls == {
        "backlog": 1,
        "pipeline_runs": 2,
        "platform_costs": 1,
    }
    assert len(catalog.write_events) == 3


def test_lost_deploy_ack_reuses_prepared_envelope_without_retry_charge() -> None:
    repository = InMemoryArtifactRepository(BASE_SHA)
    deployer = InMemoryDeploymentAdapter(
        previous_release_sha="0" * 64,
        lose_acknowledgement_once=True,
    )
    gates = _CountingSoftwareGate()
    service = SoftwareReleaseService(repository, deployer, gates=gates)

    result = _execute(
        _coordinator(software=service),
        _submission(idempotency_key="lost-deploy-ack-01"),
        config=_config(run_id="lost-deploy-ack"),
    )

    assert result.status == "completed"
    assert result.task_executions["software-engineer"].attempt_count == 1
    assert repository.commit_calls == 1
    assert gates.evaluate_calls == 3
    assert deployer.deploy_calls == 1
    assert any(
        event.event_type == "release.outcome.unknown"
        and event.worker_id == "software-engineer"
        for event in result.evidence
    )


def test_candidate_preparation_retries_with_persisted_attempt_accounting() -> None:
    service = _FailOnceCandidatePreparation()

    result = _execute(
        _coordinator(software=service),
        _submission(idempotency_key="candidate-prepare-retry-01"),
        config=_config(run_id="candidate-prepare-retry"),
    )

    execution = result.task_executions["software-engineer"]
    assert result.status == "completed"
    assert service.prepare_attempts == 2
    assert execution.preparation_attempt_count == 2
    assert execution.attempt_count == 1
    assert execution.failures == ("candidate preparation timed out",)
    assert any(
        event.event_type == "task.checkpointed"
        and event.worker_id == "software-engineer"
        and event.details["phase"] == "candidate-preparation"
        for event in result.evidence
    )


def test_candidate_preparation_exhaustion_is_terminal_and_budgeted() -> None:
    service = _AlwaysFailCandidatePreparation()

    result = _execute(
        _coordinator(software=service),
        _submission(idempotency_key="candidate-prepare-exhausted-01"),
        config=_config(run_id="candidate-prepare-exhausted"),
    )

    execution = result.task_executions["software-engineer"]
    assert result.status == "failed"
    assert service.prepare_attempts == 2
    assert execution.preparation_attempt_count == 2
    assert execution.attempt_count == 0
    assert execution.stop_reason == "maximum candidate-preparation attempts exhausted"
    assert execution.budget_consumed_usd > 0


def test_budget_stop_and_retry_exhaustion_are_explicit_terminal_states() -> None:
    budget_pipeline = _AlwaysFailPipeline()
    budget_result = _execute(
        _coordinator(
            pipeline=budget_pipeline,
            scrum_master=_OneAttemptBudgetScrumMaster(),
        ),
        _submission(idempotency_key="budget-stop-01"),
        config=_config(run_id="budget-stop"),
    )

    assert budget_result.status == "budget_stopped"
    assert budget_result.task_executions["data-engineer"].state == "budget_stopped"
    assert budget_result.task_executions["data-engineer"].stop_reason == (
        "insufficient task budget for another attempt"
    )
    assert budget_pipeline.publish_calls == 1

    failed_pipeline = _AlwaysFailPipeline()
    failed_result = _execute(
        _coordinator(pipeline=failed_pipeline),
        _submission(idempotency_key="retry-failed-01"),
        config=_config(run_id="retry-failed"),
    )

    assert failed_result.status == "failed"
    assert failed_result.task_executions["data-engineer"].state == "failed"
    assert failed_result.task_executions["data-engineer"].attempt_count == 2
    assert failed_result.task_executions["data-engineer"].stop_reason == (
        "maximum attempts exhausted"
    )


def test_non_retryable_failure_stops_after_one_attempt() -> None:
    pipeline = _NonRetryableFailPipeline()
    result = _execute(
        _coordinator(pipeline=pipeline),
        _submission(idempotency_key="non-retryable-01"),
        config=_config(run_id="non-retryable"),
    )

    execution = result.task_executions["data-engineer"]
    assert result.status == "failed"
    assert execution.state == "failed"
    assert execution.attempt_count == 1
    assert execution.stop_reason == "non-retryable failure"
    assert pipeline.publish_calls == 1


def test_read_only_preparation_failure_is_returned_as_explicit_task_state() -> None:
    coordinator = _coordinator(pipeline=_PreparationFailPipeline())
    submission = _submission(idempotency_key="prepare-failed-01")
    config = _config(run_id="prepare-failed")
    result = _execute(
        coordinator,
        submission,
        config=config,
    )
    replay = _execute(coordinator, submission, config=config)

    execution = result.task_executions["data-engineer"]
    assert replay == result
    assert result.status == "failed"
    assert execution.state == "failed"
    assert execution.attempt_count == 1
    assert execution.stop_reason == "read-only preparation failed"
    assert execution.failures == ("read-only candidate preparation failed",)


def test_data_completed_resume_prepares_only_the_software_candidate() -> None:
    pipeline = _CountingPreparePipeline()
    coordinator = _coordinator(pipeline=pipeline)
    submission = _submission(idempotency_key="phase-aware-prepare-01")
    submitted = coordinator.submit(
        submission,
        config=_config(run_id="phase-aware-prepare"),
        actor=_submitter(),
    )
    coordinator.decide_scope(submitted.workflow_id, _scope_decision(), _approver())

    original_prepare = coordinator._software_release.prepare_candidate

    def stop_after_data(*args, **kwargs):
        raise KeyboardInterrupt("process stopped after data completion")

    coordinator._software_release.prepare_candidate = stop_after_data
    with pytest.raises(KeyboardInterrupt, match="after data completion"):
        coordinator.advance(submitted.workflow_id)
    assert coordinator.ledger.get(submitted.workflow_id)["status"] == "data_completed"
    assert pipeline.prepare_calls == 1

    coordinator._software_release.prepare_candidate = original_prepare
    resumed = coordinator.advance(submitted.workflow_id)

    assert resumed.status == "release_pending"
    assert pipeline.prepare_calls == 1


def test_concurrent_phase_calls_do_not_repeat_catalog_or_deployment_mutations() -> None:
    catalog = _CountingCatalog()
    repository = InMemoryArtifactRepository(BASE_SHA)
    deployer = InMemoryDeploymentAdapter(previous_release_sha="0" * 64)
    coordinator = DeliveryCoordinator(
        data_pipeline=DataEngineeringPipeline(catalog),
        software_release=SoftwareReleaseService(repository, deployer),
    )
    submission = _submission(idempotency_key="concurrent-phase-01")
    submitted = coordinator.submit(
        submission,
        config=_config(run_id="concurrent-phase"),
        actor=_submitter(),
    )
    coordinator.decide_scope(submitted.workflow_id, _scope_decision(), _approver())

    with ThreadPoolExecutor(max_workers=2) as executor:
        pending = list(
            executor.map(lambda _: coordinator.advance(submitted.workflow_id), range(2))
        )

    assert pending[0] == pending[1]
    assert catalog.calls == 3
    assert repository.commit_calls == 1
    release = _release_decision(pending[0].prepared_release_sha)
    with ThreadPoolExecutor(max_workers=2) as executor:
        completed = list(
            executor.map(
                lambda _: coordinator.decide_release(
                    submitted.workflow_id, release, _approver()
                ),
                range(2),
            )
        )

    assert completed[0] == completed[1]
    assert deployer.deploy_calls == 1


def test_two_coordinators_treat_an_active_competing_lease_as_in_progress() -> None:
    ledger = InMemoryLedger()
    catalog = _CountingCatalog()
    pipeline = _BlockingPipeline(catalog)
    repository = InMemoryArtifactRepository(BASE_SHA)
    deployer = InMemoryDeploymentAdapter(previous_release_sha="0" * 64)

    def coordinator(instance_id: str) -> DeliveryCoordinator:
        return DeliveryCoordinator(
            coordinator_id=instance_id,
            ledger=ledger,
            data_pipeline=pipeline,
            software_release=SoftwareReleaseService(repository, deployer),
        )

    first = coordinator("coordinator-a")
    second = coordinator("coordinator-b")
    submission = _submission(idempotency_key="cross-coordinator-lease-01")
    submitted = first.submit(
        submission,
        config=_config(run_id="cross-coordinator-lease"),
        actor=_submitter(),
    )
    first.decide_scope(submitted.workflow_id, _scope_decision(), _approver())

    with ThreadPoolExecutor(max_workers=2) as executor:
        first_advance = executor.submit(first.advance, submitted.workflow_id)
        assert pipeline.entered.wait(timeout=2)
        competing = second.advance(submitted.workflow_id)
        pipeline.release.set()
        pending = first_advance.result(timeout=2)

    assert competing.status == "planned"
    assert competing.task_executions["data-engineer"].state == "running"
    assert pending.status == "release_pending"
    assert catalog.calls == 3
    assert second.advance(submitted.workflow_id) == pending


def test_first_exact_release_decision_wins_across_coordinator_instances() -> None:
    ledger = InMemoryLedger()
    repository = InMemoryArtifactRepository(BASE_SHA)
    deployer = InMemoryDeploymentAdapter(previous_release_sha="0" * 64)
    blocking_release = _BlockingRestoreSoftwareRelease(repository, deployer)

    def coordinator(
        instance_id: str,
        software_release: SoftwareReleaseService,
    ) -> DeliveryCoordinator:
        return DeliveryCoordinator(
            coordinator_id=instance_id,
            ledger=ledger,
            data_pipeline=DataEngineeringPipeline(InMemoryCatalogAdapter()),
            software_release=software_release,
        )

    first = coordinator("release-a", blocking_release)
    second = coordinator(
        "release-b", SoftwareReleaseService(repository, deployer)
    )
    submission = _submission(idempotency_key="release-decision-race-01")
    submitted = first.submit(
        submission,
        config=_config(run_id="release-decision-race"),
        actor=_submitter(),
    )
    first.decide_scope(submitted.workflow_id, _scope_decision(), _approver())
    pending = first.advance(submitted.workflow_id)
    approval = _release_decision(pending.prepared_release_sha)
    rejection = _release_decision(
        pending.prepared_release_sha,
        decision_id="release-decision-2",
        decision="rejected",
    )

    with ThreadPoolExecutor(max_workers=2) as executor:
        approving = executor.submit(
            first.decide_release,
            submitted.workflow_id,
            approval,
            _approver(),
        )
        assert blocking_release.restore_entered.wait(timeout=2)
        assert ledger.get(submitted.workflow_id)["status"] == "release_in_progress"
        with pytest.raises(DeliveryError, match="different exact decision"):
            second.decide_release(
                submitted.workflow_id,
                rejection,
                _approver(),
            )
        blocking_release.restore_release.set()
        completed = approving.result(timeout=2)

    assert completed.status == "completed"
    assert deployer.deploy_calls == 1
    event_types = [event.event_type for event in completed.evidence]
    assert event_types.count("release.decision.recorded") == 1
    assert "release.rejected" not in event_types


def test_task_and_run_terminal_state_commit_atomically_after_crash() -> None:
    ledger = _CrashOnceOnAtomicTerminalLedger()
    coordinator = DeliveryCoordinator(
        coordinator_id="terminal-atomicity",
        ledger=ledger,
        data_pipeline=_NonRetryableFailPipeline(),
        software_release=SoftwareReleaseService(
            InMemoryArtifactRepository(BASE_SHA),
            InMemoryDeploymentAdapter(previous_release_sha="0" * 64),
        ),
    )
    submission = _submission(idempotency_key="terminal-atomicity-01")
    submitted = coordinator.submit(
        submission,
        config=_config(run_id="terminal-atomicity"),
        actor=_submitter(),
    )
    coordinator.decide_scope(submitted.workflow_id, _scope_decision(), _approver())

    with pytest.raises(RuntimeError, match="before terminal commit"):
        coordinator.advance(submitted.workflow_id)
    interrupted = ledger.get(submitted.workflow_id)
    assert interrupted["status"] == "planned"
    assert interrupted["task_executions"]["data-engineer"]["state"] == "running"

    result = coordinator.advance(submitted.workflow_id)

    assert result.status == "failed"
    assert result.task_executions["data-engineer"].state == "failed"
    assert [event.event_type for event in result.evidence].count("run.failed") == 1


def test_release_pending_state_rehydrates_after_coordinator_restart() -> None:
    ledger = InMemoryLedger()
    repository = InMemoryArtifactRepository(BASE_SHA)
    deployer = InMemoryDeploymentAdapter(previous_release_sha="0" * 64)
    first = DeliveryCoordinator(
        ledger=ledger,
        data_pipeline=DataEngineeringPipeline(InMemoryCatalogAdapter()),
        software_release=SoftwareReleaseService(repository, deployer),
    )
    submission = _submission(idempotency_key="release-restart-01")
    submitted = first.submit(
        submission,
        config=_config(run_id="release-restart"),
        actor=_submitter(),
    )
    first.decide_scope(submitted.workflow_id, _scope_decision(), _approver())
    pending = first.advance(submitted.workflow_id)

    restarted = DeliveryCoordinator(
        ledger=ledger,
        data_pipeline=DataEngineeringPipeline(InMemoryCatalogAdapter()),
        software_release=SoftwareReleaseService(repository, deployer),
    )
    completed = restarted.decide_release(
        submitted.workflow_id,
        _release_decision(pending.prepared_release_sha),
        _approver(),
    )

    assert completed.status == "completed"
    assert deployer.deploy_calls == 1


def test_lease_claim_conflict_returns_current_state_without_failing_workflow() -> None:
    ledger = InMemoryLedger()
    recovery = _RecordingRecovery(ledger)
    coordinator = DeliveryCoordinator(
        ledger=ledger,
        recovery=recovery,
        data_pipeline=DataEngineeringPipeline(InMemoryCatalogAdapter()),
        software_release=SoftwareReleaseService(
            InMemoryArtifactRepository(BASE_SHA),
            InMemoryDeploymentAdapter(previous_release_sha="0" * 64),
        ),
    )
    submission = _submission(idempotency_key="lease-conflict-01")
    submitted = coordinator.submit(
        submission,
        config=_config(run_id="lease-conflict"),
        actor=_submitter(),
    )
    planned = coordinator.decide_scope(
        submitted.workflow_id,
        _scope_decision(),
        _approver(),
    )
    recovery.claim(
        submitted.workflow_id,
        "data-engineer",
        "another-coordinator",
        lease_seconds=300,
    )

    result = coordinator.advance(planned.workflow_id)

    execution = result.task_executions["data-engineer"]
    assert result.status == "planned"
    assert execution.state == "planned"
    assert execution.stop_reason is None
    assert execution.failures == ()


def test_reference_run_replays_completed_idempotency_key_exactly() -> None:
    coordinator = _coordinator()
    first = _execute(coordinator, _submission())
    replay = _execute(coordinator, _submission())

    assert replay == first


def test_two_workflows_use_distinct_candidate_branches_from_one_base() -> None:
    repository = InMemoryArtifactRepository(BASE_SHA)
    software = SoftwareReleaseService(
        repository,
        InMemoryDeploymentAdapter(previous_release_sha="0" * 64),
    )
    coordinator = _coordinator(software=software)
    first_submission = _submission(idempotency_key="branch-scope-first-01")
    second_submission = _submission(idempotency_key="branch-scope-second-01")

    first_submitted = coordinator.submit(
        first_submission,
        config=_config(run_id="branch-scope-first"),
        actor=_submitter(),
    )
    coordinator.decide_scope(
        first_submitted.workflow_id, _scope_decision(), _approver()
    )
    first = coordinator.advance(first_submitted.workflow_id)

    second_submitted = coordinator.submit(
        second_submission,
        config=_config(run_id="branch-scope-second"),
        actor=_submitter(),
    )
    coordinator.decide_scope(
        second_submitted.workflow_id, _scope_decision(), _approver()
    )
    second = coordinator.advance(second_submitted.workflow_id)

    assert first.status == second.status == "release_pending"
    first_prepared = coordinator.ledger.get(first.workflow_id)["prepared_release"]
    second_prepared = coordinator.ledger.get(second.workflow_id)["prepared_release"]
    assert first_prepared["task"]["artifact_branch"] == (
        f"steward-forge/candidates/{first.workflow_id}"
    )
    assert second_prepared["task"]["artifact_branch"] == (
        f"steward-forge/candidates/{second.workflow_id}"
    )
    assert first_prepared["task"]["artifact_branch"] != (
        second_prepared["task"]["artifact_branch"]
    )
    assert repository.commit_calls == 2


@pytest.mark.parametrize("changed", ["brief", "config", "actor"])
def test_submit_idempotency_key_rejects_different_bound_input(changed: str) -> None:
    coordinator = _coordinator()
    submission = _submission(idempotency_key="submit-binding-01")
    config = _config(run_id="submit-binding")
    actor = _submitter()
    coordinator.submit(submission, config=config, actor=actor)

    if changed == "brief":
        submission = submission.model_copy(update={"title": "Different title"})
    elif changed == "config":
        config = config.model_copy(update={"seed": config.seed + 1})
    else:
        actor = ActorContext(
            subject="different-submitter",
            roles={"submitter", "viewer"},
        )

    with pytest.raises(LedgerConflict, match="different payload"):
        coordinator.submit(submission, config=config, actor=actor)


def test_reference_brief_completes_all_four_workers_with_coherent_evidence() -> None:
    catalog = InMemoryCatalogAdapter()
    repository = InMemoryArtifactRepository(BASE_SHA)
    deployer = InMemoryDeploymentAdapter(previous_release_sha="0" * 64)
    coordinator = DeliveryCoordinator(
        data_pipeline=DataEngineeringPipeline(catalog),
        software_release=SoftwareReleaseService(repository, deployer),
    )

    result = _execute(
        coordinator,
        _submission(),
    )

    assert result.status == "completed"
    assert result.scope is not None
    assert result.plan is not None
    assert result.data_receipt is not None
    assert result.software_receipt is not None
    assert result.task_executions["data-engineer"].state == "succeeded"
    assert result.task_executions["software-engineer"].state == "succeeded"
    assert set(catalog.tables) == set(result.data_receipt.catalog_relations)
    assert deployer.deploy_calls == 1
    assert result.software_receipt.commit_sha
    assert [event.sequence for event in result.evidence] == list(
        range(1, len(result.evidence) + 1)
    )
    assert {event.worker_id for event in result.evidence if event.worker_id} == {
        "product-manager",
        "scrum-master",
        "data-engineer",
        "software-engineer",
    }
    event_types = [event.event_type for event in result.evidence]
    assert event_types.index("scope.approved") < event_types.index("plan.created")
    assert event_types.index("data.release.completed") < event_types.index(
        "software.release.completed"
    )


def test_worker_modules_do_not_import_or_call_peer_workers() -> None:
    workers_root = Path(__file__).parents[1] / "workers"
    worker_packages = {"pm", "sm", "de", "swe"}

    for package in sorted(worker_packages):
        for path in (workers_root / package).glob("*.py"):
            tree = ast.parse(path.read_text())
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom) and node.module:
                    parts = node.module.split(".")
                    if len(parts) >= 2 and parts[0] == "workers":
                        assert parts[1] == package, (
                            f"{path} imports peer worker package {parts[1]}"
                        )
                if isinstance(node, ast.Import):
                    for name in node.names:
                        parts = name.name.split(".")
                        if len(parts) >= 2 and parts[0] == "workers":
                            assert parts[1] == package, (
                                f"{path} imports peer worker package {parts[1]}"
                            )


def test_delivery_task_rejects_unbounded_or_unfunded_work() -> None:
    values = {
        "task_id": "task-1",
        "worker_id": "data-engineer",
        "depends_on": (),
        "max_attempts": 2,
        "budget_usd": 1.0,
        "attempt_cost_usd": 0.5,
        "stop_condition": "Stop after a governed receipt or a deterministic failure.",
        "expected_output": "data receipt",
    }

    with pytest.raises(ValueError):
        DeliveryTask.model_validate(values | {"max_attempts": 0})
    with pytest.raises(ValueError, match="attempt cost cannot exceed task budget"):
        DeliveryTask.model_validate(values | {"attempt_cost_usd": 2.0})
