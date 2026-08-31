"""Deterministic PM-to-SM-to-DE-to-SWE reference orchestration."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from threading import Lock, RLock
from typing import Any, TypeVar
from uuid import uuid4
from weakref import WeakValueDictionary

from data.generators import build_namespace, build_target_relations
from gates.swe.release import SoftwareReleaseService
from identity import AccessDenied, ActorContext, AuthorizationPolicy
from ledger import InMemoryLedger, Ledger
from orchestrator.delivery_models import (
    DeliveryEvidence,
    DeliveryRunResult,
    DeliveryTask,
    EscalationEvent,
    ProductScope,
    ReferenceRunConfig,
    ScrumPlan,
    TaskExecution,
)
from orchestrator.models import (
    BriefSubmission,
    ReleaseDecision,
    ScopeDecision,
)
from pipeline import DataEngineeringPipeline
from recovery import (
    InMemoryRevocationLayer,
    LeaseRejected,
    RecoveryController,
    WorkerLease,
)
from workers.de.models import DataEngineerTask
from workers.pm import ProductManagerWorker
from workers.sm import ScrumMasterWorker
from workers.swe.models import (
    PreparedSoftwareRelease,
    SoftwareEngineerTask,
    SoftwareReleaseApproval,
)

T = TypeVar("T")


class DeliveryError(ValueError):
    """The four-worker delivery contract could not proceed."""


class PreparationFailed(DeliveryError):
    """One or more read-only preparation jobs failed."""

    def __init__(self, failures: dict[str, Exception]) -> None:
        super().__init__("read-only preparation failed")
        self.failures = failures


class ExecutionLanes:
    """Concurrent read-only preparation and one serialized mutation lane."""

    def __init__(self, *, max_read_workers: int = 2) -> None:
        if max_read_workers < 1:
            raise ValueError("max_read_workers must be positive")
        self._max_read_workers = max_read_workers
        self._mutation_lock = RLock()

    def prepare_concurrently(
        self, jobs: Mapping[str, Callable[[], T]]
    ) -> dict[str, T]:
        if not jobs:
            return {}
        with ThreadPoolExecutor(
            max_workers=min(self._max_read_workers, len(jobs))
        ) as executor:
            futures = {name: executor.submit(job) for name, job in jobs.items()}
            results: dict[str, T] = {}
            failures: dict[str, Exception] = {}
            for name, future in futures.items():
                try:
                    results[name] = future.result()
                except Exception as error:
                    failures[name] = error
            if failures:
                raise PreparationFailed(failures)
            return results

    def mutate(self, operation: Callable[[], T]) -> T:
        with self._mutation_lock:
            return operation()


class DeliveryCoordinator:
    """Own approvals, retries, budgets, transitions, and ordered evidence."""

    LEASE_SECONDS = 300

    def __init__(
        self,
        *,
        data_pipeline: DataEngineeringPipeline,
        software_release: SoftwareReleaseService,
        ledger: Ledger | None = None,
        product_manager: ProductManagerWorker | None = None,
        scrum_master: ScrumMasterWorker | None = None,
        lanes: ExecutionLanes | None = None,
        recovery: RecoveryController | None = None,
        coordinator_id: str | None = None,
    ) -> None:
        self.ledger = ledger or InMemoryLedger()
        self._data_pipeline = data_pipeline
        self._software_release = software_release
        self._product_manager = product_manager or ProductManagerWorker()
        self._scrum_master = scrum_master or ScrumMasterWorker()
        self._lanes = lanes or ExecutionLanes()
        self._coordinator_id = (coordinator_id or uuid4().hex).strip()
        if not self._coordinator_id:
            raise ValueError("coordinator_id must not be empty")
        self._phase_lock_guard = Lock()
        self._phase_locks: WeakValueDictionary[str, RLock] = WeakValueDictionary()
        self._policy = AuthorizationPolicy()
        self._recovery = recovery or RecoveryController(
            self.ledger,
            layers={
                name: InMemoryRevocationLayer(name)
                for name in RecoveryController.REQUIRED_LAYERS
            },
        )

    def execute(
        self,
        submission: BriefSubmission,
        *,
        config: ReferenceRunConfig,
        submitter: ActorContext,
        scope_decision: ScopeDecision,
        scope_approver: ActorContext,
        release_decision_provider: Callable[[str], ReleaseDecision],
        release_approver: ActorContext,
    ) -> DeliveryRunResult:
        """Run the resumable phases with caller-supplied human decisions."""

        current = self.submit(submission, config=config, actor=submitter)
        if current.status in self._terminal_statuses():
            return current
        if current.status in {"scope_pending", "scope_approved"}:
            current = self.decide_scope(
                current.workflow_id,
                scope_decision,
                scope_approver,
            )
        if current.status in self._terminal_statuses():
            return current
        if current.status in {"planned", "data_completed"}:
            current = self.advance(current.workflow_id)
        if current.status in self._terminal_statuses():
            return current
        if current.status not in {"release_pending", "release_in_progress"}:
            raise DeliveryError(
                f"delivery paused in unsupported state: {current.status}"
            )
        if current.prepared_release_sha is None:
            raise DeliveryError("release-pending state has no candidate SHA")
        release_decision = release_decision_provider(
            current.prepared_release_sha
        )
        return self.decide_release(
            current.workflow_id,
            release_decision,
            release_approver,
        )

    def submit(
        self,
        submission: BriefSubmission,
        *,
        config: ReferenceRunConfig,
        actor: ActorContext,
    ) -> DeliveryRunResult:
        """Persist a brief and return the Product Manager proposal."""

        self._policy.require_submit(actor)
        brief_id = hashlib.sha256(
            f"delivery:{submission.idempotency_key}".encode()
        ).hexdigest()[:24]
        submitted_evidence = DeliveryEvidence(
            sequence=1,
            event_type="brief.submitted",
            details={"submitter": actor.subject, "run_id": config.run_id},
        )
        initial_state: dict[str, Any] = {
            "id": brief_id,
            "status": "submitting",
            "submitted_by": actor.subject,
            "brief": submission.model_dump(mode="json"),
            "config": config.model_dump(mode="json"),
            "scope": None,
            "plan": None,
            "task_executions": {},
            "data_receipt": None,
            "prepared_release": None,
            "software_receipt": None,
            "decisions": {},
            "release_decision": None,
            "delivery_evidence": [submitted_evidence.model_dump(mode="json")],
            "events": [],
        }
        self.ledger.create(
            submission.idempotency_key, initial_state
        )
        return self._ensure_scope_proposal(brief_id, submission)

    def _ensure_scope_proposal(
        self,
        brief_id: str,
        submission: BriefSubmission,
    ) -> DeliveryRunResult:
        state = self.ledger.get(brief_id)
        resumable_statuses = {"submitting", "scope_pending"}
        if state["status"] not in resumable_statuses or state["scope"] is not None:
            return self._render(brief_id)
        try:
            proposed = self._product_manager.propose(brief_id, submission)
            scope = ProductScope.model_validate(
                self._serialized_worker_output(proposed)
            )
            if scope.brief_id != brief_id:
                raise DeliveryError("scope does not match the submitted brief")
        except Exception as error:
            reason = str(error) or type(error).__name__
            with self.ledger.transaction(brief_id) as current:
                if (
                    current["status"] in resumable_statuses
                    and current["scope"] is None
                ):
                    current["status"] = "failed"
                    self._append_evidence(
                        current,
                        "scope.proposal.failed",
                        worker_id="product-manager",
                        details={"failure": reason},
                    )
            return self._render(brief_id)
        with self.ledger.transaction(brief_id) as current:
            if (
                current["status"] in resumable_statuses
                and current["scope"] is None
            ):
                current["scope"] = scope.model_dump(mode="json")
                current["status"] = "scope_pending"
                self._append_evidence(
                    current,
                    "scope.proposed",
                    worker_id="product-manager",
                    details={
                        "contract_id": self._product_manager.contract_id,
                        "contract_version": self._product_manager.contract_version,
                        "scope_version": scope.scope_version,
                    },
                )
        return self._render(brief_id)

    def decide_scope(
        self,
        brief_id: str,
        decision: ScopeDecision,
        actor: ActorContext,
    ) -> DeliveryRunResult:
        """Persist an explicit version-bound scope decision and build the plan."""

        with self._phase_lock(brief_id):
            return self._decide_scope(brief_id, decision, actor)

    def _decide_scope(
        self,
        brief_id: str,
        decision: ScopeDecision,
        actor: ActorContext,
    ) -> DeliveryRunResult:

        try:
            with self.ledger.transaction(brief_id) as state:
                scope = ProductScope.model_validate(state["scope"])
                self._policy.require_approval(actor, state)
                if actor.subject == scope.proposed_by:
                    raise AccessDenied("scope author cannot approve its own scope")
                payload = decision.model_dump(mode="json") | {
                    "actor": actor.subject
                }
                existing = state["decisions"].get(decision.decision_id)
                if existing is not None and existing != payload:
                    raise DeliveryError(
                        "scope decision ID is bound to different content"
                    )
                if existing is None:
                    if state["status"] != "scope_pending":
                        raise DeliveryError(
                            "scope decision is not valid in the current state"
                        )
                    if decision.scope_version != scope.scope_version:
                        raise DeliveryError(
                            "scope decision does not match the pending version"
                        )
                    state["decisions"][decision.decision_id] = payload
                    if decision.decision == "rejected":
                        state["status"] = "scope_rejected"
                        self._append_evidence(
                            state,
                            "scope.rejected",
                            details={
                                "actor": actor.subject,
                                "decision_id": decision.decision_id,
                                "scope_version": decision.scope_version,
                            },
                        )
                        self._append_evidence(state, "run.scope-rejected")
                    else:
                        state["status"] = "scope_approved"
                        self._append_evidence(
                            state,
                            "scope.approved",
                            details={
                                "actor": actor.subject,
                                "decision_id": decision.decision_id,
                                "scope_version": decision.scope_version,
                            },
                        )
                current_status = state["status"]
        except (AccessDenied, DeliveryError) as error:
            self._record_security_denial(brief_id, actor, "scope", error)
            raise

        if current_status != "scope_approved":
            return self._render(brief_id)
        state = self.ledger.get(brief_id)
        submission = BriefSubmission.model_validate(state["brief"])
        approved_scope_payload = state["scope"]
        scope = ProductScope.model_validate(approved_scope_payload)
        try:
            proposed_plan = self._scrum_master.plan_delivery(
                scope, submission.cost_ceiling_usd
            )
            plan = self._validate_plan(
                proposed_plan,
                scope,
                submission.cost_ceiling_usd,
            )
        except Exception as error:
            reason = str(error) or type(error).__name__
            with self.ledger.transaction(brief_id) as current:
                if current["status"] == "scope_approved":
                    current["status"] = "failed"
                    self._append_evidence(
                        current,
                        "plan.rejected",
                        worker_id="scrum-master",
                        details={"failure": reason},
                    )
            return self._render(brief_id)
        executions = {
            task.worker_id: self._new_execution(task).model_dump(mode="json")
            for task in plan.tasks
        }
        with self.ledger.transaction(brief_id) as state:
            if state["status"] not in {"scope_approved", "planned"}:
                raise DeliveryError("scope is no longer available for planning")
            if state["status"] == "scope_approved":
                state.update(
                    {
                        "status": "planned",
                        "plan": plan.model_dump(mode="json"),
                        "task_executions": executions,
                    }
                )
                self._append_evidence(
                    state,
                    "plan.created",
                    worker_id="scrum-master",
                    details={
                        "plan_id": plan.plan_id,
                        "contract_id": self._scrum_master.contract_id,
                        "contract_version": self._scrum_master.contract_version,
                        "task_count": len(plan.tasks),
                    },
                )
        return self._render(brief_id)

    def advance(self, brief_id: str) -> DeliveryRunResult:
        """Advance approved work until an explicit release decision is needed."""

        with self._phase_lock(brief_id):
            return self._advance(brief_id)

    def _advance(self, brief_id: str) -> DeliveryRunResult:

        state = self.ledger.get(brief_id)
        if state["status"] in self._terminal_statuses() | {
            "release_pending",
            "release_in_progress",
        }:
            return self._render(brief_id)
        if state["status"] not in {"planned", "data_completed"}:
            raise DeliveryError("delivery cannot advance from the current state")
        submission = BriefSubmission.model_validate(state["brief"])
        config = ReferenceRunConfig.model_validate(state["config"])
        plan = ScrumPlan.model_validate(state["plan"])
        data_task, software_task = self._specialist_tasks(
            brief_id,
            submission,
            config,
            plan,
            submitted_by=str(state["submitted_by"]),
        )
        preparation_jobs: dict[str, Callable[[], Any]] = {
            "software-engineer": lambda: self._software_release.draft(
                software_task
            )
        }
        if state["status"] == "planned":
            preparation_jobs["data-engineer"] = lambda: self._data_pipeline.prepare(
                data_task
            )
        self._record(
            brief_id,
            "preparation.started",
            details={"lane": "read-only", "job_count": len(preparation_jobs)},
        )
        try:
            prepared = self._lanes.prepare_concurrently(preparation_jobs)
        except PreparationFailed as error:
            self._record_preparation_failures(brief_id, plan, error.failures)
            return self._render(brief_id)
        for task in plan.tasks:
            if task.worker_id not in prepared:
                continue
            self._record(
                brief_id,
                "task.prepared",
                worker_id=task.worker_id,
                task_id=task.task_id,
                details={"lane": "read-only"},
            )

        if state["status"] == "planned":
            publish_session = None

            def publish_data(lease: WorkerLease):
                nonlocal publish_session
                if publish_session is None:
                    publish_session = self._data_pipeline.begin_publish(
                        data_task,
                        lease_fence=self._recovery.lease_fence,
                    )
                return self._data_pipeline.publish(
                    data_task,
                    prepared["data-engineer"],
                    session=publish_session,
                    lease_owner=lease.owner,
                    lease_epoch=lease.epoch,
                )

            def commit_data(result, delivery_state: dict[str, Any]) -> None:
                delivery_state["data_receipt"] = result.receipt.model_dump(
                    mode="json"
                )
                delivery_state["status"] = "data_completed"
                self._append_evidence(
                    delivery_state,
                    "data.release.completed",
                    worker_id="data-engineer",
                    task_id=plan.tasks[0].task_id,
                    details={"receipt_id": result.receipt.receipt_id},
                )

            data_execution, data_result = self._run_task(
                brief_id,
                plan.tasks[0],
                publish_data,
                on_success=commit_data,
            )
            if data_execution.state != "succeeded" or data_result is None:
                return self._render(brief_id)

        return self._prepare_software_release(
            brief_id,
            plan,
            software_task,
            prepared["software-engineer"],
        )

    def _prepare_software_release(
        self,
        brief_id: str,
        plan: ScrumPlan,
        task: SoftwareEngineerTask,
        candidate,
    ) -> DeliveryRunResult:
        """Commit and gate the candidate under a lease, then await a human."""

        current = self.ledger.get(brief_id)
        if current["status"] in {"release_pending", "release_in_progress"}:
            return self._render(brief_id)
        if current["status"] != "data_completed":
            raise DeliveryError(
                "software candidate cannot be prepared before data completion"
            )
        delivery_task = plan.tasks[1]

        def prepare_candidate(lease: WorkerLease):
            prepared = self._software_release.prepare_candidate(
                task,
                candidate,
                lease_owner=lease.owner,
                lease_epoch=lease.epoch,
                lease_fence=self._recovery.lease_fence,
            )
            self._recovery.validate_receipt(
                brief_id, prepared.broker_receipt
            )
            return prepared

        def commit_prepared(prepared, state: dict[str, Any]) -> None:
            if state["status"] != "data_completed":
                raise DeliveryError(
                    "software candidate state changed before commit"
                )
            state["status"] = "release_pending"
            state["prepared_release"] = prepared.model_dump(mode="json")
            self._append_evidence(
                state,
                "software.release.prepared",
                worker_id="software-engineer",
                task_id=task.task_id,
                details={
                    "commit_sha": prepared.commit.commit_sha,
                    "broker_receipt_id": prepared.broker_receipt.receipt_id,
                },
            )

        self._run_task(
            brief_id,
            delivery_task,
            prepare_candidate,
            on_success=commit_prepared,
            expected_step="planned",
            success_state="awaiting_approval",
            success_stop_reason="awaiting explicit release decision",
            attempt_counter="preparation_attempt_count",
            phase="candidate-preparation",
        )
        return self._render(brief_id)

    def decide_release(
        self,
        brief_id: str,
        decision: ReleaseDecision,
        actor: ActorContext,
    ) -> DeliveryRunResult:
        """Apply an explicit SHA-bound release decision and finish the run."""

        with self._phase_lock(brief_id):
            return self._decide_release(brief_id, decision, actor)

    def _decide_release(
        self,
        brief_id: str,
        decision: ReleaseDecision,
        actor: ActorContext,
    ) -> DeliveryRunResult:

        payload = decision.model_dump(mode="json") | {"actor": actor.subject}
        try:
            with self.ledger.transaction(brief_id) as state:
                self._policy.require_release(actor, state)
                bound_decision = state.get("release_decision")
                if state["status"] in self._terminal_statuses():
                    if bound_decision != payload:
                        raise DeliveryError(
                            "release is already bound to a different exact decision"
                        )
                    terminal_replay = True
                    rejected = state["status"] == "release_rejected"
                    prepared = None
                else:
                    terminal_replay = False
                    if state["status"] not in {
                        "release_pending",
                        "release_in_progress",
                    }:
                        raise DeliveryError(
                            "release decision is not valid in current state"
                        )
                    prepared = PreparedSoftwareRelease.model_validate(
                        state["prepared_release"]
                    )
                    if decision.commit_sha != prepared.commit.commit_sha:
                        raise DeliveryError(
                            "release decision does not match the prepared candidate SHA"
                        )
                    existing = state["decisions"].get(decision.decision_id)
                    if existing is not None and existing != payload:
                        raise DeliveryError(
                            "release decision ID is bound to different content"
                        )
                    if bound_decision is not None and bound_decision != payload:
                        raise DeliveryError(
                            "release is already bound to a different exact decision"
                        )
                    if bound_decision is None:
                        if state["status"] != "release_pending":
                            raise DeliveryError(
                                "release-in-progress state has no bound decision"
                            )
                        state["release_decision"] = payload
                        state["decisions"][decision.decision_id] = payload
                        self._append_evidence(
                            state,
                            "release.decision.recorded",
                            worker_id="software-engineer",
                            task_id=prepared.task.task_id,
                            details={
                                "actor": actor.subject,
                                "decision": decision.decision,
                                "decision_id": decision.decision_id,
                                "commit_sha": decision.commit_sha,
                            },
                        )
                    rejected = payload["decision"] == "rejected"
                    if rejected:
                        state["status"] = "release_rejected"
                        execution = TaskExecution.model_validate(
                            state["task_executions"]["software-engineer"]
                        ).model_copy(
                            update={
                                "state": "failed",
                                "stop_reason": "release approval was rejected",
                            }
                        )
                        state["task_executions"]["software-engineer"] = (
                            execution.model_dump(mode="json")
                        )
                        self._append_evidence(
                            state,
                            "release.rejected",
                            worker_id="software-engineer",
                            task_id=prepared.task.task_id,
                            details={
                                "actor": actor.subject,
                                "decision_id": decision.decision_id,
                                "commit_sha": decision.commit_sha,
                            },
                        )
                        self._append_evidence(state, "run.release-rejected")
                    else:
                        state["status"] = "release_in_progress"
        except (AccessDenied, DeliveryError) as error:
            self._record_security_denial(brief_id, actor, "release", error)
            raise

        if terminal_replay or rejected:
            return self._render(brief_id)
        if prepared is None:
            raise DeliveryError("release decision lost its prepared candidate")
        self._software_release.restore_prepared(prepared)

        approval = SoftwareReleaseApproval(
            decision_id=decision.decision_id,
            decision=decision.decision,
            approved_sha=decision.commit_sha,
        )

        def release_software(lease: WorkerLease):
            with self._recovery.worker_fence(
                brief_id,
                "software-engineer",
                lease.owner,
                lease.epoch,
            ):
                return self._software_release.release(
                    prepared,
                    approval,
                    actor,
                    idempotency_key=f"{brief_id}:deploy:v1",
                )

        def commit_software(receipt, delivery_state: dict[str, Any]) -> None:
            if (
                delivery_state["status"] != "release_in_progress"
                or delivery_state.get("release_decision") != payload
            ):
                raise DeliveryError(
                    "release completion no longer matches its exact decision binding"
                )
            delivery_state["software_receipt"] = receipt.model_dump(mode="json")
            delivery_state["status"] = "completed"
            self._append_evidence(
                delivery_state,
                "software.release.completed",
                worker_id="software-engineer",
                task_id=prepared.task.task_id,
                details={
                    "receipt_id": receipt.receipt_id,
                    "approval_actor": actor.subject,
                    "commit_sha": receipt.commit_sha,
                },
            )
            self._append_evidence(delivery_state, "run.completed")

        self._run_task(
            brief_id,
            ScrumPlan.model_validate(
                self.ledger.get(brief_id)["plan"]
            ).tasks[1],
            release_software,
            on_success=commit_software,
            expected_step="awaiting_approval",
        )
        return self._render(brief_id)

    def _run_task(
        self,
        brief_id: str,
        task: DeliveryTask,
        operation: Callable[[WorkerLease], T],
        *,
        on_success: Callable[[T, dict[str, Any]], None],
        expected_step: str = "planned",
        success_state: str = "succeeded",
        success_stop_reason: str | None = None,
        attempt_counter: str = "attempt_count",
        phase: str = "execution",
    ) -> tuple[TaskExecution, T | None]:
        if attempt_counter not in {"attempt_count", "preparation_attempt_count"}:
            raise DeliveryError("unsupported task attempt counter")
        execution = TaskExecution.model_validate(
            self.ledger.get(brief_id)["task_executions"][task.worker_id]
        )
        try:
            lease = self._recovery.claim(
                brief_id,
                task.worker_id,
                self._lease_owner(brief_id),
                lease_seconds=self.LEASE_SECONDS,
            )
        except LeaseRejected as error:
            if str(error) != "worker already has an active lease":
                return self._record_lease_claim_failure(
                    brief_id, task, execution, error
                ), None
            current = TaskExecution.model_validate(
                self.ledger.get(brief_id)["task_executions"][task.worker_id]
            )
            return current, None
        except Exception as error:
            return self._record_lease_claim_failure(
                brief_id, task, execution, error
            ), None
        while getattr(execution, attempt_counter) < task.max_attempts:
            task_remaining = task.budget_usd - execution.budget_consumed_usd
            if task_remaining + 1e-9 < task.attempt_cost_usd:
                return self._stop_task_for_budget(
                    brief_id,
                    execution,
                    lease,
                    expected_step=expected_step,
                    reason="insufficient task budget for another attempt",
                )
            state = self.ledger.get(brief_id)
            brief_ceiling = BriefSubmission.model_validate(
                state["brief"]
            ).cost_ceiling_usd
            total_consumed = sum(
                TaskExecution.model_validate(value).budget_consumed_usd
                for value in state["task_executions"].values()
            )
            if total_consumed + task.attempt_cost_usd > brief_ceiling + 1e-9:
                return self._stop_task_for_budget(
                    brief_id,
                    execution,
                    lease,
                    expected_step=expected_step,
                    reason="brief cost ceiling would be exceeded",
                )
            attempt = getattr(execution, attempt_counter) + 1
            consumed = execution.budget_consumed_usd + task.attempt_cost_usd
            execution = execution.model_copy(
                update={
                    "state": "running",
                    attempt_counter: attempt,
                    "budget_consumed_usd": consumed,
                    "budget_remaining_usd": max(task.budget_usd - consumed, 0),
                }
            )
            self._update_execution_and_record(
                brief_id,
                execution,
                "task.attempt.started",
                worker_id=task.worker_id,
                task_id=task.task_id,
                details={
                    "attempt": attempt,
                    "phase": phase,
                    "budget_consumed_usd": execution.budget_consumed_usd,
                    "budget_remaining_usd": execution.budget_remaining_usd,
                },
            )
            try:
                result = self._lanes.mutate(lambda: operation(lease))
            except Exception as error:
                reason = str(error) or type(error).__name__
                execution = execution.model_copy(
                    update={"failures": (*execution.failures, reason)}
                )
                escalation_details = self._canonical_escalation(
                    task,
                    attempt=attempt,
                    reason=reason,
                )
                self._recovery.write_worker_state(
                    brief_id,
                    task.worker_id,
                    lease.owner,
                    lease.epoch,
                    execution.model_dump(mode="json"),
                )
                checkpoint = self._recovery.checkpoint(
                    brief_id,
                    task.worker_id,
                    lease.owner,
                    lease.epoch,
                    checkpoint_id=f"{task.task_id}:{phase}:attempt:{attempt}",
                    payload={
                        "attempt": attempt,
                        "phase": phase,
                        "failure": reason,
                        "next_action": "orchestrator-retry-decision",
                    },
                )
                self._record(
                    brief_id,
                    "task.escalated",
                    worker_id=task.worker_id,
                    task_id=task.task_id,
                    details=escalation_details,
                )
                self._record(
                    brief_id,
                    "task.checkpointed",
                    worker_id=task.worker_id,
                    task_id=task.task_id,
                    details={
                        "checkpoint_id": checkpoint.checkpoint_id,
                        "lease_epoch": checkpoint.lease_epoch,
                        "phase": phase,
                    },
                )
                terminal: tuple[str, str] | None = None
                if not isinstance(error, (TimeoutError, ConnectionError)):
                    terminal = ("failed", "non-retryable failure")
                elif attempt >= task.max_attempts:
                    reason = (
                        "maximum attempts exhausted"
                        if phase == "execution"
                        else f"maximum {phase} attempts exhausted"
                    )
                    terminal = ("failed", reason)
                elif (
                    execution.budget_remaining_usd + 1e-9
                    < task.attempt_cost_usd
                ):
                    terminal = (
                        "budget_stopped",
                        "insufficient task budget for another attempt",
                    )
                if terminal is None:
                    self._persist_execution(brief_id, execution)
                    continue
                execution = execution.model_copy(
                    update={"state": terminal[0], "stop_reason": terminal[1]}
                )
                self._complete_task_transition(
                    brief_id,
                    execution,
                    lease,
                    expected_step=expected_step,
                    commit_binding={
                        "state": execution.state,
                        "stop_reason": execution.stop_reason,
                        "failures": execution.failures,
                    },
                )
                return execution, None

            execution = execution.model_copy(
                update={
                    "state": success_state,
                    "stop_reason": success_stop_reason or task.stop_condition,
                }
            )

            def commit_result(
                state: dict[str, Any], committed_result: T = result
            ) -> None:
                on_success(committed_result, state)

            self._complete_task_transition(
                brief_id,
                execution,
                lease,
                expected_step=expected_step,
                commit_binding=self._result_binding(result),
                on_success=commit_result,
            )
            return execution, result

        exhausted = execution.model_copy(
            update={
                "state": "failed",
                "stop_reason": (
                    "maximum attempts exhausted"
                    if phase == "execution"
                    else f"maximum {phase} attempts exhausted"
                ),
            }
        )
        self._complete_task_transition(
            brief_id,
            exhausted,
            lease,
            expected_step=expected_step,
            commit_binding={
                "state": exhausted.state,
                "stop_reason": exhausted.stop_reason,
                "failures": exhausted.failures,
            },
        )
        return exhausted, None

    def _record_preparation_failures(
        self,
        brief_id: str,
        plan: ScrumPlan,
        failures: dict[str, Exception],
    ) -> None:
        tasks = {task.worker_id: task for task in plan.tasks}
        with self.ledger.transaction(brief_id) as state:
            for worker_id, error in failures.items():
                task = tasks[worker_id]
                reason = str(error) or type(error).__name__
                execution = self._new_execution(task).model_copy(
                    update={
                        "state": "failed",
                        "attempt_count": 1,
                        "budget_consumed_usd": task.attempt_cost_usd,
                        "budget_remaining_usd": max(
                            task.budget_usd - task.attempt_cost_usd, 0
                        ),
                        "stop_reason": "read-only preparation failed",
                        "failures": (reason,),
                    }
                )
                self._store_execution(state, execution)
                self._append_evidence(
                    state,
                    "task.failed",
                    worker_id=task.worker_id,
                    task_id=task.task_id,
                    details={
                        "phase": "read-only-preparation",
                        "attempt_count": 1,
                        "failure": reason,
                        "stop_reason": execution.stop_reason,
                    },
                )
            self._set_terminal_status(state, "failed")

    def _complete_task_transition(
        self,
        brief_id: str,
        execution: TaskExecution,
        lease,
        *,
        expected_step: str,
        commit_binding: Mapping[str, Any],
        on_success: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        def commit_delivery_state(state: dict[str, Any]) -> None:
            self._store_execution(state, execution)
            self._append_evidence(
                state,
                f"task.{execution.state.replace('_', '-')}",
                worker_id=execution.worker_id,
                task_id=execution.task_id,
                details={
                    "attempt_count": execution.attempt_count,
                    "budget_consumed_usd": execution.budget_consumed_usd,
                    "budget_remaining_usd": execution.budget_remaining_usd,
                    "stop_reason": execution.stop_reason,
                },
            )
            if on_success is not None:
                on_success(state)
            if execution.state in {"failed", "budget_stopped"}:
                self._set_terminal_status(state, execution.state)

        self._recovery.transition(
            brief_id,
            execution.worker_id,
            lease.owner,
            lease.epoch,
            transition_id=f"{execution.task_id}:{execution.state}",
            expected_step=expected_step,
            next_step=execution.state,
            worker_state_updates=execution.model_dump(mode="json"),
            commit_binding=commit_binding,
            on_commit=commit_delivery_state,
            release_lease=True,
        )

    def _record_lease_claim_failure(
        self,
        brief_id: str,
        task: DeliveryTask,
        execution: TaskExecution,
        error: Exception,
    ) -> TaskExecution:
        reason = str(error) or type(error).__name__
        failed = execution.model_copy(
            update={
                "state": "failed",
                "stop_reason": "worker lease claim failed",
                "failures": (*execution.failures, reason),
            }
        )
        with self.ledger.transaction(brief_id) as state:
            self._store_execution(state, failed)
            self._append_evidence(
                state,
                "task.failed",
                worker_id=task.worker_id,
                task_id=task.task_id,
                details={
                    "phase": "lease-claim",
                    "failure": reason,
                    "stop_reason": failed.stop_reason,
                },
            )
            self._set_terminal_status(state, "failed")
        return failed

    @staticmethod
    def _validate_plan(
        plan: Any,
        scope: ProductScope,
        cost_ceiling_usd: float,
    ) -> ScrumPlan:
        plan = ScrumPlan.model_validate(
            DeliveryCoordinator._serialized_worker_output(plan)
        )
        if plan.brief_id != scope.brief_id:
            raise DeliveryError("plan does not match the approved brief")
        if plan.scope_version != scope.scope_version:
            raise DeliveryError("plan does not match the approved scope version")
        if plan.approved_scope_sha256 != scope.fingerprint():
            raise DeliveryError("plan does not match the exact approved scope")
        if len({task.task_id for task in plan.tasks}) != len(plan.tasks):
            raise DeliveryError("plan task IDs must be unique")
        total_budget = sum(task.budget_usd for task in plan.tasks)
        if total_budget > cost_ceiling_usd + 1e-9:
            raise DeliveryError("plan budget exceeds the brief cost ceiling")
        return plan

    @staticmethod
    def _serialized_worker_output(value: Any) -> Any:
        if isinstance(value, Mapping):
            return dict(value)
        model_dump = getattr(value, "model_dump", None)
        if model_dump is None:
            return value
        return model_dump(mode="json")

    def _lease_owner(self, brief_id: str) -> str:
        return f"delivery-coordinator:{self._coordinator_id}:{brief_id}"

    def _canonical_escalation(
        self,
        task: DeliveryTask,
        *,
        attempt: int,
        reason: str,
    ) -> dict[str, Any]:
        canonical = EscalationEvent(
            task_id=task.task_id,
            worker_id=task.worker_id,
            attempt=attempt,
            reason=reason,
        )
        try:
            worker_report = self._scrum_master.escalate(
                task,
                attempt=attempt,
                reason=reason,
            )
            validated = EscalationEvent.model_validate(
                self._serialized_worker_output(worker_report)
            )
        except Exception:
            report_status = "invalid"
        else:
            report_status = "accepted" if validated == canonical else "canonicalized"
        return canonical.model_dump(mode="json") | {
            "worker_report_status": report_status
        }

    @staticmethod
    def _result_binding(result: T) -> Mapping[str, Any]:
        receipt = getattr(result, "receipt", None)
        if receipt is not None and hasattr(receipt, "model_dump"):
            return {"receipt": receipt.model_dump(mode="json")}
        if hasattr(result, "model_dump"):
            return {"result": result.model_dump(mode="json")}
        raise DeliveryError("task result has no canonical commit binding")

    @staticmethod
    def _new_execution(task: DeliveryTask) -> TaskExecution:
        return TaskExecution(
            task_id=task.task_id,
            worker_id=task.worker_id,
            max_attempts=task.max_attempts,
            budget_usd=task.budget_usd,
            budget_remaining_usd=task.budget_usd,
        )

    def _specialist_tasks(
        self,
        brief_id: str,
        submission: BriefSubmission,
        config: ReferenceRunConfig,
        plan: ScrumPlan,
        *,
        submitted_by: str,
    ) -> tuple[DataEngineerTask, SoftwareEngineerTask]:
        data_task = DataEngineerTask(
            task_id=plan.tasks[0].task_id,
            brief_id=brief_id,
            run_id=config.run_id,
            seed=config.seed,
            sandbox_catalog=config.sandbox_catalog,
            sandbox_schema=config.sandbox_schema,
        )
        source_tables = build_target_relations(
            config.sandbox_catalog,
            config.sandbox_schema,
            build_namespace(brief_id, config.run_id),
        )
        software_task = SoftwareEngineerTask(
            task_id=plan.tasks[1].task_id,
            brief_id=brief_id,
            submitted_by=submitted_by,
            release_approver=submission.release_approver,
            sandbox_catalog=config.sandbox_catalog,
            sandbox_schema=config.sandbox_schema,
            generated_prefix=config.generated_prefix,
            artifact_branch=(
                f"{config.artifact_branch.rstrip('/')}/{brief_id}"
            ),
            trusted_base_sha=config.trusted_base_sha,
            dashboard_title=config.dashboard_title,
            source_tables=source_tables,
        )
        return data_task, software_task

    def _persist_execution(
        self, brief_id: str, execution: TaskExecution
    ) -> None:
        with self.ledger.transaction(brief_id) as state:
            self._store_execution(state, execution)

    def _update_execution_and_record(
        self,
        brief_id: str,
        execution: TaskExecution,
        event_type: str,
        *,
        worker_id: str | None = None,
        task_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        with self.ledger.transaction(brief_id) as state:
            self._store_execution(state, execution)
            self._append_evidence(
                state,
                event_type,
                worker_id=worker_id,
                task_id=task_id,
                details=details,
            )

    def _update_and_record(
        self,
        brief_id: str,
        event_type: str,
        *,
        updates: dict[str, Any],
        worker_id: str | None = None,
        task_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        with self.ledger.transaction(brief_id) as state:
            state.update(updates)
            self._append_evidence(
                state,
                event_type,
                worker_id=worker_id,
                task_id=task_id,
                details=details,
            )

    def _record(
        self,
        brief_id: str,
        event_type: str,
        *,
        worker_id: str | None = None,
        task_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        self._update_and_record(
            brief_id,
            event_type,
            updates={},
            worker_id=worker_id,
            task_id=task_id,
            details=details,
        )

    def _stop_task_for_budget(
        self,
        brief_id: str,
        execution: TaskExecution,
        lease: WorkerLease,
        *,
        expected_step: str,
        reason: str,
    ) -> tuple[TaskExecution, None]:
        stopped = execution.model_copy(
            update={"state": "budget_stopped", "stop_reason": reason}
        )
        self._complete_task_transition(
            brief_id,
            stopped,
            lease,
            expected_step=expected_step,
            commit_binding={
                "state": stopped.state,
                "stop_reason": stopped.stop_reason,
            },
        )
        return stopped, None

    def _record_security_denial(
        self,
        brief_id: str,
        actor: ActorContext,
        gate: str,
        error: Exception,
    ) -> None:
        self._record(
            brief_id,
            "security.denied",
            details={
                "actor": actor.subject,
                "gate": gate,
                "reason": str(error),
            },
        )

    @staticmethod
    def _store_execution(state: dict[str, Any], execution: TaskExecution) -> None:
        state["task_executions"][execution.worker_id] = execution.model_dump(mode="json")

    @staticmethod
    def _append_evidence(
        state: dict[str, Any],
        event_type: str,
        *,
        worker_id: str | None = None,
        task_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        evidence = state["delivery_evidence"]
        event = DeliveryEvidence(
            sequence=len(evidence) + 1,
            event_type=event_type,
            worker_id=worker_id,
            task_id=task_id,
            details=details or {},
        )
        evidence.append(event.model_dump(mode="json"))

    def _finish(self, brief_id: str, status: str) -> DeliveryRunResult:
        if status not in {"completed", "budget_stopped", "failed"}:
            raise DeliveryError(f"unsupported terminal delivery state: {status}")
        with self.ledger.transaction(brief_id) as state:
            self._set_terminal_status(state, status)
        return self._render(brief_id)

    def _set_terminal_status(
        self,
        state: dict[str, Any],
        status: str,
    ) -> None:
        current = state["status"]
        if current == status:
            return
        if current in self._terminal_statuses():
            raise DeliveryError(
                f"delivery is already terminal in state: {current}"
            )
        state["status"] = status
        self._append_evidence(state, f"run.{status.replace('_', '-')}")

    @staticmethod
    def _terminal_statuses() -> set[str]:
        return {
            "scope_rejected",
            "release_rejected",
            "completed",
            "budget_stopped",
            "failed",
        }

    def _render(self, brief_id: str) -> DeliveryRunResult:
        state = self.ledger.get(brief_id)
        prepared = state.get("prepared_release")
        prepared_sha = None
        if prepared is not None:
            prepared_sha = PreparedSoftwareRelease.model_validate(
                prepared
            ).commit.commit_sha
        return DeliveryRunResult(
            workflow_id=brief_id,
            status=state["status"],
            scope=state["scope"],
            plan=state["plan"],
            task_executions=state["task_executions"],
            data_receipt=state["data_receipt"],
            prepared_release_sha=prepared_sha,
            software_receipt=state["software_receipt"],
            evidence=tuple(state["delivery_evidence"]),
        )

    def _phase_lock(self, brief_id: str) -> RLock:
        with self._phase_lock_guard:
            lock = self._phase_locks.get(brief_id)
            if lock is None:
                lock = RLock()
                self._phase_locks[brief_id] = lock
            return lock


__all__ = [
    "DeliveryCoordinator",
    "DeliveryError",
    "ExecutionLanes",
    "ReferenceRunConfig",
]
