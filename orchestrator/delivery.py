"""Deterministic PM-to-SM-to-DE-to-SWE reference orchestration."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from threading import Lock, RLock
from typing import Any, TypeVar
from uuid import uuid4
from weakref import WeakValueDictionary

from data.generators import build_namespace, build_target_relations
from evidence import (
    EvidenceRecord,
    ProtectedHead,
    TrustedEvidenceSource,
    canonical_json_bytes,
    thaw_json,
)
from evidence import append as append_chain_record
from gates.swe.release import SoftwareReleaseService
from identity import AccessDenied, ActorContext, AuthorizationPolicy
from ledger import InMemoryLedger, Ledger
from model_governance import (
    BriefBudgetSummary,
    GovernedModelGateway,
    ModelTraceSummary,
    usd_ceiling_to_minor_units,
    usd_to_minor_units,
)
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
from release_evidence import (
    GovernedReleaseReceipt,
    InMemoryReleaseEvidencePointerStore,
    InMemoryReleaseEvidenceStore,
    PublishedReleaseEvidence,
    ReleaseEvidencePointer,
    ReleaseEvidencePublisher,
    ReleaseIntent,
)
from workers.de.models import DataEngineerReceipt, DataEngineerTask
from workers.pm import ProductManagerWorker
from workers.sm import ScrumMasterWorker
from workers.swe.deployment import DeploymentAcknowledgementLost
from workers.swe.models import (
    PreparedSoftwareRelease,
    SoftwareEngineerTask,
    SoftwareReleaseApproval,
    SoftwareReleaseReceipt,
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
    STATE_SCHEMA_ID = "steward-forge.delivery-state"
    STATE_SCHEMA_VERSION = 2

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
        release_evidence_publisher: ReleaseEvidencePublisher | None = None,
        model_gateway: GovernedModelGateway | None = None,
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
        self._release_evidence = release_evidence_publisher or ReleaseEvidencePublisher(
            InMemoryReleaseEvidenceStore(),
            InMemoryReleaseEvidencePointerStore(),
        )
        self._model_gateway = model_gateway
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
            details={
                "submitter": actor.subject,
                "run_id": config.run_id,
                "brief_sha256": self._canonical_sha256(
                    submission.model_dump(mode="json")
                ),
                "config_sha256": self._canonical_sha256(
                    config.model_dump(mode="json")
                ),
            },
        )
        submitted_record, submitted_head = append_chain_record(
            None,
            workflow_id=brief_id,
            record_type=submitted_evidence.event_type,
            payload=submitted_evidence.model_dump(mode="json"),
            trusted_source="orchestrator",
        )
        initial_state: dict[str, Any] = {
            "schema_id": self.STATE_SCHEMA_ID,
            "schema_version": self.STATE_SCHEMA_VERSION,
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
            "governed_release_receipt": None,
            "release_evidence_pointer": None,
            "release_intent": None,
            "release_evidence_chain_reference": None,
            "release_outcome_pending": False,
            "scope_decision_id": None,
            "decisions": {},
            "release_decision": None,
            "evidence_chain": [submitted_record.to_dict()],
            "evidence_head": submitted_head.to_dict(),
            "events": [],
        }
        self.ledger.create(
            submission.idempotency_key, initial_state
        )
        return self._ensure_scope_proposal(brief_id, submission)

    def read_model_budget(
        self, brief_id: str, actor: ActorContext
    ) -> BriefBudgetSummary:
        """Return the payload-free model budget after normal brief-row checks."""

        state = self._load_state(brief_id)
        self._policy.require_view(actor, state)
        return self._model_budget(state)

    def read_model_traces(
        self, brief_id: str, actor: ActorContext
    ) -> tuple[ModelTraceSummary, ...]:
        """Return scoped trace metadata without model prompts or outputs."""

        state = self._load_state(brief_id)
        self._policy.require_view(actor, state)
        if self._model_gateway is None:
            return ()
        self._register_model_budget(state)
        return self._model_gateway.read_trace_summaries(brief_id, actor)

    def _ensure_scope_proposal(
        self,
        brief_id: str,
        submission: BriefSubmission,
    ) -> DeliveryRunResult:
        state = self._load_state(brief_id)
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
                        "scope_sha256": self._canonical_sha256(
                            scope.model_dump(mode="json")
                        ),
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

        self._load_state(brief_id)
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
                            trusted_source="approval-gateway",
                            details={
                                "actor": actor.subject,
                                "decision_id": decision.decision_id,
                                "scope_version": decision.scope_version,
                                "scope_sha256": self._canonical_sha256(
                                    scope.model_dump(mode="json")
                                ),
                            },
                        )
                        self._append_evidence(state, "run.scope-rejected")
                    else:
                        state["status"] = "scope_approved"
                        state["scope_decision_id"] = decision.decision_id
                        self._append_evidence(
                            state,
                            "scope.approved",
                            trusted_source="approval-gateway",
                            details={
                                "actor": actor.subject,
                                "decision_id": decision.decision_id,
                                "scope_version": decision.scope_version,
                                "scope_sha256": self._canonical_sha256(
                                    scope.model_dump(mode="json")
                                ),
                            },
                        )
                current_status = state["status"]
        except (AccessDenied, DeliveryError) as error:
            self._record_security_denial(brief_id, actor, "scope", error)
            raise

        if current_status != "scope_approved":
            return self._render(brief_id)
        state = self._load_state(brief_id)
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
                        "plan_sha256": self._canonical_sha256(
                            plan.model_dump(mode="json")
                        ),
                    },
                )
        return self._render(brief_id)

    def advance(self, brief_id: str) -> DeliveryRunResult:
        """Advance approved work until an explicit release decision is needed."""

        with self._phase_lock(brief_id):
            return self._advance(brief_id)

    def _advance(self, brief_id: str) -> DeliveryRunResult:

        state = self._load_state(brief_id)
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
                    trusted_source="capability-broker",
                    worker_id="data-engineer",
                    task_id=plan.tasks[0].task_id,
                    details={
                        "receipt_id": result.receipt.receipt_id,
                        "receipt_sha256": self._canonical_sha256(
                            result.receipt.model_dump(mode="json")
                        ),
                    },
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

        current = self._load_state(brief_id)
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
                trusted_source="capability-broker",
                worker_id="software-engineer",
                task_id=task.task_id,
                details={
                    "commit_sha": prepared.commit.commit_sha,
                    "broker_receipt_id": prepared.broker_receipt.receipt_id,
                    "prepared_sha256": self._canonical_sha256(
                        prepared.model_dump(mode="json")
                    ),
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

        self._load_state(brief_id)
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
                    terminal_status = str(state["status"])
                    rejected = state["status"] == "release_rejected"
                    prepared = None
                else:
                    terminal_replay = False
                    terminal_status = None
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
                            trusted_source="approval-gateway",
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
                            trusted_source="approval-gateway",
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
                        if bound_decision is None:
                            intent = self._build_release_intent(
                                state,
                                prepared,
                                release_decision=decision,
                            )
                            state["release_intent"] = intent.model_dump(mode="json")
                            self._append_evidence(
                                state,
                                "release.intent.recorded",
                                trusted_source="release-gateway",
                                worker_id="software-engineer",
                                task_id=prepared.task.task_id,
                                details={
                                    "receipt_id": intent.receipt_id,
                                    "request_hash": intent.request_hash,
                                },
                            )
                            state["release_evidence_chain_reference"] = (
                                self._chain_reference(state)
                            )
                        elif (
                            state.get("release_intent") is None
                            or state.get("release_evidence_chain_reference") is None
                        ):
                            raise DeliveryError(
                                "release-in-progress state has no durable release intent"
                            )
        except (AccessDenied, DeliveryError) as error:
            self._record_security_denial(brief_id, actor, "release", error)
            raise

        if terminal_replay:
            if terminal_status == "completed":
                return self._reconcile_completed_release(
                    brief_id,
                    release_decision=decision,
                )
            return self._render(brief_id)
        if rejected:
            return self._render(brief_id)
        if prepared is None:
            raise DeliveryError("release decision lost its prepared candidate")
        release_state = self._load_state(brief_id)
        release_intent, evidence_chain_reference = self._validated_release_binding(
            release_state,
            prepared,
            release_decision=decision,
        )
        deployment_idempotency_key = release_intent.deployment_idempotency_key
        receipt_location = self._release_receipt_location(release_intent)
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
                receipt = self._software_release.release_governed(
                    prepared,
                    approval,
                    actor,
                    idempotency_key=deployment_idempotency_key,
                    release_intent=release_intent,
                    evidence_chain_reference=evidence_chain_reference,
                    lease_owner=lease.owner,
                    lease_epoch=lease.epoch,
                )
                return self._release_evidence.reconcile(
                    receipt,
                    receipt_location=receipt_location,
                )

        def observe_software(lease: WorkerLease):
            receipt = self._software_release.observe_governed(
                prepared,
                approval,
                actor,
                idempotency_key=deployment_idempotency_key,
                release_intent=release_intent,
                evidence_chain_reference=evidence_chain_reference,
            )
            if receipt is None:
                return None
            return self._release_evidence.reconcile(
                receipt,
                receipt_location=receipt_location,
            )

        def commit_software(
            published: PublishedReleaseEvidence,
            delivery_state: dict[str, Any],
        ) -> None:
            if (
                delivery_state["status"] != "release_in_progress"
                or delivery_state.get("release_decision") != payload
            ):
                raise DeliveryError(
                    "release completion no longer matches its exact decision binding"
                )
            receipt = published.receipt
            software_receipt = SoftwareReleaseReceipt(
                receipt_id=receipt.receipt_id,
                task_id=receipt.task_id,
                commit_sha=receipt.code_sha256,
                approval_id=receipt.release_approval_id,
                deployment_idempotency_key=receipt.deployment_idempotency_key,
                broker_receipt_id=receipt.broker_receipt_id,
                gate_results=receipt.gate_results,
                workspace_ids=thaw_json(receipt.deployment.workspace_ids),
                rollback_state=thaw_json(receipt.deployment.rollback_state),
            )
            delivery_state["software_receipt"] = software_receipt.model_dump(
                mode="json"
            )
            delivery_state["governed_release_receipt"] = (
                published.receipt.model_dump(mode="json")
            )
            delivery_state["release_evidence_pointer"] = (
                published.pointer.model_dump(mode="json")
            )
            delivery_state["release_outcome_pending"] = False
            delivery_state["status"] = "completed"
            self._append_evidence(
                delivery_state,
                "software.release.completed",
                trusted_source="release-gateway",
                worker_id="software-engineer",
                task_id=prepared.task.task_id,
                details={
                    "receipt_id": receipt.receipt_id,
                    "approval_actor": actor.subject,
                    "commit_sha": receipt.code_sha256,
                },
            )
            self._append_evidence(delivery_state, "run.completed")

        self._run_task(
            brief_id,
            ScrumPlan.model_validate(
                self._load_state(brief_id)["plan"]
            ).tasks[1],
            release_software,
            on_success=commit_software,
            expected_step="awaiting_approval",
            resume_operation=observe_software,
            unmetered_errors=(DeploymentAcknowledgementLost,),
            recovery_pending_key="release_outcome_pending",
        )
        return self._render(brief_id)

    def _build_release_intent(
        self,
        state: dict[str, Any],
        prepared: PreparedSoftwareRelease,
        *,
        release_decision: ReleaseDecision,
    ) -> ReleaseIntent:
        data_receipt_payload = state.get("data_receipt")
        scope_decision_id = state.get("scope_decision_id")
        if data_receipt_payload is None or not scope_decision_id:
            raise DeliveryError("release intent requires data and scope approval evidence")
        parsed_data_receipt = DataEngineerReceipt.model_validate(data_receipt_payload)
        brief = BriefSubmission.model_validate(state["brief"])
        config = ReferenceRunConfig.model_validate(state["config"])
        scope = ProductScope.model_validate(state["scope"])
        plan = ScrumPlan.model_validate(state["plan"])
        self._validate_plan(plan, scope, brief.cost_ceiling_usd)
        self._validate_protected_provenance(
            state,
            brief=brief,
            config=config,
            scope=scope,
            plan=plan,
            data_receipt=parsed_data_receipt,
            prepared=prepared,
            release_decision=release_decision,
        )
        release_task = next(
            task for task in plan.tasks if task.worker_id == "software-engineer"
        )
        consumed, release_execution = self._validated_attempt_costs(state, plan)
        remaining_attempts = min(
            release_task.max_attempts - release_execution.attempt_count,
            int(
                (release_execution.budget_remaining_usd + 1e-9)
                // release_task.attempt_cost_usd
            ),
        )
        authorized_cost_ceiling = consumed + (
            Decimal(max(remaining_attempts, 0))
            * Decimal(str(release_task.attempt_cost_usd))
        )
        gate_report_hash = hashlib.sha256(
            canonical_json_bytes(prepared.gates.model_dump(mode="json"))
        ).hexdigest()
        return ReleaseIntent(
            brief_id=str(state["id"]),
            workflow_id=str(state["id"]),
            run_id=config.run_id,
            task_id=prepared.task.task_id,
            code_sha256=prepared.commit.commit_sha,
            artifact_hashes=prepared.commit.artifact_hashes,
            broker_receipt_id=prepared.broker_receipt.receipt_id,
            data_receipt_id=parsed_data_receipt.receipt_id,
            data_manifest_sha256=parsed_data_receipt.manifest_sha,
            data_relations=parsed_data_receipt.catalog_relations,
            scope_approval_id=str(scope_decision_id),
            release_approval_id=release_decision.decision_id,
            gate_results=prepared.gates.results,
            gate_report_sha256=gate_report_hash,
            cost_basis="authorized_ceiling",
            cost_minor_units=usd_ceiling_to_minor_units(authorized_cost_ceiling),
            cost_currency="USD",
            model_usage_status="not_used",
            deployment_idempotency_key=f"{state['id']}:deploy:v1",
        )

    def _validate_protected_provenance(
        self,
        state: dict[str, Any],
        *,
        brief: BriefSubmission,
        config: ReferenceRunConfig,
        scope: ProductScope,
        plan: ScrumPlan,
        data_receipt: DataEngineerReceipt,
        prepared: PreparedSoftwareRelease,
        release_decision: ReleaseDecision,
    ) -> None:
        workflow_id = str(state["id"])
        submitted_record, submitted = self._protected_event(
            state, "brief.submitted", source="orchestrator"
        )
        if (
            submitted_record.sequence != 1
            or submitted.details.get("submitter") != state["submitted_by"]
            or submitted.details.get("run_id") != config.run_id
            or submitted.details.get("brief_sha256")
            != self._canonical_sha256(brief.model_dump(mode="json"))
            or submitted.details.get("config_sha256")
            != self._canonical_sha256(config.model_dump(mode="json"))
        ):
            raise DeliveryError("brief or run config diverges from protected evidence")

        _, scope_event = self._protected_event(
            state, "scope.approved", source="approval-gateway"
        )
        if (
            scope.brief_id != workflow_id
            or scope_event.details.get("decision_id") != state["scope_decision_id"]
            or scope_event.details.get("scope_version") != scope.scope_version
            or scope_event.details.get("scope_sha256")
            != self._canonical_sha256(scope.model_dump(mode="json"))
        ):
            raise DeliveryError("scope approval diverges from protected evidence")

        _, plan_event = self._protected_event(
            state, "plan.created", source="orchestrator"
        )
        if (
            plan_event.details.get("plan_id") != plan.plan_id
            or plan_event.details.get("task_count") != len(plan.tasks)
            or plan_event.details.get("plan_sha256")
            != self._canonical_sha256(plan.model_dump(mode="json"))
        ):
            raise DeliveryError("delivery plan diverges from protected evidence")

        data_task, release_task = plan.tasks
        _, data_event = self._protected_event(
            state, "data.release.completed", source="capability-broker"
        )
        if (
            data_event.worker_id != "data-engineer"
            or data_event.task_id != data_task.task_id
            or data_receipt.task_id != data_task.task_id
            or data_receipt.receipt_id != self._data_receipt_id(data_receipt)
            or data_event.details.get("receipt_id") != data_receipt.receipt_id
            or data_event.details.get("receipt_sha256")
            != self._canonical_sha256(data_receipt.model_dump(mode="json"))
        ):
            raise DeliveryError("data receipt diverges from protected evidence")

        _, prepared_event = self._protected_event(
            state, "software.release.prepared", source="capability-broker"
        )
        expected_branch = f"{config.artifact_branch.rstrip('/')}/{workflow_id}"
        if (
            prepared_event.worker_id != "software-engineer"
            or prepared_event.task_id != release_task.task_id
            or prepared.task.task_id != release_task.task_id
            or prepared.task.brief_id != workflow_id
            or prepared.task.submitted_by != state["submitted_by"]
            or prepared.task.release_approver != brief.release_approver
            or prepared.task.sandbox_catalog != config.sandbox_catalog
            or prepared.task.sandbox_schema != config.sandbox_schema
            or prepared.task.generated_prefix != config.generated_prefix
            or prepared.task.artifact_branch != expected_branch
            or prepared.task.trusted_base_sha != config.trusted_base_sha
            or prepared.task.dashboard_title != config.dashboard_title
            or prepared.task.source_tables != data_receipt.catalog_relations
            or prepared_event.details.get("commit_sha")
            != prepared.commit.commit_sha
            or prepared_event.details.get("broker_receipt_id")
            != prepared.broker_receipt.receipt_id
            or prepared_event.details.get("prepared_sha256")
            != self._canonical_sha256(prepared.model_dump(mode="json"))
        ):
            raise DeliveryError("prepared release diverges from protected evidence")

        _, decision_event = self._protected_event(
            state, "release.decision.recorded", source="approval-gateway"
        )
        if (
            decision_event.worker_id != "software-engineer"
            or decision_event.task_id != release_task.task_id
            or decision_event.details.get("decision") != release_decision.decision
            or decision_event.details.get("decision_id")
            != release_decision.decision_id
            or decision_event.details.get("commit_sha")
            != release_decision.commit_sha
        ):
            raise DeliveryError("release decision diverges from protected evidence")

    def _validated_attempt_costs(
        self,
        state: dict[str, Any],
        plan: ScrumPlan,
    ) -> tuple[Decimal, TaskExecution]:
        attempt_events: dict[tuple[str, str], list[DeliveryEvidence]] = {}
        for persisted in state["evidence_chain"]:
            record = EvidenceRecord.from_dict(persisted)
            if record.record_type != "task.attempt.started":
                continue
            if record.source != "orchestrator":
                raise DeliveryError("task attempt has an untrusted evidence source")
            event = DeliveryEvidence.model_validate(thaw_json(record.payload))
            if event.event_type != record.record_type or event.task_id is None:
                raise DeliveryError("task attempt evidence is malformed")
            phase = str(event.details.get("phase"))
            attempt_events.setdefault((event.task_id, phase), []).append(event)

        consumed = Decimal(0)
        release_execution: TaskExecution | None = None
        for task in plan.tasks:
            execution = TaskExecution.model_validate(
                state["task_executions"][task.worker_id]
            )
            execution_events = attempt_events.get((task.task_id, "execution"), [])
            preparation_events = attempt_events.get(
                (task.task_id, "candidate-preparation"), []
            )
            event_count = len(execution_events) + len(preparation_events)
            expected_consumed = event_count * task.attempt_cost_usd
            if (
                execution.task_id != task.task_id
                or execution.worker_id != task.worker_id
                or execution.max_attempts != task.max_attempts
                or execution.budget_usd != task.budget_usd
                or execution.attempt_count != len(execution_events)
                or execution.preparation_attempt_count != len(preparation_events)
                or abs(execution.budget_consumed_usd - expected_consumed) > 1e-9
                or abs(
                    execution.budget_remaining_usd
                    - max(task.budget_usd - expected_consumed, 0)
                )
                > 1e-9
            ):
                raise DeliveryError("task accounting diverges from protected evidence")
            for phase_events in (preparation_events, execution_events):
                for expected_attempt, event in enumerate(phase_events, start=1):
                    if (
                        event.worker_id != task.worker_id
                        or event.details.get("attempt") != expected_attempt
                    ):
                        raise DeliveryError("task attempt evidence is not sequential")
            consumed += Decimal(event_count) * Decimal(str(task.attempt_cost_usd))
            if task.worker_id == "software-engineer":
                release_execution = execution
        if release_execution is None:
            raise DeliveryError("delivery plan has no software release execution")
        return consumed, release_execution

    @staticmethod
    def _data_receipt_id(receipt: DataEngineerReceipt) -> str:
        payload = {
            "task_id": receipt.task_id,
            "manifest_sha": receipt.manifest_sha,
            "catalog_relations": receipt.catalog_relations,
            "mutation_receipt_ids": receipt.mutation_receipt_ids,
            "repair_attempts": receipt.repair_attempts,
            "gate_results": receipt.gate_results,
        }
        canonical = canonical_json_bytes(payload).decode("ascii")
        return hashlib.sha256(f"receipt:{canonical}".encode("ascii")).hexdigest()[:24]

    @staticmethod
    def _canonical_sha256(value: object) -> str:
        return hashlib.sha256(canonical_json_bytes(value)).hexdigest()

    @staticmethod
    def _protected_event(
        state: dict[str, Any],
        event_type: str,
        *,
        source: TrustedEvidenceSource,
    ) -> tuple[EvidenceRecord, DeliveryEvidence]:
        matches: list[tuple[EvidenceRecord, DeliveryEvidence]] = []
        for persisted in state["evidence_chain"]:
            record = EvidenceRecord.from_dict(persisted)
            if record.record_type != event_type:
                continue
            event = DeliveryEvidence.model_validate(thaw_json(record.payload))
            if record.source != source or event.event_type != event_type:
                raise DeliveryError(f"{event_type} has an invalid protected envelope")
            matches.append((record, event))
        if len(matches) != 1:
            raise DeliveryError(f"expected one protected {event_type} event")
        return matches[0]

    def _validated_release_binding(
        self,
        state: dict[str, Any],
        prepared: PreparedSoftwareRelease,
        *,
        release_decision: ReleaseDecision,
    ) -> tuple[ReleaseIntent, str]:
        release_intent = ReleaseIntent.model_validate(state.get("release_intent"))
        record, event = self._protected_event(
            state, "release.intent.recorded", source="release-gateway"
        )
        expected_reference = (
            f"{record.chain_id}:{record.sequence}:{record.current_hash}"
        )
        if (
            event.worker_id != "software-engineer"
            or event.task_id != prepared.task.task_id
            or event.details.get("receipt_id") != release_intent.receipt_id
            or event.details.get("request_hash") != release_intent.request_hash
            or state.get("release_evidence_chain_reference") != expected_reference
        ):
            raise DeliveryError("release intent diverges from its protected event")
        expected_intent = self._build_release_intent(
            state,
            prepared,
            release_decision=release_decision,
        )
        if release_intent != expected_intent:
            raise DeliveryError("release intent diverges from protected provenance")
        return release_intent, expected_reference

    def _reconcile_completed_release(
        self,
        brief_id: str,
        *,
        release_decision: ReleaseDecision,
    ) -> DeliveryRunResult:
        state = self._load_state(brief_id)
        prepared = PreparedSoftwareRelease.model_validate(state["prepared_release"])
        release_intent, evidence_reference = self._validated_release_binding(
            state,
            prepared,
            release_decision=release_decision,
        )
        receipt = GovernedReleaseReceipt.model_validate(
            state.get("governed_release_receipt")
        )
        expected_receipt = GovernedReleaseReceipt.from_intent(
            release_intent,
            receipt.deployment,
            evidence_chain_reference=evidence_reference,
        )
        if receipt != expected_receipt:
            raise DeliveryError("completed receipt diverges from protected release intent")
        expected_pointer = self._release_evidence.pointer_for(
            receipt,
            receipt_location=self._release_receipt_location(release_intent),
        )
        pointer = ReleaseEvidencePointer.model_validate(
            state.get("release_evidence_pointer")
        )
        if pointer != expected_pointer:
            raise DeliveryError("completed pointer diverges from its governed receipt")
        self._release_evidence.reconcile(
            receipt,
            receipt_location=expected_pointer.receipt_location,
        )
        return self._render(brief_id)

    @staticmethod
    def _chain_reference(state: dict[str, Any]) -> str:
        head = ProtectedHead.from_dict(state["evidence_head"])
        return f"{head.chain_id}:{head.sequence}:{head.current_hash}"

    @staticmethod
    def _release_receipt_location(intent: ReleaseIntent) -> str:
        return (
            "delta://steward_forge_evidence.release_receipts/"
            f"{intent.receipt_id}"
        )

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
        resume_operation: Callable[[WorkerLease], T | None] | None = None,
        unmetered_errors: tuple[type[Exception], ...] = (),
        recovery_pending_key: str | None = None,
    ) -> tuple[TaskExecution, T | None]:
        if attempt_counter not in {"attempt_count", "preparation_attempt_count"}:
            raise DeliveryError("unsupported task attempt counter")
        execution = TaskExecution.model_validate(
            self._load_state(brief_id)["task_executions"][task.worker_id]
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
                self._load_state(brief_id)["task_executions"][task.worker_id]
            )
            return current, None
        except Exception as error:
            return self._record_lease_claim_failure(
                brief_id, task, execution, error
            ), None

        def complete_from_observation(
            observed_result: T,
            observed_execution: TaskExecution,
        ) -> tuple[TaskExecution, T]:
            completed_execution = observed_execution.model_copy(
                update={
                    "state": success_state,
                    "stop_reason": success_stop_reason or task.stop_condition,
                }
            )

            def commit_observed(
                state: dict[str, Any],
                committed_result: T = observed_result,
            ) -> None:
                on_success(committed_result, state)

            self._complete_task_transition(
                brief_id,
                completed_execution,
                lease,
                expected_step=expected_step,
                commit_binding=self._result_binding(observed_result),
                on_success=commit_observed,
            )
            return completed_execution, observed_result

        def observe_bounded() -> T | None:
            if resume_operation is None:
                return None
            for _ in range(task.max_attempts):
                resumed_result = resume_operation(lease)
                if resumed_result is not None:
                    return resumed_result
            return None

        recovery_pending = bool(
            recovery_pending_key
            and self._load_state(brief_id).get(recovery_pending_key)
        )
        if recovery_pending:
            resumed_result = observe_bounded()
            if resumed_result is None:
                return execution, None
            return complete_from_observation(resumed_result, execution)
        if resume_operation is not None:
            resumed_result = resume_operation(lease)
            if resumed_result is not None:
                return complete_from_observation(resumed_result, execution)
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
            state = self._load_state(brief_id)
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
                if isinstance(error, unmetered_errors):
                    execution = execution.model_copy(
                        update={
                            "failures": (*execution.failures, reason),
                        }
                    )
                    self._recovery.write_worker_state(
                        brief_id,
                        task.worker_id,
                        lease.owner,
                        lease.epoch,
                        execution.model_dump(mode="json"),
                    )
                    with self.ledger.transaction(brief_id) as state:
                        self._store_execution(state, execution)
                        if recovery_pending_key is not None:
                            state[recovery_pending_key] = True
                        self._append_evidence(
                            state,
                            "release.outcome.unknown",
                            worker_id=task.worker_id,
                            task_id=task.task_id,
                            details={
                                "attempt": attempt,
                                "failure": reason,
                                "additional_retry_charged": False,
                            },
                        )
                    resumed_result = observe_bounded()
                    if resumed_result is None:
                        return execution, None
                    return complete_from_observation(resumed_result, execution)
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

    @classmethod
    def _append_evidence(
        cls,
        state: dict[str, Any],
        event_type: str,
        *,
        trusted_source: TrustedEvidenceSource = "orchestrator",
        worker_id: str | None = None,
        task_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        cls._require_current_state(state)
        event = DeliveryEvidence(
            sequence=len(state["evidence_chain"]) + 1,
            event_type=event_type,
            worker_id=worker_id,
            task_id=task_id,
            details=details or {},
        )
        event_payload = event.model_dump(mode="json")
        previous_head = ProtectedHead.from_dict(state["evidence_head"])
        record, head = append_chain_record(
            previous_head,
            workflow_id=str(state["id"]),
            record_type=event_type,
            payload=event_payload,
            trusted_source=trusted_source,
        )
        state["evidence_chain"].append(record.to_dict())
        state["evidence_head"] = head.to_dict()

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
        state = self._load_state(brief_id)
        evidence = tuple(
            DeliveryEvidence.model_validate(record["payload"])
            for record in state["evidence_chain"]
        )
        prepared = state.get("prepared_release")
        prepared_sha = None
        if prepared is not None:
            prepared_sha = PreparedSoftwareRelease.model_validate(
                prepared
            ).commit.commit_sha
        return DeliveryRunResult(
            schema_id="steward-forge.delivery-run-result",
            schema_version=2,
            workflow_id=brief_id,
            status=state["status"],
            scope=state["scope"],
            plan=state["plan"],
            task_executions=state["task_executions"],
            data_receipt=state["data_receipt"],
            prepared_release_sha=prepared_sha,
            software_receipt=state["software_receipt"],
            governed_release_receipt=state["governed_release_receipt"],
            release_evidence_pointer=state["release_evidence_pointer"],
            model_budget=self._model_budget(state),
            evidence=evidence,
            evidence_chain=tuple(state["evidence_chain"]),
            evidence_head=state["evidence_head"],
        )

    def _model_budget(self, state: dict[str, Any]) -> BriefBudgetSummary:
        ceiling = self._model_ceiling(state)
        if self._model_gateway is None:
            return BriefBudgetSummary(
                brief_id=state["id"],
                authorized_ceiling_minor_units=ceiling,
                budget_committed_minor_units=0,
                metered_actual_minor_units=0,
                remaining_authorization_minor_units=ceiling,
                request_count=0,
                throttle_count=0,
                incomplete_usage_count=0,
                reconciliation_failure_count=0,
                usage_status="not_used",
            )
        self._register_model_budget(state)
        return self._model_gateway._budget_summary(state["id"])

    def _register_model_budget(self, state: dict[str, Any]) -> None:
        if self._model_gateway is None:
            return
        brief = BriefSubmission.model_validate(state["brief"])
        config = ReferenceRunConfig.model_validate(state["config"])
        self._model_gateway.register_brief(
            brief_id=state["id"],
            run_id=config.run_id,
            owner_subject=str(state["submitted_by"]),
            viewer_subjects=tuple(
                dict.fromkeys((brief.release_approver, *brief.viewer_subjects))
            ),
            authorized_ceiling_minor_units=self._model_ceiling(state),
        )

    @staticmethod
    def _model_ceiling(state: dict[str, Any]) -> int:
        brief = BriefSubmission.model_validate(state["brief"])
        return usd_to_minor_units(brief.cost_ceiling_usd)

    def _load_state(self, brief_id: str) -> dict[str, Any]:
        state = self.ledger.get(brief_id)
        self._require_current_state(state)
        return state

    @classmethod
    def _require_current_state(cls, state: Mapping[str, Any]) -> None:
        schema_id = state.get("schema_id")
        schema_version = state.get("schema_version")
        if schema_id is None and schema_version is None:
            if "delivery_evidence" in state:
                raise DeliveryError(
                    "legacy delivery state cannot be resumed safely: delivery_evidence "
                    "has no trusted-source provenance; resubmit the original brief to "
                    "create a protected version 2 workflow using a new idempotency key"
                )
            raise DeliveryError(
                "unversioned delivery state cannot be resumed safely; resubmit the "
                "original brief to create a protected version 2 workflow using a new "
                "idempotency key"
            )
        if (
            schema_id != cls.STATE_SCHEMA_ID
            or schema_version != cls.STATE_SCHEMA_VERSION
        ):
            raise DeliveryError(
                "unsupported delivery state contract "
                f"{schema_id!r} version {schema_version!r}; expected "
                f"{cls.STATE_SCHEMA_ID!r} version {cls.STATE_SCHEMA_VERSION}"
            )
        if "evidence_chain" not in state or "evidence_head" not in state:
            raise DeliveryError(
                "delivery state version 2 requires a protected evidence_chain and "
                "evidence_head"
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
