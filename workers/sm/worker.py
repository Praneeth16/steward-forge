"""Bounded, deterministic Scrum Master behavior for the tracer."""

import hashlib

from broker.contracts import ArtifactWriteArgs, MutationRequest, TaskRecordArgs
from orchestrator.delivery_models import (
    DeliveryTask,
    EscalationEvent,
    ProductScope,
    ScrumPlan,
)
from orchestrator.models import BriefSubmission, CandidateArtifact, PlannedTask


class ScrumMasterWorker:
    """Turns an approved brief into one bounded task and stub candidate."""

    worker_id = "scrum-master"
    contract_id = "scrum-master-tracer"
    contract_version = 1

    def plan_delivery(
        self, scope: ProductScope, cost_ceiling_usd: float
    ) -> ScrumPlan:
        """Create two bounded tasks without deciding their retries."""

        data_budget = cost_ceiling_usd * 0.6
        software_budget = cost_ceiling_usd - data_budget
        data_task_id = self._delivery_task_id(scope.brief_id, "data-engineer")
        software_task_id = self._delivery_task_id(
            scope.brief_id, "software-engineer"
        )
        tasks = (
            DeliveryTask(
                task_id=data_task_id,
                worker_id="data-engineer",
                max_attempts=2,
                budget_usd=data_budget,
                attempt_cost_usd=data_budget / 2,
                stop_condition=(
                    "Stop after a governed data receipt or a deterministic "
                    "terminal state."
                ),
                expected_output="Versioned Data Engineer receipt and sandbox relations.",
            ),
            DeliveryTask(
                task_id=software_task_id,
                worker_id="software-engineer",
                depends_on=(data_task_id,),
                max_attempts=2,
                budget_usd=software_budget,
                # Preparation and release each receive the same bounded retry
                # count while sharing one task-level budget.
                attempt_cost_usd=software_budget / 4,
                stop_condition=(
                    "Stop after a SHA-bound release receipt or a deterministic "
                    "terminal state."
                ),
                expected_output="Versioned Software Engineer release receipt.",
            ),
        )
        scope_fingerprint = scope.fingerprint()
        plan_id = hashlib.sha256(
            (
                f"{scope.brief_id}:scope:{scope.scope_version}:"
                f"{scope_fingerprint}:scrum-plan:v1"
            ).encode()
        ).hexdigest()[:24]
        return ScrumPlan(
            plan_id=plan_id,
            brief_id=scope.brief_id,
            scope_version=scope.scope_version,
            approved_scope_sha256=scope_fingerprint,
            tasks=tasks,
        )

    @staticmethod
    def escalate(
        task: DeliveryTask, *, attempt: int, reason: str
    ) -> EscalationEvent:
        """Report a failed attempt without deciding whether it retries."""

        return EscalationEvent(
            task_id=task.task_id,
            worker_id=task.worker_id,
            attempt=attempt,
            reason=reason,
        )

    @staticmethod
    def _delivery_task_id(brief_id: str, worker_id: str) -> str:
        return hashlib.sha256(
            f"{brief_id}:{worker_id}:task:v1".encode()
        ).hexdigest()[:24]

    def plan(self, brief_id: str, brief: BriefSubmission) -> PlannedTask:
        task_id = hashlib.sha256(f"{brief_id}:task:1".encode()).hexdigest()[:16]
        return PlannedTask(
            id=task_id,
            worker_id=self.worker_id,
            owner="specialist-stub",
            budget_usd=brief.cost_ceiling_usd,
            stop_condition="Stop after one candidate artifact is returned.",
            expected_output="generated/tracer/delivery-health-signal.json",
        )

    def run_specialist_stub(
        self, brief_id: str, brief: BriefSubmission, task: PlannedTask
    ) -> CandidateArtifact:
        content = (
            '{"brief_id":"'
            + brief_id
            + '","team":"fictional-platform-team","signal":"needs-review"}'
        )
        sha = hashlib.sha256(content.encode()).hexdigest()
        return CandidateArtifact(path=task.expected_output, content=content, sha=sha)

    def propose_task(self, brief_id: str, brief: BriefSubmission) -> MutationRequest:
        """Produce the versioned mutation that records one bounded task."""

        task = self.plan(brief_id, brief)
        arguments = TaskRecordArgs(brief_id=brief_id, task=task)
        return MutationRequest(
            contract_id=self.contract_id,
            contract_version=self.contract_version,
            worker_id=self.worker_id,
            tool_id="workflow.record-task",
            arguments=arguments.model_dump(mode="json"),
            idempotency_key=f"{brief_id}:task:{task.id}:v1",
        )

    def propose_candidate(
        self,
        brief_id: str,
        brief: BriefSubmission,
        task: PlannedTask,
    ) -> MutationRequest:
        """Produce the versioned mutation that accepts a candidate artifact."""

        artifact = self.run_specialist_stub(brief_id, brief, task)
        arguments = ArtifactWriteArgs(brief_id=brief_id, artifact=artifact)
        return MutationRequest(
            contract_id=self.contract_id,
            contract_version=self.contract_version,
            worker_id=self.worker_id,
            tool_id="artifact.accept-candidate",
            arguments=arguments.model_dump(mode="json"),
            idempotency_key=f"{brief_id}:candidate:{task.id}:v1",
        )
