"""Bounded, deterministic Scrum Master behavior for the tracer."""

import hashlib

from broker.contracts import ArtifactWriteArgs, MutationRequest, TaskRecordArgs
from orchestrator.models import BriefSubmission, CandidateArtifact, PlannedTask


class ScrumMasterWorker:
    """Turns an approved brief into one bounded task and stub candidate."""

    worker_id = "scrum-master"
    contract_id = "scrum-master-tracer"
    contract_version = 1

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
