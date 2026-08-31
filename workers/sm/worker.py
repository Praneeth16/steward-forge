"""Bounded, deterministic Scrum Master behavior for the tracer."""

import hashlib

from orchestrator.models import BriefSubmission, CandidateArtifact, PlannedTask


class ScrumMasterWorker:
    """Turns an approved brief into one bounded task and stub candidate."""

    worker_id = "scrum-master"

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
