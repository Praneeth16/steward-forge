"""In-process orchestration for the first end-to-end tracer."""

import hashlib
from collections.abc import Callable
from copy import deepcopy
from typing import Any

from broker.contracts import ArtifactWriteArgs, MutationReceipt, MutationRequest, TaskRecordArgs
from broker.service import BrokerDenied, CapabilityBroker, create_tracer_broker
from broker.zero_ops import HealthSnapshot
from gates.release import ReleaseAdapter
from gates.test_gate import TestGate
from identity import AccessDenied, ActorContext, AuthorizationPolicy
from ledger import InMemoryLedger, Ledger
from orchestrator.models import (
    BriefSubmission,
    CandidateArtifact,
    PlannedTask,
    ReleaseDecision,
    ScopeDecision,
)
from workers.sm import ScrumMasterWorker


class WorkflowError(ValueError):
    """A deterministic workflow contract was violated."""


class Orchestrator:
    """Owns workflow state transitions and replay-safe decisions."""

    def __init__(
        self,
        ledger: Ledger | None = None,
        policy: AuthorizationPolicy | None = None,
        worker: ScrumMasterWorker | None = None,
        broker: CapabilityBroker | None = None,
        health_probe: Callable[[], HealthSnapshot] | None = None,
    ) -> None:
        if broker is not None and health_probe is not None:
            raise ValueError("provide a broker or a health probe, not both")
        self.ledger = ledger or InMemoryLedger()
        self._policy = policy or AuthorizationPolicy()
        self._worker = worker or ScrumMasterWorker()
        self._broker = broker or create_tracer_broker(health_probe)
        self._gate = TestGate()
        self._release = ReleaseAdapter()

    def submit(
        self, submission: BriefSubmission, actor: ActorContext
    ) -> tuple[dict[str, Any], bool]:
        self._policy.require_submit(actor)
        brief_id = hashlib.sha256(f"brief:{submission.idempotency_key}".encode()).hexdigest()[:24]
        state: dict[str, Any] = {
            "id": brief_id,
            "status": "scope_pending",
            "scope_version": 1,
            "submitted_by": actor.subject,
            "brief": submission.model_dump(mode="json"),
            "tasks": [],
            "mutation_receipts": [],
            "candidate_sha": None,
            "receipt": None,
            "decisions": {},
            "events": [
                {
                    "type": "brief.submitted",
                    "idempotency_key": submission.idempotency_key,
                }
            ],
        }
        stored, created = self.ledger.create(submission.idempotency_key, state)
        return self._render(stored), created

    def decide_scope(
        self, brief_id: str, decision: ScopeDecision, actor: ActorContext
    ) -> dict[str, Any]:
        try:
            with self.ledger.transaction(brief_id) as state:
                self._policy.require_approval(actor, state)
                payload = decision.model_dump() | {"actor": actor.subject}
                replay = self._replay(state, decision.decision_id, payload)
                if replay:
                    return self._render(state)
                if state["status"] != "scope_pending":
                    raise WorkflowError("scope decision is not valid in the current state")
                if decision.scope_version != state["scope_version"]:
                    raise WorkflowError("scope decision does not match the current version")
                if decision.decision != "approved":
                    state["status"] = "scope_rejected"
                    self._record_decision(state, decision.decision_id, payload)
                    state["events"].append({"type": "scope.rejected", "actor": actor.subject})
                    return self._render(state)

                brief = BriefSubmission.model_validate(state["brief"])
                task_request = MutationRequest.model_validate(
                    self._worker.propose_task(brief_id, brief)
                )
                task_receipt = self._broker.execute(task_request)
                task = self._consume_task_receipt(task_receipt, brief_id)

                candidate_request = MutationRequest.model_validate(
                    self._worker.propose_candidate(brief_id, brief, task)
                )
                candidate_receipt = self._broker.execute(candidate_request)
                candidate = self._consume_candidate_receipt(candidate_receipt, brief_id)
                test_results = self._gate.evaluate(candidate)
                if "failed" in test_results.values():
                    raise WorkflowError("candidate failed the deterministic test gate")

                state["tasks"] = [task.model_dump(mode="json")]
                state["mutation_receipts"] = [
                    task_receipt.model_dump(mode="json"),
                    candidate_receipt.model_dump(mode="json"),
                ]
                state["candidate"] = candidate.model_dump(mode="json")
                state["candidate_sha"] = candidate.sha
                state["test_results"] = test_results
                state["status"] = "pending_release"
                self._record_decision(state, decision.decision_id, payload)
                state["events"].extend(
                    [
                        {
                            "type": "scope.approved",
                            "scope_version": decision.scope_version,
                            "actor": actor.subject,
                        },
                        {"type": "task.planned", "task_id": task.id},
                        {"type": "candidate.tested", "candidate_sha": candidate.sha},
                    ]
                )
                return self._render(state)
        except BrokerDenied as error:
            self._log_denial(brief_id, "broker", actor, str(error))
            raise WorkflowError(f"worker mutation denied by broker: {error}") from error
        except (AccessDenied, WorkflowError) as error:
            self._log_denial(brief_id, "scope", actor, str(error))
            raise

    def decide_release(
        self, brief_id: str, decision: ReleaseDecision, actor: ActorContext
    ) -> dict[str, Any]:
        try:
            with self.ledger.transaction(brief_id) as state:
                self._policy.require_release(actor, state)
                payload = decision.model_dump() | {"actor": actor.subject}
                replay = self._replay(state, decision.decision_id, payload)
                if replay:
                    return self._render(state)
                if state["status"] != "pending_release":
                    raise WorkflowError("release decision is not valid in the current state")
                if decision.commit_sha != state["candidate_sha"]:
                    raise WorkflowError("release decision does not match the candidate SHA")
                if decision.decision != "approved":
                    state["status"] = "release_rejected"
                    self._record_decision(state, decision.decision_id, payload)
                    state["events"].append({"type": "release.rejected", "actor": actor.subject})
                    return self._render(state)

                candidate = CandidateArtifact.model_validate(state["candidate"])
                receipt = self._release.release(brief_id, candidate, state["test_results"])
                state["receipt"] = receipt.model_dump(mode="json")
                state["status"] = "released"
                self._record_decision(state, decision.decision_id, payload)
                state["events"].append(
                    {
                        "type": "release.completed",
                        "receipt_id": receipt.id,
                        "actor": actor.subject,
                    }
                )
                return self._render(state)
        except (AccessDenied, WorkflowError) as error:
            self._log_denial(brief_id, "release", actor, str(error))
            raise

    def get(self, brief_id: str, actor: ActorContext) -> dict[str, Any]:
        state = self.ledger.get(brief_id)
        try:
            self._policy.require_view(actor, state)
        except AccessDenied as error:
            self._log_denial(brief_id, "view", actor, str(error))
            raise
        return self._render(state)

    def _log_denial(self, brief_id: str, gate: str, actor: ActorContext, reason: str) -> None:
        with self.ledger.transaction(brief_id) as state:
            state["events"].append(
                {
                    "type": "security.denied",
                    "gate": gate,
                    "actor": actor.subject,
                    "reason": reason,
                }
            )

    @staticmethod
    def _consume_task_receipt(receipt: MutationReceipt, brief_id: str) -> PlannedTask:
        if receipt.tool_id != "workflow.record-task":
            raise WorkflowError("broker returned the wrong task receipt type")
        record = TaskRecordArgs.model_validate(receipt.result)
        if record.brief_id != brief_id or record.task.worker_id != receipt.worker_id:
            raise WorkflowError("task receipt is not bound to this brief and worker")
        return record.task

    @staticmethod
    def _consume_candidate_receipt(
        receipt: MutationReceipt, brief_id: str
    ) -> CandidateArtifact:
        if receipt.tool_id != "artifact.accept-candidate":
            raise WorkflowError("broker returned the wrong candidate receipt type")
        record = ArtifactWriteArgs.model_validate(receipt.result)
        if record.brief_id != brief_id:
            raise WorkflowError("candidate receipt is not bound to this brief")
        return record.artifact

    @staticmethod
    def _record_decision(state: dict[str, Any], decision_id: str, payload: dict[str, Any]) -> None:
        state["decisions"][decision_id] = payload

    @staticmethod
    def _replay(state: dict[str, Any], decision_id: str, payload: dict[str, Any]) -> bool:
        existing = state["decisions"].get(decision_id)
        if existing is None:
            return False
        if existing != payload:
            raise WorkflowError("decision ID was already used with different content")
        return True

    @staticmethod
    def _render(state: dict[str, Any]) -> dict[str, Any]:
        rendered = deepcopy(state)
        rendered.pop("decisions", None)
        rendered.pop("candidate", None)
        rendered["event_count"] = len(rendered.pop("events"))
        return rendered
