"""In-process orchestration for the first end-to-end tracer."""

import hashlib
from copy import deepcopy
from typing import Any

from gates.release import ReleaseAdapter
from gates.test_gate import TestGate
from identity import AccessDenied, ActorContext, AuthorizationPolicy
from ledger import InMemoryLedger, Ledger
from orchestrator.models import BriefSubmission, ReleaseDecision, ScopeDecision
from workers.sm import ScrumMasterWorker


class WorkflowError(ValueError):
    """A deterministic workflow contract was violated."""


class Orchestrator:
    """Owns workflow state transitions and replay-safe decisions."""

    def __init__(
        self,
        ledger: Ledger | None = None,
        policy: AuthorizationPolicy | None = None,
    ) -> None:
        self.ledger = ledger or InMemoryLedger()
        self._policy = policy or AuthorizationPolicy()
        self._worker = ScrumMasterWorker()
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
                task = self._worker.plan(brief_id, brief)
                candidate = self._worker.run_specialist_stub(brief_id, brief, task)
                test_results = self._gate.evaluate(candidate)
                if "failed" in test_results.values():
                    raise WorkflowError("candidate failed the deterministic test gate")

                state["tasks"] = [task.model_dump(mode="json")]
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

                from orchestrator.models import CandidateArtifact

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
