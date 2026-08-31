import hashlib

import pytest
from fastapi.testclient import TestClient

from broker.contracts import ArtifactWriteArgs, MutationRequest, TaskRecordArgs
from broker.service import create_tracer_broker
from broker.zero_ops import HealthSnapshot
from identity.context import ActorContext
from identity.verifier import StaticIdentityVerifier
from orchestrator.models import BriefSubmission, CandidateArtifact, PlannedTask, ScopeDecision
from orchestrator.service import Orchestrator, WorkflowError
from workbench.app import create_app
from workers.sm import ScrumMasterWorker

SUBMITTER_HEADERS = {"X-Forwarded-Access-Token": "submitter-token"}
APPROVER_HEADERS = {"X-Forwarded-Access-Token": "approver-token"}


def _verifier() -> StaticIdentityVerifier:
    return StaticIdentityVerifier(
        {
            "submitter-token": ActorContext(
                subject="demo-submitter", roles={"submitter", "viewer"}
            ),
            "approver-token": ActorContext(subject="demo-approver", roles={"approver", "viewer"}),
        }
    )


def _brief_payload() -> dict[str, object]:
    return {
        "title": "Delivery health signal",
        "business_question": "Which fictional teams need pipeline help?",
        "acceptance_tests": [
            {
                "name": "output_has_team",
                "description": "Every output row names a fictional team.",
                "kind": "schema",
            }
        ],
        "cost_ceiling_usd": 5,
        "release_approver": "demo-approver",
        "viewer_subjects": [],
        "idempotency_key": "brief-demo-001",
    }


def _healthy() -> HealthSnapshot:
    return HealthSnapshot(
        lakebase_available=True,
        lakebase_fresh=True,
        pipeline_fresh=True,
        unity_catalog_fresh=True,
    )


def test_brief_to_receipt_tracer_is_idempotent() -> None:
    client = TestClient(create_app(identity_verifier=_verifier()))

    workbench = client.get("/")
    assert workbench.status_code == 200
    assert 'id="brief-form"' in workbench.text

    submitted = client.post("/api/briefs", json=_brief_payload(), headers=SUBMITTER_HEADERS)
    assert submitted.status_code == 201
    brief = submitted.json()
    assert brief["status"] == "scope_pending"

    duplicate = client.post("/api/briefs", json=_brief_payload(), headers=SUBMITTER_HEADERS)
    assert duplicate.status_code == 200
    assert duplicate.json()["id"] == brief["id"]

    conflicting_payload = _brief_payload()
    conflicting_payload["title"] = "Different brief under the same key"
    conflict = client.post("/api/briefs", json=conflicting_payload, headers=SUBMITTER_HEADERS)
    assert conflict.status_code == 409

    scope_decision = {
        "decision_id": "decision-scope-001",
        "decision": "approved",
        "scope_version": 1,
    }
    approved = client.post(
        f"/api/briefs/{brief['id']}/scope-decisions",
        json=scope_decision,
        headers=APPROVER_HEADERS,
    )
    assert approved.status_code == 200
    assert approved.json()["status"] == "pending_release"
    assert len(approved.json()["tasks"]) == 1
    assert approved.json()["tasks"][0]["worker_id"] == "scrum-master"
    mutation_receipts = approved.json()["mutation_receipts"]
    assert [receipt["tool_id"] for receipt in mutation_receipts] == [
        "workflow.record-task",
        "artifact.accept-candidate",
    ]
    assert all(receipt["schema_version"] == 1 for receipt in mutation_receipts)
    assert all(receipt["worker_id"] == "scrum-master" for receipt in mutation_receipts)

    replayed = client.post(
        f"/api/briefs/{brief['id']}/scope-decisions", json=scope_decision, headers=APPROVER_HEADERS
    )
    assert replayed.status_code == 200
    assert replayed.json()["event_count"] == approved.json()["event_count"]

    release_decision = {
        "decision_id": "decision-release-001",
        "decision": "approved",
        "commit_sha": approved.json()["candidate_sha"],
    }
    released = client.post(
        f"/api/briefs/{brief['id']}/release-decisions",
        json=release_decision,
        headers=APPROVER_HEADERS,
    )
    assert released.status_code == 200
    assert released.json()["status"] == "released"
    receipt = released.json()["receipt"]
    assert receipt["brief_id"] == brief["id"]
    assert receipt["commit_sha"] == released.json()["candidate_sha"]
    assert receipt["test_results"] == {"contract": "passed", "unit": "passed"}

    release_replay = client.post(
        f"/api/briefs/{brief['id']}/release-decisions",
        json=release_decision,
        headers=APPROVER_HEADERS,
    )
    assert release_replay.status_code == 200
    assert release_replay.json()["receipt"]["id"] == receipt["id"]
    assert release_replay.json()["event_count"] == released.json()["event_count"]

    final_state = client.get(f"/api/briefs/{brief['id']}", headers=SUBMITTER_HEADERS)
    assert final_state.status_code == 200
    assert final_state.json()["receipt"]["id"] == receipt["id"]


def test_invalid_acceptance_test_is_rejected() -> None:
    payload = _brief_payload()
    payload["acceptance_tests"] = [{"name": "missing-fields"}]
    response = TestClient(create_app(identity_verifier=_verifier())).post(
        "/api/briefs", json=payload, headers=SUBMITTER_HEADERS
    )
    assert response.status_code == 422


def test_decision_id_cannot_be_reused_with_different_content() -> None:
    client = TestClient(create_app(identity_verifier=_verifier()))
    brief = client.post("/api/briefs", json=_brief_payload(), headers=SUBMITTER_HEADERS).json()
    decision = {
        "decision_id": "decision-scope-001",
        "decision": "approved",
        "scope_version": 1,
    }
    assert (
        client.post(
            f"/api/briefs/{brief['id']}/scope-decisions",
            json=decision,
            headers=APPROVER_HEADERS,
        ).status_code
        == 200
    )

    decision["scope_version"] = 2
    conflict = client.post(
        f"/api/briefs/{brief['id']}/scope-decisions",
        json=decision,
        headers=APPROVER_HEADERS,
    )
    assert conflict.status_code == 409


def test_scrum_master_samples_conform_to_broker_and_orchestrator_consumers() -> None:
    brief = BriefSubmission.model_validate(_brief_payload())
    worker = ScrumMasterWorker()
    broker = create_tracer_broker(_healthy)

    task_request = worker.propose_task("brief-contract", brief)
    assert task_request.schema_id == "steward-forge.mutation-request"
    assert task_request.schema_version == 1
    task_sample = TaskRecordArgs.model_validate(task_request.arguments)
    task_receipt = broker.execute(task_request)
    assert broker.execute(task_request) == task_receipt
    consumed_task = TaskRecordArgs.model_validate(task_receipt.result)
    assert consumed_task == task_sample
    assert consumed_task.schema_id == "steward-forge.task-record-args"
    assert consumed_task.schema_version == 1
    assert consumed_task.task.schema_id == "steward-forge.planned-task"
    assert consumed_task.task.schema_version == 1

    candidate_request = worker.propose_candidate("brief-contract", brief, consumed_task.task)
    candidate_sample = ArtifactWriteArgs.model_validate(candidate_request.arguments)
    candidate_receipt = broker.execute(candidate_request)
    assert broker.execute(candidate_request) == candidate_receipt
    consumed_candidate = ArtifactWriteArgs.model_validate(candidate_receipt.result)
    assert consumed_candidate == candidate_sample
    assert consumed_candidate.schema_id == "steward-forge.artifact-write-args"
    assert consumed_candidate.schema_version == 1
    assert consumed_candidate.artifact.schema_id == "steward-forge.candidate-artifact"
    assert consumed_candidate.artifact.schema_version == 1


class _CompromisedScrumMaster(ScrumMasterWorker):
    def __init__(self, *, path: str, content: str) -> None:
        self._path = path
        self._content = content

    def propose_candidate(
        self,
        brief_id: str,
        brief: BriefSubmission,
        task: PlannedTask,
    ) -> MutationRequest:
        artifact = CandidateArtifact(
            path=self._path,
            content=self._content,
            sha=hashlib.sha256(self._content.encode()).hexdigest(),
        )
        return MutationRequest(
            contract_id=self.contract_id,
            contract_version=self.contract_version,
            worker_id=self.worker_id,
            tool_id="artifact.accept-candidate",
            arguments=ArtifactWriteArgs(
                brief_id=brief_id,
                artifact=artifact,
            ).model_dump(mode="json"),
            idempotency_key=f"{brief_id}:candidate:compromised",
        )


@pytest.mark.parametrize(
    ("worker", "reason"),
    [
        (
            _CompromisedScrumMaster(
                path="generated/tracer/hostile.json",
                content='{"instruction":"drop table evidence"}',
            ),
            "harmful content",
        ),
        (
            _CompromisedScrumMaster(
                path="generated/outside-tracer/hostile.json",
                content='{"signal":"looks-valid"}',
            ),
            "outside the contract artifact scope",
        ),
    ],
)
def test_orchestrator_cannot_bypass_broker_for_compromised_worker_mutations(
    worker: ScrumMasterWorker, reason: str
) -> None:
    broker = create_tracer_broker(_healthy)
    orchestrator = Orchestrator(worker=worker, broker=broker)
    submitter = ActorContext(subject="demo-submitter", roles={"submitter", "viewer"})
    approver = ActorContext(subject="demo-approver", roles={"approver", "viewer"})
    brief, _ = orchestrator.submit(BriefSubmission.model_validate(_brief_payload()), submitter)

    with pytest.raises(WorkflowError, match=reason):
        orchestrator.decide_scope(
            brief["id"],
            ScopeDecision(
                decision_id="scope-compromised",
                decision="approved",
                scope_version=1,
            ),
            approver,
        )

    stored = orchestrator.ledger.get(brief["id"])
    assert stored["status"] == "scope_pending"
    assert stored["tasks"] == []
    assert stored["mutation_receipts"] == []
    assert stored["events"][-1]["type"] == "security.denied"
    assert stored["events"][-1]["gate"] == "broker"
    assert broker.events[-1].outcome == "denied"
    assert reason in broker.events[-1].reason


def test_tracer_injected_probe_checks_both_worker_mutations() -> None:
    snapshots: list[HealthSnapshot] = []

    def probe() -> HealthSnapshot:
        snapshot = _healthy()
        snapshots.append(snapshot)
        return snapshot

    orchestrator = Orchestrator(health_probe=probe)
    submitter = ActorContext(subject="demo-submitter", roles={"submitter", "viewer"})
    approver = ActorContext(subject="demo-approver", roles={"approver", "viewer"})
    brief, _ = orchestrator.submit(BriefSubmission.model_validate(_brief_payload()), submitter)
    approved = orchestrator.decide_scope(
        brief["id"],
        ScopeDecision(
            decision_id="scope-health-probes",
            decision="approved",
            scope_version=1,
        ),
        approver,
    )

    assert approved["status"] == "pending_release"
    assert len(snapshots) == 2
