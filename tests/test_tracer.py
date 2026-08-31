from fastapi.testclient import TestClient

from identity.context import ActorContext
from identity.verifier import StaticIdentityVerifier
from workbench.app import create_app

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
