from fastapi.testclient import TestClient

from workbench.app import create_app


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
        "submitted_by": "demo-submitter",
        "release_approver": "demo-approver",
        "idempotency_key": "brief-demo-001",
    }


def test_brief_to_receipt_tracer_is_idempotent() -> None:
    client = TestClient(create_app())

    workbench = client.get("/")
    assert workbench.status_code == 200
    assert 'id="brief-form"' in workbench.text

    submitted = client.post("/api/briefs", json=_brief_payload())
    assert submitted.status_code == 201
    brief = submitted.json()
    assert brief["status"] == "scope_pending"

    duplicate = client.post("/api/briefs", json=_brief_payload())
    assert duplicate.status_code == 200
    assert duplicate.json()["id"] == brief["id"]

    scope_decision = {
        "decision_id": "decision-scope-001",
        "decision": "approved",
        "scope_version": 1,
        "actor": "demo-approver",
    }
    approved = client.post(f"/api/briefs/{brief['id']}/scope-decisions", json=scope_decision)
    assert approved.status_code == 200
    assert approved.json()["status"] == "pending_release"
    assert len(approved.json()["tasks"]) == 1
    assert approved.json()["tasks"][0]["worker_id"] == "scrum-master"

    replayed = client.post(
        f"/api/briefs/{brief['id']}/scope-decisions", json=scope_decision
    )
    assert replayed.status_code == 200
    assert replayed.json()["event_count"] == approved.json()["event_count"]

    release_decision = {
        "decision_id": "decision-release-001",
        "decision": "approved",
        "commit_sha": approved.json()["candidate_sha"],
        "actor": "demo-approver",
    }
    released = client.post(
        f"/api/briefs/{brief['id']}/release-decisions", json=release_decision
    )
    assert released.status_code == 200
    assert released.json()["status"] == "released"
    receipt = released.json()["receipt"]
    assert receipt["brief_id"] == brief["id"]
    assert receipt["commit_sha"] == released.json()["candidate_sha"]
    assert receipt["test_results"] == {"contract": "passed", "unit": "passed"}

    release_replay = client.post(
        f"/api/briefs/{brief['id']}/release-decisions", json=release_decision
    )
    assert release_replay.status_code == 200
    assert release_replay.json()["receipt"]["id"] == receipt["id"]
    assert release_replay.json()["event_count"] == released.json()["event_count"]

    final_state = client.get(f"/api/briefs/{brief['id']}")
    assert final_state.status_code == 200
    assert final_state.json()["receipt"]["id"] == receipt["id"]


def test_invalid_acceptance_test_is_rejected() -> None:
    payload = _brief_payload()
    payload["acceptance_tests"] = [{"name": "missing-fields"}]
    response = TestClient(create_app()).post("/api/briefs", json=payload)
    assert response.status_code == 422


def test_decision_id_cannot_be_reused_with_different_content() -> None:
    client = TestClient(create_app())
    brief = client.post("/api/briefs", json=_brief_payload()).json()
    decision = {
        "decision_id": "decision-scope-001",
        "decision": "approved",
        "scope_version": 1,
        "actor": "demo-approver",
    }
    assert (
        client.post(f"/api/briefs/{brief['id']}/scope-decisions", json=decision).status_code
        == 200
    )

    decision["actor"] = "different-actor"
    conflict = client.post(
        f"/api/briefs/{brief['id']}/scope-decisions", json=decision
    )
    assert conflict.status_code == 409
