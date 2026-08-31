from __future__ import annotations

from types import SimpleNamespace

from fastapi.testclient import TestClient

from identity.context import ActorContext
from identity.verifier import DatabricksIdentityVerifier, StaticIdentityVerifier
from workbench.app import create_app

SUBMITTER_HEADERS = {"X-Forwarded-Access-Token": "submitter-token"}
APPROVER_HEADERS = {"X-Forwarded-Access-Token": "approver-token"}
OTHER_APPROVER_HEADERS = {"X-Forwarded-Access-Token": "other-approver-token"}
VIEWER_HEADERS = {"X-Forwarded-Access-Token": "viewer-token"}
STRANGER_HEADERS = {"X-Forwarded-Access-Token": "stranger-token"}


def _verifier() -> StaticIdentityVerifier:
    return StaticIdentityVerifier(
        {
            "submitter-token": ActorContext(
                subject="user-submitter", roles={"submitter", "viewer"}
            ),
            "approver-token": ActorContext(subject="user-approver", roles={"approver", "viewer"}),
            "other-approver-token": ActorContext(
                subject="user-other-approver", roles={"approver", "viewer"}
            ),
            "viewer-token": ActorContext(subject="user-viewer", roles={"viewer"}),
            "stranger-token": ActorContext(subject="user-stranger", roles={"viewer"}),
        }
    )


def _payload() -> dict[str, object]:
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
        "release_approver": "user-approver",
        "viewer_subjects": ["user-viewer"],
        "idempotency_key": "identity-brief-001",
    }


def test_missing_or_unknown_user_token_is_rejected() -> None:
    client = TestClient(create_app(identity_verifier=_verifier()))

    assert client.post("/api/briefs", json=_payload()).status_code == 401
    assert (
        client.post(
            "/api/briefs",
            json=_payload(),
            headers={"X-Forwarded-Access-Token": "forged-token"},
        ).status_code
        == 401
    )


def test_databricks_verifier_normalizes_bearer_and_maps_groups(monkeypatch) -> None:
    observed: dict[str, str] = {}

    class FakeCurrentUser:
        @staticmethod
        def me():
            return SimpleNamespace(
                id="workspace-subject",
                groups=[SimpleNamespace(display="Forge Approvers")],
            )

    class FakeWorkspaceClient:
        def __init__(self, *, host: str, token: str) -> None:
            observed.update(host=host, token=token)
            self.current_user = FakeCurrentUser()

    monkeypatch.setenv("DATABRICKS_HOST", "workspace.example.invalid")
    monkeypatch.setattr("identity.verifier.WorkspaceClient", FakeWorkspaceClient)
    verifier = DatabricksIdentityVerifier(role_groups={"approver": "Forge Approvers"})

    actor = verifier.verify("Bearer forwarded-token")

    assert actor == ActorContext(subject="workspace-subject", roles={"approver"})
    assert observed == {
        "host": "https://workspace.example.invalid",
        "token": "forwarded-token",
    }


def test_body_supplied_actor_identity_is_rejected() -> None:
    client = TestClient(create_app(identity_verifier=_verifier()))
    payload = _payload()
    payload["submitted_by"] = "forged-submitter"
    assert client.post("/api/briefs", json=payload, headers=SUBMITTER_HEADERS).status_code == 422

    brief = client.post("/api/briefs", json=_payload(), headers=SUBMITTER_HEADERS).json()
    decision = {
        "decision_id": "identity-scope-001",
        "decision": "approved",
        "scope_version": 1,
        "actor": "forged-approver",
    }
    assert (
        client.post(
            f"/api/briefs/{brief['id']}/scope-decisions",
            json=decision,
            headers=APPROVER_HEADERS,
        ).status_code
        == 422
    )


def test_roles_rows_versions_and_sha_are_enforced_from_token_claims() -> None:
    client = TestClient(create_app(identity_verifier=_verifier()))
    submitted = client.post("/api/briefs", json=_payload(), headers=SUBMITTER_HEADERS)
    assert submitted.status_code == 201
    brief = submitted.json()
    assert brief["submitted_by"] == "user-submitter"

    assert client.get(f"/api/briefs/{brief['id']}", headers=STRANGER_HEADERS).status_code == 403
    assert client.get(f"/api/briefs/{brief['id']}", headers=VIEWER_HEADERS).status_code == 200

    stale = client.post(
        f"/api/briefs/{brief['id']}/scope-decisions",
        json={
            "decision_id": "identity-stale-scope",
            "decision": "approved",
            "scope_version": 99,
        },
        headers=APPROVER_HEADERS,
    )
    assert stale.status_code == 409

    wrong_approver = client.post(
        f"/api/briefs/{brief['id']}/scope-decisions",
        json={
            "decision_id": "identity-wrong-approver",
            "decision": "approved",
            "scope_version": 1,
        },
        headers=OTHER_APPROVER_HEADERS,
    )
    assert wrong_approver.status_code == 403

    scoped = client.post(
        f"/api/briefs/{brief['id']}/scope-decisions",
        json={
            "decision_id": "identity-scope-001",
            "decision": "approved",
            "scope_version": 1,
        },
        headers=APPROVER_HEADERS,
    )
    assert scoped.status_code == 200
    candidate = scoped.json()

    submitter_release = client.post(
        f"/api/briefs/{brief['id']}/release-decisions",
        json={
            "decision_id": "identity-submitter-release",
            "decision": "approved",
            "commit_sha": candidate["candidate_sha"],
        },
        headers=SUBMITTER_HEADERS,
    )
    assert submitter_release.status_code == 403

    wrong_sha = client.post(
        f"/api/briefs/{brief['id']}/release-decisions",
        json={
            "decision_id": "identity-wrong-sha",
            "decision": "approved",
            "commit_sha": "0" * 64,
        },
        headers=APPROVER_HEADERS,
    )
    assert wrong_sha.status_code == 409

    released = client.post(
        f"/api/briefs/{brief['id']}/release-decisions",
        json={
            "decision_id": "identity-release-001",
            "decision": "approved",
            "commit_sha": candidate["candidate_sha"],
        },
        headers=APPROVER_HEADERS,
    )
    assert released.status_code == 200
    assert released.json()["status"] == "released"

    replay = client.post(
        f"/api/briefs/{brief['id']}/release-decisions",
        json={
            "decision_id": "identity-release-001",
            "decision": "approved",
            "commit_sha": candidate["candidate_sha"],
        },
        headers=APPROVER_HEADERS,
    )
    assert replay.status_code == 200
    assert replay.json()["event_count"] == released.json()["event_count"]
