from __future__ import annotations

import shutil
import socket
import subprocess
import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

import psycopg
import pytest
from fastapi.testclient import TestClient

from ledger.postgres import PostgresLedger
from ledger.store import LedgerConflict
from recovery import InMemoryRevocationLayer, RecoveryController
from workbench.app import create_app


def _free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


@pytest.fixture
def postgres_url(tmp_path: Path) -> Iterator[str]:
    initdb = shutil.which("initdb")
    postgres = shutil.which("postgres")
    if not initdb or not postgres:
        pytest.skip("PostgreSQL server binaries are not installed")

    data_dir = tmp_path / "pgdata"
    subprocess.run(
        [
            initdb,
            "-D",
            str(data_dir),
            "-A",
            "trust",
            "--no-locale",
            "--encoding=UTF8",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    port = _free_port()
    server = subprocess.Popen(
        [postgres, "-D", str(data_dir), "-k", "/tmp", "-p", str(port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    url = f"postgresql://127.0.0.1:{port}/postgres"
    try:
        deadline = time.monotonic() + 10
        while True:
            try:
                with psycopg.connect(url):
                    break
            except psycopg.OperationalError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.05)
        yield url
    finally:
        server.terminate()
        server.wait(timeout=10)


def _state(brief_id: str = "brief-1", title: str = "Delivery health") -> dict[str, object]:
    return {
        "id": brief_id,
        "status": "scope_pending",
        "title": title,
        "counter": 0,
    }


def test_postgres_ledger_migrates_and_persists_transactions(postgres_url: str) -> None:
    ledger = PostgresLedger.from_conninfo(postgres_url)
    ledger.open()
    try:
        ledger.migrate()
        ledger.migrate()

        stored, created = ledger.create("submission-1", _state())
        assert created is True
        assert stored == _state()

        duplicate, created = ledger.create("submission-1", _state())
        assert created is False
        assert duplicate == stored

        with ledger.transaction("brief-1") as state:
            state["status"] = "pending_release"

        assert ledger.get("brief-1")["status"] == "pending_release"
    finally:
        ledger.close()


def test_postgres_ledger_rejects_idempotency_key_reuse(postgres_url: str) -> None:
    ledger = PostgresLedger.from_conninfo(postgres_url)
    ledger.open()
    try:
        ledger.migrate()
        ledger.create("submission-1", _state())

        with pytest.raises(LedgerConflict, match="different payload"):
            ledger.create("submission-1", _state(brief_id="brief-2", title="Other"))
    finally:
        ledger.close()


def test_postgres_transactions_serialize_concurrent_writers(postgres_url: str) -> None:
    ledger = PostgresLedger.from_conninfo(postgres_url)
    ledger.open()
    try:
        ledger.migrate()
        ledger.create("submission-1", _state())

        def increment() -> None:
            with ledger.transaction("brief-1") as state:
                state["counter"] = int(state["counter"]) + 1

        with ThreadPoolExecutor(max_workers=2) as executor:
            list(executor.map(lambda _: increment(), range(2)))

        assert ledger.get("brief-1")["counter"] == 2
    finally:
        ledger.close()


def test_brief_to_receipt_survives_an_app_restart(postgres_url: str) -> None:
    ledger = PostgresLedger.from_conninfo(postgres_url)
    with TestClient(create_app(ledger)) as client:
        assert 'id="brief-form"' in client.get("/").text
        submitted = client.post(
            "/api/briefs",
            json={
                "title": "Delivery health signal",
                "business_question": "Which fictional teams need help?",
                "acceptance_tests": [
                    {
                        "name": "output_has_team",
                        "description": "Every row names a fictional team.",
                        "kind": "schema",
                    }
                ],
                "cost_ceiling_usd": 5,
                "release_approver": "local-approver",
                "viewer_subjects": [],
                "idempotency_key": "restart-brief-1",
            },
            headers={"X-Forwarded-Access-Token": "local-submitter"},
        )
        assert submitted.status_code == 201
        brief = submitted.json()
        scoped = client.post(
            f"/api/briefs/{brief['id']}/scope-decisions",
            json={
                "decision_id": "restart-scope-1",
                "decision": "approved",
                "scope_version": 1,
            },
            headers={"X-Forwarded-Access-Token": "local-approver"},
        ).json()
        released = client.post(
            f"/api/briefs/{brief['id']}/release-decisions",
            json={
                "decision_id": "restart-release-1",
                "decision": "approved",
                "commit_sha": scoped["candidate_sha"],
            },
            headers={"X-Forwarded-Access-Token": "local-approver"},
        )
        assert released.status_code == 200
        receipt_id = released.json()["receipt"]["id"]

    restarted = PostgresLedger.from_conninfo(postgres_url)
    restarted.open()
    try:
        state = restarted.get(brief["id"])
        assert state["status"] == "released"
        assert state["receipt"]["id"] == receipt_id
        assert list(state["decisions"]) == ["restart-scope-1", "restart-release-1"]
    finally:
        restarted.close()


def test_recovery_lease_and_checkpoint_survive_a_postgres_restart(
    postgres_url: str,
) -> None:
    now = [datetime(2026, 8, 31, 9, 0, tzinfo=UTC)]
    layers = {
        name: InMemoryRevocationLayer(name)
        for name in ("gateway_access", "uc_grants", "credentials")
    }
    ledger = PostgresLedger.from_conninfo(postgres_url)
    ledger.open()
    try:
        ledger.migrate()
        ledger.create("recovery-submission", _state())
        controller = RecoveryController(ledger, layers=layers, clock=lambda: now[0])
        lease = controller.claim(
            "brief-1", "data-engineer", "process-a", lease_seconds=5
        )
        controller.checkpoint(
            "brief-1",
            "data-engineer",
            lease.owner,
            lease.epoch,
            checkpoint_id="postgres-in-flight",
            payload={"next_action": "write"},
        )
    finally:
        ledger.close()

    now[0] += timedelta(seconds=6)
    restarted = PostgresLedger.from_conninfo(postgres_url)
    restarted.open()
    try:
        recovered = RecoveryController(
            restarted, layers=layers, clock=lambda: now[0]
        ).resume_expired(
            "brief-1",
            "data-engineer",
            recovery_id="postgres-restart",
            new_owner="process-b",
            lease_seconds=30,
        )
        assert recovered.lease.epoch == lease.epoch + 1
        assert recovered.checkpoint.checkpoint_id == "postgres-in-flight"
        assert recovered.checkpoint.resume_count == 1
    finally:
        restarted.close()
