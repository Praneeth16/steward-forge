from __future__ import annotations

import shutil
import socket
import subprocess
import time
from collections.abc import Iterator
from dataclasses import asdict

import psycopg
import pytest
from psycopg.conninfo import make_conninfo
from psycopg.types.json import Jsonb

from evidence import EvidenceIntegrityError, EvidenceRecord, ProtectedHead, append
from ledger import InMemoryLedger, LedgerNotFound
from ledger.postgres import HeadProtectionError, PostgresLedger

LedgerImplementation = InMemoryLedger | PostgresLedger
RUNTIME_ROLE = "steward_test_runtime"


def _free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


@pytest.fixture(scope="module")
def postgres_url(tmp_path_factory: pytest.TempPathFactory) -> Iterator[str]:
    initdb = shutil.which("initdb")
    postgres = shutil.which("postgres")
    if not initdb or not postgres:
        pytest.skip("PostgreSQL server binaries are not installed")

    data_dir = tmp_path_factory.mktemp("ledger-evidence-postgres") / "pgdata"
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


@pytest.fixture(params=["memory", "postgres"])
def ledger(request: pytest.FixtureRequest) -> Iterator[LedgerImplementation]:
    if request.param == "memory":
        yield InMemoryLedger()
        return

    postgres_url = request.getfixturevalue("postgres_url")
    with psycopg.connect(postgres_url, autocommit=True) as connection:
        connection.execute("DROP SCHEMA IF EXISTS steward_ledger CASCADE")
    postgres_ledger = PostgresLedger.from_conninfo(postgres_url)
    postgres_ledger.open()
    postgres_ledger.migrate()
    try:
        yield postgres_ledger
    finally:
        postgres_ledger.close()


def _head_dict(head: ProtectedHead) -> dict[str, object]:
    return asdict(head)


def _evidence_state(workflow_id: str = "brief-evidence") -> dict[str, object]:
    first, head = append(
        None,
        workflow_id=workflow_id,
        record_type="brief.submitted",
        payload={"title": "Delivery health"},
        trusted_source="orchestrator",
    )
    second, head = append(
        head,
        workflow_id=workflow_id,
        record_type="scope.approved",
        payload={"approver": "reviewer-1"},
        trusted_source="approval-gateway",
    )
    return {
        "id": workflow_id,
        "status": "scope_approved",
        "evidence_chain": [first.to_dict(), second.to_dict()],
        "evidence_head": _head_dict(head),
    }


def _legacy_state(workflow_id: str = "brief-legacy") -> dict[str, object]:
    return {"id": workflow_id, "status": "scope_pending"}


def _protected_head(state: dict[str, object]) -> ProtectedHead:
    persisted = state["evidence_head"]
    assert isinstance(persisted, dict)
    return ProtectedHead(**persisted)


def _append_valid_suffix(
    state: dict[str, object],
) -> tuple[list[dict[str, object]], dict[str, object]]:
    head = _protected_head(state)
    third, head = append(
        head,
        workflow_id=str(state["id"]),
        record_type="worker.completed",
        payload={"worker": "data-engineer"},
        trusted_source="orchestrator",
    )
    fourth, head = append(
        head,
        workflow_id=str(state["id"]),
        record_type="release.completed",
        payload={"receipt_id": "receipt-1"},
        trusted_source="release-gateway",
    )
    chain = state["evidence_chain"]
    assert isinstance(chain, list)
    chain.extend([third.to_dict(), fourth.to_dict()])
    persisted_head = _head_dict(head)
    state["evidence_head"] = persisted_head
    return [third.to_dict(), fourth.to_dict()], persisted_head


def test_ledger_accepts_a_valid_suffix_and_returns_detached_verified_evidence(
    ledger: LedgerImplementation,
) -> None:
    initial = _evidence_state()
    ledger.create("submission-evidence", initial)

    with ledger.transaction("brief-evidence") as state:
        suffix, expected_head = _append_valid_suffix(state)

    records, head = ledger.get_evidence("brief-evidence")
    assert records[-2:] == suffix
    assert head == expected_head

    records[0]["payload"] = {"tampered": True}
    assert head is not None
    head["current_hash"] = "f" * 64
    detached_records, detached_head = ledger.get_evidence("brief-evidence")
    assert detached_records[0]["payload"] == {"title": "Delivery health"}
    assert detached_head == expected_head


@pytest.mark.parametrize("attack", ["mutation", "deletion", "reordering", "head-forgery"])
def test_ledger_rejects_non_append_evidence_edits_and_preserves_the_snapshot(
    ledger: LedgerImplementation, attack: str
) -> None:
    initial = _evidence_state()
    ledger.create(f"submission-{attack}", initial)

    with (
        pytest.raises(EvidenceIntegrityError),
        ledger.transaction("brief-evidence") as state,
    ):
        chain = state["evidence_chain"]
        assert isinstance(chain, list)
        if attack == "mutation":
            record = chain[0]
            assert isinstance(record, dict)
            record["payload"] = {"title": "rewritten"}
        elif attack == "deletion":
            chain.pop(0)
        elif attack == "reordering":
            chain.reverse()
        else:
            head = state["evidence_head"]
            assert isinstance(head, dict)
            head["current_hash"] = "f" * 64

    assert ledger.get("brief-evidence") == initial


def test_ledger_rejects_removing_the_chain_or_embedded_head(
    ledger: LedgerImplementation,
) -> None:
    initial = _evidence_state()
    ledger.create("submission-remove", initial)

    for removed_field in ("evidence_chain", "evidence_head"):
        with (
            pytest.raises(EvidenceIntegrityError),
            ledger.transaction("brief-evidence") as state,
        ):
            del state[removed_field]

        assert ledger.get("brief-evidence") == initial


def test_failed_transaction_does_not_advance_workflow_or_protected_head(
    ledger: LedgerImplementation,
) -> None:
    initial = _evidence_state()
    ledger.create("submission-rollback", initial)
    before = ledger.get_evidence("brief-evidence")

    with (
        pytest.raises(RuntimeError, match="simulated crash"),
        ledger.transaction("brief-evidence") as state,
    ):
        _append_valid_suffix(state)
        raise RuntimeError("simulated crash")

    assert ledger.get("brief-evidence") == initial
    assert ledger.get_evidence("brief-evidence") == before


def test_generic_transaction_can_start_evidence_for_a_legacy_workflow(
    ledger: LedgerImplementation,
) -> None:
    ledger.create("submission-upgrade", _legacy_state())

    with ledger.transaction("brief-legacy") as state:
        record, head = append(
            None,
            workflow_id="brief-legacy",
            record_type="workflow.upgraded",
            payload={"from": "legacy"},
            trusted_source="orchestrator",
        )
        state["evidence_chain"] = [record.to_dict()]
        state["evidence_head"] = _head_dict(head)

    assert ledger.get_evidence("brief-legacy") == ([record.to_dict()], _head_dict(head))


def test_ledger_verifies_evidence_during_create(ledger: LedgerImplementation) -> None:
    forged = _evidence_state()
    head = forged["evidence_head"]
    assert isinstance(head, dict)
    head["current_hash"] = "f" * 64

    with pytest.raises(EvidenceIntegrityError):
        ledger.create("submission-forged", forged)

    with pytest.raises(LedgerNotFound):
        ledger.get("brief-evidence")


def test_ledger_verifies_persisted_evidence_during_get_and_transaction(
    ledger: LedgerImplementation,
) -> None:
    ledger.create("submission-at-rest", _evidence_state())
    if isinstance(ledger, InMemoryLedger):
        ledger._briefs["brief-evidence"]["evidence_chain"][0]["payload"] = {  # type: ignore[index]
            "tampered": True
        }
    else:
        with ledger._pool.connection() as connection, connection.transaction():
            row = connection.execute(
                "SELECT state FROM steward_ledger.workflow WHERE id = %s",
                ("brief-evidence",),
            ).fetchone()
            assert row is not None
            state = dict(row[0])
            state["evidence_chain"][0]["payload"] = {"tampered": True}
            connection.execute(
                "UPDATE steward_ledger.workflow SET state = %s WHERE id = %s",
                (Jsonb(state), "brief-evidence"),
            )

    with pytest.raises(EvidenceIntegrityError):
        ledger.get("brief-evidence")
    with pytest.raises(EvidenceIntegrityError), ledger.transaction("brief-evidence"):
        pass


def test_legacy_workflow_without_evidence_remains_supported(
    ledger: LedgerImplementation,
) -> None:
    legacy = _legacy_state()
    stored, created = ledger.create("submission-legacy", legacy)
    assert created is True
    assert stored == legacy
    assert ledger.get_evidence("brief-legacy") == ([], None)

    with ledger.transaction("brief-legacy") as state:
        state["status"] = "scope_approved"

    assert ledger.get("brief-legacy") == {
        "id": "brief-legacy",
        "status": "scope_approved",
    }
    assert ledger.get_evidence("brief-legacy") == ([], None)


def test_postgres_migration_creates_a_private_versioned_head_table(postgres_url: str) -> None:
    with psycopg.connect(postgres_url, autocommit=True) as connection:
        connection.execute("DROP SCHEMA IF EXISTS steward_ledger CASCADE")
    ledger = PostgresLedger.from_conninfo(postgres_url)
    ledger.open()
    try:
        ledger.migrate()
        with psycopg.connect(postgres_url) as connection:
            columns = {
                str(row[0])
                for row in connection.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'steward_ledger'
                      AND table_name = 'evidence_head'
                    """
                ).fetchall()
            }
            foreign_key_delete_action = connection.execute(
                """
                SELECT confdeltype
                FROM pg_constraint
                WHERE conrelid = 'steward_ledger.evidence_head'::regclass
                  AND contype = 'f'
                """
            ).fetchone()
            public_can_select = connection.execute(
                "SELECT has_table_privilege('public', 'steward_ledger.evidence_head', 'SELECT')"
            ).fetchone()
            public_can_execute = connection.execute(
                """
                SELECT has_function_privilege(
                    'public',
                    'steward_ledger.advance_evidence_head(text,text,text,text,bigint,text,bigint)',
                    'EXECUTE'
                )
                """
            ).fetchone()
    finally:
        ledger.close()

    assert {
        "workflow_id",
        "serialization",
        "hash_algorithm",
        "chain_id",
        "sequence",
        "current_hash",
        "version",
    } <= columns
    assert foreign_key_delete_action == ("r",)
    assert public_can_select == (False,)
    assert public_can_execute == (False,)


@pytest.fixture
def protected_postgres_runtime(
    postgres_url: str,
) -> Iterator[tuple[str, PostgresLedger]]:
    with psycopg.connect(postgres_url, autocommit=True) as connection:
        connection.execute("DROP SCHEMA IF EXISTS steward_ledger CASCADE")
        connection.execute(f'DROP ROLE IF EXISTS "{RUNTIME_ROLE}"')
        connection.execute(f'CREATE ROLE "{RUNTIME_ROLE}" LOGIN')

    owner_ledger = PostgresLedger.from_conninfo(postgres_url)
    owner_ledger.open()
    try:
        owner_ledger.migrate(runtime_role=RUNTIME_ROLE)
    finally:
        owner_ledger.close()

    runtime_conninfo = make_conninfo(postgres_url, user=RUNTIME_ROLE)
    runtime_ledger = PostgresLedger.from_conninfo(
        runtime_conninfo,
        expected_runtime_role=RUNTIME_ROLE,
        enforce_head_protection=True,
    )
    runtime_ledger.open()
    runtime_ledger.migrate()
    try:
        yield runtime_conninfo, runtime_ledger
    finally:
        runtime_ledger.close()
        with psycopg.connect(postgres_url, autocommit=True) as connection:
            connection.execute("DROP SCHEMA IF EXISTS steward_ledger CASCADE")
            connection.execute(f'DROP ROLE IF EXISTS "{RUNTIME_ROLE}"')


def test_runtime_role_uses_only_the_narrow_head_cas_path(
    postgres_url: str,
    protected_postgres_runtime: tuple[str, PostgresLedger],
) -> None:
    runtime_conninfo, runtime_ledger = protected_postgres_runtime
    initial = _evidence_state()
    runtime_ledger.create("protected-submission", initial)

    with runtime_ledger.transaction("brief-evidence") as state:
        _, expected_head = _append_valid_suffix(state)

    assert runtime_ledger.get_evidence("brief-evidence")[1] == expected_head

    with psycopg.connect(postgres_url) as connection:
        privileges = connection.execute(
            """
            SELECT
                has_schema_privilege(%s, 'steward_ledger', 'CREATE'),
                has_table_privilege(%s, 'steward_ledger.workflow', 'SELECT,INSERT,UPDATE'),
                has_table_privilege(%s, 'steward_ledger.workflow', 'DELETE,TRUNCATE'),
                has_table_privilege(%s, 'steward_ledger.evidence_head', 'SELECT'),
                has_table_privilege(
                    %s,
                    'steward_ledger.evidence_head',
                    'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'
                ),
                has_function_privilege(
                    %s,
                    'steward_ledger.advance_evidence_head(text,text,text,text,bigint,text,bigint)',
                    'EXECUTE'
                )
            """,
            (RUNTIME_ROLE,) * 6,
        ).fetchone()

    assert privileges == (False, True, False, True, False, True)

    forbidden_statements = (
        "UPDATE steward_ledger.evidence_head SET current_hash = repeat('f', 64)",
        "DELETE FROM steward_ledger.evidence_head",
        "TRUNCATE steward_ledger.evidence_head",
        "ALTER TABLE steward_ledger.evidence_head ADD COLUMN forged text",
        "DELETE FROM steward_ledger.workflow WHERE id = 'brief-evidence'",
    )
    with psycopg.connect(runtime_conninfo, autocommit=True) as runtime_connection:
        for statement in forbidden_statements:
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                runtime_connection.execute(statement)


@pytest.mark.parametrize("attack", ["rehashed-history", "sequence-jump"])
def test_runtime_cannot_advance_head_from_forged_workflow_json(
    postgres_url: str,
    protected_postgres_runtime: tuple[str, PostgresLedger],
    attack: str,
) -> None:
    runtime_conninfo, runtime_ledger = protected_postgres_runtime
    runtime_ledger.create("forgery-submission", _evidence_state())
    original_state = runtime_ledger.get("brief-evidence")
    original_head = _protected_head(original_state)

    forged_state = _evidence_state()
    if attack == "rehashed-history":
        first, forged_head = append(
            None,
            workflow_id="brief-evidence",
            record_type="brief.submitted",
            payload={"title": "rewritten history"},
            trusted_source="orchestrator",
        )
        second, forged_head = append(
            forged_head,
            workflow_id="brief-evidence",
            record_type="scope.approved",
            payload={"approver": "attacker"},
            trusted_source="approval-gateway",
        )
        last_record, forged_head = append(
            forged_head,
            workflow_id="brief-evidence",
            record_type="release.completed",
            payload={"receipt_id": "forged"},
            trusted_source="release-gateway",
        )
        forged_state["evidence_chain"] = [
            first.to_dict(),
            second.to_dict(),
            last_record.to_dict(),
        ]
    else:
        chain = forged_state["evidence_chain"]
        assert isinstance(chain, list)
        last_record = {
            "serialization": original_head.serialization,
            "hash_algorithm": original_head.hash_algorithm,
            "chain_id": original_head.chain_id,
            "sequence": original_head.sequence + 2,
            "previous_hash": original_head.current_hash,
            "current_hash": "f" * 64,
            "record_type": "release.completed",
            "source": "release-gateway",
            "payload": {"receipt_id": "forged"},
        }
        chain.append(last_record)
        forged_head = ProtectedHead.from_record(EvidenceRecord.from_dict(last_record))
    forged_state["evidence_head"] = forged_head.to_dict()

    with psycopg.connect(runtime_conninfo) as runtime_connection:
        protected_version = int(
            runtime_connection.execute(
                """
                SELECT version
                FROM steward_ledger.evidence_head
                WHERE workflow_id = %s
                """,
                ("brief-evidence",),
            ).fetchone()[0]
        )
        runtime_connection.execute(
            "UPDATE steward_ledger.workflow SET state = %s WHERE id = %s",
            (Jsonb(forged_state), "brief-evidence"),
        )
        rejected = runtime_connection.execute(
            """
            SELECT steward_ledger.advance_evidence_head(%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                "brief-evidence",
                forged_head.serialization,
                forged_head.hash_algorithm,
                forged_head.chain_id,
                forged_head.sequence,
                forged_head.current_hash,
                protected_version,
            ),
        ).fetchone()
        assert rejected == (None,)
        unchanged_in_transaction = runtime_connection.execute(
            """
            SELECT chain_id, sequence, current_hash, version
            FROM steward_ledger.evidence_head
            WHERE workflow_id = %s
            """,
            ("brief-evidence",),
        ).fetchone()
        assert unchanged_in_transaction == (
            original_head.chain_id,
            original_head.sequence,
            original_head.current_hash,
            protected_version,
        )
        runtime_connection.rollback()

    with psycopg.connect(postgres_url) as owner_connection:
        persisted_head = owner_connection.execute(
            """
            SELECT chain_id, sequence, current_hash, version
            FROM steward_ledger.evidence_head
            WHERE workflow_id = %s
            """,
            ("brief-evidence",),
        ).fetchone()

    assert persisted_head == (
        original_head.chain_id,
        original_head.sequence,
        original_head.current_hash,
        protected_version,
    )


def test_workflow_delete_cannot_cascade_away_the_protected_anchor(
    postgres_url: str,
    protected_postgres_runtime: tuple[str, PostgresLedger],
) -> None:
    _, runtime_ledger = protected_postgres_runtime
    runtime_ledger.create("retained-submission", _evidence_state())

    with psycopg.connect(postgres_url, autocommit=True) as owner_connection:
        with pytest.raises(psycopg.errors.ForeignKeyViolation):
            owner_connection.execute(
                "DELETE FROM steward_ledger.workflow WHERE id = %s",
                ("brief-evidence",),
            )
        retained = owner_connection.execute(
            "SELECT current_hash FROM steward_ledger.evidence_head WHERE workflow_id = %s",
            ("brief-evidence",),
        ).fetchone()

    assert retained is not None


def test_protected_migration_rejects_the_runtime_as_object_owner(
    postgres_url: str,
) -> None:
    with psycopg.connect(postgres_url, autocommit=True) as connection:
        connection.execute("DROP SCHEMA IF EXISTS steward_ledger CASCADE")
        current_user = str(connection.execute("SELECT current_user").fetchone()[0])

    ledger = PostgresLedger.from_conninfo(postgres_url)
    ledger.open()
    try:
        with pytest.raises(HeadProtectionError, match="distinct migration owner"):
            ledger.migrate(runtime_role=current_user)
    finally:
        ledger.close()


def test_enforced_runtime_fails_closed_on_self_owned_objects(postgres_url: str) -> None:
    with psycopg.connect(postgres_url, autocommit=True) as connection:
        connection.execute("DROP SCHEMA IF EXISTS steward_ledger CASCADE")
        current_user = str(connection.execute("SELECT current_user").fetchone()[0])

    owner_ledger = PostgresLedger.from_conninfo(postgres_url)
    owner_ledger.open()
    try:
        owner_ledger.migrate()
    finally:
        owner_ledger.close()

    unseparated_runtime = PostgresLedger.from_conninfo(
        postgres_url,
        expected_runtime_role=current_user,
        enforce_head_protection=True,
    )
    unseparated_runtime.open()
    try:
        with pytest.raises(HeadProtectionError, match="must not own or inherit"):
            unseparated_runtime.migrate()
    finally:
        unseparated_runtime.close()
