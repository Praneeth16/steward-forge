"""PostgreSQL implementation of the deterministic workflow ledger."""

from __future__ import annotations

import hashlib
import json
import os
import re
from collections.abc import Iterator
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path
from typing import Any

import psycopg
from databricks.sdk import WorkspaceClient
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from evidence import ProtectedHead
from ledger.store import (
    EvidenceView,
    LedgerConflict,
    LedgerNotFound,
    WorkflowState,
    detached_evidence,
    parse_evidence_state,
    verify_evidence_state,
    verify_evidence_transition,
)

MIGRATIONS_DIR = Path(__file__).with_name("migrations")
MIGRATION_NAME = re.compile(r"^(?P<version>[0-9]+)_[a-z0-9_]+\.sql$")
MIGRATION_LOCK_KEY = 7_351_928_441_021_149_027
ADVANCE_HEAD_SIGNATURE = (
    "steward_ledger.advance_evidence_head(text,text,text,text,bigint,text,bigint)"
)


class HeadProtectionError(RuntimeError):
    """The database cannot enforce the protected-head trust boundary."""


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def _protected_head_from_row(brief_id: str, row: Any | None) -> ProtectedHead | None:
    if row is None or row[0] is None:
        return None
    return ProtectedHead(
        serialization=str(row[0]),
        hash_algorithm=str(row[1]),
        chain_id=str(row[2]).strip(),
        sequence=int(row[3]),
        current_hash=str(row[4]).strip(),
    )


class OAuthConnection(psycopg.Connection[Any]):
    """Mint a fresh Lakebase OAuth credential for each physical connection."""

    @classmethod
    def connect(cls, conninfo: str = "", **kwargs: Any) -> OAuthConnection:
        endpoint = os.environ["ENDPOINT_NAME"]
        credential = WorkspaceClient().postgres.generate_database_credential(endpoint=endpoint)
        kwargs["password"] = credential.token
        return super().connect(conninfo, **kwargs)


class PostgresLedger:
    """Persist workflow state with row-locked, replay-safe transactions."""

    def __init__(
        self,
        pool: ConnectionPool,
        *,
        expected_runtime_role: str | None = None,
        enforce_head_protection: bool = False,
    ) -> None:
        if enforce_head_protection and not expected_runtime_role:
            raise ValueError("protected-head enforcement requires an expected runtime role")
        self._pool = pool
        self._expected_runtime_role = expected_runtime_role
        self._enforce_head_protection = enforce_head_protection

    @classmethod
    def from_conninfo(
        cls,
        conninfo: str,
        *,
        expected_runtime_role: str | None = None,
        enforce_head_protection: bool = False,
    ) -> PostgresLedger:
        return cls(
            ConnectionPool(
                conninfo=conninfo,
                min_size=1,
                max_size=5,
                max_lifetime=2700,
                open=False,
            ),
            expected_runtime_role=expected_runtime_role,
            enforce_head_protection=enforce_head_protection,
        )

    @classmethod
    def from_environment(cls) -> PostgresLedger:
        runtime_role = os.environ["PGUSER"]
        connection_values = {
            "dbname": os.environ["PGDATABASE"],
            "user": runtime_role,
            "host": os.environ["PGHOST"],
            "port": os.getenv("PGPORT", "5432"),
            "sslmode": os.getenv("PGSSLMODE", "require"),
        }
        conninfo = make_conninfo(**connection_values)
        return cls(
            ConnectionPool(
                conninfo=conninfo,
                connection_class=OAuthConnection,
                min_size=1,
                max_size=10,
                max_lifetime=2700,
                open=False,
            ),
            expected_runtime_role=runtime_role,
            enforce_head_protection=True,
        )

    def open(self) -> None:
        self._pool.open(wait=True, timeout=30)

    def close(self) -> None:
        self._pool.close()

    def migrate(self, *, runtime_role: str | None = None) -> None:
        migrations: list[tuple[int, str, str, str]] = []
        versions: set[int] = set()
        for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
            match = MIGRATION_NAME.fullmatch(path.name)
            if match is None:
                raise ValueError(f"invalid migration filename: {path.name}")
            version = int(match.group("version"))
            if version <= 0 or version in versions:
                raise ValueError(f"invalid or duplicate migration version: {version}")
            versions.add(version)
            sql = path.read_text(encoding="utf-8")
            migrations.append(
                (
                    version,
                    path.name,
                    hashlib.sha256(sql.encode()).hexdigest(),
                    sql,
                )
            )

        if self._enforce_head_protection:
            if runtime_role is not None:
                raise HeadProtectionError(
                    "a runtime connection cannot configure its own protected-head grants"
                )
            self._validate_protected_runtime(migrations)
            return

        with self._pool.connection() as connection, connection.transaction():
            current_user = str(connection.execute("SELECT current_user").fetchone()[0])
            if runtime_role == current_user:
                raise HeadProtectionError(
                    "protected-head setup requires a distinct migration owner and runtime role"
                )
            connection.execute("CREATE SCHEMA IF NOT EXISTS steward_ledger")
            connection.execute(
                """
                    CREATE TABLE IF NOT EXISTS steward_ledger.schema_migration (
                        version integer PRIMARY KEY CHECK (version > 0),
                        name text NOT NULL UNIQUE,
                        checksum char(64) NOT NULL CHECK (checksum ~ '^[0-9a-f]{64}$'),
                        applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
                    )
                    """
            )
            connection.execute("SELECT pg_advisory_xact_lock(%s)", (MIGRATION_LOCK_KEY,))
            for version, name, checksum, sql in migrations:
                applied = connection.execute(
                    """
                        SELECT name, checksum
                        FROM steward_ledger.schema_migration
                        WHERE version = %s
                        """,
                    (version,),
                ).fetchone()
                if applied is not None:
                    actual = (str(applied[0]), str(applied[1]).strip())
                    expected = (name, checksum)
                    if actual != expected:
                        raise LedgerConflict(
                            f"migration {version} has changed: "
                            f"database={actual!r}, file={expected!r}"
                        )
                    continue
                connection.execute(sql)
                connection.execute(
                    """
                        INSERT INTO steward_ledger.schema_migration (version, name, checksum)
                        VALUES (%s, %s, %s)
                        """,
                    (version, name, checksum),
                )
            if runtime_role is not None:
                self._configure_protected_runtime(connection, runtime_role)

    def _validate_protected_runtime(self, migrations: list[tuple[int, str, str, str]]) -> None:
        runtime_role = self._expected_runtime_role
        assert runtime_role is not None
        with self._pool.connection() as connection, connection.transaction():
            current_user = str(connection.execute("SELECT current_user").fetchone()[0])
            if current_user != runtime_role:
                raise HeadProtectionError(
                    f"database authenticated as {current_user!r}, expected {runtime_role!r}"
                )
            try:
                applied_rows = connection.execute(
                    "SELECT version, name, checksum FROM steward_ledger.schema_migration"
                ).fetchall()
            except (psycopg.errors.InsufficientPrivilege, psycopg.errors.UndefinedTable) as error:
                raise HeadProtectionError(
                    "trusted migrations must be applied before the runtime starts"
                ) from error
            applied = {int(row[0]): (str(row[1]), str(row[2]).strip()) for row in applied_rows}
            for version, name, checksum, _ in migrations:
                if applied.get(version) != (name, checksum):
                    raise HeadProtectionError(
                        f"trusted migration {version} is missing or has a different checksum"
                    )
            self._assert_protected_runtime_boundary(connection, runtime_role)

    @staticmethod
    def _configure_protected_runtime(
        connection: psycopg.Connection[Any], runtime_role: str
    ) -> None:
        role_exists = connection.execute("SELECT to_regrole(%s)", (runtime_role,)).fetchone()
        if role_exists is None or role_exists[0] is None:
            raise HeadProtectionError(f"runtime database role does not exist: {runtime_role!r}")

        role = sql.Identifier(runtime_role)
        statements = (
            sql.SQL("REVOKE ALL ON SCHEMA steward_ledger FROM {}").format(role),
            sql.SQL("GRANT USAGE ON SCHEMA steward_ledger TO {}").format(role),
            sql.SQL(
                "REVOKE ALL ON TABLE steward_ledger.schema_migration, "
                "steward_ledger.workflow, steward_ledger.evidence_head FROM {}"
            ).format(role),
            sql.SQL("GRANT SELECT ON TABLE steward_ledger.schema_migration TO {}").format(role),
            sql.SQL("GRANT SELECT, INSERT, UPDATE ON TABLE steward_ledger.workflow TO {}").format(
                role
            ),
            sql.SQL("GRANT SELECT ON TABLE steward_ledger.evidence_head TO {}").format(role),
            sql.SQL(
                "REVOKE ALL ON FUNCTION "
                "steward_ledger.advance_evidence_head"
                "(text,text,text,text,bigint,text,bigint) FROM {}"
            ).format(role),
            sql.SQL(
                "GRANT EXECUTE ON FUNCTION "
                "steward_ledger.advance_evidence_head"
                "(text,text,text,text,bigint,text,bigint) TO {}"
            ).format(role),
        )
        for statement in statements:
            connection.execute(statement)
        PostgresLedger._assert_protected_runtime_boundary(connection, runtime_role)

    @staticmethod
    def _assert_protected_runtime_boundary(
        connection: psycopg.Connection[Any], runtime_role: str
    ) -> None:
        owners = connection.execute(
            """
            SELECT owner_name
            FROM (
                SELECT pg_get_userbyid(nspowner) AS owner_name
                FROM pg_namespace
                WHERE nspname = 'steward_ledger'
                UNION ALL
                SELECT pg_get_userbyid(relowner) AS owner_name
                FROM pg_class
                WHERE oid IN (
                    'steward_ledger.workflow'::regclass,
                    'steward_ledger.evidence_head'::regclass
                )
                UNION ALL
                SELECT pg_get_userbyid(proowner) AS owner_name
                FROM pg_proc
                WHERE oid = %s::regprocedure
            ) AS protected_owners
            """,
            (ADVANCE_HEAD_SIGNATURE,),
        ).fetchall()
        if len(owners) != 4:
            raise HeadProtectionError("protected-head database objects are incomplete")
        owner_names = {str(row[0]) for row in owners}
        for owner_name in owner_names:
            membership = connection.execute(
                "SELECT pg_has_role(%s, %s, 'MEMBER')",
                (runtime_role, owner_name),
            ).fetchone()
            if membership is None or bool(membership[0]):
                raise HeadProtectionError(
                    "runtime role must not own or inherit a protected database object"
                )

        privileges = connection.execute(
            """
            SELECT
                has_schema_privilege(%s, 'steward_ledger', 'CREATE'),
                has_table_privilege(
                    %s,
                    'steward_ledger.evidence_head',
                    'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'
                ),
                has_table_privilege(
                    %s,
                    'steward_ledger.workflow',
                    'DELETE,TRUNCATE,REFERENCES,TRIGGER'
                ),
                has_table_privilege(%s, 'steward_ledger.evidence_head', 'SELECT'),
                has_table_privilege(
                    %s,
                    'steward_ledger.workflow',
                    'SELECT,INSERT,UPDATE'
                ),
                has_function_privilege(%s, %s, 'EXECUTE')
            """,
            (
                runtime_role,
                runtime_role,
                runtime_role,
                runtime_role,
                runtime_role,
                runtime_role,
                ADVANCE_HEAD_SIGNATURE,
            ),
        ).fetchone()
        if privileges != (False, False, False, True, True, True):
            raise HeadProtectionError(
                "runtime role privileges do not match the protected-head boundary"
            )

    def create(
        self, idempotency_key: str, initial_state: WorkflowState
    ) -> tuple[WorkflowState, bool]:
        brief_id = str(initial_state["id"])
        _, initial_head = parse_evidence_state(initial_state, workflow_id=brief_id)
        input_hash = hashlib.sha256(_canonical_bytes(initial_state)).hexdigest()
        with self._pool.connection() as connection, connection.transaction():
            inserted = connection.execute(
                """
                    INSERT INTO steward_ledger.workflow (
                        id, idempotency_key, input_hash, state
                    ) VALUES (%s, %s, %s, %s)
                    ON CONFLICT (idempotency_key) DO NOTHING
                    RETURNING state
                    """,
                (
                    brief_id,
                    idempotency_key,
                    input_hash,
                    Jsonb(initial_state),
                ),
            ).fetchone()
            if inserted is not None:
                if initial_head is not None:
                    self._advance_protected_head(
                        connection,
                        brief_id,
                        initial_head,
                        expected_version=0,
                    )
                return dict(inserted[0]), True

            existing = connection.execute(
                """
                    SELECT
                        workflow.input_hash,
                        workflow.state,
                        evidence_head.serialization,
                        evidence_head.hash_algorithm,
                        evidence_head.chain_id,
                        evidence_head.sequence,
                        evidence_head.current_hash
                    FROM steward_ledger.workflow
                    LEFT JOIN steward_ledger.evidence_head
                        ON evidence_head.workflow_id = workflow.id
                    WHERE workflow.idempotency_key = %s
                    """,
                (idempotency_key,),
            ).fetchone()
            if existing is None:
                raise RuntimeError("idempotent workflow disappeared")
            if str(existing[0]).strip() != input_hash:
                raise LedgerConflict("idempotency key is already bound to a different payload")
            existing_state = dict(existing[1])
            protected_head = _protected_head_from_row(brief_id, existing[2:7])
            verify_evidence_state(existing_state, protected_head, workflow_id=brief_id)
            return existing_state, False

    def get(self, brief_id: str) -> WorkflowState:
        with self._pool.connection() as connection:
            row = connection.execute(
                """
                SELECT
                    workflow.state,
                    evidence_head.serialization,
                    evidence_head.hash_algorithm,
                    evidence_head.chain_id,
                    evidence_head.sequence,
                    evidence_head.current_hash
                FROM steward_ledger.workflow
                LEFT JOIN steward_ledger.evidence_head
                    ON evidence_head.workflow_id = workflow.id
                WHERE workflow.id = %s
                """,
                (brief_id,),
            ).fetchone()
        if row is None:
            raise LedgerNotFound(brief_id)
        state = dict(row[0])
        protected_head = _protected_head_from_row(brief_id, row[1:6])
        verify_evidence_state(state, protected_head, workflow_id=brief_id)
        return state

    def get_evidence(self, brief_id: str) -> EvidenceView:
        with self._pool.connection() as connection:
            row = connection.execute(
                """
                SELECT
                    workflow.state,
                    evidence_head.serialization,
                    evidence_head.hash_algorithm,
                    evidence_head.chain_id,
                    evidence_head.sequence,
                    evidence_head.current_hash
                FROM steward_ledger.workflow
                LEFT JOIN steward_ledger.evidence_head
                    ON evidence_head.workflow_id = workflow.id
                WHERE workflow.id = %s
                """,
                (brief_id,),
            ).fetchone()
        if row is None:
            raise LedgerNotFound(brief_id)
        state = dict(row[0])
        protected_head = _protected_head_from_row(brief_id, row[1:6])
        records, protected_head = verify_evidence_state(
            state,
            protected_head,
            workflow_id=brief_id,
        )
        return detached_evidence(records, protected_head)

    @contextmanager
    def transaction(self, brief_id: str) -> Iterator[WorkflowState]:
        with self._pool.connection() as connection, connection.transaction():
            row = connection.execute(
                """
                    SELECT state, version
                    FROM steward_ledger.workflow
                    WHERE id = %s
                    FOR UPDATE
                    """,
                (brief_id,),
            ).fetchone()
            if row is None:
                raise LedgerNotFound(brief_id)
            state = dict(row[0])
            protected_row = connection.execute(
                """
                SELECT serialization, hash_algorithm, chain_id, sequence, current_hash, version
                FROM steward_ledger.evidence_head
                WHERE workflow_id = %s
                """,
                (brief_id,),
            ).fetchone()
            protected_head = _protected_head_from_row(brief_id, protected_row)
            verify_evidence_state(state, protected_head, workflow_id=brief_id)
            previous_state = deepcopy(state)
            before = _canonical_bytes(state)
            yield state
            current_head = verify_evidence_transition(
                previous_state,
                protected_head,
                state,
                workflow_id=brief_id,
            )
            if _canonical_bytes(state) == before:
                return
            if current_head == protected_head:
                self._update_workflow_state(
                    connection,
                    brief_id,
                    state,
                    expected_version=int(row[1]),
                )
                return
            if current_head is None:
                raise RuntimeError("protected evidence head cannot be removed")

            previous_records, _ = parse_evidence_state(
                previous_state,
                workflow_id=brief_id,
            )
            current_records, _ = parse_evidence_state(state, workflow_id=brief_id)
            appended_records = current_records[len(previous_records) :]
            if not appended_records:
                raise RuntimeError("evidence head changed without an appended record")

            workflow_version = int(row[1])
            head_version = int(protected_row[5]) if protected_row is not None else 0
            for offset, record in enumerate(appended_records, start=1):
                intermediate_state = deepcopy(state)
                intermediate_records = previous_records + appended_records[:offset]
                intermediate_head = ProtectedHead.from_record(record)
                intermediate_state["evidence_chain"] = [
                    item.to_dict() for item in intermediate_records
                ]
                intermediate_state["evidence_head"] = intermediate_head.to_dict()
                workflow_version = self._update_workflow_state(
                    connection,
                    brief_id,
                    intermediate_state,
                    expected_version=workflow_version,
                )
                self._advance_protected_head(
                    connection,
                    brief_id,
                    intermediate_head,
                    expected_version=head_version,
                )
                head_version += 1

    @staticmethod
    def _update_workflow_state(
        connection: psycopg.Connection[Any],
        brief_id: str,
        state: WorkflowState,
        *,
        expected_version: int,
    ) -> int:
        updated = connection.execute(
            """
            UPDATE steward_ledger.workflow
            SET state = %s, version = version + 1, updated_at = clock_timestamp()
            WHERE id = %s AND version = %s
            """,
            (Jsonb(state), brief_id, expected_version),
        )
        if updated.rowcount != 1:
            raise LedgerConflict("workflow version changed concurrently")
        return expected_version + 1

    @staticmethod
    def _advance_protected_head(
        connection: psycopg.Connection[Any],
        brief_id: str,
        current_head: ProtectedHead,
        *,
        expected_version: int,
    ) -> None:
        advanced = connection.execute(
            """
            SELECT steward_ledger.advance_evidence_head(%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                brief_id,
                current_head.serialization,
                current_head.hash_algorithm,
                current_head.chain_id,
                current_head.sequence,
                current_head.current_hash,
                expected_version,
            ),
        ).fetchone()
        if advanced is None or advanced[0] is None:
            raise LedgerConflict("protected evidence head version changed concurrently")
