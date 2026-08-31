"""PostgreSQL implementation of the deterministic workflow ledger."""

from __future__ import annotations

import hashlib
import json
import os
import re
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import psycopg
from databricks.sdk import WorkspaceClient
from psycopg.conninfo import make_conninfo
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from ledger.store import LedgerConflict, LedgerNotFound, WorkflowState

MIGRATIONS_DIR = Path(__file__).with_name("migrations")
MIGRATION_NAME = re.compile(r"^(?P<version>[0-9]+)_[a-z0-9_]+\.sql$")
MIGRATION_LOCK_KEY = 7_351_928_441_021_149_027


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


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

    def __init__(self, pool: ConnectionPool) -> None:
        self._pool = pool

    @classmethod
    def from_conninfo(cls, conninfo: str) -> PostgresLedger:
        return cls(
            ConnectionPool(
                conninfo=conninfo,
                min_size=1,
                max_size=5,
                max_lifetime=2700,
                open=False,
            )
        )

    @classmethod
    def from_environment(cls) -> PostgresLedger:
        connection_values = {
            "dbname": os.environ["PGDATABASE"],
            "user": os.environ["PGUSER"],
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
            )
        )

    def open(self) -> None:
        self._pool.open(wait=True, timeout=30)

    def close(self) -> None:
        self._pool.close()

    def migrate(self) -> None:
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

        with self._pool.connection() as connection, connection.transaction():
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

    def create(
        self, idempotency_key: str, initial_state: WorkflowState
    ) -> tuple[WorkflowState, bool]:
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
                    str(initial_state["id"]),
                    idempotency_key,
                    input_hash,
                    Jsonb(initial_state),
                ),
            ).fetchone()
            if inserted is not None:
                return dict(inserted[0]), True

            existing = connection.execute(
                """
                    SELECT input_hash, state
                    FROM steward_ledger.workflow
                    WHERE idempotency_key = %s
                    """,
                (idempotency_key,),
            ).fetchone()
            if existing is None:
                raise RuntimeError("idempotent workflow disappeared")
            if str(existing[0]).strip() != input_hash:
                raise LedgerConflict("idempotency key is already bound to a different payload")
            return dict(existing[1]), False

    def get(self, brief_id: str) -> WorkflowState:
        with self._pool.connection() as connection:
            row = connection.execute(
                "SELECT state FROM steward_ledger.workflow WHERE id = %s", (brief_id,)
            ).fetchone()
        if row is None:
            raise LedgerNotFound(brief_id)
        return dict(row[0])

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
            before = _canonical_bytes(state)
            yield state
            if _canonical_bytes(state) == before:
                return
            updated = connection.execute(
                """
                    UPDATE steward_ledger.workflow
                    SET state = %s, version = version + 1, updated_at = clock_timestamp()
                    WHERE id = %s AND version = %s
                    """,
                (Jsonb(state), brief_id, int(row[1])),
            )
            if updated.rowcount != 1:
                raise LedgerConflict("workflow version changed concurrently")
