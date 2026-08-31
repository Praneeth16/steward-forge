CREATE TABLE IF NOT EXISTS steward_ledger.workflow (
    id text PRIMARY KEY CHECK (btrim(id) <> ''),
    idempotency_key text NOT NULL UNIQUE CHECK (btrim(idempotency_key) <> ''),
    input_hash char(64) NOT NULL CHECK (input_hash ~ '^[0-9a-f]{64}$'),
    state jsonb NOT NULL CHECK (state ->> 'id' = id),
    version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

REVOKE ALL ON SCHEMA steward_ledger FROM PUBLIC;
REVOKE ALL ON steward_ledger.workflow FROM PUBLIC;
