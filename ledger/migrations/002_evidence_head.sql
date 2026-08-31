CREATE TABLE IF NOT EXISTS steward_ledger.evidence_head (
    workflow_id text PRIMARY KEY
        REFERENCES steward_ledger.workflow (id) ON DELETE RESTRICT,
    serialization text NOT NULL
        CHECK (serialization = 'steward-forge-json-v1'),
    hash_algorithm text NOT NULL
        CHECK (hash_algorithm = 'sha256'),
    chain_id char(64) NOT NULL
        CHECK (chain_id ~ '^[0-9a-f]{64}$'),
    sequence bigint NOT NULL CHECK (sequence > 0),
    current_hash char(64) NOT NULL
        CHECK (current_hash ~ '^[0-9a-f]{64}$'),
    version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

REVOKE ALL ON steward_ledger.evidence_head FROM PUBLIC;

CREATE OR REPLACE FUNCTION steward_ledger.advance_evidence_head(
    requested_workflow_id text,
    requested_serialization text,
    requested_hash_algorithm text,
    requested_chain_id text,
    requested_sequence bigint,
    requested_current_hash text,
    expected_version bigint
) RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    embedded_head jsonb;
    evidence_chain jsonb;
    final_record jsonb;
    stored_serialization text;
    stored_hash_algorithm text;
    stored_chain_id text;
    stored_sequence bigint;
    stored_current_hash text;
    stored_version bigint;
    head_exists boolean;
    advanced_version bigint;
BEGIN
    IF requested_workflow_id IS NULL OR btrim(requested_workflow_id) = '' THEN
        RAISE EXCEPTION 'workflow ID must not be empty' USING ERRCODE = '23514';
    END IF;
    IF requested_serialization <> 'steward-forge-json-v1' THEN
        RAISE EXCEPTION 'unsupported evidence serialization' USING ERRCODE = '23514';
    END IF;
    IF requested_hash_algorithm <> 'sha256' THEN
        RAISE EXCEPTION 'unsupported evidence hash algorithm' USING ERRCODE = '23514';
    END IF;
    IF requested_chain_id !~ '^[0-9a-f]{64}$' THEN
        RAISE EXCEPTION 'evidence chain ID must be lowercase SHA-256' USING ERRCODE = '23514';
    END IF;
    IF requested_sequence <= 0 THEN
        RAISE EXCEPTION 'evidence sequence must be positive' USING ERRCODE = '23514';
    END IF;
    IF requested_current_hash !~ '^[0-9a-f]{64}$' THEN
        RAISE EXCEPTION 'evidence hash must be lowercase SHA-256' USING ERRCODE = '23514';
    END IF;
    IF expected_version IS NULL OR expected_version < 0 THEN
        RAISE EXCEPTION 'expected head version must be a non-negative integer'
            USING ERRCODE = '23514';
    END IF;

    SELECT state -> 'evidence_head', state -> 'evidence_chain'
    INTO embedded_head, evidence_chain
    FROM steward_ledger.workflow
    WHERE id = requested_workflow_id
    FOR UPDATE;

    IF embedded_head IS NULL OR jsonb_typeof(evidence_chain) IS DISTINCT FROM 'array' THEN
        RETURN NULL;
    END IF;
    IF jsonb_array_length(evidence_chain) <> requested_sequence THEN
        RETURN NULL;
    END IF;
    final_record := evidence_chain -> -1;

    IF final_record IS NULL
        OR embedded_head ->> 'serialization' IS DISTINCT FROM requested_serialization
        OR embedded_head ->> 'hash_algorithm' IS DISTINCT FROM requested_hash_algorithm
        OR embedded_head ->> 'chain_id' IS DISTINCT FROM requested_chain_id
        OR (embedded_head ->> 'sequence')::bigint IS DISTINCT FROM requested_sequence
        OR embedded_head ->> 'current_hash' IS DISTINCT FROM requested_current_hash
        OR final_record ->> 'serialization' IS DISTINCT FROM requested_serialization
        OR final_record ->> 'hash_algorithm' IS DISTINCT FROM requested_hash_algorithm
        OR final_record ->> 'chain_id' IS DISTINCT FROM requested_chain_id
        OR (final_record ->> 'sequence')::bigint IS DISTINCT FROM requested_sequence
        OR final_record ->> 'current_hash' IS DISTINCT FROM requested_current_hash
    THEN
        RETURN NULL;
    END IF;

    SELECT
        serialization,
        hash_algorithm,
        chain_id,
        sequence,
        current_hash,
        version
    INTO
        stored_serialization,
        stored_hash_algorithm,
        stored_chain_id,
        stored_sequence,
        stored_current_hash,
        stored_version
    FROM steward_ledger.evidence_head
    WHERE workflow_id = requested_workflow_id
    FOR UPDATE;
    head_exists := FOUND;

    IF head_exists THEN
        IF expected_version IS DISTINCT FROM stored_version
            OR requested_serialization IS DISTINCT FROM stored_serialization
            OR requested_hash_algorithm IS DISTINCT FROM stored_hash_algorithm
            OR requested_chain_id IS DISTINCT FROM stored_chain_id
            OR requested_sequence IS DISTINCT FROM stored_sequence + 1
            OR final_record ->> 'previous_hash' IS DISTINCT FROM stored_current_hash
        THEN
            RETURN NULL;
        END IF;
    ELSIF expected_version <> 0 THEN
        RETURN NULL;
    END IF;

    IF NOT head_exists AND EXISTS (
        SELECT 1
        FROM (
            SELECT
                value AS record,
                ordinality,
                lag(value ->> 'current_hash') OVER (ORDER BY ordinality) AS prior_hash
            FROM jsonb_array_elements(evidence_chain) WITH ORDINALITY
        ) AS records
        WHERE (record ->> 'sequence')::bigint IS DISTINCT FROM ordinality
            OR record ->> 'serialization' IS DISTINCT FROM requested_serialization
            OR record ->> 'hash_algorithm' IS DISTINCT FROM requested_hash_algorithm
            OR record ->> 'chain_id' IS DISTINCT FROM requested_chain_id
            OR record ->> 'previous_hash' IS DISTINCT FROM CASE
                WHEN ordinality = 1 THEN repeat('0', 64)
                ELSE prior_hash
            END
    ) THEN
        RETURN NULL;
    END IF;

    IF NOT head_exists THEN
        INSERT INTO steward_ledger.evidence_head (
            workflow_id,
            serialization,
            hash_algorithm,
            chain_id,
            sequence,
            current_hash
        ) VALUES (
            requested_workflow_id,
            requested_serialization,
            requested_hash_algorithm,
            requested_chain_id,
            requested_sequence,
            requested_current_hash
        )
        ON CONFLICT (workflow_id) DO NOTHING
        RETURNING version INTO advanced_version;
    ELSE
        UPDATE steward_ledger.evidence_head
        SET
            serialization = requested_serialization,
            hash_algorithm = requested_hash_algorithm,
            chain_id = requested_chain_id,
            sequence = requested_sequence,
            current_hash = requested_current_hash,
            version = version + 1,
            updated_at = clock_timestamp()
        WHERE workflow_id = requested_workflow_id
          AND version = expected_version
          AND sequence + 1 = requested_sequence
          AND current_hash = final_record ->> 'previous_hash'
        RETURNING version INTO advanced_version;
    END IF;

    RETURN advanced_version;
END;
$$;

REVOKE ALL ON FUNCTION steward_ledger.advance_evidence_head(
    text, text, text, text, bigint, text, bigint
) FROM PUBLIC;
