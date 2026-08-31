# Release evidence and reconciliation

## Canonical chain

Steward Forge serializes evidence as compact, key-sorted ASCII JSON under the
`steward-forge-json-v1` contract. Non-finite numbers are rejected. Every record
contains the serialization name, `sha256` algorithm, chain ID, positive sequence,
previous hash, record type, trusted source, payload, and current hash. The chain ID
is the SHA-256 digest of the serialization name and workflow ID. Sequence one uses
64 zeroes as its previous hash. Later records bind the prior current hash.

The ledger verifies the full chain on create and read. A transaction may preserve
the chain or append a valid suffix; mutation, deletion, reordering, truncation, and
forking fail closed. The current head is duplicated in workflow state for portable
readback and stored separately as a protected trust anchor. The transactional ledger
locks and updates the workflow row and protected head in one transaction. Its schema
revokes public access. A production grant must keep worker identities read-only and
reserve head updates for the trusted ledger role.

## Durable release intent

The exact release decision, workflow transition to `release_in_progress`, and
release intent commit in one ledger transaction before the deployment adapter can
run. The intent binds the brief, run, task, code and artifact hashes, data receipt
and relations, scope and release approvals, deterministic gates, cost, explicit
model-usage status, and deployment idempotency key.

The full 64-character request hash is the collision-resistant identity. A
24-character receipt ID derived from that hash preserves compatibility with the
existing broker receipt format. The deployment observation, governed receipt, and
operational pointer carry both values.

The deployment adapter observes remote state before mutation. A lost acknowledgement
therefore resumes from the original remote result without another deployment or a
second worker-attempt charge. A reused idempotency key with different release input
fails closed.

## Cross-store publication

The canonical governed receipt store is insert-only. The operational pointer store is
also insert-only and binds the same receipt ID to the full request hash, canonical
receipt hash, evidence-chain reference, and explicit canonical-store location.
Publication preflights both stores, rejects conflicting bytes, and repairs either a
missing receipt or a missing pointer from the canonical outbox result.

The public run result exposes the governed receipt, pointer, readable evidence chain,
and protected head. Its receipt links output to brief, code, data, approvals, gates,
cost, deployment output, and model usage. The current deterministic local workers set
`model_usage_status` to `not_used`; later model-backed workers must record or mark
usage unavailable explicitly.

The transactional protected-head path and cross-store algorithms are exercised
locally, including crash reconstruction with new coordinator, release-service, and
deployment adapter instances. The receipt and pointer stores used by these tests are
in-memory adapters.

## Databricks reference-deployment mapping

The Databricks deployment maps the canonical governed receipt store to a Unity
Catalog-managed Delta evidence table and the operational pointer store to Lakebase.
Its PostgreSQL ledger stores the protected trust anchor in
`steward_ledger.evidence_head`. These are reference-deployment choices, not
requirements of the release-evidence or reconciliation contracts.

A live Delta write, Lakebase pointer write, and workspace deployment remain
unverified until the target deployment records redacted evidence.
