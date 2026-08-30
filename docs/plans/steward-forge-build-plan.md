---
title: Steward Forge build plan
type: feat
execution: code
status: active
---

# Steward Forge build plan

## Outcome

Build four governed digital workers that turn an approved engineering brief into a tested sandbox data product with human approvals, deterministic gates, recovery controls, cost visibility, and a verifiable evidence receipt.

The repository must deploy through one Databricks Asset Bundle. A fresh deployment creates its own Autoscaling Lakebase project, logical database, Unity Catalog catalog, applications, experiments, jobs, and dashboards. Existing application resources are never adopted by default.

## Actors

- Employee: submits a brief and follows progress.
- Named approver: approves a scope version and a release SHA.
- Product Manager worker: proposes outcome, scope, assumptions, and acceptance tests.
- Scrum Master worker: plans bounded tasks and emits escalations.
- Data Engineer worker: produces governed pipeline artifacts.
- Software Engineer worker: produces dashboard and interface artifacts.
- Operator: controls worker access and recovery.
- Auditor: verifies evidence and receipts.

## Core requirements

- R1: Each worker has a distinct identity, contract, tool boundary, and evidence trail.
- R2: The Scrum Master is the first real worker in an end-to-end slice.
- R3: Workers communicate only through a deterministic orchestrator and versioned ledger contracts.
- R4: Deterministic software owns authorization, transitions, approvals, tests, release, and receipts.
- R5: Every mutation is idempotent and returns a receipt.
- R6: Leases, epochs, checkpoints, and a reversible kill switch prevent stale-worker writes.
- R7: Governed model services enforce worker limits and brief budgets.
- R8: Evidence links released output to its brief, data, code, model, tools, approvals, tests, and cost.
- R9: A fail-closed pre-act check protects every mutation while evidence writes remain exempt.
- R10: Approval identity comes from validated user claims with separation of duties and row visibility.
- R11: Worker runs are traced to access-controlled MLflow experiments.
- R12: Each installation owns one sandbox catalog and one Autoscaling Lakebase project.
- R13: One bundle deploys the system using overridable environment variables.
- R14: Preview dependencies have tested fallbacks that preserve the IDE-like experience.
- R15: Demonstration data is deterministic, synthetic, classified, and retention-aware.
- R16: Evaluation reports exact denominators for safety, completeness, acceptance, recovery, time, and cost.
- R17: The demo proves approval, repair, denial, recovery, release, evidence, and cost from a live brief.
- R18: Regulated-use documentation excludes final critical decisions by probabilistic workers.
- R19: Evidence is append-only, canonicalized at trusted boundaries, hash-chained, and independently verifiable.
- R20: Adversarial evaluation includes hostile briefs, poisoned data, malicious artifacts, and harmful valid actions.
- R21: Generated code executes with no egress under a minimal identity and passes secret and diff scanning.
- R22: Approval and its transition commit atomically against an exact version or SHA.

## Technical decisions

### Dedicated resources

The bundle variables include 

- `lakebase_project_id`, default `steward-forge-lakebase`
- `lakebase_database`, default `steward_forge`
- `catalog_name`, default `steward_forge`
- model endpoints, artifact repository, deployment root, and optional feature flags

The bundle owns the production database branch and endpoint. Evaluation and demo branches are short-lived operational resources created outside bundle ownership so a normal deploy cannot overwrite frozen evidence.

### Deterministic orchestration

The orchestrator is the only writer of workflow transitions. Compare-and-set transitions, unique event bindings, leader epochs, and worker lease epochs prevent double execution after concurrency or restart.

### Governed mutation

Workers use direct connections only for approved reads. Each mutation routes through a capability broker that validates the worker contract, typed arguments, identity, pre-act health, idempotency key, and budget before execution.

### Approval integrity

The workbench forwards a user token to the orchestrator. The orchestrator validates the token, role, named approver, separation of duties, gate order, decision ID, and scope version or commit SHA. Approval and transition share one database transaction.

### Evidence and release

Trusted boundaries canonicalize evidence. Task events and receipts share a chain ID, sequence, previous hash, current hash, and separately protected chain head. Release intent is written to an outbox before deployment. Delta evidence and its Lakebase pointer share one receipt ID and reconcile after partial failure.

### Generated artifacts

The default PoC stores generated artifacts under `generated/**` in this repository. The artifact broker rejects platform, infrastructure, secret, and `.github/**` paths. An installation may point the broker at a separate repository without changing code.

## Delivery slices

1. Bootstrap the portable bundle and verify a fresh target.
2. Ship the first governed brief-to-receipt Scrum Master path.
3. Enforce contracts, arguments, idempotency, and pre-act health.
4. Bind approvals to validated identity, roles, rows, versions, and SHAs.
5. Fence leases and prove kill, restore, checkpoint, and restart recovery.
6. Deliver the deterministic synthetic-data pipeline through the Data Engineer.
7. Deliver dashboard and optional Genie output through the Software Engineer release path.
8. Complete Product Manager, Scrum Master, Data Engineer, and Software Engineer orchestration.
9. Prove hash-chain integrity, durable release intent, and torn-write reconciliation.
10. Enforce model limits, privacy logging, MLflow traces, and cost ceilings.
11. Run cooperative and adversarial proof gates with exact denominators.
12. Harden the IDE-like workbench, portable deployment, demo, evidence pack, and regulated-use boundary.

The public GitHub issues contain acceptance criteria and dependencies for each slice.

## Scope boundaries

- No production data or credentials.
- No regulated automation or final critical decision by a model.
- No enterprise identity federation implementation.
- No customer-specific network, gateway, or control-plane integration.
- No claim that a preview capability is generally available.
- No reuse of another application's Lakebase project, catalog, identities, or secrets.

## Verification contract

A capability is complete only when its tests pass, its real integration path is exercised, deployment evidence is recorded, failure behavior is known, and public documentation states the observed result without overstating it.
