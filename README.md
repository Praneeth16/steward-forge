# Steward Forge

Governed digital workers that turn approved briefs into verifiable data products.

Steward Forge is an evidence-first Databricks reference application. A Product Manager, Scrum Master, Data Engineer, and Software Engineer collaborate through versioned contracts and a deterministic orchestrator. Software, not a model, owns permissions, state transitions, approvals, tests, deployment, and receipts.

The project is under active construction. The issue tracker is the execution queue; completed capabilities must include tests and deployment evidence before they are described as working.

## Product principles

- Workers propose and perform bounded work; deterministic services authorize mutations.
- Human decisions bind to an exact scope version or commit SHA.
- Every mutation is idempotent and returns a receipt.
- Lakebase holds operational state; Unity Catalog holds governed data and durable release evidence.
- Generated code runs in an isolated environment and passes policy, quality, secret, and harmful-diff checks.
- One Databricks Asset Bundle creates project-owned resources using overridable variables.

## Planned deployment

The bundle will create a dedicated Autoscaling Lakebase project and database, dedicated schemas in a caller-supplied writable Unity Catalog catalog, applications, experiments, jobs, and dashboards. Catalog storage is an account-level prerequisite because some workspaces do not permit catalog creation through the Catalog API. Steward Forge will not adopt existing application schemas or Lakebase resources. Environment-specific values belong in bundle variables or local authentication profiles, never in committed source.

See [the public build plan](docs/plans/steward-forge-build-plan.md), [product vision](docs/vision/steward-forge-vision.md), [four-worker orchestration](docs/architecture/four-worker-orchestration.md), and [recovery contract](docs/security/recovery.md).

## Governed model boundary

All model-backed worker traffic must enter through `GovernedModelGateway`. One trusted App-environment loader builds the four worker policies; requests cannot choose an endpoint, model, or service identity. The gateway reserves the worst-case authorized cost before a call, applies request and concurrency limits, retries throttles separately from domain repair, redacts trace payloads, and reconciles provider usage with configured prices and guardrail evidence.

The checked-in workers remain deterministic while provider integration is incomplete. Their budget summary therefore reports `usage_status=not_used`, zero committed reservation, and zero metered actual cost. It does not generate fake traffic or estimated spend. The workbench exposes only cost and trace metadata to the brief owner, named viewers, and auditors. It never returns prompt or output content from those endpoints.

The bundle creates a dedicated MLflow experiment, grants the App edit access, and grants the configured auditor group read access. Model calls are disabled by default. An installation must inject both a provider transport and a scoped trace adapter before setting `model_calls_enabled=true`; no provider credential or captured vendor response payload is stored in this repository. See [the model governance contract](docs/architecture/model-governance.md).

## Deploy the foundation

Prerequisites are Databricks CLI 0.285 or newer, `uv`, an authenticated serverless workspace with Lakebase Autoscaling, and an existing writable Unity Catalog catalog.

```bash
uv sync --dev
uv run pytest
uv run python scripts/deploy.py --profile <your-profile> --catalog <your-writable-catalog>
```

The helper derives the Lakebase owner-role ID from the authenticated user and invokes one `databricks bundle deploy`. Override `lakebase_project_id` or `lakebase_database` with normal bundle variables when the defaults already exist.

Database migrations remain an owner-run deployment prerequisite. The App runtime validates the protected schema under its separate runtime role and fails closed when the owner has not applied the required migrations.
