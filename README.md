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

See [the public build plan](docs/plans/steward-forge-build-plan.md), [product vision](docs/vision/steward-forge-vision.md), and [recovery contract](docs/security/recovery.md).

## Deploy the foundation

Prerequisites are Databricks CLI 0.285 or newer, `uv`, an authenticated serverless workspace with Lakebase Autoscaling, and an existing writable Unity Catalog catalog.

```bash
uv sync --dev
uv run pytest
uv run python scripts/deploy.py --profile <your-profile> --catalog <your-writable-catalog>
```

The helper derives the Lakebase owner-role ID from the authenticated user and invokes one `databricks bundle deploy`. Override `lakebase_project_id` or `lakebase_database` with normal bundle variables when the defaults already exist.
