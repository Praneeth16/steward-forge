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

The bundle will create a dedicated Autoscaling Lakebase project, database, Unity Catalog catalog, applications, experiments, jobs, and dashboards. It will not adopt existing application resources. Environment-specific values belong in bundle variables or local authentication profiles, never in committed source.

See [the public build plan](docs/plans/steward-forge-build-plan.md) and [product vision](docs/vision/steward-forge-vision.md).
