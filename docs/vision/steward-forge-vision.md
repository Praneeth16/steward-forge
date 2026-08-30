# Steward Forge product vision

## The product

Steward Forge is a governed engineering workbench for digital workers. A user supplies an outcome and acceptance tests. Specialized workers plan and build the data product, while deterministic services control permissions, state, approvals, testing, release, and evidence.

The primary product promise is not autonomous code generation. It is controlled delegation with proof: users can see what each worker was asked to do, what it attempted, what controls ran, who approved the result, what was deployed, and how to stop or reproduce it.

## Experience

The workbench behaves like an engineering workspace rather than a chat window. It presents the brief, versioned contracts, task graph, generated files, test results, approvals, attention items, costs, and evidence receipt in one navigable surface.

The demonstration should show one success, one bounded repair, one denied action, and one recovery. A happy path alone cannot establish control.

## System boundary

Digital workers provide judgment within narrow contracts. They do not grant access, advance authoritative workflow state, approve their own work, bypass tests, deploy directly, or author their own canonical evidence.

Lakebase stores live operational state and coordination. Unity Catalog stores governed data and durable release evidence. MLflow records worker traces and evaluation. Unity AI Gateway governs model access. Databricks Asset Bundles make the installation reproducible.

## Portability

An installation supplies workspace authentication and bundle variables. The bundle creates dedicated resources. Worker contracts use portable schemas, generated data is synthetic, and released tables expose a documented interoperability path. Environment-specific preview features are optional and have explicit fallbacks.

## Success

Steward Forge succeeds when a reviewer can reproduce a run, detect evidence tampering, prove that unsafe mutations were denied, recover an interrupted task without duplicate effects, measure outcome quality and cost, and deploy the same code into another eligible workspace without editing source.
