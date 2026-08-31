# Model governance contract

## Implemented and locally verified

`load_model_governance_config` is the only configuration path that constructs worker model policies. It reads endpoint, model, logical service identity, token limits, concurrency, retry limits, configured prices, and required guardrails from Databricks App environment values. Missing settings fail closed. Endpoint names containing a passthrough form are rejected. The registry requires exactly one policy for each Product Manager, Scrum Master, Data Engineer, and Software Engineer identity, and those logical identities must be distinct.

`GovernedModelGateway` is the provider-call boundary. Worker requests contain no route or identity fields. The gateway supplies those trusted values, reserves worst-case authorized cost before I/O, bounds concurrency and request counts, records throttles without incrementing domain-repair attempts, redacts secrets and classified prompt/output content before persistence, and reconciles token usage, configured prices, and required guardrail results. Unknown provider outcomes and missing telemetry keep authorization committed and report no metered actual cost.

Provider traffic and trace persistence use injected protocols. The repository includes an in-memory scoped trace adapter for deterministic tests, but it does not include a live provider adapter, credentials, or captured vendor response bodies. Enabling model calls without injecting the governed gateway fails App startup.

Both delivery paths register the brief owner, named approver and viewers, run ID, and authorized ceiling at the governance boundary. Deterministic workers are the explicit fallback. Until a model-backed worker is connected, the API and workbench show:

- authorized ceiling from the submitted brief;
- zero committed reservation and metered actual;
- full remaining authorization;
- zero throttles and reconciliation failures;
- `usage_status=not_used`.

Authorized USD ceilings are exact contracts, not estimates. They must be positive, finite, and resolve exactly to whole minor units. For example, `$4` becomes 400 minor units, while `$1.005` is rejected instead of silently rounded.

Internally computed maximum-authorized costs follow a different conservative rule: any fractional minor unit rounds upward. A receipt therefore never understates the authorization ceiling because of binary floating-point or banker's rounding.

Authorized budget and trace endpoints follow the normal brief-row policy. Brief owners, named viewers, and auditors can read them. Trace responses include IDs, worker, run, reconciliation status, and actual cost only. They exclude prompts and outputs, including redacted forms.

## Databricks deployment mapping

The bundle declares a dedicated MLflow experiment. The App resource receives `CAN_EDIT` through an experiment binding, while the configured auditor group receives `CAN_READ`. All four endpoint/model settings are overridable bundle variables, and model calls default to disabled. The service-identity settings are distinct logical identities recorded by the gateway; the current single App still runs under one Databricks App principal. Separate workspace principals per worker are not implemented or claimed.

The installed Databricks CLI schema recognizes the experiment resource and App experiment binding used here. On 31 August 2026, authenticated validation passed on the dedicated target with both default and alternate model-route variables. The experiment permission set includes auditor read access and the generated App service principal's edit access, preventing the explicit permission resource from overwriting the App binding. The updated bundle has not been deployed. A deployer must still install a real scoped trace adapter and provider transport before enabling model calls.

Database migrations remain owner-run. The App runtime role does not apply grants or schema migrations; it validates that the owner-prepared protected schema is present and fails closed otherwise.

## Required live proof

Before describing model governance as deployed, run one brief through each configured endpoint and prove routing identity, throttle evidence, budget denial, payload redaction, MLflow ACL denial for an unrelated viewer, exact cost reconciliation, and restart behavior against the target workspace. Preserve public-safe evidence without credentials, workspace hostnames, prompts, outputs, or vendor payloads.
