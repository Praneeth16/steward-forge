# Environment verification

Observed on 31 August 2026 against the first serverless deployment target. Sensitive account, host, user, role, and unrelated resource identifiers are intentionally omitted.

| Capability | Status | Observed result | Fallback or next action |
| --- | --- | --- | --- |
| CLI authentication | PASS | The selected local profile authenticated to the intended workspace. | A deployer supplies their own profile or environment authentication. |
| Bundle schema | PASS | The installed CLI recognizes Autoscaling Postgres projects, branches, endpoints, roles, databases, catalogs, schemas, Apps, jobs, dashboards, and experiments. | Minimum supported CLI is documented. |
| Portable validation | PASS | Default Lakebase values and a second set of alternate values both validated without source edits. | Installation-specific values are bundle variables. |
| Dedicated Lakebase project | PASS | Bundle deployment created `steward-forge-lakebase` on PostgreSQL 17 without adopting another project. | Override `lakebase_project_id` when the default is unavailable. |
| Production branch and endpoint | PASS | The production branch reported READY and the primary read-write endpoint reported ACTIVE at 0.5-2 CU with suspension enabled. | Endpoint limits are variables. |
| Logical database | PASS | `steward_forge` was created by the bundle and accepted an OAuth-authenticated PostgreSQL query. | The deploy helper derives the project-owner role ID. |
| Unity Catalog isolation | PASS WITH PREREQUISITE | The Catalog API could not create a catalog because this metastore uses UI-managed default storage. The bundle created dedicated development-prefixed `sandbox` and `evidence` schemas in a supplied writable catalog. | Every deployer supplies `catalog_name`; Steward Forge never adopts another application's schemas. |
| Databricks Apps | PASS | DAB created the Steward Forge App, attached the dedicated Lakebase database, installed dependencies, and reached RUNNING with an HTTP 200 health response. | The workspace-unique App name is overridable through `app_name`. |
| App-to-Lakebase tracer | PASS | A remote brief completed scope approval, bounded Scrum Master work, deterministic tests, release approval, and receipt persistence. Replaying the submission returned HTTP 200 and a later read returned the same receipt with five events. | The current database connection uses the App service principal; user-scoped OBO is a separate unverified control. |
| Forwarded App user identity | PASS WITH ONE-USER LIMIT | The deployed App validated the forwarded user token through the workspace current-user API. `/api/me` returned HTTP 200 with group-derived roles. A forged body identity returned 422, an unnamed approver returned 403, self-release returned 403, and the submitter could read their own row. | Negative row visibility is covered locally but needs a second target-workspace user for live proof. Cross-App forwarding remains unverified. |
| Brokered worker mutations | PASS WITH HEALTH-PROBE LIMIT | A live scope approval passed the Scrum Master task and candidate through typed, versioned broker contracts and returned two mutation receipts. Harmful and out-of-scope candidates fail in integration tests before workflow state advances. | The current tracer uses a deterministic healthy snapshot. Live Lakebase, pipeline, and Unity Catalog freshness probes remain unverified and must replace it before the pre-act control is called operational. |
| Claude Sonnet and Opus model services | PASS | Both configured model services returned `OK` from a live request. Each request used 14 prompt and 4 completion tokens. The services rejected the unsupported `temperature` parameter, so the runtime must omit it. | Model names remain variables and tests cover request compatibility. |
| Genie API | PARTIAL | The Genie Spaces API returned accessible spaces, proving read access. Programmatic create and delete were not exercised. | Dashboard output is primary until the release slice verifies Genie creation. |
| Omnigent managed surface | NOT VERIFIED | No Omnigent CLI surface was exposed by the installed CLI. | Verify through the managed UI; retain the IDE-like Steward Forge workbench fallback. |
| Cross-App OBO forwarding | NOT VERIFIED | No two-App chain exists yet. Documentation alone is not accepted as proof. | Exercise token forwarding and forged-identity rejection in the first App integration slice. |
| No-egress generated-code runner | NOT VERIFIED | No dedicated target compute policy or NCC path has been exercised yet. | Build the isolated test gate and record a blocked outbound request before enabling generated-code execution. |
| Compliance security profile interaction | NOT VERIFIED | The workspace-level profile state was not proven through an authoritative API. | Verify with workspace administration before relying on Omnigent or Sandbox. |

## Model-governance bundle update

The Issue 10 bundle update adds overridable worker routes and a dedicated MLflow experiment with App and auditor access. The installed CLI schema recognizes both `resources.experiments` and the App `experiment` resource binding. Authenticated validation and deployment of this update are not verified: on 31 August 2026, the local profile refresh token was expired. The PASS results above describe the earlier foundation deployment and do not prove the new model-governance resources.

## Commands exercised

The following public-safe forms were executed. Replace placeholders with local values.

```bash
databricks bundle validate --target dev --profile <profile> \
  --var='catalog_name=<catalog>' \
  --var='lakebase_owner_role_id=<derived-role-id>'

databricks bundle plan --target dev --profile <profile> \
  --var='catalog_name=<catalog>' \
  --var='lakebase_owner_role_id=<derived-role-id>'

uv run python scripts/deploy.py --profile <profile> --catalog <catalog>
```

The final deployment completed successfully after the bundle created or adopted only its own declared resources. A subsequent plan must report no unexpected deletion or adoption before further deployment.
