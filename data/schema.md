# Synthetic Steward Forge engineering data schema

All names and records in these datasets are fictional. The six teams are Atlas Insights,
Bluejay Platform, Comet Commerce, Drift Mobile, Ember Operations, and Fjord Developer
Experience. The generators use a fixed scenario clock and a caller-supplied integer seed;
they do not read wall-clock time or external services.

Each run lands in tables named `steward_forge_<brief_id>_<run_id>__<dataset>` inside the
DAB-owned sandbox schema. The generator never creates a schema. It lowercases brief and run
identifiers and replaces non-alphanumeric runs with underscores. Every table row has
`synthetic = true`, and the generated Unity Catalog DDL applies
`data_classification = SYNTHETIC` as both a table property and a governed tag. A generated
bundle contains canonical UTF-8 JSONL plus the matching `uc_ddl.sql`.

Nullability below is the actual Unity Catalog DDL contract. Dates use ISO `YYYY-MM-DD` and
timestamps use ISO-8601 UTC strings in JSONL before loading into the typed Delta tables.

### backlog

| Column | UC type | Nullable | Meaning |
| --- | --- | --- | --- |
| namespace | STRING | no | Per-run dataset namespace. |
| synthetic | BOOLEAN | no | Always true for generated rows. |
| item_id | STRING | no | Stable backlog-item identifier. |
| team_id | STRING | no | Fictional product-team identifier. |
| team_name | STRING | no | Fictional product-team display name. |
| title | STRING | no | Synthetic work-item title. |
| description | STRING | no | Synthetic work-item description. |
| item_type | STRING | no | feature, bug, or tech_debt. |
| status | STRING | no | planned, in_progress, blocked, or done. |
| priority | STRING | no | low, medium, high, or critical. |
| story_points | INT | no | Estimated Fibonacci story points. |
| created_at | TIMESTAMP | no | UTC creation timestamp. |
| target_date | DATE | no | Planned completion date. |
| sprint_id | STRING | no | Synthetic sprint identifier. |

### pipeline_runs

| Column | UC type | Nullable | Meaning |
| --- | --- | --- | --- |
| namespace | STRING | no | Per-run dataset namespace. |
| synthetic | BOOLEAN | no | Always true for generated rows. |
| pipeline_run_id | STRING | no | Stable pipeline-run identifier. |
| team_id | STRING | no | Fictional product-team identifier. |
| team_name | STRING | no | Fictional product-team display name. |
| pipeline_name | STRING | no | Synthetic delivery pipeline name. |
| started_at | TIMESTAMP | no | UTC run start timestamp. |
| finished_at | TIMESTAMP | no | UTC run finish timestamp. |
| status | STRING | no | succeeded, failed, or cancelled. |
| trigger_type | STRING | no | commit, schedule, or manual. |
| duration_seconds | INT | no | Run duration in seconds. |
| records_read | BIGINT | no | Input records processed. |
| records_written | BIGINT | no | Output records emitted. |
| error_message | STRING | yes | Synthetic failure detail. |

### platform_costs

| Column | UC type | Nullable | Meaning |
| --- | --- | --- | --- |
| namespace | STRING | no | Per-run dataset namespace. |
| synthetic | BOOLEAN | no | Always true for generated rows. |
| cost_record_id | STRING | no | Stable platform-cost identifier. |
| team_id | STRING | no | Fictional product-team identifier. |
| team_name | STRING | no | Fictional product-team display name. |
| usage_date | DATE | no | Synthetic usage date. |
| service | STRING | no | jobs, sql, serving, or storage. |
| sku | STRING | no | Synthetic billing SKU. |
| usage_quantity | DOUBLE | no | Synthetic metered quantity. |
| unit | STRING | no | DBU or GB_MONTH. |
| unit_rate_usd | DOUBLE | no | Synthetic unit rate in USD. |
| cost_usd | DOUBLE | no | Synthetic extended cost in USD. |
| charge_description | STRING | no | Synthetic charge description. |

## Quality contract

`quality_expectations.yml` is executable. It checks the namespace, synthetic marker,
fictional team membership, domain values, and the three planted defects. The expected
findings are part of the contract, so any additional failure makes the dataset invalid.
Schema validation and quality validation remain separate: planted defects are valid typed
rows that violate business expectations.

Generate a bundle with:

```bash
python -m data.generators --seed 2026 --brief-id brief-01 --run-id run-01 \
  --catalog my_catalog --schema my_sandbox_schema --output generated-data
```
