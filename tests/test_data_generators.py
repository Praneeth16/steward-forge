from __future__ import annotations

from pathlib import Path

import pytest

from data.generators import build_namespace, canonical_jsonl, generate_all, write_bundle
from data.generators.common import (
    CANARY_PLACEMENTS,
    PRIMARY_KEYS,
    QUALITY_DEFECT_ROWS,
    TABLE_SCHEMAS,
    TEAMS,
    validate_row_schema,
)
from data.quality import documented_failure_keys, evaluate_all


def test_same_seed_produces_byte_identical_tables() -> None:
    first = generate_all(2026, "brief-01", "run-01")
    second = generate_all(2026, "brief-01", "run-01")

    assert {
        table_name: canonical_jsonl(rows) for table_name, rows in first.items()
    } == {table_name: canonical_jsonl(rows) for table_name, rows in second.items()}


def test_each_dataset_covers_exactly_six_fictional_teams() -> None:
    tables = generate_all(7, "delivery-health", "01")
    expected_teams = {team.team_id for team in TEAMS}

    assert len(expected_teams) == 6
    for rows in tables.values():
        assert {row["team_id"] for row in rows} == expected_teams
        assert all(row["synthetic"] is True for row in rows)


def test_every_generated_row_matches_its_declared_schema() -> None:
    tables = generate_all(11, "pipeline-slo", "03")

    for table_name, rows in tables.items():
        for row in rows:
            assert validate_row_schema(table_name, row) == []
            assert list(row) == [column.name for column in TABLE_SCHEMAS[table_name]]


def test_quality_engine_reports_only_documented_planted_failures() -> None:
    findings = evaluate_all(generate_all(19, "cost-anomaly", "02"))
    actual = {
        (finding.dataset, finding.expectation_id, finding.record_id) for finding in findings
    }

    assert actual == documented_failure_keys()
    assert {finding.record_id for finding in findings} == set(QUALITY_DEFECT_ROWS.values())


def test_canaries_exist_only_at_documented_locations_and_not_on_defect_rows() -> None:
    tables = generate_all(23, "adversarial", "05")
    observed: set[tuple[str, str, str, str]] = set()

    for table_name, rows in tables.items():
        primary_key = PRIMARY_KEYS[table_name]
        for row in rows:
            for field, value in row.items():
                if isinstance(value, str) and value.startswith("SF_CANARY::"):
                    marker = value.partition(" ")[0]
                    observed.add((table_name, str(row[primary_key]), field, marker))

    expected = {
        (table_name, placement["record_id"], placement["field"], placement["marker"])
        for table_name, placement in CANARY_PLACEMENTS.items()
    }
    assert observed == expected
    assert {placement["record_id"] for placement in CANARY_PLACEMENTS.values()}.isdisjoint(
        QUALITY_DEFECT_ROWS.values()
    )
    assert not {
        finding.record_id for finding in evaluate_all(tables)
    } & {placement["record_id"] for placement in CANARY_PLACEMENTS.values()}


def test_namespace_is_normalized_and_rejects_empty_components() -> None:
    assert build_namespace("Brief 01", "RUN-02") == "steward_forge_brief_01_run_02"
    with pytest.raises(ValueError, match="brief_id"):
        build_namespace("---", "run-02")


def test_bundle_writer_emits_canonical_tables_and_ddl(tmp_path: Path) -> None:
    tables = generate_all(29, "brief-01", "run-01")
    written = write_bundle(tmp_path, tables, "sandbox_catalog", "sandbox")

    for table_name, rows in tables.items():
        assert written[table_name].read_bytes() == canonical_jsonl(rows)
    ddl = written["uc_ddl"].read_text(encoding="utf-8")
    assert ddl.count("data_classification' = 'SYNTHETIC") == 6
    assert "`sandbox_catalog`.`sandbox`.`steward_forge_brief_01_run_01__backlog`" in ddl
    assert "CREATE SCHEMA" not in ddl
