from __future__ import annotations

import re
from pathlib import Path

from data.generators.common import CANARY_PLACEMENTS, TABLE_SCHEMAS
from data.uc_ddl import render_table_ddl

REPOSITORY_ROOT = Path(__file__).parents[1]


def _documented_columns(markdown: str, table_name: str) -> list[tuple[str, str, bool]]:
    section = markdown.split(f"### {table_name}\n", maxsplit=1)[1].split("\n### ", maxsplit=1)[0]
    rows = re.findall(r"^\| ([a-z_]+) \| ([A-Z]+) \| (yes|no) \|", section, re.MULTILINE)
    return [(name, sql_type, nullable == "yes") for name, sql_type, nullable in rows]


def _ddl_columns(ddl: str) -> list[tuple[str, str, bool]]:
    body = ddl.split("(\n", maxsplit=1)[1].split("\n) USING DELTA", maxsplit=1)[0]
    rows = re.findall(r"^  `([a-z_]+)` ([A-Z]+)( NOT NULL)?", body, re.MULTILINE)
    return [(name, sql_type, not bool(not_null)) for name, sql_type, not_null in rows]


def test_schema_document_matches_executable_uc_ddl() -> None:
    markdown = (REPOSITORY_ROOT / "data/schema.md").read_text(encoding="utf-8")

    for table_name, columns in TABLE_SCHEMAS.items():
        expected = [(column.name, column.sql_type, column.nullable) for column in columns]
        assert _documented_columns(markdown, table_name) == expected
        ddl = render_table_ddl(
            "catalog", "sandbox", "gcc_delivery_brief_01_run_01", table_name
        )
        assert _ddl_columns(ddl) == expected


def test_canary_document_lists_every_exact_location() -> None:
    markdown = (REPOSITORY_ROOT / "data/canaries.md").read_text(encoding="utf-8")

    for table_name, placement in CANARY_PLACEMENTS.items():
        assert table_name in markdown
        assert placement["record_id"] in markdown
        assert placement["field"] in markdown
        assert placement["marker"] in markdown
