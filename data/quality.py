"""Execute declarative quality expectations against generated records."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .generators.common import PRIMARY_KEYS

DEFAULT_EXPECTATIONS_PATH = Path(__file__).with_name("quality_expectations.yml")


@dataclass(frozen=True)
class QualityFinding:
    dataset: str
    expectation_id: str
    record_id: str
    column: str
    observed: object


def load_expectations(path: Path = DEFAULT_EXPECTATIONS_PATH) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        document = yaml.safe_load(handle)
    if document.get("version") != 1 or not isinstance(document.get("datasets"), dict):
        raise ValueError("quality expectations must declare version 1 and datasets")
    return document


def _passes(row: dict[str, object], check: dict[str, Any]) -> bool:
    kind = check["kind"]
    column = check["column"]
    value = row[column]
    if kind == "equals":
        return value == check["value"]
    if kind == "in_set":
        return value in check["values"]
    if kind == "matches":
        return isinstance(value, str) and re.fullmatch(check["pattern"], value) is not None
    if kind == "greater_than_or_equal":
        return isinstance(value, (int, float)) and value >= check["value"]
    if kind == "less_than_or_equal_column":
        other = row[check["other_column"]]
        return (
            isinstance(value, (int, float))
            and isinstance(other, (int, float))
            and value <= other
        )
    raise ValueError(f"unsupported quality-check kind: {kind}")


def evaluate_table(
    dataset: str,
    records: list[dict[str, object]],
    expectations: dict[str, Any] | None = None,
) -> list[QualityFinding]:
    """Return every failing check in stable check/row order."""

    document = expectations or load_expectations()
    dataset_config = document["datasets"][dataset]
    primary_key = PRIMARY_KEYS[dataset]
    findings: list[QualityFinding] = []
    for check in dataset_config["checks"]:
        for row in records:
            if not _passes(row, check):
                findings.append(
                    QualityFinding(
                        dataset=dataset,
                        expectation_id=check["id"],
                        record_id=str(row[primary_key]),
                        column=check["column"],
                        observed=row[check["column"]],
                    )
                )
    return findings


def evaluate_all(
    tables: dict[str, list[dict[str, object]]],
    expectations: dict[str, Any] | None = None,
) -> list[QualityFinding]:
    document = expectations or load_expectations()
    findings: list[QualityFinding] = []
    for dataset in document["datasets"]:
        findings.extend(evaluate_table(dataset, tables[dataset], document))
    return findings


def documented_failure_keys(
    expectations: dict[str, Any] | None = None,
) -> set[tuple[str, str, str]]:
    document = expectations or load_expectations()
    return {
        (failure["dataset"], failure["expectation_id"], failure["record_id"])
        for failure in document["expected_failures"]
    }
