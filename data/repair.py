"""One bounded, deterministic repair for the documented synthetic defects."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass

from .generators.common import PRIMARY_KEYS, QUALITY_DEFECT_ROWS
from .quality import QualityFinding, documented_failure_keys, evaluate_all


class RepairDenied(ValueError):
    """The input is not the exact, documented repair scenario."""


@dataclass(frozen=True, slots=True)
class RepairReport:
    attempt: int
    repaired_count: int


def repair_documented_defects(
    tables: dict[str, list[dict[str, object]]],
    findings: list[QualityFinding],
    *,
    attempt: int,
) -> tuple[dict[str, list[dict[str, object]]], RepairReport]:
    """Repair only the three planted fixtures and never interpret row text."""

    if attempt != 1:
        raise RepairDenied("repairs are limited to one deterministic attempt")
    actual = {
        (finding.dataset, finding.expectation_id, finding.record_id) for finding in findings
    }
    if actual != documented_failure_keys():
        raise RepairDenied("only the documented planted defects can be repaired")

    repaired = deepcopy(tables)
    by_id = {
        dataset: {str(row[PRIMARY_KEYS[dataset]]): row for row in rows}
        for dataset, rows in repaired.items()
    }
    by_id["backlog"][QUALITY_DEFECT_ROWS["backlog"]]["story_points"] = 13
    pipeline_row = by_id["pipeline_runs"][QUALITY_DEFECT_ROWS["pipeline_runs"]]
    pipeline_row["records_written"] = pipeline_row["records_read"]
    cost_row = by_id["platform_costs"][QUALITY_DEFECT_ROWS["platform_costs"]]
    cost_row["cost_usd"] = round(
        float(cost_row["usage_quantity"]) * float(cost_row["unit_rate_usd"]), 2
    )

    remaining = evaluate_all(repaired)
    if remaining:
        raise RepairDenied("deterministic repair did not satisfy the quality contract")
    return repaired, RepairReport(attempt=attempt, repaired_count=len(actual))
