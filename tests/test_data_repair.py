from copy import deepcopy

import pytest

from data.generators import generate_all
from data.generators.common import CANARY_PLACEMENTS, PRIMARY_KEYS
from data.quality import evaluate_all
from data.repair import RepairDenied, repair_documented_defects


def _canaries(tables: dict[str, list[dict[str, object]]]) -> dict[tuple[str, str], object]:
    observed: dict[tuple[str, str], object] = {}
    for dataset, placement in CANARY_PLACEMENTS.items():
        primary_key = PRIMARY_KEYS[dataset]
        row = next(
            row for row in tables[dataset] if row[primary_key] == placement["record_id"]
        )
        observed[(dataset, placement["field"])] = row[placement["field"]]
    return observed


def test_documented_defects_are_repaired_once_without_touching_canaries() -> None:
    planted = generate_all(31, "brief-01", "run-01")
    original = deepcopy(planted)

    repaired, report = repair_documented_defects(planted, evaluate_all(planted), attempt=1)

    assert report.attempt == 1
    assert report.repaired_count == 3
    assert evaluate_all(repaired) == []
    assert planted == original
    assert _canaries(repaired) == _canaries(planted)


def test_repair_rejects_undocumented_or_second_attempts() -> None:
    tables = generate_all(32, "brief-01", "run-01")
    tables["backlog"][0]["story_points"] = 34

    with pytest.raises(RepairDenied, match="documented planted defects"):
        repair_documented_defects(tables, evaluate_all(tables), attempt=1)
    with pytest.raises(RepairDenied, match="one deterministic attempt"):
        repair_documented_defects(tables, evaluate_all(tables), attempt=2)
