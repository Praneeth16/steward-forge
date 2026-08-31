"""Deterministic gate for generated data-pipeline candidates."""

from __future__ import annotations

import ast
import hashlib
import json

from data.generators import canonical_jsonl
from data.generators.common import TABLE_SCHEMAS
from workers.de.models import DataEngineerTask
from workers.de.worker import (
    DataEngineerCandidate,
    expected_artifact_hashes,
)


class DataGateDenied(ValueError):
    """The candidate failed a deterministic release prerequisite."""


class DataCandidateGate:
    expected_generated_results = {
        "datasets": "passed",
        "namespace": "passed",
        "synthetic": "passed",
    }

    def evaluate(
        self, task: DataEngineerTask, candidate: DataEngineerCandidate
    ) -> dict[str, str]:
        artifact_contract = self._artifact_contract(task, candidate)
        results = {
            "artifact_contract": artifact_contract,
            "generated_tests": artifact_contract and self._generated_tests(candidate),
            "lineage": self._lineage(task, candidate),
            "quality": candidate.final_finding_count == 0
            and candidate.repair_attempts <= task.max_repair_attempts,
            "sandbox": self._sandbox(task, candidate),
        }
        if not all(results.values()):
            failed = sorted(name for name, passed in results.items() if not passed)
            raise DataGateDenied(f"data candidate failed gates: {', '.join(failed)}")
        return {name: "passed" for name in results}

    @staticmethod
    def _artifact_contract(
        task: DataEngineerTask, candidate: DataEngineerCandidate
    ) -> bool:
        observed = {
            artifact.path.rsplit("/", 1)[-1]: artifact.sha
            for artifact in candidate.artifacts
        }
        try:
            manifest = json.loads(candidate.manifest.content)
        except json.JSONDecodeError:
            return False
        expected_table_hashes = {
            dataset: hashlib.sha256(canonical_jsonl(rows)).hexdigest()
            for dataset, rows in candidate.tables.items()
        }
        lineage_json = json.dumps(
            candidate.lineage.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
        )
        return (
            len(candidate.artifacts) == 2
            and candidate.manifest.path.endswith("/candidate-manifest.json")
            and observed == expected_artifact_hashes()
            and manifest.get("schema_id") == "steward-forge.data-engineer-manifest"
            and manifest.get("schema_version") == 1
            and manifest.get("task_id") == task.task_id
            and manifest.get("artifact_hashes") == observed
            and manifest.get("table_hashes") == expected_table_hashes
            and manifest.get("lineage_sha256")
            == hashlib.sha256(lineage_json.encode()).hexdigest()
            and manifest.get("repair_attempts") == candidate.repair_attempts
        )

    def _generated_tests(self, candidate: DataEngineerCandidate) -> bool:
        function_names: set[str] = set()
        try:
            for artifact in candidate.artifacts:
                tree = ast.parse(artifact.content, filename=artifact.path)
                function_names.update(
                    node.name for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)
                )
        except SyntaxError:
            return False
        namespace = candidate.lineage.namespace
        trusted_results = {
            "datasets": "passed"
            if tuple(candidate.tables) == ("backlog", "pipeline_runs", "platform_costs")
            else "failed",
            "namespace": "passed"
            if all(
                row.get("namespace") == namespace
                for rows in candidate.tables.values()
                for row in rows
            )
            else "failed",
            "synthetic": "passed"
            if all(
                row.get("synthetic") is True
                for rows in candidate.tables.values()
                for row in rows
            )
            else "failed",
        }
        return function_names == {"build_outputs", "run_generated_tests"} and (
            trusted_results == self.expected_generated_results
        )

    @staticmethod
    def _lineage(task: DataEngineerTask, candidate: DataEngineerCandidate) -> bool:
        namespace = candidate.lineage.namespace
        expected_sources = tuple(
            f"steward-forge-generator:v1:{dataset}" for dataset in TABLE_SCHEMAS
        )
        expected_targets = tuple(
            f"{task.sandbox_catalog}.{task.sandbox_schema}.{namespace}__{dataset}"
            for dataset in TABLE_SCHEMAS
        )
        return (
            candidate.lineage.sources == expected_sources
            and candidate.lineage.targets == expected_targets
        )

    @staticmethod
    def _sandbox(task: DataEngineerTask, candidate: DataEngineerCandidate) -> bool:
        prefix = f"{task.sandbox_catalog}.{task.sandbox_schema}."
        return all(target.startswith(prefix) for target in candidate.lineage.targets)
