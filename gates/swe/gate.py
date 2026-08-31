"""Isolated deterministic checks for Software Engineer candidates."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable

from broker.contracts import DraftArtifact
from broker.security import contains_secret
from workers.swe.models import (
    GateCheck,
    SoftwareCandidate,
    SoftwareEngineerTask,
    SoftwareGateReport,
)

CHECK_NAMES = ("unit", "integration", "quality", "policy", "secret", "harmful_diff")
EXPECTED_TESTS = {
    "dashboard_has_three_signals",
    "dashboard_uses_governed_sources",
    "dashboard_has_no_egress",
}
HARMFUL_CONTENT = ("fetch(", "xmlhttprequest", "eval(", "document.cookie")
DENIED_PATH_PARTS = {".github", "infrastructure", "platform", "secret", "secrets", "resources"}


class SoftwareGateSuite:
    """Runs every check independently and reports the exact denominator."""

    def evaluate(
        self,
        task: SoftwareEngineerTask,
        candidate: SoftwareCandidate,
        *,
        committed_artifacts: tuple[DraftArtifact, ...],
    ) -> SoftwareGateReport:
        checks: tuple[tuple[str, Callable[[], bool]], ...] = (
            ("unit", lambda: self._unit(candidate)),
            ("integration", lambda: self._integration(candidate, committed_artifacts)),
            ("quality", lambda: self._quality(candidate)),
            ("policy", lambda: self._policy(task, candidate)),
            ("secret", lambda: self._secret(candidate)),
            ("harmful_diff", lambda: self._harmful_diff(candidate)),
        )
        results = tuple(self._run(name, check) for name, check in checks)
        return SoftwareGateReport(
            checks=results,
            passed=len(results) == len(CHECK_NAMES)
            and all(check.status == "passed" for check in results),
        )

    @staticmethod
    def _run(name: str, check: Callable[[], bool]) -> GateCheck:
        try:
            passed = check()
            detail = "deterministic check passed" if passed else "deterministic check failed"
        except Exception as error:  # Each gate must report even if one checker breaks.
            passed = False
            detail = f"checker error: {type(error).__name__}"
        return GateCheck(name=name, status="passed" if passed else "failed", detail=detail)

    @staticmethod
    def _unit(candidate: SoftwareCandidate) -> bool:
        expected_sha = hashlib.sha256(
            json.dumps(
                [(artifact.path, artifact.sha) for artifact in candidate.artifacts],
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        tests_artifact = next(
            artifact for artifact in candidate.artifacts if artifact.path.endswith(".tests.json")
        )
        tests = json.loads(tests_artifact.content)
        return (
            candidate.candidate_sha == expected_sha
            and tests.get("schema_version") == 1
            and set(tests.get("checks", [])) == EXPECTED_TESTS
        )

    @staticmethod
    def _integration(
        candidate: SoftwareCandidate, committed_artifacts: tuple[DraftArtifact, ...]
    ) -> bool:
        html = next(
            artifact.content
            for artifact in candidate.artifacts
            if artifact.path.endswith("dashboard.html")
        )
        return committed_artifacts == candidate.artifacts and 'src="dashboard.js"' in html

    @staticmethod
    def _quality(candidate: SoftwareCandidate) -> bool:
        html = next(
            artifact.content
            for artifact in candidate.artifacts
            if artifact.path.endswith("dashboard.html")
        ).casefold()
        signals = ("backlog health", "pipeline reliability", "platform cost")
        return (
            html.startswith("<!doctype html>")
            and "<main>" in html
            and 'aria-live="polite"' in html
            and all(signal in html for signal in signals)
        )

    @staticmethod
    def _policy(task: SoftwareEngineerTask, candidate: SoftwareCandidate) -> bool:
        prefix = f"{task.generated_prefix}/"
        genie_paths = [path for path in candidate.paths if path.endswith("genie-space.json")]
        genie_valid = bool(genie_paths) == candidate.genie_included and (
            not candidate.genie_included
            or task.request_genie and task.genie_creation_verified
        )
        return all(path.startswith(prefix) for path in candidate.paths) and genie_valid

    @staticmethod
    def _secret(candidate: SoftwareCandidate) -> bool:
        return not any(contains_secret(artifact.content) for artifact in candidate.artifacts)

    @staticmethod
    def _harmful_diff(candidate: SoftwareCandidate) -> bool:
        for artifact in candidate.artifacts:
            path_parts = {part.casefold() for part in artifact.path.split("/")}
            content = artifact.content.casefold()
            if path_parts & DENIED_PATH_PARTS or any(
                marker in content for marker in HARMFUL_CONTENT
            ):
                return False
        return True
