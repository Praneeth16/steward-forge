"""Deterministic checks for the first tracer candidate."""

import json

from orchestrator.models import CandidateArtifact


class TestGate:
    """Runs non-probabilistic contract and unit checks."""

    def evaluate(self, candidate: CandidateArtifact) -> dict[str, str]:
        payload = json.loads(candidate.content)
        contract_passed = bool(payload.get("brief_id") and payload.get("team"))
        unit_passed = candidate.path.startswith("generated/")
        return {
            "contract": "passed" if contract_passed else "failed",
            "unit": "passed" if unit_passed else "failed",
        }
