"""Creates a stable release receipt for an approved candidate."""

import hashlib

from orchestrator.models import CandidateArtifact, ReleaseReceipt


class ReleaseAdapter:
    """Releases only candidates whose deterministic checks passed."""

    def release(
        self,
        brief_id: str,
        candidate: CandidateArtifact,
        test_results: dict[str, str],
    ) -> ReleaseReceipt:
        expected_gates = {"contract", "unit"}
        if set(test_results) != expected_gates or set(test_results.values()) != {"passed"}:
            raise ValueError("candidate did not pass every release gate")

        receipt_id = hashlib.sha256(
            f"{brief_id}:{candidate.sha}:release".encode()
        ).hexdigest()[:24]
        return ReleaseReceipt(
            id=receipt_id,
            brief_id=brief_id,
            commit_sha=candidate.sha,
            test_results=test_results,
            artifact_path=candidate.path,
        )
