"""Deterministic Software Engineer drafts with no direct repository authority."""

from __future__ import annotations

import hashlib
import html
import json

from broker.contracts import ArtifactCommitArgs, DraftArtifact, MutationRequest
from workers.swe.models import SoftwareCandidate, SoftwareEngineerTask


class SoftwareEngineerWorker:
    worker_id = "software-engineer"
    contract_id = "software-engineer-artifact-writer"
    contract_version = 1

    def draft(self, task: SoftwareEngineerTask) -> SoftwareCandidate:
        root = task.generated_prefix
        title = html.escape(task.dashboard_title)
        dashboard = (
            "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">"
            f"<title>{title}</title></head><body><main><h1>{title}</h1>"
            "<section id=\"backlog\"><h2>Backlog health</h2></section>"
            "<section id=\"pipelines\"><h2>Pipeline reliability</h2></section>"
            "<section id=\"costs\"><h2>Platform cost</h2></section>"
            "<div id=\"dashboard-status\" aria-live=\"polite\"></div>"
            "<script src=\"dashboard.js\"></script></main></body></html>"
        )
        javascript = (
            "'use strict';\n"
            f"const SOURCE_TABLES = Object.freeze({json.dumps(task.source_tables)});\n"
            "const status = document.getElementById('dashboard-status');\n"
            "status.textContent = `${SOURCE_TABLES.length} governed sources configured`;\n"
        )
        tests = json.dumps(
            {
                "schema_version": 1,
                "checks": [
                    "dashboard_has_three_signals",
                    "dashboard_uses_governed_sources",
                    "dashboard_has_no_egress",
                ],
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        artifacts = [
            _artifact(f"{root}/dashboard.html", dashboard),
            _artifact(f"{root}/dashboard.js", javascript),
            _artifact(f"{root}/dashboard.tests.json", tests),
        ]
        include_genie = task.request_genie and task.genie_creation_verified
        if include_genie:
            genie = json.dumps(
                {
                    "schema_version": 1,
                    "title": task.dashboard_title,
                    "source_tables": task.source_tables,
                    "status": "creation-verified",
                    "verification_id": task.genie_verification_id,
                },
                sort_keys=True,
                separators=(",", ":"),
            )
            artifacts.append(_artifact(f"{root}/genie-space.json", genie))
        artifact_tuple = tuple(artifacts)
        candidate_sha = _candidate_sha(artifact_tuple)
        return SoftwareCandidate(
            task_id=task.task_id,
            candidate_sha=candidate_sha,
            artifacts=artifact_tuple,
            genie_included=include_genie,
        )

    def propose_candidate_commit(
        self, task: SoftwareEngineerTask, candidate: SoftwareCandidate
    ) -> MutationRequest:
        arguments = ArtifactCommitArgs(
            branch=task.artifact_branch,
            parent_sha=task.trusted_base_sha,
            message=f"feat(generated): candidate for {task.task_id}",
            artifacts=candidate.artifacts,
        )
        return MutationRequest(
            contract_id=self.contract_id,
            contract_version=self.contract_version,
            worker_id=self.worker_id,
            workflow_id=task.brief_id,
            tool_id="artifact.commit-candidate",
            arguments=arguments.model_dump(mode="json"),
            idempotency_key=f"{task.task_id}:candidate:{candidate.candidate_sha}:v1",
        )


def _artifact(path: str, content: str) -> DraftArtifact:
    return DraftArtifact(
        path=path,
        content=content,
        sha=hashlib.sha256(content.encode()).hexdigest(),
    )


def _candidate_sha(artifacts: tuple[DraftArtifact, ...]) -> str:
    payload = [(artifact.path, artifact.sha) for artifact in artifacts]
    canonical = json.dumps(payload, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()
