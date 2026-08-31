"""SHA-bound release coordination for Software Engineer candidates."""

from __future__ import annotations

import hashlib
import json

from broker.service import create_software_engineer_broker
from identity import AccessDenied, ActorContext, AuthorizationPolicy
from workers.swe.deployment import InMemoryDeploymentAdapter
from workers.swe.models import (
    ArtifactCommit,
    PreparedSoftwareRelease,
    SoftwareEngineerTask,
    SoftwareReleaseApproval,
    SoftwareReleaseReceipt,
)
from workers.swe.repository import InMemoryArtifactRepository
from workers.swe.worker import SoftwareEngineerWorker

from .gate import CHECK_NAMES, SoftwareGateSuite


class ReleaseDenied(ValueError):
    """A deterministic release prerequisite failed."""


class SoftwareReleaseService:
    """Owns candidate commit, gate, identity, deployment, and receipt transitions."""

    def __init__(
        self,
        repository: InMemoryArtifactRepository,
        deployer: InMemoryDeploymentAdapter,
    ) -> None:
        self._repository = repository
        self._deployer = deployer
        self._worker = SoftwareEngineerWorker()
        self._gates = SoftwareGateSuite()
        self._policy = AuthorizationPolicy()
        self._prepared: dict[str, PreparedSoftwareRelease] = {}
        self._decisions: dict[
            str, tuple[str, SoftwareReleaseReceipt | None]
        ] = {}
        self._release_results: dict[str, tuple[str, SoftwareReleaseReceipt]] = {}

    def prepare(self, task: SoftwareEngineerTask) -> PreparedSoftwareRelease:
        candidate = self._worker.draft(task)
        broker = create_software_engineer_broker(
            generated_prefix=task.generated_prefix,
            artifact_branch=task.artifact_branch,
            commit_executor=self._repository.commit,
        )
        broker_receipt = broker.execute(
            self._worker.propose_candidate_commit(task, candidate)
        )
        commit = ArtifactCommit.model_validate(broker_receipt.result)
        committed_artifacts = self._repository.read(commit.commit_sha)
        gates = self._gates.evaluate(
            task, candidate, committed_artifacts=committed_artifacts
        )
        prepared = PreparedSoftwareRelease(
            task=task,
            candidate=candidate,
            commit=commit,
            broker_receipt=broker_receipt,
            gates=gates,
        )
        existing = self._prepared.get(commit.commit_sha)
        if existing is not None and existing != prepared:
            raise ReleaseDenied("prepared release SHA is bound to different content")
        self._prepared[commit.commit_sha] = prepared
        return prepared

    def release(
        self,
        prepared: PreparedSoftwareRelease,
        approval: SoftwareReleaseApproval,
        actor: ActorContext,
        *,
        idempotency_key: str,
    ) -> SoftwareReleaseReceipt:
        task = prepared.task
        try:
            self._policy.require_release(
                actor,
                {
                    "submitted_by": task.submitted_by,
                    "brief": {"release_approver": task.release_approver},
                },
            )
        except AccessDenied as error:
            raise ReleaseDenied(str(error)) from error
        stored = self._prepared.get(prepared.commit.commit_sha)
        if stored is None or stored != prepared:
            raise ReleaseDenied("prepared release envelope is not registered")
        decision_fingerprint = hashlib.sha256(
            json.dumps(
                {
                    "approval": approval.model_dump(mode="json"),
                    "actor": actor.subject,
                    "commit_sha": prepared.commit.commit_sha,
                    "idempotency_key": idempotency_key,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        existing = self._decisions.get(approval.decision_id)
        if existing is not None:
            previous_fingerprint, receipt = existing
            if previous_fingerprint != decision_fingerprint:
                raise ReleaseDenied("release decision ID is bound to different content")
            if receipt is not None:
                return receipt
        else:
            self._decisions[approval.decision_id] = (decision_fingerprint, None)
        previous_release = self._release_results.get(idempotency_key)
        if previous_release is not None:
            previous_fingerprint, receipt = previous_release
            if previous_fingerprint != decision_fingerprint:
                raise ReleaseDenied("release idempotency key is bound to different content")
            return receipt
        if approval.decision != "approved":
            raise ReleaseDenied("release approval was rejected")
        if approval.approved_sha != prepared.commit.commit_sha:
            raise ReleaseDenied("approval must bind the exact candidate SHA")
        recorded_commit = ArtifactCommit.model_validate(prepared.broker_receipt.result)
        if recorded_commit != prepared.commit or prepared.commit.paths != prepared.candidate.paths:
            raise ReleaseDenied("candidate is not bound to the broker commit receipt")
        committed_artifacts = self._repository.read(prepared.commit.commit_sha)
        artifact_hashes = {
            artifact.path: artifact.sha for artifact in committed_artifacts
        }
        if artifact_hashes != prepared.commit.artifact_hashes:
            raise ReleaseDenied("committed artifact bytes do not match the broker receipt")
        fresh_gates = self._gates.evaluate(
            task,
            prepared.candidate,
            committed_artifacts=committed_artifacts,
        )
        gate_results = fresh_gates.results
        if (
            fresh_gates != prepared.gates
            or not fresh_gates.passed
            or set(gate_results) != set(CHECK_NAMES)
            or set(gate_results.values()) != {"passed"}
        ):
            raise ReleaseDenied("candidate did not pass every isolated gate")
        if not self._repository.is_descendant(
            prepared.commit.commit_sha, task.trusted_base_sha
        ):
            raise ReleaseDenied("candidate is not descended from the trusted base")
        deployment = self._deployer.deploy(
            commit_sha=prepared.commit.commit_sha,
            include_genie=prepared.candidate.genie_included,
            idempotency_key=idempotency_key,
        )
        payload = {
            "task_id": task.task_id,
            "commit_sha": prepared.commit.commit_sha,
            "approval_id": approval.decision_id,
            "deployment_idempotency_key": idempotency_key,
            "broker_receipt_id": prepared.broker_receipt.receipt_id,
            "gate_results": gate_results,
            "workspace_ids": deployment.workspace_ids,
            "rollback_state": deployment.rollback_state,
        }
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        receipt = SoftwareReleaseReceipt(
            receipt_id=hashlib.sha256(f"release:{canonical}".encode()).hexdigest()[:24],
            **payload,
        )
        self._decisions[approval.decision_id] = (decision_fingerprint, receipt)
        self._release_results[idempotency_key] = (decision_fingerprint, receipt)
        return receipt
