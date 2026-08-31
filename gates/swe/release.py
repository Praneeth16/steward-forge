"""SHA-bound release coordination for Software Engineer candidates."""

from __future__ import annotations

import hashlib
import json
from threading import RLock

from broker.service import (
    LeaseFence,
    create_software_engineer_broker,
    mutation_receipt_id,
    mutation_request_hash,
)
from identity import AccessDenied, ActorContext, AuthorizationPolicy
from workers.swe.deployment import InMemoryDeploymentAdapter
from workers.swe.models import (
    ArtifactCommit,
    PreparedSoftwareRelease,
    SoftwareCandidate,
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
        *,
        gates: SoftwareGateSuite | None = None,
    ) -> None:
        self._repository = repository
        self._deployer = deployer
        self._worker = SoftwareEngineerWorker()
        self._gates = gates or SoftwareGateSuite()
        self._policy = AuthorizationPolicy()
        self._lock = RLock()
        self._prepared: dict[str, PreparedSoftwareRelease] = {}
        self._decisions: dict[
            str, tuple[str, SoftwareReleaseReceipt | None]
        ] = {}
        self._release_results: dict[str, tuple[str, SoftwareReleaseReceipt]] = {}

    def prepare(self, task: SoftwareEngineerTask) -> PreparedSoftwareRelease:
        candidate = self.draft(task)
        return self.prepare_candidate(task, candidate)

    def draft(self, task: SoftwareEngineerTask) -> SoftwareCandidate:
        """Build a read-only candidate without repository or workspace writes."""

        return self._worker.draft(task)

    def restore_prepared(
        self, prepared: PreparedSoftwareRelease
    ) -> PreparedSoftwareRelease:
        """Rehydrate a ledger-persisted envelope after validating trusted readback."""

        with self._lock:
            existing = self._prepared.get(prepared.commit.commit_sha)
            if existing is not None:
                if existing != prepared:
                    raise ReleaseDenied(
                        "prepared release SHA is bound to different content"
                    )
                return existing
            self._validate_prepared(prepared)
            self._store_prepared(prepared)
            return prepared

    def prepare_candidate(
        self,
        task: SoftwareEngineerTask,
        candidate: SoftwareCandidate,
        *,
        lease_owner: str | None = None,
        lease_epoch: int | None = None,
        lease_fence: LeaseFence | None = None,
    ) -> PreparedSoftwareRelease:
        """Commit and gate a caller-provided candidate through governed paths."""

        with self._lock:
            return self._prepare_candidate(
                task,
                candidate,
                lease_owner=lease_owner,
                lease_epoch=lease_epoch,
                lease_fence=lease_fence,
            )

    def _prepare_candidate(
        self,
        task: SoftwareEngineerTask,
        candidate: SoftwareCandidate,
        *,
        lease_owner: str | None,
        lease_epoch: int | None,
        lease_fence: LeaseFence | None,
    ) -> PreparedSoftwareRelease:
        if candidate.task_id != task.task_id:
            raise ReleaseDenied("software candidate is not bound to this task")
        broker = create_software_engineer_broker(
            generated_prefix=task.generated_prefix,
            artifact_branch=task.artifact_branch,
            commit_executor=self._repository.commit,
            lease_fence=lease_fence,
        )
        broker_receipt = broker.execute(
            self._worker.propose_candidate_commit(
                task,
                candidate,
                lease_owner=lease_owner,
                lease_epoch=lease_epoch,
            )
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
        self._store_prepared(prepared)
        return prepared

    def _store_prepared(self, prepared: PreparedSoftwareRelease) -> None:
        existing = self._prepared.get(prepared.commit.commit_sha)
        if existing is not None and existing != prepared:
            raise ReleaseDenied("prepared release SHA is bound to different content")
        self._prepared[prepared.commit.commit_sha] = prepared

    def _validate_prepared(self, prepared: PreparedSoftwareRelease) -> None:
        receipt = prepared.broker_receipt
        request = self._worker.propose_candidate_commit(
            prepared.task,
            prepared.candidate,
            lease_owner=receipt.lease_owner,
            lease_epoch=receipt.lease_epoch,
        )
        request_hash = mutation_request_hash(request)
        receipt_id = mutation_receipt_id(request_hash)
        recorded_commit = ArtifactCommit.model_validate(receipt.result)
        if (
            receipt.request_hash != request_hash
            or receipt.receipt_id != receipt_id
            or receipt.worker_id != self._worker.worker_id
            or receipt.workflow_id != prepared.task.brief_id
            or receipt.tool_id != "artifact.commit-candidate"
            or recorded_commit != prepared.commit
            or prepared.commit.paths != prepared.candidate.paths
        ):
            raise ReleaseDenied("persisted candidate is not bound to its broker receipt")
        try:
            committed_artifacts = self._repository.read(prepared.commit.commit_sha)
        except (KeyError, ValueError) as error:
            raise ReleaseDenied(str(error)) from error
        if committed_artifacts != prepared.candidate.artifacts:
            raise ReleaseDenied("persisted candidate does not match committed artifact bytes")
        fresh_gates = self._gates.evaluate(
            prepared.task,
            prepared.candidate,
            committed_artifacts=committed_artifacts,
        )
        if fresh_gates != prepared.gates:
            raise ReleaseDenied("persisted candidate gate report does not match trusted checks")

    def release(
        self,
        prepared: PreparedSoftwareRelease,
        approval: SoftwareReleaseApproval,
        actor: ActorContext,
        *,
        idempotency_key: str,
    ) -> SoftwareReleaseReceipt:
        with self._lock:
            return self._release(
                prepared,
                approval,
                actor,
                idempotency_key=idempotency_key,
            )

    def _release(
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
