from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest

from workers.swe import (
    DeploymentAcknowledgementLost,
    DeploymentConflict,
    InMemoryDeploymentAdapter,
    InMemoryDeploymentBackend,
)

COMMIT_SHA = "a" * 64


def test_new_adapter_observes_shared_remote_result_before_mutating() -> None:
    backend = InMemoryDeploymentBackend(
        previous_release_sha="0" * 64,
        previous_workspace_ids={"dashboard": "dashboard-previous"},
    )
    first_adapter = InMemoryDeploymentAdapter(backend=backend)
    original = first_adapter.ensure_deployed(
        commit_sha=COMMIT_SHA,
        include_genie=True,
        idempotency_key="deploy-001",
    )
    restarted_adapter = InMemoryDeploymentAdapter(backend=backend)

    observed = restarted_adapter.observe(
        commit_sha=COMMIT_SHA,
        include_genie=True,
        idempotency_key="deploy-001",
    )
    replay = restarted_adapter.ensure_deployed(
        commit_sha=COMMIT_SHA,
        include_genie=True,
        idempotency_key="deploy-001",
    )

    assert observed is original
    assert replay is original
    assert backend.deploy_calls == 1
    assert first_adapter.deploy_calls == 1
    assert restarted_adapter.deploy_calls == 0
    assert original.rollback_state == {
        "release_sha": "0" * 64,
        "workspace_ids": {"dashboard": "dashboard-previous"},
    }


@pytest.mark.parametrize(
    ("commit_sha", "include_genie"),
    [("b" * 64, False), (COMMIT_SHA, True)],
    ids=["commit", "genie-input"],
)
def test_reused_remote_key_with_different_input_fails_closed(
    commit_sha: str,
    include_genie: bool,
) -> None:
    backend = InMemoryDeploymentBackend()
    InMemoryDeploymentAdapter(backend=backend).ensure_deployed(
        commit_sha=COMMIT_SHA,
        include_genie=False,
        idempotency_key="deploy-conflict",
    )
    restarted_adapter = InMemoryDeploymentAdapter(backend=backend)

    with pytest.raises(DeploymentConflict, match="another request"):
        restarted_adapter.ensure_deployed(
            commit_sha=commit_sha,
            include_genie=include_genie,
            idempotency_key="deploy-conflict",
        )

    assert backend.deploy_calls == 1
    assert restarted_adapter.deploy_calls == 0


def test_lost_acknowledgement_is_recovered_without_second_remote_deployment() -> None:
    backend = InMemoryDeploymentBackend()
    interrupted_adapter = InMemoryDeploymentAdapter(
        backend=backend,
        lose_acknowledgement_once=True,
    )

    with pytest.raises(DeploymentAcknowledgementLost, match="deploy-lost-ack"):
        interrupted_adapter.ensure_deployed(
            commit_sha=COMMIT_SHA,
            include_genie=False,
            idempotency_key="deploy-lost-ack",
        )

    assert backend.deploy_calls == 1
    assert interrupted_adapter.deploy_calls == 1

    restarted_adapter = InMemoryDeploymentAdapter(backend=backend)
    recovered = restarted_adapter.ensure_deployed(
        commit_sha=COMMIT_SHA,
        include_genie=False,
        idempotency_key="deploy-lost-ack",
    )

    assert recovered.commit_sha == COMMIT_SHA
    assert backend.deploy_calls == 1
    assert restarted_adapter.deploy_calls == 0


def test_shared_backend_serializes_concurrent_ensure_requests() -> None:
    backend = InMemoryDeploymentBackend()
    adapters = [InMemoryDeploymentAdapter(backend=backend) for _ in range(12)]

    def deploy(adapter: InMemoryDeploymentAdapter):
        return adapter.ensure_deployed(
            commit_sha=COMMIT_SHA,
            include_genie=False,
            idempotency_key="deploy-concurrent",
        )

    with ThreadPoolExecutor(max_workers=6) as executor:
        results = list(executor.map(deploy, adapters))

    assert all(result is results[0] for result in results)
    assert backend.deploy_calls == 1
    assert sum(adapter.deploy_calls for adapter in adapters) == 1


def test_deploy_remains_a_compatibility_wrapper_for_ensure_deployed() -> None:
    adapter = InMemoryDeploymentAdapter()

    first = adapter.deploy(
        commit_sha=COMMIT_SHA,
        include_genie=False,
        idempotency_key="deploy-wrapper",
    )
    replay = adapter.ensure_deployed(
        commit_sha=COMMIT_SHA,
        include_genie=False,
        idempotency_key="deploy-wrapper",
    )

    assert replay is first
    assert adapter.deploy_calls == 1
