from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

import pytest
import yaml
from fastapi.testclient import TestClient

from gates.swe.release import SoftwareReleaseService
from identity import ActorContext
from identity.verifier import StaticIdentityVerifier
from model_governance import (
    GovernedModelGateway,
    GuardrailDecision,
    InMemoryScopedTraceStore,
    ModelRequest,
    ProviderInvocation,
    ProviderResponse,
    load_model_governance_config,
    usd_ceiling_to_minor_units,
    usd_to_minor_units,
)
from orchestrator.delivery import DeliveryCoordinator
from orchestrator.models import AcceptanceTest, BriefSubmission
from pipeline import DataEngineeringPipeline
from workbench.app import create_app
from workers.de import InMemoryCatalogAdapter
from workers.swe import InMemoryArtifactRepository, InMemoryDeploymentAdapter

ROOT = Path(__file__).parents[1]


def test_usd_ceiling_conversion_is_exact_and_never_uses_bankers_rounding() -> None:
    assert usd_to_minor_units(4) == 400
    assert usd_to_minor_units("4.00") == 400
    assert usd_ceiling_to_minor_units("1.001") == 101
    assert usd_ceiling_to_minor_units("2.4100000000000001") == 242


@pytest.mark.parametrize(
    "value",
    ("1.005", "0.001", "NaN", "Infinity", "-Infinity"),
)
def test_usd_ceiling_rejects_sub_cent_and_non_finite_values(value: str) -> None:
    with pytest.raises(ValueError, match="USD ceiling"):
        usd_to_minor_units(value)


def test_brief_and_api_reject_ambiguous_sub_cent_ceiling() -> None:
    values = _submission().model_dump(mode="json")
    values["cost_ceiling_usd"] = 1.005
    with pytest.raises(ValueError, match="whole minor units"):
        BriefSubmission.model_validate(values)

    response = TestClient(create_app(identity_verifier=_verifier())).post(
        "/api/briefs",
        json=values,
        headers={"X-Forwarded-Access-Token": "owner-token"},
    )
    assert response.status_code == 422


@pytest.mark.parametrize("value", (float("nan"), float("inf"), float("-inf")))
def test_brief_rejects_non_finite_ceiling(value: float) -> None:
    values = _submission().model_dump(mode="json")
    values["cost_ceiling_usd"] = value

    with pytest.raises(ValueError, match="finite"):
        BriefSubmission.model_validate(values)


def _model_environment(**overrides: str) -> dict[str, str]:
    values = {
        "STEWARD_FORGE_MODEL_CALLS_ENABLED": "false",
        "STEWARD_FORGE_MODEL_TRACE_EXPERIMENT_ID": "experiment-123",
        "STEWARD_FORGE_MODEL_MAX_INPUT_TOKENS": "4096",
        "STEWARD_FORGE_MODEL_MAX_OUTPUT_TOKENS": "1024",
        "STEWARD_FORGE_MODEL_MAX_REQUESTS_PER_BRIEF": "8",
        "STEWARD_FORGE_MODEL_MAX_CONCURRENT_REQUESTS": "1",
        "STEWARD_FORGE_MODEL_MAX_THROTTLE_RETRIES": "2",
        "STEWARD_FORGE_MODEL_INPUT_COST_PER_MILLION_MINOR_UNITS": "300",
        "STEWARD_FORGE_MODEL_OUTPUT_COST_PER_MILLION_MINOR_UNITS": "900",
        "STEWARD_FORGE_MODEL_REQUIRED_GUARDRAILS": "safety,sensitive-data",
    }
    for prefix, worker in (
        ("PRODUCT_MANAGER", "product-manager"),
        ("SCRUM_MASTER", "scrum-master"),
        ("DATA_ENGINEER", "data-engineer"),
        ("SOFTWARE_ENGINEER", "software-engineer"),
    ):
        values[f"STEWARD_FORGE_{prefix}_MODEL_SERVICE_IDENTITY"] = f"steward-forge-{worker}-model"
        values[f"STEWARD_FORGE_{prefix}_MODEL_ENDPOINT"] = f"governed-{worker}"
        values[f"STEWARD_FORGE_{prefix}_MODEL_ID"] = f"model-{worker}"
    values.update(overrides)
    return values


def test_one_environment_path_builds_all_worker_policies_and_allows_endpoint_overrides() -> None:
    config = load_model_governance_config(
        _model_environment(
            STEWARD_FORGE_PRODUCT_MANAGER_MODEL_ENDPOINT="pm-endpoint-alternate",
            STEWARD_FORGE_SOFTWARE_ENGINEER_MODEL_ID="swe-model-alternate",
        )
    )

    assert config.enabled is False
    assert config.trace_experiment_id == "experiment-123"
    assert len(config.policies.policies) == 4
    assert len({policy.service_identity for policy in config.policies.policies}) == 4
    assert config.policies.for_worker("product-manager").endpoint_name == "pm-endpoint-alternate"
    assert config.policies.for_worker("software-engineer").model_id == "swe-model-alternate"


def test_environment_path_fails_closed_for_missing_or_passthrough_routes() -> None:
    missing: Mapping[str, str] = {
        key: value
        for key, value in _model_environment().items()
        if key != "STEWARD_FORGE_DATA_ENGINEER_MODEL_ENDPOINT"
    }
    with pytest.raises(ValueError, match="STEWARD_FORGE_DATA_ENGINEER_MODEL_ENDPOINT"):
        load_model_governance_config(missing)

    with pytest.raises(ValueError, match="passthrough endpoints are forbidden"):
        load_model_governance_config(
            _model_environment(STEWARD_FORGE_SCRUM_MASTER_MODEL_ENDPOINT="vendor-passthrough")
        )


def test_enabled_app_runtime_requires_an_injected_governed_gateway(monkeypatch) -> None:
    for name, value in _model_environment(STEWARD_FORGE_MODEL_CALLS_ENABLED="true").items():
        monkeypatch.setenv(name, value)

    with pytest.raises(RuntimeError, match="no governed provider/trace adapter"):
        create_app(identity_verifier=_verifier())


def _submission() -> BriefSubmission:
    return BriefSubmission(
        title="Model governance integration",
        business_question="Show the governed delivery result.",
        acceptance_tests=[
            AcceptanceTest(
                name="governed-output",
                description="The output comes from governed synthetic inputs.",
                kind="contract",
            )
        ],
        cost_ceiling_usd=4,
        release_approver="approver-1",
        viewer_subjects=["viewer-1"],
        idempotency_key="governance-integration-001",
    )


def _coordinator(
    model_gateway: GovernedModelGateway | None = None,
) -> DeliveryCoordinator:
    return DeliveryCoordinator(
        data_pipeline=DataEngineeringPipeline(InMemoryCatalogAdapter()),
        software_release=SoftwareReleaseService(
            InMemoryArtifactRepository("1" * 64),
            InMemoryDeploymentAdapter(previous_release_sha="0" * 64),
        ),
        model_gateway=model_gateway,
    )


def test_delivery_registers_deterministic_fallback_as_not_used_without_fabricated_cost() -> None:
    result = _coordinator().submit(
        _submission(),
        config=_reference_config(),
        actor=ActorContext(subject="owner-1", roles={"submitter", "viewer"}),
    )

    assert result.model_budget is not None
    assert result.model_budget.authorized_ceiling_minor_units == 400
    assert result.model_budget.budget_committed_minor_units == 0
    assert result.model_budget.metered_actual_minor_units == 0
    assert result.model_budget.remaining_authorization_minor_units == 400
    assert result.model_budget.throttle_count == 0
    assert result.model_budget.reconciliation_failure_count == 0
    assert result.model_budget.usage_status == "not_used"


def _reference_config():
    from orchestrator.delivery import ReferenceRunConfig

    return ReferenceRunConfig(
        run_id="run-model-governance",
        seed=2026,
        sandbox_catalog="demo_catalog",
        sandbox_schema="steward_forge_sandbox",
        trusted_base_sha="1" * 64,
        generated_prefix="generated/software-engineer",
        artifact_branch="steward-forge/candidates",
        dashboard_title="Governed delivery",
    )


class _OneResponseTransport:
    def invoke(self, invocation: ProviderInvocation) -> ProviderResponse:
        return ProviderResponse(
            content=f"governed result for {invocation.worker_id}",
            usage_status="recorded",
            input_tokens=3,
            output_tokens=4,
            cost_minor_units=2,
            guardrails=(
                GuardrailDecision(name="safety", outcome="passed"),
                GuardrailDecision(name="sensitive-data", outcome="passed"),
            ),
        )


def _gateway() -> GovernedModelGateway:
    config = load_model_governance_config(
        _model_environment(
            STEWARD_FORGE_MODEL_INPUT_COST_PER_MILLION_MINOR_UNITS="10000",
            STEWARD_FORGE_MODEL_OUTPUT_COST_PER_MILLION_MINOR_UNITS="20000",
        )
    )
    return GovernedModelGateway(
        policies=config.policies,
        transport=_OneResponseTransport(),
        trace_store=InMemoryScopedTraceStore(config.trace_experiment_id),
        token_counter=lambda prompt: len(prompt.split()),
    )


def test_delivery_budget_and_trace_reads_follow_brief_row_access_without_payloads() -> None:
    gateway = _gateway()
    coordinator = _coordinator(gateway)
    owner = ActorContext(subject="owner-1", roles={"submitter", "viewer"})
    brief = coordinator.submit(_submission(), config=_reference_config(), actor=owner)
    gateway.invoke(
        ModelRequest(
            request_id="request-integration-1",
            brief_id=brief.workflow_id,
            worker_id="product-manager",
            prompt="private prompt content",
            classification="confidential",
        )
    )

    viewer = ActorContext(subject="viewer-1", roles={"viewer"})
    auditor = ActorContext(subject="audit-1", roles={"auditor"})
    stranger = ActorContext(subject="stranger-1", roles={"viewer"})
    assert coordinator.read_model_budget(brief.workflow_id, viewer).metered_actual_minor_units == 2
    traces = coordinator.read_model_traces(brief.workflow_id, viewer)
    assert len(traces) == 1
    assert traces == coordinator.read_model_traces(brief.workflow_id, auditor)
    trace_json = traces[0].model_dump(mode="json")
    assert "prompt" not in trace_json
    assert "output" not in trace_json
    with pytest.raises(PermissionError, match="view this brief"):
        coordinator.read_model_budget(brief.workflow_id, stranger)
    with pytest.raises(PermissionError, match="view this brief"):
        coordinator.read_model_traces(brief.workflow_id, stranger)


def _verifier() -> StaticIdentityVerifier:
    return StaticIdentityVerifier(
        {
            "owner-token": ActorContext(subject="owner-1", roles={"submitter", "viewer"}),
            "viewer-token": ActorContext(subject="viewer-1", roles={"viewer"}),
            "stranger-token": ActorContext(subject="stranger-1", roles={"viewer"}),
            "auditor-token": ActorContext(subject="audit-1", roles={"auditor"}),
        }
    )


def test_workbench_budget_api_and_ui_expose_no_model_payload_content() -> None:
    client = TestClient(create_app(identity_verifier=_verifier()))
    payload = _submission().model_dump(mode="json")
    submitted = client.post(
        "/api/briefs", json=payload, headers={"X-Forwarded-Access-Token": "owner-token"}
    )
    assert submitted.status_code == 201
    brief_id = submitted.json()["id"]

    budget = client.get(
        f"/api/briefs/{brief_id}/model-budget",
        headers={"X-Forwarded-Access-Token": "viewer-token"},
    )
    assert budget.status_code == 200
    assert budget.json() == {
        "schema_id": "steward-forge.model-budget-summary",
        "schema_version": 1,
        "brief_id": brief_id,
        "currency": "USD",
        "authorized_ceiling_minor_units": 400,
        "budget_committed_minor_units": 0,
        "metered_actual_minor_units": 0,
        "remaining_authorization_minor_units": 400,
        "request_count": 0,
        "throttle_count": 0,
        "incomplete_usage_count": 0,
        "reconciliation_failure_count": 0,
        "usage_status": "not_used",
    }
    denied = client.get(
        f"/api/briefs/{brief_id}/model-budget",
        headers={"X-Forwarded-Access-Token": "stranger-token"},
    )
    assert denied.status_code == 403
    page = client.get("/")
    assert "Authorized ceiling" in page.text
    assert "Committed reservation" in page.text
    assert "Metered actual" in page.text
    assert "Remaining authorization" in page.text
    assert "Throttle count" in page.text
    assert "Reconciliation status" in page.text


def test_workbench_trace_api_allows_scoped_viewers_and_auditors_but_denies_strangers() -> None:
    gateway = _gateway()
    client = TestClient(create_app(identity_verifier=_verifier(), model_gateway=gateway))
    submitted = client.post(
        "/api/briefs",
        json=_submission().model_dump(mode="json"),
        headers={"X-Forwarded-Access-Token": "owner-token"},
    )
    brief_id = submitted.json()["id"]
    gateway.invoke(
        ModelRequest(
            request_id="workbench-trace-1",
            brief_id=brief_id,
            worker_id="scrum-master",
            prompt="do not expose this prompt",
            classification="confidential",
        )
    )

    for token in ("viewer-token", "auditor-token"):
        response = client.get(
            f"/api/briefs/{brief_id}/model-traces",
            headers={"X-Forwarded-Access-Token": token},
        )
        assert response.status_code == 200
        assert len(response.json()) == 1
        assert "prompt" not in response.json()[0]
        assert "output" not in response.json()[0]
    denied = client.get(
        f"/api/briefs/{brief_id}/model-traces",
        headers={"X-Forwarded-Access-Token": "stranger-token"},
    )
    assert denied.status_code == 403


def test_bundle_wires_overridable_worker_routes_and_a_scoped_experiment() -> None:
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    expected_variables = {
        "model_calls_enabled",
        "model_trace_experiment_name",
        "model_max_input_tokens",
        "model_max_output_tokens",
        "model_max_requests_per_brief",
        "model_max_concurrent_requests",
        "model_max_throttle_retries",
        "model_input_cost_per_million_minor_units",
        "model_output_cost_per_million_minor_units",
        "model_required_guardrails",
    }
    for worker in (
        "product_manager",
        "scrum_master",
        "data_engineer",
        "software_engineer",
    ):
        expected_variables.update(
            {
                f"{worker}_model_service_identity",
                f"{worker}_model_endpoint",
                f"{worker}_model_id",
            }
        )
    assert expected_variables <= set(bundle["variables"])

    governance = yaml.safe_load((ROOT / "resources" / "model_governance.yml").read_text())[
        "resources"
    ]
    experiment = governance["experiments"]["model_traces"]
    assert experiment["name"] == "${var.model_trace_experiment_name}"
    assert experiment["permissions"] == [
        {"level": "CAN_READ", "group_name": "${var.auditor_group_name}"},
        {
            "level": "CAN_EDIT",
            "service_principal_name": (
                "${resources.apps.workbench.service_principal_client_id}"
            ),
        },
    ]

    app = yaml.safe_load((ROOT / "resources" / "app.yml").read_text())["resources"]["apps"][
        "workbench"
    ]
    experiment_binding = next(
        item["experiment"] for item in app["resources"] if "experiment" in item
    )
    assert experiment_binding == {
        "experiment_id": "${resources.experiments.model_traces.id}",
        "permission": "CAN_EDIT",
    }
    env = {item["name"]: item["value"] for item in app["config"]["env"]}
    assert env["STEWARD_FORGE_MODEL_TRACE_EXPERIMENT_ID"] == (
        "${resources.experiments.model_traces.id}"
    )
    assert env["STEWARD_FORGE_MODEL_CALLS_ENABLED"] == "${var.model_calls_enabled}"
    for suffix, variable in (
        ("MAX_INPUT_TOKENS", "model_max_input_tokens"),
        ("MAX_OUTPUT_TOKENS", "model_max_output_tokens"),
        ("MAX_REQUESTS_PER_BRIEF", "model_max_requests_per_brief"),
        ("MAX_CONCURRENT_REQUESTS", "model_max_concurrent_requests"),
        ("MAX_THROTTLE_RETRIES", "model_max_throttle_retries"),
        (
            "INPUT_COST_PER_MILLION_MINOR_UNITS",
            "model_input_cost_per_million_minor_units",
        ),
        (
            "OUTPUT_COST_PER_MILLION_MINOR_UNITS",
            "model_output_cost_per_million_minor_units",
        ),
        ("REQUIRED_GUARDRAILS", "model_required_guardrails"),
    ):
        assert env[f"STEWARD_FORGE_MODEL_{suffix}"] == f"${{var.{variable}}}"
    for prefix, variable_prefix in (
        ("PRODUCT_MANAGER", "product_manager"),
        ("SCRUM_MASTER", "scrum_master"),
        ("DATA_ENGINEER", "data_engineer"),
        ("SOFTWARE_ENGINEER", "software_engineer"),
    ):
        assert env[f"STEWARD_FORGE_{prefix}_MODEL_ENDPOINT"] == (
            f"${{var.{variable_prefix}_model_endpoint}}"
        )
        assert env[f"STEWARD_FORGE_{prefix}_MODEL_ID"] == (f"${{var.{variable_prefix}_model_id}}")
        assert env[f"STEWARD_FORGE_{prefix}_MODEL_SERVICE_IDENTITY"] == (
            f"${{var.{variable_prefix}_model_service_identity}}"
        )
