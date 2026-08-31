from __future__ import annotations

import json
from collections.abc import Callable
from threading import Event, Thread

import pytest
from pydantic import ValidationError

from identity import AccessDenied, ActorContext
from model_governance import (
    BriefBudgetExceeded,
    CostReconciliationError,
    GovernedModelGateway,
    GuardrailDecision,
    IdempotencyConflict,
    InMemoryScopedTraceStore,
    ModelOutcomeUnknown,
    ModelPolicyDenied,
    ModelRequest,
    ModelThrottled,
    ProviderInvocation,
    ProviderResponse,
    UnknownModelOutcome,
    WorkerLimitExceeded,
    WorkerModelPolicy,
    WorkerPolicyRegistry,
)


class ScriptedTransport:
    def __init__(self, *outcomes: ProviderResponse | Exception) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[ProviderInvocation] = []

    def invoke(self, invocation: ProviderInvocation) -> ProviderResponse:
        self.calls.append(invocation)
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def _policy(
    worker_id: str,
    *,
    max_requests_per_brief: int = 3,
    max_concurrent_requests: int = 1,
    max_throttle_retries: int = 2,
) -> WorkerModelPolicy:
    slug = worker_id.replace("-", "_")
    return WorkerModelPolicy(
        worker_id=worker_id,
        service_identity=f"model-client-{slug}",
        endpoint_name=f"governed-{slug}",
        model_id=f"model-{slug}",
        max_input_tokens=100,
        max_output_tokens=50,
        max_requests_per_brief=max_requests_per_brief,
        max_concurrent_requests=max_concurrent_requests,
        max_throttle_retries=max_throttle_retries,
        input_cost_per_million_minor_units=1_000_000,
        output_cost_per_million_minor_units=1_000_000,
        required_guardrails=("safety", "sensitive-data"),
    )


def _registry(**overrides: WorkerModelPolicy) -> WorkerPolicyRegistry:
    policies = {
        worker_id: _policy(worker_id)
        for worker_id in (
            "product-manager",
            "scrum-master",
            "data-engineer",
            "software-engineer",
        )
    }
    policies.update(overrides)
    return WorkerPolicyRegistry(policies=tuple(policies.values()))


def _response(
    *,
    content: str = "bounded answer",
    input_tokens: int = 5,
    output_tokens: int = 3,
    cost_minor_units: int = 8,
) -> ProviderResponse:
    return ProviderResponse(
        content=content,
        usage_status="recorded",
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cost_minor_units=cost_minor_units,
        guardrails=(
            GuardrailDecision(name="safety", outcome="passed"),
            GuardrailDecision(name="sensitive-data", outcome="passed"),
        ),
    )


def _request(
    *,
    request_id: str = "request-001",
    brief_id: str = "brief-001",
    worker_id: str = "product-manager",
    prompt: str = "Summarize the synthetic brief",
    classification: str = "public",
    domain_repair_attempt: int = 0,
) -> ModelRequest:
    return ModelRequest(
        request_id=request_id,
        brief_id=brief_id,
        worker_id=worker_id,
        prompt=prompt,
        classification=classification,
        domain_repair_attempt=domain_repair_attempt,
    )


def _owner() -> ActorContext:
    return ActorContext(subject="brief-owner", roles={"viewer"})


def _gateway(
    transport: object,
    *,
    registry: WorkerPolicyRegistry | None = None,
    trace_store: InMemoryScopedTraceStore | None = None,
    on_throttle: Callable[[int], None] | None = None,
) -> GovernedModelGateway:
    gateway = GovernedModelGateway(
        policies=registry or _registry(),
        transport=transport,
        trace_store=trace_store or InMemoryScopedTraceStore("experiment-model-traces"),
        token_counter=lambda prompt: len(prompt.split()),
        on_throttle=on_throttle,
    )
    gateway.register_brief(
        brief_id="brief-001",
        run_id="run-001",
        owner_subject="brief-owner",
        authorized_ceiling_minor_units=300,
    )
    return gateway


def test_registry_requires_four_distinct_immutable_worker_policies_and_identities() -> None:
    registry = _registry()

    assert tuple(policy.worker_id for policy in registry.policies) == (
        "product-manager",
        "scrum-master",
        "data-engineer",
        "software-engineer",
    )
    assert len({policy.service_identity for policy in registry.policies}) == 4
    with pytest.raises(ValidationError):
        registry.policies[0].endpoint_name = "changed"

    duplicate = _policy("software-engineer").model_copy(
        update={"service_identity": registry.for_worker("data-engineer").service_identity}
    )
    with pytest.raises(ValueError, match="service identities must be distinct"):
        _registry(**{"software-engineer": duplicate})


@pytest.mark.parametrize(
    "endpoint_name",
    (
        "passthrough",
        "pass through",
        "vendor-PASS_THROUGH-route",
        "https://api.example.invalid/passthrough",
    ),
)
def test_passthrough_endpoint_configuration_is_rejected(endpoint_name: str) -> None:
    with pytest.raises(ValidationError, match="passthrough endpoints are forbidden"):
        _policy("product-manager").model_copy(
            update={"endpoint_name": endpoint_name}
        ).__class__.model_validate(
            {
                **_policy("product-manager").model_dump(mode="json"),
                "endpoint_name": endpoint_name,
            }
        )


def test_happy_path_routes_from_trusted_policy_and_reconciles_exact_usage() -> None:
    transport = ScriptedTransport(_response())
    gateway = _gateway(transport)

    result = gateway.invoke(_request())

    assert result.output == "bounded answer"
    assert result.actual_cost_minor_units == 8
    assert result.reconciliation.status == "reconciled"
    assert result.reconciliation.app_cost_minor_units == 8
    assert result.reconciliation.provider_cost_minor_units == 8
    assert result.domain_repair_attempt == 0
    assert transport.calls == [
        ProviderInvocation(
            request_id="request-001",
            brief_id="brief-001",
            worker_id="product-manager",
            service_identity="model-client-product_manager",
            endpoint_name="governed-product_manager",
            model_id="model-product_manager",
            prompt="Summarize the synthetic brief",
            max_output_tokens=50,
        )
    ]
    summary = gateway.read_budget_summary("brief-001", _owner())
    assert summary.metered_actual_minor_units == 8
    assert summary.budget_committed_minor_units == 8
    assert summary.remaining_authorization_minor_units == 292
    assert summary.usage_status == "complete"


def test_secret_and_classified_payloads_are_redacted_from_evidence_and_traces() -> None:
    trace_store = InMemoryScopedTraceStore("experiment-model-traces")
    secret = "api_key=do-not-log-this"
    classified = "Project Nightingale launch plan"
    gateway = _gateway(
        ScriptedTransport(_response(content=f"Use {secret} for {classified}")),
        trace_store=trace_store,
    )

    result = gateway.invoke(
        _request(prompt=f"Analyze {classified}; {secret}", classification="confidential")
    )

    evidence_json = json.dumps(
        [event.model_dump(mode="json") for event in gateway.events], sort_keys=True
    )
    assert secret not in evidence_json
    assert classified not in evidence_json
    assert "[REDACTED:CONFIDENTIAL]" in evidence_json
    assert all(event.trusted_source == "model-gateway" for event in gateway.events)

    owner = ActorContext(subject="brief-owner", roles={"viewer"})
    trace = trace_store.read_trace(result.trace_id, owner)
    trace_json = json.dumps(trace.model_dump(mode="json"), sort_keys=True)
    assert secret not in trace_json
    assert classified not in trace_json
    assert trace.prompt == "[REDACTED:CONFIDENTIAL]"
    assert trace.output == "[REDACTED:CONFIDENTIAL]"

    public_gateway = _gateway(ScriptedTransport(_response(content=f"Never return {secret}")))
    public_gateway.invoke(_request(prompt=f"Do not log {secret}"))
    public_evidence = json.dumps(
        [event.model_dump(mode="json") for event in public_gateway.events],
        sort_keys=True,
    )
    assert secret not in public_evidence
    assert "[REDACTED:SECRET]" in public_evidence


def test_throttling_retries_with_trusted_evidence_without_using_domain_repair() -> None:
    waits: list[int] = []
    transport = ScriptedTransport(
        ModelThrottled(retry_after_seconds=2),
        ModelThrottled(retry_after_seconds=3),
        _response(),
    )
    gateway = _gateway(transport, on_throttle=waits.append)

    result = gateway.invoke(_request(domain_repair_attempt=4))

    assert len(transport.calls) == 3
    assert waits == [2, 3]
    assert result.throttle_retries == 2
    assert result.domain_repair_attempt == 4
    throttle_events = [event for event in gateway.events if event.event_type == "model.throttled"]
    assert len(throttle_events) == 2
    assert all(
        event.details["domain_repair_attempt_incremented"] is False for event in throttle_events
    )
    assert gateway.read_budget_summary("brief-001", _owner()).metered_actual_minor_units == 8


def test_budget_blocks_before_transport_when_worst_case_authorization_would_exceed_ceiling() -> (
    None
):
    transport = ScriptedTransport(_response())
    gateway = GovernedModelGateway(
        policies=_registry(),
        transport=transport,
        trace_store=InMemoryScopedTraceStore("experiment-model-traces"),
        token_counter=lambda prompt: len(prompt.split()),
    )
    gateway.register_brief(
        brief_id="brief-001",
        run_id="run-001",
        owner_subject="brief-owner",
        authorized_ceiling_minor_units=149,
    )

    with pytest.raises(BriefBudgetExceeded, match="authorized ceiling"):
        gateway.invoke(_request())

    assert transport.calls == []
    summary = gateway.read_budget_summary("brief-001", _owner())
    assert summary.budget_committed_minor_units == 0
    assert summary.metered_actual_minor_units == 0


def test_exact_budget_boundary_is_allowed() -> None:
    gateway = GovernedModelGateway(
        policies=_registry(),
        transport=ScriptedTransport(_response()),
        trace_store=InMemoryScopedTraceStore("experiment-model-traces"),
        token_counter=lambda prompt: len(prompt.split()),
    )
    gateway.register_brief(
        brief_id="brief-001",
        run_id="run-001",
        owner_subject="brief-owner",
        authorized_ceiling_minor_units=150,
    )

    result = gateway.invoke(_request())

    assert result.actual_cost_minor_units == 8
    assert (
        gateway.read_budget_summary("brief-001", _owner()).remaining_authorization_minor_units
        == 142
    )


def test_request_token_and_concurrency_limits_fail_closed() -> None:
    with pytest.raises(WorkerLimitExceeded, match="input token limit"):
        _gateway(ScriptedTransport(_response())).invoke(
            _request(prompt=" ".join(f"token-{index}" for index in range(101)))
        )

    one_request_policy = _policy("product-manager", max_requests_per_brief=1)
    transport = ScriptedTransport(_response(), _response())
    gateway = _gateway(
        transport,
        registry=_registry(**{"product-manager": one_request_policy}),
    )
    gateway.invoke(_request())
    with pytest.raises(WorkerLimitExceeded, match="request limit"):
        gateway.invoke(_request(request_id="request-002"))
    assert len(transport.calls) == 1

    started = Event()
    release = Event()

    class BlockingTransport:
        def invoke(self, invocation: ProviderInvocation) -> ProviderResponse:
            started.set()
            assert release.wait(timeout=2)
            return _response()

    concurrent = _gateway(BlockingTransport())
    first = Thread(target=concurrent.invoke, args=(_request(),), daemon=True)
    first.start()
    assert started.wait(timeout=2)
    with pytest.raises(WorkerLimitExceeded, match="concurrency limit"):
        concurrent.invoke(_request(request_id="request-concurrent"))
    release.set()
    first.join(timeout=2)
    assert not first.is_alive()


def test_successful_request_replays_without_a_second_call_or_charge() -> None:
    transport = ScriptedTransport(_response())
    gateway = _gateway(transport)
    request = _request()

    first = gateway.invoke(request)
    replay = gateway.invoke(request)

    assert replay == first
    assert len(transport.calls) == 1
    assert gateway.read_budget_summary("brief-001", _owner()).metered_actual_minor_units == 8
    with pytest.raises(IdempotencyConflict, match="different request"):
        gateway.invoke(request.model_copy(update={"prompt": "Different content"}))


def test_exhausted_throttle_retries_release_unspent_authorization() -> None:
    gateway = _gateway(
        ScriptedTransport(
            ModelThrottled(retry_after_seconds=1),
            ModelThrottled(retry_after_seconds=1),
        ),
        registry=_registry(
            **{
                "product-manager": _policy(
                    "product-manager",
                    max_throttle_retries=1,
                )
            }
        ),
    )

    with pytest.raises(ModelThrottled):
        gateway.invoke(_request(domain_repair_attempt=2))

    summary = gateway.read_budget_summary("brief-001", _owner())
    assert summary.budget_committed_minor_units == 0
    assert summary.metered_actual_minor_units == 0
    assert summary.throttle_count == 1


def test_ambiguous_transport_error_holds_budget_and_replay_never_calls_again() -> None:
    secret = "access_token=must-not-escape"
    transport = ScriptedTransport(
        ModelThrottled(retry_after_seconds=1),
        ConnectionError(f"lost acknowledgement {secret}"),
    )
    gateway = _gateway(transport)
    request = _request(domain_repair_attempt=3)

    with pytest.raises(ModelOutcomeUnknown) as first_error:
        gateway.invoke(request)

    first_receipt = first_error.value.receipt
    assert isinstance(first_receipt, UnknownModelOutcome)
    assert first_receipt.schema_id == "steward-forge.unknown-model-outcome"
    assert first_receipt.schema_version == 1
    assert first_receipt.request_id == request.request_id
    assert first_receipt.brief_id == request.brief_id
    assert first_receipt.worker_id == request.worker_id
    assert first_receipt.held_authorized_minor_units == 150
    assert first_receipt.throttle_retry_count == 1
    assert first_receipt.domain_repair_attempt == 3
    assert first_receipt.usage_status == "incomplete"
    receipt_payload = first_receipt.model_dump(mode="json")
    assert "prompt" not in receipt_payload
    assert "output" not in receipt_payload
    assert "actual_cost_minor_units" not in receipt_payload

    summary = gateway.read_budget_summary("brief-001", _owner())
    assert summary.budget_committed_minor_units == 150
    assert summary.metered_actual_minor_units == 0
    assert summary.incomplete_usage_count == 1
    assert summary.usage_status == "incomplete"
    unknown = [event for event in gateway.events if event.event_type == "model.outcome.unknown"]
    assert len(unknown) == 1
    assert unknown[0].details == {
        "reason": "ConnectionError",
        "usage_recorded_as_actual": False,
    }
    assert secret not in json.dumps([event.model_dump(mode="json") for event in gateway.events])

    with pytest.raises(ModelOutcomeUnknown) as replay_error:
        gateway.invoke(request)
    assert replay_error.value.receipt == first_receipt
    assert replay_error.value.receipt.model_dump_json() == first_receipt.model_dump_json()
    assert len(transport.calls) == 2
    with pytest.raises(IdempotencyConflict, match="different request"):
        gateway.invoke(request.model_copy(update={"prompt": "conflicting payload"}))


def test_trace_scope_allows_only_brief_owner_or_auditor() -> None:
    trace_store = InMemoryScopedTraceStore("experiment-model-traces")
    result = _gateway(ScriptedTransport(_response()), trace_store=trace_store).invoke(_request())
    owner = ActorContext(subject="brief-owner", roles={"viewer"})
    stranger = ActorContext(subject="other-viewer", roles={"viewer"})
    auditor = ActorContext(subject="audit-user", roles={"auditor"})

    assert trace_store.read_trace(result.trace_id, owner).brief_id == "brief-001"
    assert trace_store.read_trace(result.trace_id, auditor).run_id == "run-001"
    with pytest.raises(AccessDenied, match="trace"):
        trace_store.read_trace(result.trace_id, stranger)
    assert trace_store.list_run("run-001", owner) == (
        trace_store.read_trace(result.trace_id, owner),
    )
    assert trace_store.list_run("different-run", owner) == ()


def test_missing_usage_remains_incomplete_and_is_never_reported_as_actual_cost() -> None:
    unavailable = ProviderResponse(
        content="answer without telemetry",
        usage_status="unavailable",
        guardrails=(
            GuardrailDecision(name="safety", outcome="passed"),
            GuardrailDecision(name="sensitive-data", outcome="passed"),
        ),
    )
    gateway = _gateway(ScriptedTransport(unavailable))

    result = gateway.invoke(_request())

    assert result.actual_cost_minor_units is None
    assert result.reconciliation.status == "incomplete"
    assert result.reconciliation.app_cost_minor_units is None
    assert result.reconciliation.provider_cost_minor_units is None
    summary = gateway.read_budget_summary("brief-001", _owner())
    assert summary.usage_status == "incomplete"
    assert summary.metered_actual_minor_units == 0
    assert summary.budget_committed_minor_units == 150
    assert summary.incomplete_usage_count == 1


def test_trace_write_failure_holds_authorization_and_never_reports_actual_cost() -> None:
    class FailingTraceStore:
        experiment_id = "experiment-model-traces"

        @staticmethod
        def append(trace: object) -> None:
            raise ConnectionError("trace backend unavailable")

    gateway = GovernedModelGateway(
        policies=_registry(),
        transport=ScriptedTransport(_response()),
        trace_store=FailingTraceStore(),
        token_counter=lambda prompt: len(prompt.split()),
    )
    gateway.register_brief(
        brief_id="brief-001",
        run_id="run-001",
        owner_subject="brief-owner",
        authorized_ceiling_minor_units=300,
    )

    with pytest.raises(ModelPolicyDenied, match="trace persistence failed closed"):
        gateway.invoke(_request())

    summary = gateway.read_budget_summary("brief-001", _owner())
    assert summary.metered_actual_minor_units == 0
    assert summary.budget_committed_minor_units == 150
    assert summary.usage_status == "incomplete"
    assert any(event.event_type == "model.trace.failed" for event in gateway.events)


@pytest.mark.parametrize(
    "response",
    (
        _response(cost_minor_units=9),
        _response(input_tokens=5, output_tokens=51, cost_minor_units=56),
        ProviderResponse(
            content="guardrail evidence missing",
            usage_status="recorded",
            input_tokens=5,
            output_tokens=3,
            cost_minor_units=8,
            guardrails=(GuardrailDecision(name="safety", outcome="passed"),),
        ),
    ),
)
def test_cost_or_guardrail_mismatch_fails_closed_without_inventing_actual_cost(
    response: ProviderResponse,
) -> None:
    gateway = _gateway(ScriptedTransport(response))

    with pytest.raises(CostReconciliationError, match="reconciliation"):
        gateway.invoke(_request())

    summary = gateway.read_budget_summary("brief-001", _owner())
    assert summary.reconciliation_failure_count == 1
    assert summary.usage_status == "incomplete"
    assert summary.metered_actual_minor_units == 0
    assert summary.budget_committed_minor_units == 150
    assert any(event.event_type == "model.reconciliation.failed" for event in gateway.events)


def test_conflicting_duplicate_guardrail_evidence_cannot_reconcile_as_passed() -> None:
    response = ProviderResponse(
        content="conflicting guardrail result",
        usage_status="recorded",
        input_tokens=5,
        output_tokens=3,
        cost_minor_units=8,
        guardrails=(
            GuardrailDecision(name="safety", outcome="passed"),
            GuardrailDecision(name="safety", outcome="blocked"),
            GuardrailDecision(name="sensitive-data", outcome="passed"),
        ),
    )
    trace_store = InMemoryScopedTraceStore("experiment-model-traces")
    gateway = _gateway(ScriptedTransport(response), trace_store=trace_store)

    with pytest.raises(CostReconciliationError, match="reconciliation"):
        gateway.invoke(_request())

    traces = trace_store.list_run("run-001", ActorContext(subject="audit-user", roles={"auditor"}))
    assert len(traces) == 1
    trace = traces[0]
    assert trace.reconciliation_status == "mismatch"
    failed = next(
        event for event in gateway.events if event.event_type == "model.reconciliation.failed"
    )
    assert failed.details["conflicting_guardrails"] == ["safety"]


def test_worker_and_brief_registration_cannot_be_forged_or_rebound() -> None:
    transport = ScriptedTransport(_response())
    gateway = _gateway(transport)

    with pytest.raises(ModelPolicyDenied, match="not registered"):
        gateway.invoke(_request(brief_id="unregistered"))
    with pytest.raises(ModelPolicyDenied, match="already bound"):
        gateway.register_brief(
            brief_id="brief-001",
            run_id="other-run",
            owner_subject="other-owner",
            authorized_ceiling_minor_units=300,
        )


def test_budget_summary_requires_brief_owner_or_auditor() -> None:
    gateway = _gateway(ScriptedTransport(_response()))
    auditor = ActorContext(subject="audit-user", roles={"auditor"})
    stranger = ActorContext(subject="other-viewer", roles={"viewer"})

    assert not hasattr(gateway, "budget_summary")
    assert gateway.read_budget_summary("brief-001", _owner()).brief_id == "brief-001"
    assert gateway.read_budget_summary("brief-001", auditor).brief_id == "brief-001"
    with pytest.raises(AccessDenied, match="cannot view"):
        gateway.read_budget_summary("brief-001", stranger)
