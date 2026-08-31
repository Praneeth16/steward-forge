"""Trusted gateway enforcement for model routing, limits, privacy, and cost."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass, field
from threading import RLock
from typing import Protocol

from identity import AccessDenied, ActorContext
from model_governance.contracts import (
    BriefBudgetSummary,
    CostReconciliation,
    GatewayEvidence,
    GuardrailDecision,
    ModelInvocationResult,
    ModelRequest,
    ProviderInvocation,
    ProviderResponse,
    UnknownModelOutcome,
    WorkerId,
    WorkerModelPolicy,
    WorkerPolicyRegistry,
)
from model_governance.privacy import redact_mapping
from model_governance.tracing import (
    ModelTrace,
    ModelTraceSummary,
    TraceReader,
    TraceStore,
)


class ModelGovernanceError(ValueError):
    """Base class for a trusted gateway denial."""


class ModelPolicyDenied(ModelGovernanceError):
    """The request did not match trusted registration or policy."""


class WorkerLimitExceeded(ModelGovernanceError):
    """A worker request, token, or concurrency limit was reached."""


class BriefBudgetExceeded(ModelGovernanceError):
    """Worst-case request authorization would cross the brief ceiling."""


class CostReconciliationError(ModelGovernanceError):
    """Provider usage or guardrails did not reconcile with app enforcement."""


class IdempotencyConflict(ModelGovernanceError):
    """A request ID was reused with different immutable content."""


class ModelOutcomeUnknown(ModelGovernanceError):
    """A billable provider call has no trusted terminal usage observation."""

    def __init__(self, receipt: UnknownModelOutcome) -> None:
        super().__init__("model outcome is unknown; operator reconciliation is required")
        self.receipt = receipt


class ModelThrottled(ModelGovernanceError):
    """The provider requested a bounded retry after a delay."""

    def __init__(self, *, retry_after_seconds: int) -> None:
        if retry_after_seconds < 0:
            raise ValueError("retry_after_seconds cannot be negative")
        super().__init__("model endpoint throttled the request")
        self.retry_after_seconds = retry_after_seconds


class ModelTransport(Protocol):
    def invoke(self, invocation: ProviderInvocation) -> ProviderResponse: ...


@dataclass(slots=True)
class _BriefState:
    run_id: str
    owner_subject: str
    ceiling: int
    viewer_subjects: tuple[str, ...] = ()
    committed: int = 0
    actual: int = 0
    request_count: int = 0
    throttle_count: int = 0
    incomplete_count: int = 0
    reconciliation_failure_count: int = 0
    worker_requests: dict[WorkerId, int] = field(default_factory=dict)


class GovernedModelGateway:
    """Select trusted routes and enforce model governance before adapter I/O."""

    def __init__(
        self,
        *,
        policies: WorkerPolicyRegistry,
        transport: ModelTransport,
        trace_store: TraceStore,
        trace_reader: TraceReader | None = None,
        token_counter: Callable[[str], int],
        on_throttle: Callable[[int], None] | None = None,
    ) -> None:
        self._policies = policies
        self._transport = transport
        self._trace_store = trace_store
        self._trace_reader = trace_reader or (
            trace_store if hasattr(trace_store, "list_run") else None
        )
        self._token_counter = token_counter
        self._on_throttle = on_throttle or (lambda _: None)
        self._briefs: dict[str, _BriefState] = {}
        self._active: dict[WorkerId, int] = {}
        self._request_hashes: dict[str, str] = {}
        self._results: dict[str, ModelInvocationResult] = {}
        self._unknown_outcomes: dict[str, UnknownModelOutcome] = {}
        self._lock = RLock()
        self._events: list[GatewayEvidence] = []

    @property
    def events(self) -> tuple[GatewayEvidence, ...]:
        """Return an immutable view of evidence emitted by the trusted boundary."""

        with self._lock:
            return tuple(self._events)

    def register_brief(
        self,
        *,
        brief_id: str,
        run_id: str,
        owner_subject: str,
        authorized_ceiling_minor_units: int,
        viewer_subjects: tuple[str, ...] = (),
    ) -> None:
        if not all(value.strip() for value in (brief_id, run_id, owner_subject)):
            raise ModelPolicyDenied("brief registration fields must not be empty")
        if authorized_ceiling_minor_units < 0:
            raise ModelPolicyDenied("authorized ceiling cannot be negative")
        candidate = _BriefState(
            run_id=run_id,
            owner_subject=owner_subject,
            ceiling=authorized_ceiling_minor_units,
            viewer_subjects=tuple(sorted(set(viewer_subjects))),
        )
        with self._lock:
            existing = self._briefs.get(brief_id)
            if existing is None:
                self._briefs[brief_id] = candidate
            elif (
                existing.run_id != run_id
                or existing.owner_subject != owner_subject
                or existing.ceiling != authorized_ceiling_minor_units
                or existing.viewer_subjects != candidate.viewer_subjects
            ):
                raise ModelPolicyDenied("brief is already bound to different governance inputs")

    def invoke(self, request: ModelRequest) -> ModelInvocationResult:
        policy = self._policies.for_worker(request.worker_id)
        request_hash = self._request_hash(request)
        input_tokens = self._token_counter(request.prompt)
        if not isinstance(input_tokens, int) or input_tokens < 0:
            raise ModelPolicyDenied("token counter returned an invalid value")
        if input_tokens > policy.max_input_tokens:
            raise WorkerLimitExceeded("worker input token limit exceeded")
        reservation = policy.maximum_authorized_minor_units
        replay = self._authorize(request, request_hash, policy, reservation)
        if replay is not None:
            self._event(
                request,
                "model.replayed",
                {"original_trace_id": replay.trace_id},
            )
            return replay

        invocation = ProviderInvocation(
            request_id=request.request_id,
            brief_id=request.brief_id,
            worker_id=request.worker_id,
            service_identity=policy.service_identity,
            endpoint_name=policy.endpoint_name,
            model_id=policy.model_id,
            prompt=request.prompt,
            max_output_tokens=policy.max_output_tokens,
        )
        throttle_retries = 0
        try:
            while True:
                try:
                    response = self._transport.invoke(invocation)
                    break
                except ModelThrottled as error:
                    if throttle_retries >= policy.max_throttle_retries:
                        self._release_failed_reservation(request, reservation)
                        raise
                    throttle_retries += 1
                    with self._lock:
                        self._briefs[request.brief_id].throttle_count += 1
                    self._event(
                        request,
                        "model.throttled",
                        {
                            "retry_number": throttle_retries,
                            "retry_after_seconds": error.retry_after_seconds,
                            "domain_repair_attempt_incremented": False,
                        },
                    )
                    self._on_throttle(error.retry_after_seconds)
        except ModelThrottled:
            self._event(
                request,
                "model.failed",
                {"reason": "throttle retries exhausted"},
            )
            raise
        except Exception as error:
            receipt = UnknownModelOutcome(
                request_id=request.request_id,
                brief_id=request.brief_id,
                worker_id=request.worker_id,
                held_authorized_minor_units=reservation,
                throttle_retry_count=throttle_retries,
                domain_repair_attempt=request.domain_repair_attempt,
            )
            with self._lock:
                self._briefs[request.brief_id].incomplete_count += 1
                self._unknown_outcomes[request.request_id] = receipt
                self._decrement_active(request.worker_id)
            self._event(
                request,
                "model.outcome.unknown",
                {
                    "reason": type(error).__name__,
                    "usage_recorded_as_actual": False,
                },
            )
            raise ModelOutcomeUnknown(receipt) from None

        reconciliation = self._reconcile(policy, response, reservation)
        trace_id = self._trace_id(request_hash)
        actual_cost = (
            reconciliation.app_cost_minor_units if reconciliation.status == "reconciled" else None
        )
        try:
            self._record_trace(
                request,
                response,
                trace_id=trace_id,
                reconciliation=reconciliation,
                actual_cost_minor_units=actual_cost,
            )
        except Exception as error:
            with self._lock:
                self._briefs[request.brief_id].incomplete_count += 1
                self._decrement_active(request.worker_id)
            self._event(
                request,
                "model.trace.failed",
                {
                    "prompt": request.prompt,
                    "output": response.content,
                    "reason": type(error).__name__,
                    "usage_recorded_as_actual": False,
                },
            )
            raise ModelPolicyDenied("trace persistence failed closed") from error
        if reconciliation.status == "mismatch":
            with self._lock:
                state = self._briefs[request.brief_id]
                state.reconciliation_failure_count += 1
                self._decrement_active(request.worker_id)
            self._event(
                request,
                "model.reconciliation.failed",
                {
                    "prompt": request.prompt,
                    "output": response.content,
                    "missing_guardrails": reconciliation.missing_guardrails,
                    "conflicting_guardrails": reconciliation.conflicting_guardrails,
                    "usage_recorded_as_actual": False,
                },
            )
            raise CostReconciliationError("model cost and guardrail reconciliation failed")

        with self._lock:
            state = self._briefs[request.brief_id]
            if reconciliation.status == "reconciled":
                assert actual_cost is not None
                state.committed = state.committed - reservation + actual_cost
                state.actual += actual_cost
            else:
                state.incomplete_count += 1
            self._decrement_active(request.worker_id)
        result = ModelInvocationResult(
            request_id=request.request_id,
            brief_id=request.brief_id,
            worker_id=request.worker_id,
            endpoint_name=policy.endpoint_name,
            model_id=policy.model_id,
            output=response.content,
            actual_cost_minor_units=actual_cost,
            throttle_retries=throttle_retries,
            domain_repair_attempt=request.domain_repair_attempt,
            trace_id=trace_id,
            reconciliation=reconciliation,
        )
        with self._lock:
            self._results[request.request_id] = result
        self._event(
            request,
            "model.completed",
            {
                "prompt": request.prompt,
                "output": response.content,
                "usage_status": reconciliation.status,
                "actual_cost_minor_units": actual_cost,
            },
        )
        return result

    def _budget_summary(self, brief_id: str) -> BriefBudgetSummary:
        with self._lock:
            try:
                state = self._briefs[brief_id]
            except KeyError as error:
                raise ModelPolicyDenied("brief is not registered for model governance") from error
            if state.incomplete_count or state.reconciliation_failure_count:
                usage_status = "incomplete"
            elif state.request_count:
                usage_status = "complete"
            else:
                usage_status = "not_used"
            return BriefBudgetSummary(
                brief_id=brief_id,
                authorized_ceiling_minor_units=state.ceiling,
                budget_committed_minor_units=state.committed,
                metered_actual_minor_units=state.actual,
                remaining_authorization_minor_units=max(state.ceiling - state.committed, 0),
                request_count=state.request_count,
                throttle_count=state.throttle_count,
                incomplete_usage_count=state.incomplete_count,
                reconciliation_failure_count=state.reconciliation_failure_count,
                usage_status=usage_status,
            )

    def read_budget_summary(self, brief_id: str, actor: ActorContext) -> BriefBudgetSummary:
        """Return cost-only governance state after row-level access enforcement."""

        self._require_brief_view(brief_id, actor)
        return self._budget_summary(brief_id)

    def read_trace_summaries(
        self, brief_id: str, actor: ActorContext
    ) -> tuple[ModelTraceSummary, ...]:
        """Return authorized metadata without prompt or output content."""

        with self._lock:
            state = self._require_brief_view_locked(brief_id, actor)
            reader = self._trace_reader
            run_id = state.run_id
        if reader is None:
            raise ModelPolicyDenied("configured trace adapter does not support scoped reads")
        traces = reader.list_run(run_id, actor)
        return tuple(
            ModelTraceSummary.from_trace(trace) for trace in traces if trace.brief_id == brief_id
        )

    def _authorize(
        self,
        request: ModelRequest,
        request_hash: str,
        policy: WorkerModelPolicy,
        reservation: int,
    ) -> ModelInvocationResult | None:
        with self._lock:
            previous_hash = self._request_hashes.get(request.request_id)
            if previous_hash is not None:
                if previous_hash != request_hash:
                    raise IdempotencyConflict("request ID is bound to a different request")
                replay = self._results.get(request.request_id)
                if replay is not None:
                    return replay
                unknown = self._unknown_outcomes.get(request.request_id)
                if unknown is not None:
                    raise ModelOutcomeUnknown(unknown)
                raise ModelPolicyDenied("request has no replayable terminal result")
            try:
                state = self._briefs[request.brief_id]
            except KeyError as error:
                raise ModelPolicyDenied("brief is not registered for model governance") from error
            if self._active.get(request.worker_id, 0) >= policy.max_concurrent_requests:
                raise WorkerLimitExceeded("worker concurrency limit exceeded")
            worker_requests = state.worker_requests.get(request.worker_id, 0)
            if worker_requests >= policy.max_requests_per_brief:
                raise WorkerLimitExceeded("worker request limit exceeded for this brief")
            if state.committed + reservation > state.ceiling:
                self._event_locked(
                    request,
                    "model.budget.denied",
                    {
                        "authorized_request_minor_units": reservation,
                        "remaining_authorization_minor_units": state.ceiling - state.committed,
                    },
                )
                raise BriefBudgetExceeded("brief authorized ceiling would be exceeded")
            state.committed += reservation
            state.request_count += 1
            state.worker_requests[request.worker_id] = worker_requests + 1
            self._active[request.worker_id] = self._active.get(request.worker_id, 0) + 1
            self._request_hashes[request.request_id] = request_hash
            self._event_locked(
                request,
                "model.authorized",
                {
                    "endpoint_name": policy.endpoint_name,
                    "model_id": policy.model_id,
                    "prompt": request.prompt,
                    "authorized_request_minor_units": reservation,
                },
            )
        return None

    @staticmethod
    def _reconcile(
        policy: WorkerModelPolicy,
        response: ProviderResponse,
        reservation: int,
    ) -> CostReconciliation:
        decisions_by_name: dict[str, list[GuardrailDecision]] = {}
        for decision in response.guardrails:
            decisions_by_name.setdefault(decision.name, []).append(decision)
        missing_guardrails = tuple(
            guardrail
            for guardrail in policy.required_guardrails
            if guardrail not in decisions_by_name
        )
        conflicting_guardrails = tuple(
            guardrail
            for guardrail in policy.required_guardrails
            if len(decisions_by_name.get(guardrail, ())) > 1
        )
        guardrails_reconciled = (
            not missing_guardrails
            and not conflicting_guardrails
            and all(
                decisions_by_name[guardrail][0].enforced
                and decisions_by_name[guardrail][0].outcome == "passed"
                for guardrail in policy.required_guardrails
            )
        )
        if response.usage_status == "unavailable":
            status = "incomplete" if guardrails_reconciled else "mismatch"
            return CostReconciliation(
                status=status,
                authorized_cost_minor_units=reservation,
                guardrails_reconciled=guardrails_reconciled,
                missing_guardrails=missing_guardrails,
                conflicting_guardrails=conflicting_guardrails,
            )
        assert response.input_tokens is not None
        assert response.output_tokens is not None
        assert response.cost_minor_units is not None
        within_limits = (
            response.input_tokens <= policy.max_input_tokens
            and response.output_tokens <= policy.max_output_tokens
        )
        app_cost = policy.metered_cost_minor_units(
            response.input_tokens,
            response.output_tokens,
        )
        reconciled = (
            within_limits
            and guardrails_reconciled
            and app_cost == response.cost_minor_units
            and app_cost <= reservation
        )
        return CostReconciliation(
            status="reconciled" if reconciled else "mismatch",
            authorized_cost_minor_units=reservation,
            app_cost_minor_units=app_cost,
            provider_cost_minor_units=response.cost_minor_units,
            guardrails_reconciled=guardrails_reconciled,
            missing_guardrails=missing_guardrails,
            conflicting_guardrails=conflicting_guardrails,
        )

    def _record_trace(
        self,
        request: ModelRequest,
        response: ProviderResponse,
        *,
        trace_id: str,
        reconciliation: CostReconciliation,
        actual_cost_minor_units: int | None,
    ) -> None:
        with self._lock:
            state = self._briefs[request.brief_id]
            scope = (state.run_id, state.owner_subject, state.viewer_subjects)
        redacted = redact_mapping(
            {"prompt": request.prompt, "output": response.content},
            classification=request.classification,
        )
        self._trace_store.append(
            ModelTrace(
                trace_id=trace_id,
                experiment_id=self._trace_store.experiment_id,
                run_id=scope[0],
                brief_id=request.brief_id,
                worker_id=request.worker_id,
                request_id=request.request_id,
                owner_subject=scope[1],
                viewer_subjects=scope[2],
                prompt=redacted["prompt"],
                output=redacted["output"],
                reconciliation_status=reconciliation.status,
                actual_cost_minor_units=actual_cost_minor_units,
            )
        )

    def _release_failed_reservation(self, request: ModelRequest, reservation: int) -> None:
        with self._lock:
            state = self._briefs[request.brief_id]
            state.committed -= reservation
            self._decrement_active(request.worker_id)

    def _decrement_active(self, worker_id: WorkerId) -> None:
        active = self._active.get(worker_id, 0)
        if active <= 1:
            self._active.pop(worker_id, None)
        else:
            self._active[worker_id] = active - 1

    def _require_brief_view(self, brief_id: str, actor: ActorContext) -> None:
        with self._lock:
            self._require_brief_view_locked(brief_id, actor)

    def _require_brief_view_locked(self, brief_id: str, actor: ActorContext) -> _BriefState:
        try:
            state = self._briefs[brief_id]
        except KeyError as error:
            raise ModelPolicyDenied("brief is not registered for model governance") from error
        if "auditor" in actor.roles:
            return state
        if "viewer" in actor.roles and actor.subject in {
            state.owner_subject,
            *state.viewer_subjects,
        }:
            return state
        raise AccessDenied("actor cannot view this brief")

    def _event(
        self,
        request: ModelRequest,
        event_type: str,
        details: dict[str, object],
    ) -> None:
        with self._lock:
            self._event_locked(request, event_type, details)

    def _event_locked(
        self,
        request: ModelRequest,
        event_type: str,
        details: dict[str, object],
    ) -> None:
        redacted = redact_mapping(details, classification=request.classification)
        self._events.append(
            GatewayEvidence(
                sequence=len(self._events) + 1,
                event_type=event_type,
                request_id=request.request_id,
                brief_id=request.brief_id,
                worker_id=request.worker_id,
                details=redacted,
            )
        )

    @staticmethod
    def _request_hash(request: ModelRequest) -> str:
        payload = json.dumps(
            request.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(payload.encode()).hexdigest()

    @staticmethod
    def _trace_id(request_hash: str) -> str:
        return f"trace-{hashlib.sha256(f'model-trace:{request_hash}'.encode()).hexdigest()[:24]}"
