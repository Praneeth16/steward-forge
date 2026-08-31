"""Immutable contracts for governed model requests and metering."""

from __future__ import annotations

import re
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

WorkerId = Literal[
    "product-manager",
    "scrum-master",
    "data-engineer",
    "software-engineer",
]
DataClassification = Literal["public", "internal", "confidential", "restricted"]

WORKER_ORDER: tuple[WorkerId, ...] = (
    "product-manager",
    "scrum-master",
    "data-engineer",
    "software-engineer",
)


class _ImmutableContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class WorkerModelPolicy(_ImmutableContract):
    """Trusted route, price, and limit policy for one logical worker identity."""

    schema_id: Literal["steward-forge.worker-model-policy"] = "steward-forge.worker-model-policy"
    schema_version: Literal[1] = 1
    worker_id: WorkerId
    service_identity: str = Field(min_length=1)
    endpoint_name: str = Field(min_length=1)
    model_id: str = Field(min_length=1)
    max_input_tokens: int = Field(gt=0)
    max_output_tokens: int = Field(gt=0)
    max_requests_per_brief: int = Field(gt=0)
    max_concurrent_requests: int = Field(gt=0)
    max_throttle_retries: int = Field(ge=0, le=10)
    input_cost_per_million_minor_units: int = Field(ge=0)
    output_cost_per_million_minor_units: int = Field(ge=0)
    required_guardrails: tuple[str, ...] = Field(min_length=1)

    @field_validator("endpoint_name")
    @classmethod
    def reject_passthrough(cls, value: str) -> str:
        normalized = re.sub(r"[^a-z0-9]", "", value.casefold())
        if "passthrough" in normalized:
            raise ValueError("passthrough endpoints are forbidden")
        return value

    @field_validator("required_guardrails")
    @classmethod
    def guardrails_are_unique(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not item.strip() for item in value) or len(value) != len(set(value)):
            raise ValueError("required guardrails must be non-empty and unique")
        return value

    @property
    def maximum_authorized_minor_units(self) -> int:
        """Return the exact worst-case request cost in integer currency units."""

        return _ceil_per_million(
            self.max_input_tokens,
            self.input_cost_per_million_minor_units,
        ) + _ceil_per_million(
            self.max_output_tokens,
            self.output_cost_per_million_minor_units,
        )

    def metered_cost_minor_units(self, input_tokens: int, output_tokens: int) -> int:
        return _ceil_per_million(
            input_tokens,
            self.input_cost_per_million_minor_units,
        ) + _ceil_per_million(
            output_tokens,
            self.output_cost_per_million_minor_units,
        )


class WorkerPolicyRegistry(_ImmutableContract):
    """Complete trusted policy registry for all four workers."""

    policies: tuple[WorkerModelPolicy, ...]

    @model_validator(mode="after")
    def require_complete_distinct_registry(self) -> WorkerPolicyRegistry:
        worker_ids = tuple(policy.worker_id for policy in self.policies)
        if set(worker_ids) != set(WORKER_ORDER) or len(worker_ids) != len(WORKER_ORDER):
            raise ValueError("model policy registry must define each worker exactly once")
        identities = [policy.service_identity for policy in self.policies]
        if len(identities) != len(set(identities)):
            raise ValueError("worker service identities must be distinct")
        ordered = tuple(self.for_worker(worker_id) for worker_id in WORKER_ORDER)
        object.__setattr__(self, "policies", ordered)
        return self

    def for_worker(self, worker_id: WorkerId) -> WorkerModelPolicy:
        for policy in self.policies:
            if policy.worker_id == worker_id:
                return policy
        raise KeyError(worker_id)


class ModelRequest(_ImmutableContract):
    """Worker intent. Routing and identity are deliberately absent and not overridable."""

    schema_id: Literal["steward-forge.model-request"] = "steward-forge.model-request"
    schema_version: Literal[1] = 1
    request_id: str = Field(min_length=1)
    brief_id: str = Field(min_length=1)
    worker_id: WorkerId
    prompt: str = Field(min_length=1)
    classification: DataClassification = "public"
    domain_repair_attempt: int = Field(default=0, ge=0)


class ProviderInvocation(_ImmutableContract):
    """Trusted adapter input assembled from a worker request and registry policy."""

    request_id: str
    brief_id: str
    worker_id: WorkerId
    service_identity: str
    endpoint_name: str
    model_id: str
    prompt: str
    max_output_tokens: int = Field(gt=0)


class GuardrailDecision(_ImmutableContract):
    name: str = Field(min_length=1)
    outcome: Literal["passed", "blocked"]
    enforced: bool = True


class ProviderResponse(_ImmutableContract):
    """Provider output with explicit recorded or unavailable usage telemetry."""

    content: str
    usage_status: Literal["recorded", "unavailable"]
    input_tokens: int | None = Field(default=None, ge=0)
    output_tokens: int | None = Field(default=None, ge=0)
    cost_minor_units: int | None = Field(default=None, ge=0)
    guardrails: tuple[GuardrailDecision, ...]

    @model_validator(mode="after")
    def usage_fields_match_status(self) -> ProviderResponse:
        usage = (self.input_tokens, self.output_tokens, self.cost_minor_units)
        if self.usage_status == "recorded" and any(value is None for value in usage):
            raise ValueError("recorded usage requires token counts and provider cost")
        if self.usage_status == "unavailable" and any(value is not None for value in usage):
            raise ValueError("unavailable usage cannot carry estimated cost or token counts")
        return self


class CostReconciliation(_ImmutableContract):
    status: Literal["reconciled", "incomplete", "mismatch"]
    authorized_cost_minor_units: int = Field(ge=0)
    app_cost_minor_units: int | None = Field(default=None, ge=0)
    provider_cost_minor_units: int | None = Field(default=None, ge=0)
    guardrails_reconciled: bool
    missing_guardrails: tuple[str, ...] = ()
    conflicting_guardrails: tuple[str, ...] = ()


class ModelInvocationResult(_ImmutableContract):
    schema_id: Literal["steward-forge.model-invocation-result"] = (
        "steward-forge.model-invocation-result"
    )
    schema_version: Literal[1] = 1
    request_id: str
    brief_id: str
    worker_id: WorkerId
    endpoint_name: str
    model_id: str
    output: str
    actual_cost_minor_units: int | None = Field(default=None, ge=0)
    throttle_retries: int = Field(ge=0)
    domain_repair_attempt: int = Field(ge=0)
    trace_id: str = Field(min_length=1)
    reconciliation: CostReconciliation


class UnknownModelOutcome(_ImmutableContract):
    """Replayable receipt for a possibly billable call with no trusted response."""

    schema_id: Literal["steward-forge.unknown-model-outcome"] = (
        "steward-forge.unknown-model-outcome"
    )
    schema_version: Literal[1] = 1
    request_id: str = Field(min_length=1)
    brief_id: str = Field(min_length=1)
    worker_id: WorkerId
    held_authorized_minor_units: int = Field(ge=0)
    throttle_retry_count: int = Field(ge=0)
    domain_repair_attempt: int = Field(ge=0)
    usage_status: Literal["incomplete"] = "incomplete"


class GatewayEvidence(_ImmutableContract):
    sequence: int = Field(gt=0)
    event_type: str = Field(min_length=1)
    trusted_source: Literal["model-gateway"] = "model-gateway"
    request_id: str = Field(min_length=1)
    brief_id: str = Field(min_length=1)
    worker_id: WorkerId
    details: dict[str, Any]


class BriefBudgetSummary(_ImmutableContract):
    schema_id: Literal["steward-forge.model-budget-summary"] = "steward-forge.model-budget-summary"
    schema_version: Literal[1] = 1
    brief_id: str
    currency: Literal["USD"] = "USD"
    authorized_ceiling_minor_units: int = Field(ge=0)
    budget_committed_minor_units: int = Field(ge=0)
    metered_actual_minor_units: int = Field(ge=0)
    remaining_authorization_minor_units: int = Field(ge=0)
    request_count: int = Field(ge=0)
    throttle_count: int = Field(ge=0)
    incomplete_usage_count: int = Field(ge=0)
    reconciliation_failure_count: int = Field(ge=0)
    usage_status: Literal["not_used", "complete", "incomplete"]


def _ceil_per_million(tokens: int, rate: int) -> int:
    numerator = tokens * rate
    return (numerator + 999_999) // 1_000_000
