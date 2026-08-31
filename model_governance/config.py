"""Trusted model-policy construction from Databricks App environment values."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

from model_governance.contracts import (
    WorkerId,
    WorkerModelPolicy,
    WorkerPolicyRegistry,
)

_WORKER_ENV_PREFIXES: tuple[tuple[WorkerId, str], ...] = (
    ("product-manager", "PRODUCT_MANAGER"),
    ("scrum-master", "SCRUM_MASTER"),
    ("data-engineer", "DATA_ENGINEER"),
    ("software-engineer", "SOFTWARE_ENGINEER"),
)


@dataclass(frozen=True, slots=True)
class ModelGovernanceConfig:
    """Validated App settings and the complete trusted worker-policy registry."""

    enabled: bool
    trace_experiment_id: str
    policies: WorkerPolicyRegistry


def load_model_governance_config(
    environ: Mapping[str, str] | None = None,
) -> ModelGovernanceConfig:
    """Build all model policies from one non-request-controlled configuration path."""

    values = os.environ if environ is None else environ
    enabled = _required_bool(values, "STEWARD_FORGE_MODEL_CALLS_ENABLED")
    experiment_id = _required(values, "STEWARD_FORGE_MODEL_TRACE_EXPERIMENT_ID")
    shared = {
        "max_input_tokens": _required_int(
            values, "STEWARD_FORGE_MODEL_MAX_INPUT_TOKENS", minimum=1
        ),
        "max_output_tokens": _required_int(
            values, "STEWARD_FORGE_MODEL_MAX_OUTPUT_TOKENS", minimum=1
        ),
        "max_requests_per_brief": _required_int(
            values, "STEWARD_FORGE_MODEL_MAX_REQUESTS_PER_BRIEF", minimum=1
        ),
        "max_concurrent_requests": _required_int(
            values, "STEWARD_FORGE_MODEL_MAX_CONCURRENT_REQUESTS", minimum=1
        ),
        "max_throttle_retries": _required_int(
            values,
            "STEWARD_FORGE_MODEL_MAX_THROTTLE_RETRIES",
            minimum=0,
            maximum=10,
        ),
        "input_cost_per_million_minor_units": _required_int(
            values,
            "STEWARD_FORGE_MODEL_INPUT_COST_PER_MILLION_MINOR_UNITS",
            minimum=0,
        ),
        "output_cost_per_million_minor_units": _required_int(
            values,
            "STEWARD_FORGE_MODEL_OUTPUT_COST_PER_MILLION_MINOR_UNITS",
            minimum=0,
        ),
        "required_guardrails": tuple(
            item.strip()
            for item in _required(values, "STEWARD_FORGE_MODEL_REQUIRED_GUARDRAILS").split(",")
        ),
    }
    policies = tuple(
        WorkerModelPolicy(
            worker_id=worker_id,
            service_identity=_required(values, f"STEWARD_FORGE_{prefix}_MODEL_SERVICE_IDENTITY"),
            endpoint_name=_required(values, f"STEWARD_FORGE_{prefix}_MODEL_ENDPOINT"),
            model_id=_required(values, f"STEWARD_FORGE_{prefix}_MODEL_ID"),
            **shared,
        )
        for worker_id, prefix in _WORKER_ENV_PREFIXES
    )
    return ModelGovernanceConfig(
        enabled=enabled,
        trace_experiment_id=experiment_id,
        policies=WorkerPolicyRegistry(policies=policies),
    )


def _required(values: Mapping[str, str], name: str) -> str:
    try:
        value = values[name].strip()
    except KeyError as error:
        raise ValueError(f"missing required model setting: {name}") from error
    if not value:
        raise ValueError(f"model setting must not be empty: {name}")
    return value


def _required_bool(values: Mapping[str, str], name: str) -> bool:
    value = _required(values, name).casefold()
    if value == "true":
        return True
    if value == "false":
        return False
    raise ValueError(f"model setting must be true or false: {name}")


def _required_int(
    values: Mapping[str, str],
    name: str,
    *,
    minimum: int,
    maximum: int | None = None,
) -> int:
    raw = _required(values, name)
    try:
        value = int(raw)
    except ValueError as error:
        raise ValueError(f"model setting must be an integer: {name}") from error
    if value < minimum or (maximum is not None and value > maximum):
        boundary = f">= {minimum}" if maximum is None else f"between {minimum} and {maximum}"
        raise ValueError(f"model setting must be {boundary}: {name}")
    return value
