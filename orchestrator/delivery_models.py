"""Versioned contracts for the governed four-worker delivery flow."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from orchestrator.models import AcceptanceTest
from release_evidence import GovernedReleaseReceipt, ReleaseEvidencePointer
from workers.de.models import DataEngineerReceipt
from workers.swe.models import SoftwareReleaseReceipt


class ProductScope(BaseModel):
    """Product Manager proposal that requires a separate approval."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_id: Literal["steward-forge.product-scope"] = (
        "steward-forge.product-scope"
    )
    schema_version: Literal[1] = 1
    brief_id: str = Field(min_length=1)
    scope_version: int = Field(default=1, gt=0)
    outcome: str = Field(min_length=1)
    scope: tuple[str, ...] = Field(min_length=1)
    assumptions: tuple[str, ...] = Field(min_length=1)
    acceptance_tests: tuple[AcceptanceTest, ...] = Field(min_length=1)
    proposed_by: Literal["product-manager"] = "product-manager"

    def fingerprint(self) -> str:
        """Return the canonical binding used by an approved Scrum plan."""

        canonical = json.dumps(
            self.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        )
        return hashlib.sha256(canonical.encode()).hexdigest()


class DeliveryTask(BaseModel):
    """One bounded task assigned by the Scrum Master."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_id: Literal["steward-forge.delivery-task"] = (
        "steward-forge.delivery-task"
    )
    schema_version: Literal[1] = 1
    task_id: str = Field(min_length=1)
    worker_id: Literal["data-engineer", "software-engineer"]
    depends_on: tuple[str, ...] = ()
    max_attempts: int = Field(ge=1, le=3)
    budget_usd: float = Field(gt=0)
    attempt_cost_usd: float = Field(gt=0)
    stop_condition: str = Field(min_length=1)
    expected_output: str = Field(min_length=1)

    @model_validator(mode="after")
    def attempt_fits_inside_budget(self) -> DeliveryTask:
        if self.attempt_cost_usd > self.budget_usd:
            raise ValueError("attempt cost cannot exceed task budget")
        return self


class ScrumPlan(BaseModel):
    """Ordered bounded work accepted from the Scrum Master."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_id: Literal["steward-forge.scrum-plan"] = "steward-forge.scrum-plan"
    schema_version: Literal[1] = 1
    plan_id: str = Field(min_length=1)
    brief_id: str = Field(min_length=1)
    scope_version: int = Field(gt=0)
    approved_scope_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    created_by: Literal["scrum-master"] = "scrum-master"
    tasks: tuple[DeliveryTask, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def require_ordered_specialist_tasks(self) -> ScrumPlan:
        if tuple(task.worker_id for task in self.tasks) != (
            "data-engineer",
            "software-engineer",
        ):
            raise ValueError("plan must order Data Engineer before Software Engineer")
        if self.tasks[1].depends_on != (self.tasks[0].task_id,):
            raise ValueError("Software Engineer task must depend on the Data Engineer task")
        return self


class EscalationEvent(BaseModel):
    """Worker-authored signal; deterministic code chooses retry or stop."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_id: Literal["steward-forge.escalation"] = "steward-forge.escalation"
    schema_version: Literal[1] = 1
    task_id: str = Field(min_length=1)
    worker_id: Literal["data-engineer", "software-engineer"]
    attempt: int = Field(gt=0)
    reason: str = Field(min_length=1)
    retry_owner: Literal["orchestrator"] = "orchestrator"
    action: Literal["retry-or-stop"] = "retry-or-stop"


TaskState = Literal[
    "planned",
    "running",
    "awaiting_approval",
    "succeeded",
    "budget_stopped",
    "failed",
]


class TaskExecution(BaseModel):
    """Persisted attempts, budget, and terminal state for one task."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    task_id: str
    worker_id: Literal["data-engineer", "software-engineer"]
    state: TaskState = "planned"
    preparation_attempt_count: int = Field(default=0, ge=0)
    attempt_count: int = Field(default=0, ge=0)
    max_attempts: int = Field(ge=1)
    budget_usd: float = Field(gt=0)
    budget_consumed_usd: float = Field(default=0, ge=0)
    budget_remaining_usd: float = Field(ge=0)
    stop_reason: str | None = None
    failures: tuple[str, ...] = ()


class DeliveryEvidence(BaseModel):
    """Ordered orchestration evidence emitted at deterministic boundaries."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    sequence: int = Field(gt=0)
    event_type: str = Field(min_length=1)
    worker_id: Literal[
        "product-manager",
        "scrum-master",
        "data-engineer",
        "software-engineer",
    ] | None = None
    task_id: str | None = None
    details: dict[str, Any] = Field(default_factory=dict)


class ReferenceRunConfig(BaseModel):
    """Portable inputs for the local four-worker reference flow."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    run_id: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]*$")
    seed: int
    sandbox_catalog: str = Field(min_length=1)
    sandbox_schema: str = Field(min_length=1)
    trusted_base_sha: str = Field(pattern=r"^[a-f0-9]{64}$")
    generated_prefix: str = Field(pattern=r"^generated(?:/[a-z][a-z0-9_-]*)+$")
    artifact_branch: str = Field(pattern=r"^[a-z0-9][a-z0-9_/-]*$")
    dashboard_title: str = Field(min_length=1, max_length=120)


class DeliveryRunResult(BaseModel):
    """Public result of a completed or deterministically stopped run."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_id: Literal["steward-forge.delivery-run-result"]
    schema_version: Literal[2]
    workflow_id: str
    status: Literal[
        "scope_pending",
        "scope_approved",
        "scope_rejected",
        "planned",
        "data_completed",
        "release_pending",
        "release_in_progress",
        "release_rejected",
        "completed",
        "budget_stopped",
        "failed",
    ]
    scope: ProductScope | None = None
    plan: ScrumPlan | None = None
    task_executions: dict[str, TaskExecution]
    data_receipt: DataEngineerReceipt | None = None
    prepared_release_sha: str | None = Field(
        default=None, pattern=r"^[a-f0-9]{64}$"
    )
    software_receipt: SoftwareReleaseReceipt | None = None
    governed_release_receipt: GovernedReleaseReceipt | None = None
    release_evidence_pointer: ReleaseEvidencePointer | None = None
    evidence: tuple[DeliveryEvidence, ...]
    evidence_chain: tuple[dict[str, Any], ...]
    evidence_head: dict[str, Any] | None = None

    @model_validator(mode="before")
    @classmethod
    def require_explicit_version_marker(cls, value: Any) -> Any:
        if isinstance(value, Mapping) and (
            "schema_id" not in value or "schema_version" not in value
        ):
            raise ValueError(
                "legacy delivery run result is unversioned; deserialize it with "
                "the pre-Issue-9 result contract"
            )
        return value
