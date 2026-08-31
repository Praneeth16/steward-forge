"""Versioned contracts crossing the worker-to-broker boundary."""

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from orchestrator.models import CandidateArtifact, PlannedTask


class WorkerContract(BaseModel):
    """The exact tool and data boundary assigned to one worker."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_id: Literal["steward-forge.worker-contract"] = (
        "steward-forge.worker-contract"
    )
    schema_version: Literal[1] = 1
    contract_id: str = Field(min_length=1)
    contract_version: int = Field(gt=0)
    worker_id: str = Field(min_length=1)
    allowed_tools: frozenset[str] = Field(min_length=1)
    sandbox_catalog: str | None = None
    sandbox_schema: str | None = None
    allowed_artifact_prefixes: frozenset[str] = Field(default_factory=frozenset)


class MutationRequest(BaseModel):
    """One replay-safe request from a worker to a trusted tool."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_id: Literal["steward-forge.mutation-request"] = (
        "steward-forge.mutation-request"
    )
    schema_version: Literal[1] = 1
    contract_id: str = Field(min_length=1)
    contract_version: int = Field(gt=0)
    worker_id: str = Field(min_length=1)
    tool_id: str = Field(min_length=1)
    arguments: dict[str, Any]
    idempotency_key: str = Field(min_length=1)


class SandboxWriteArgs(BaseModel):
    """Typed arguments for one sandbox-only write."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    catalog: str = Field(min_length=1)
    schema_name: str = Field(alias="schema", min_length=1)
    table: str = Field(pattern=r"^[a-z][a-z0-9_]{0,254}$")
    rows: list[dict[str, Any]]


class EvidenceAppendArgs(BaseModel):
    """Typed evidence input that remains writable during health failures."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    event_id: str = Field(min_length=1)
    payload: dict[str, Any]


class TaskRecordArgs(BaseModel):
    """Typed Scrum Master task accepted into workflow state."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_id: Literal["steward-forge.task-record-args"] = (
        "steward-forge.task-record-args"
    )
    schema_version: Literal[1] = 1
    brief_id: str = Field(min_length=1)
    task: PlannedTask


class ArtifactWriteArgs(BaseModel):
    """Typed candidate artifact accepted into workflow state."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_id: Literal["steward-forge.artifact-write-args"] = (
        "steward-forge.artifact-write-args"
    )
    schema_version: Literal[1] = 1
    brief_id: str = Field(min_length=1)
    artifact: CandidateArtifact


class MutationReceipt(BaseModel):
    """Stable result returned for the first request and every replay."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_id: Literal["steward-forge.mutation-receipt"] = (
        "steward-forge.mutation-receipt"
    )
    schema_version: Literal[1] = 1
    receipt_id: str
    request_hash: str
    worker_id: str
    tool_id: str
    result: dict[str, Any]
