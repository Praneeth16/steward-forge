"""Versioned contracts crossing the worker-to-broker boundary."""

import hashlib
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

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
    artifact_branch: str | None = None


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
    workflow_id: str | None = Field(default=None, min_length=1)
    lease_owner: str | None = Field(default=None, min_length=1)
    lease_epoch: int | None = Field(default=None, gt=0)
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


class SyntheticTableWriteArgs(BaseModel):
    """Typed, non-executable rows bound to one generated sandbox table."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_id: Literal["steward-forge.synthetic-table-write-args"] = (
        "steward-forge.synthetic-table-write-args"
    )
    schema_version: Literal[1] = 1
    catalog: str = Field(min_length=1)
    schema_name: str = Field(alias="schema", min_length=1)
    namespace: str = Field(pattern=r"^steward_forge_[a-z0-9_]+_[a-z0-9_]+$")
    dataset: Literal["backlog", "pipeline_runs", "platform_costs"]
    rows: list[dict[str, Any]] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_row_scope(self) -> "SyntheticTableWriteArgs":
        if any(
            row.get("namespace") != self.namespace or row.get("synthetic") is not True
            for row in self.rows
        ):
            raise ValueError("every row must be synthetic and match the requested namespace")
        return self


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


class DraftArtifact(BaseModel):
    """One immutable file proposed for a broker-owned candidate commit."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    path: str = Field(min_length=1)
    content: str
    sha: str = Field(pattern=r"^[a-f0-9]{64}$")

    @model_validator(mode="after")
    def verify_sha(self) -> "DraftArtifact":
        if hashlib.sha256(self.content.encode()).hexdigest() != self.sha:
            raise ValueError("draft artifact SHA does not match its content")
        return self


class ArtifactCommitArgs(BaseModel):
    """Typed request for one broker-owned commit on the candidate branch."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_id: Literal["steward-forge.artifact-commit-args"] = (
        "steward-forge.artifact-commit-args"
    )
    schema_version: Literal[1] = 1
    branch: str = Field(min_length=1)
    parent_sha: str = Field(pattern=r"^[a-f0-9]{64}$")
    message: str = Field(min_length=1, max_length=200)
    artifacts: tuple[DraftArtifact, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def paths_are_unique(self) -> "ArtifactCommitArgs":
        paths = [artifact.path for artifact in self.artifacts]
        if len(paths) != len(set(paths)):
            raise ValueError("candidate artifact paths must be unique")
        return self


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
    workflow_id: str | None = None
    lease_owner: str | None = None
    lease_epoch: int | None = Field(default=None, gt=0)
    tool_id: str
    result: dict[str, Any]
