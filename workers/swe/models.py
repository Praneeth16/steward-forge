"""Versioned contracts for Software Engineer candidate and release evidence."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from broker.contracts import DraftArtifact, MutationReceipt


class SoftwareEngineerTask(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_id: Literal["steward-forge.software-engineer-task"] = (
        "steward-forge.software-engineer-task"
    )
    schema_version: Literal[1] = 1
    task_id: str = Field(min_length=1)
    brief_id: str = Field(min_length=1)
    submitted_by: str = Field(min_length=1)
    release_approver: str = Field(min_length=1)
    sandbox_catalog: str = Field(min_length=1)
    sandbox_schema: str = Field(min_length=1)
    generated_prefix: str = Field(pattern=r"^generated(?:/[a-z][a-z0-9_-]*)+$")
    artifact_branch: str = Field(pattern=r"^[a-z0-9][a-z0-9_/-]*$")
    trusted_base_sha: str = Field(pattern=r"^[a-f0-9]{64}$")
    dashboard_title: str = Field(min_length=1, max_length=120)
    source_tables: tuple[str, ...] = Field(min_length=1)
    request_genie: bool = False
    genie_creation_verified: bool = False
    genie_verification_id: str | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def genie_requires_verification_evidence(self) -> SoftwareEngineerTask:
        if self.genie_creation_verified != bool(self.genie_verification_id):
            raise ValueError("Genie verification status requires one evidence ID")
        if self.genie_creation_verified and not self.request_genie:
            raise ValueError("Genie cannot be verified when it was not requested")
        source_prefix = f"{self.sandbox_catalog}.{self.sandbox_schema}."
        if any(not table.startswith(source_prefix) for table in self.source_tables):
            raise ValueError("dashboard sources must stay inside the configured sandbox")
        return self


class SoftwareCandidate(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_id: Literal["steward-forge.software-candidate"] = (
        "steward-forge.software-candidate"
    )
    schema_version: Literal[1] = 1
    task_id: str
    candidate_sha: str = Field(pattern=r"^[a-f0-9]{64}$")
    artifacts: tuple[DraftArtifact, ...] = Field(min_length=1)
    genie_included: bool

    @property
    def paths(self) -> tuple[str, ...]:
        return tuple(artifact.path for artifact in self.artifacts)

    @model_validator(mode="after")
    def paths_are_unique(self) -> SoftwareCandidate:
        if len(self.paths) != len(set(self.paths)):
            raise ValueError("software candidate paths must be unique")
        return self


class ArtifactCommit(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    commit_sha: str = Field(pattern=r"^[a-f0-9]{64}$")
    parent_sha: str = Field(pattern=r"^[a-f0-9]{64}$")
    branch: str
    paths: tuple[str, ...]
    artifact_hashes: dict[str, str]


class GateCheck(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    name: Literal["unit", "integration", "quality", "policy", "secret", "harmful_diff"]
    status: Literal["passed", "failed"]
    detail: str


class SoftwareGateReport(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    checks: tuple[GateCheck, ...]
    passed: bool

    @property
    def results(self) -> dict[str, str]:
        return {check.name: check.status for check in self.checks}


class PreparedSoftwareRelease(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    task: SoftwareEngineerTask
    candidate: SoftwareCandidate
    commit: ArtifactCommit
    broker_receipt: MutationReceipt
    gates: SoftwareGateReport


class SoftwareReleaseApproval(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    decision_id: str = Field(min_length=1)
    decision: Literal["approved", "rejected"]
    approved_sha: str = Field(pattern=r"^[a-f0-9]{64}$")


class DeploymentResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    commit_sha: str = Field(pattern=r"^[a-f0-9]{64}$")
    workspace_ids: dict[str, str]
    rollback_state: dict[str, object]


class SoftwareReleaseReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_id: Literal["steward-forge.software-release-receipt"] = (
        "steward-forge.software-release-receipt"
    )
    schema_version: Literal[1] = 1
    receipt_id: str = Field(pattern=r"^[a-f0-9]{24}$")
    task_id: str
    commit_sha: str = Field(pattern=r"^[a-f0-9]{64}$")
    approval_id: str
    deployment_idempotency_key: str
    broker_receipt_id: str
    gate_results: dict[str, str]
    workspace_ids: dict[str, str]
    rollback_state: dict[str, object]
