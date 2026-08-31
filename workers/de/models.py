"""Versioned Data Engineer task, artifact, lineage, and receipt models."""

from __future__ import annotations

import hashlib
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class DataEngineerTask(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_id: Literal["steward-forge.data-engineer-task"] = (
        "steward-forge.data-engineer-task"
    )
    schema_version: Literal[1] = 1
    task_id: str = Field(min_length=1)
    brief_id: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    seed: int
    sandbox_catalog: str = Field(min_length=1)
    sandbox_schema: str = Field(min_length=1)
    max_repair_attempts: int = Field(default=1, ge=0, le=1)


class GeneratedArtifact(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    path: str = Field(pattern=r"^generated/data-engineer/[a-z0-9_/.-]+$")
    content: str
    sha: str = Field(pattern=r"^[a-f0-9]{64}$")

    @model_validator(mode="after")
    def verify_sha(self) -> GeneratedArtifact:
        if hashlib.sha256(self.content.encode()).hexdigest() != self.sha:
            raise ValueError("generated artifact SHA does not match its content")
        return self


class CatalogTableOutput(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    dataset: Literal["backlog", "pipeline_runs", "platform_costs"]
    relation: str = Field(min_length=1)
    row_count: int = Field(gt=0)
    data_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")


class LineageRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_id: Literal["steward-forge.data-lineage"] = "steward-forge.data-lineage"
    schema_version: Literal[1] = 1
    namespace: str = Field(pattern=r"^steward_forge_[a-z0-9_]+_[a-z0-9_]+$")
    sources: tuple[str, ...] = Field(min_length=1)
    targets: tuple[str, ...] = Field(min_length=1)
    transformation: Literal["generate-repair-validate-publish"] = (
        "generate-repair-validate-publish"
    )


class ProgressEvent(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    sequence: int = Field(gt=0)
    stage: Literal[
        "task.accepted",
        "data.generated",
        "quality.failed",
        "repair.applied",
        "quality.passed",
        "catalog.written",
        "candidate.built",
        "gate.passed",
        "receipt.emitted",
    ]
    detail: str = Field(min_length=1)


class DataEngineerReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_id: Literal["steward-forge.data-engineer-receipt"] = (
        "steward-forge.data-engineer-receipt"
    )
    schema_version: Literal[1] = 1
    receipt_id: str = Field(pattern=r"^[a-f0-9]{24}$")
    task_id: str
    manifest_sha: str = Field(pattern=r"^[a-f0-9]{64}$")
    catalog_relations: tuple[str, ...]
    mutation_receipt_ids: tuple[str, ...]
    repair_attempts: int = Field(ge=0, le=1)
    gate_results: dict[str, Literal["passed", "failed"]]
