"""Typed recovery records persisted inside a workflow ledger row."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class WorkerLease(BaseModel):
    """One exclusive, expiring right to act for a worker."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    worker_id: str = Field(min_length=1)
    owner: str = Field(min_length=1)
    heartbeat_at: datetime
    expires_at: datetime
    epoch: int = Field(gt=0)


class CheckpointRecord(BaseModel):
    """Durable work position used to continue without repeating mutations."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    checkpoint_id: str = Field(min_length=1)
    worker_id: str = Field(min_length=1)
    lease_epoch: int = Field(gt=0)
    created_at: datetime
    deadline_at: datetime
    payload: dict[str, Any]
    reason: Literal["worker", "kill"]
    resumed_at: datetime | None = None
    resume_id: str | None = None
    resume_count: int = Field(default=0, ge=0, le=1)


class TransitionResult(BaseModel):
    """Outcome of an idempotent compare-and-set workflow transition."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    transition_id: str
    committed: bool
    step: str


class KillResult(BaseModel):
    """Observed outcome of a checkpoint-and-revoke operation."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    operation_id: str
    status: Literal["checkpointed"]
    checkpoint: CheckpointRecord
    revoked_layers: frozenset[str]


class ResumeResult(BaseModel):
    """A new lease plus the single checkpoint it is authorized to resume."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    recovery_id: str
    lease: WorkerLease
    checkpoint: CheckpointRecord
