"""Access-controlled redacted trace contracts for an injectable MLflow adapter."""

from __future__ import annotations

from threading import RLock
from typing import Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field

from identity import AccessDenied, ActorContext
from model_governance.contracts import WorkerId


class ModelTrace(BaseModel):
    """Redacted trace payload suitable for persistence in a scoped experiment."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_id: Literal["steward-forge.model-trace"] = "steward-forge.model-trace"
    schema_version: Literal[1] = 1
    trace_id: str = Field(min_length=1)
    experiment_id: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    brief_id: str = Field(min_length=1)
    worker_id: WorkerId
    request_id: str = Field(min_length=1)
    owner_subject: str = Field(min_length=1)
    viewer_subjects: tuple[str, ...] = ()
    prompt: str
    output: str
    reconciliation_status: Literal["reconciled", "incomplete", "mismatch"]
    actual_cost_minor_units: int | None = Field(default=None, ge=0)


class ModelTraceSummary(BaseModel):
    """Payload-free trace metadata safe for an authorized workbench response."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_id: Literal["steward-forge.model-trace-summary"] = "steward-forge.model-trace-summary"
    schema_version: Literal[1] = 1
    trace_id: str = Field(min_length=1)
    experiment_id: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    brief_id: str = Field(min_length=1)
    worker_id: WorkerId
    request_id: str = Field(min_length=1)
    reconciliation_status: Literal["reconciled", "incomplete", "mismatch"]
    actual_cost_minor_units: int | None = Field(default=None, ge=0)

    @classmethod
    def from_trace(cls, trace: ModelTrace) -> ModelTraceSummary:
        return cls(
            trace_id=trace.trace_id,
            experiment_id=trace.experiment_id,
            run_id=trace.run_id,
            brief_id=trace.brief_id,
            worker_id=trace.worker_id,
            request_id=trace.request_id,
            reconciliation_status=trace.reconciliation_status,
            actual_cost_minor_units=trace.actual_cost_minor_units,
        )


class TraceStore(Protocol):
    """Write-only gateway boundary implemented by an MLflow trace adapter."""

    experiment_id: str

    def append(self, trace: ModelTrace) -> None: ...


class TraceReader(Protocol):
    """Read boundary implemented only by trace stores with scoped access checks."""

    def list_run(self, run_id: str, actor: ActorContext) -> tuple[ModelTrace, ...]: ...


class InMemoryScopedTraceStore:
    """Reference store enforcing the same read scope expected from MLflow ACLs."""

    def __init__(self, experiment_id: str) -> None:
        if not experiment_id.strip():
            raise ValueError("experiment_id must not be empty")
        self.experiment_id = experiment_id
        self._records: dict[str, ModelTrace] = {}
        self._lock = RLock()

    def append(self, trace: ModelTrace) -> None:
        if trace.experiment_id != self.experiment_id:
            raise ValueError("trace belongs to a different experiment")
        with self._lock:
            existing = self._records.get(trace.trace_id)
            if existing is not None and existing != trace:
                raise ValueError("trace ID is bound to different content")
            self._records[trace.trace_id] = trace

    def read_trace(self, trace_id: str, actor: ActorContext) -> ModelTrace:
        with self._lock:
            try:
                trace = self._records[trace_id]
            except KeyError as error:
                raise AccessDenied("trace is not available in this scope") from error
        self._require_read(trace, actor)
        return trace

    def list_run(self, run_id: str, actor: ActorContext) -> tuple[ModelTrace, ...]:
        with self._lock:
            records = tuple(
                trace
                for trace in self._records.values()
                if trace.run_id == run_id and self._can_read(trace, actor)
            )
        return tuple(sorted(records, key=lambda trace: trace.trace_id))

    @staticmethod
    def _can_read(trace: ModelTrace, actor: ActorContext) -> bool:
        if "auditor" in actor.roles:
            return True
        return "viewer" in actor.roles and actor.subject in {
            trace.owner_subject,
            *trace.viewer_subjects,
        }

    def _require_read(self, trace: ModelTrace, actor: ActorContext) -> None:
        if not self._can_read(trace, actor):
            raise AccessDenied("actor cannot read this trace")
