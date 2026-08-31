"""Versioned contracts used by the first Steward Forge tracer."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class AcceptanceTest(BaseModel):
    """A deterministic check that a candidate output must satisfy."""

    model_config = ConfigDict(extra="forbid")

    schema_id: Literal["steward-forge.acceptance-test"] = (
        "steward-forge.acceptance-test"
    )
    schema_version: Literal[1] = 1
    name: str = Field(min_length=1)
    description: str = Field(min_length=1)
    kind: Literal["schema", "contract", "unit", "quality"]


class BriefSubmission(BaseModel):
    """User-authored intent entering the governed workflow."""

    model_config = ConfigDict(extra="forbid")

    title: str = Field(min_length=1)
    business_question: str = Field(min_length=1)
    acceptance_tests: list[AcceptanceTest] = Field(min_length=1)
    cost_ceiling_usd: float = Field(gt=0)
    submitted_by: str = Field(min_length=1)
    release_approver: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)


class ScopeDecision(BaseModel):
    """An approval bound to one exact brief scope version."""

    model_config = ConfigDict(extra="forbid")

    decision_id: str = Field(min_length=1)
    decision: Literal["approved", "rejected"]
    scope_version: int = Field(gt=0)
    actor: str = Field(min_length=1)


class ReleaseDecision(BaseModel):
    """An approval bound to one exact candidate commit."""

    model_config = ConfigDict(extra="forbid")

    decision_id: str = Field(min_length=1)
    decision: Literal["approved", "rejected"]
    commit_sha: str = Field(min_length=1)
    actor: str = Field(min_length=1)


class PlannedTask(BaseModel):
    """A bounded unit of work produced by the Scrum Master."""

    id: str
    worker_id: str
    owner: str
    budget_usd: float
    stop_condition: str
    expected_output: str


class CandidateArtifact(BaseModel):
    """A deterministic artifact returned by the tracer specialist."""

    path: str
    content: str
    sha: str


class ReleaseReceipt(BaseModel):
    """Evidence returned after a candidate passes release."""

    id: str
    brief_id: str
    commit_sha: str
    test_results: dict[str, Literal["passed", "failed"]]
    artifact_path: str
