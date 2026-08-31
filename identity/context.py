"""Token-derived identity presented to deterministic policy code."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

Role = Literal["submitter", "viewer", "approver", "operator", "auditor"]


class ActorContext(BaseModel):
    """Validated subject and roles; never constructed from request body fields."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    subject: str = Field(min_length=1)
    roles: frozenset[Role] = Field(min_length=1)
