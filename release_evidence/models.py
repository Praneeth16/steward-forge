"""Immutable contracts for release intent and externally stored evidence."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from datetime import datetime
from functools import cached_property
from typing import Annotated, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    computed_field,
    field_serializer,
    field_validator,
    model_validator,
)

from evidence import canonical_json_bytes, freeze_json, thaw_json

Sha256Hex = Annotated[str, Field(pattern=r"^[a-f0-9]{64}$")]
ReceiptId = Annotated[str, Field(pattern=r"^[a-f0-9]{24}$")]
GateStatus = Literal["passed", "failed"]
ModelUsageStatus = Literal["not_used", "recorded", "unavailable"]
EvidenceChainReference = Annotated[
    str,
    Field(pattern=r"^[a-f0-9]{64}:[1-9][0-9]*:[a-f0-9]{64}$"),
]


def _receipt_id_for_request_hash(request_hash: str) -> str:
    """Match the receipt-ID derivation already used by the capability broker."""

    return hashlib.sha256(f"receipt:{request_hash}".encode("ascii")).hexdigest()[:24]


class _ImmutableContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class _ReleaseProvenance(_ImmutableContract):
    """Fields fixed before deployment and therefore covered by intent identity."""

    brief_id: str = Field(min_length=1)
    workflow_id: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    task_id: str = Field(min_length=1)
    code_sha256: Sha256Hex
    artifact_hashes: dict[str, Sha256Hex] = Field(min_length=1)
    broker_receipt_id: ReceiptId
    data_receipt_id: ReceiptId
    data_manifest_sha256: Sha256Hex
    data_relations: tuple[str, ...] = Field(min_length=1)
    scope_approval_id: str = Field(min_length=1)
    release_approval_id: str = Field(min_length=1)
    gate_results: dict[str, GateStatus] = Field(min_length=1)
    gate_report_sha256: Sha256Hex
    cost_minor_units: int = Field(
        ge=0,
        description="Maximum authorized total cost at release-intent creation.",
    )
    cost_currency: str = Field(pattern=r"^[A-Z]{3}$")
    model_usage_status: ModelUsageStatus
    model_id: str | None = Field(default=None, min_length=1)
    model_input_tokens: int | None = Field(default=None, ge=0)
    model_output_tokens: int | None = Field(default=None, ge=0)
    deployment_idempotency_key: str = Field(min_length=1)

    @model_validator(mode="before")
    @classmethod
    def verify_serialized_cost_basis(cls, value: object) -> object:
        if not isinstance(value, Mapping) or "cost_basis" not in value:
            return value
        payload = dict(value)
        if payload.pop("cost_basis") != "authorized_ceiling":
            raise ValueError("cost_basis must be authorized_ceiling")
        return payload

    @computed_field(return_type=Literal["authorized_ceiling"])
    @property
    def cost_basis(self) -> Literal["authorized_ceiling"]:
        """Label pre-deployment cost as a maximum authorization, not actual spend."""

        return "authorized_ceiling"

    @field_validator("artifact_hashes")
    @classmethod
    def artifact_paths_are_nonempty(cls, value: dict[str, str]) -> dict[str, str]:
        if any(not path for path in value):
            raise ValueError("artifact hash paths must be non-empty")
        return value

    @field_validator("data_relations")
    @classmethod
    def data_relations_are_nonempty(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not relation for relation in value):
            raise ValueError("data relations must be non-empty")
        if len(value) != len(set(value)):
            raise ValueError("data relations must be unique")
        return value

    @model_validator(mode="after")
    def model_usage_is_explicit_and_coherent(self) -> _ReleaseProvenance:
        usage_fields = (
            self.model_id,
            self.model_input_tokens,
            self.model_output_tokens,
        )
        if self.model_usage_status == "not_used" and any(
            field is not None for field in usage_fields
        ):
            raise ValueError("not_used model status cannot carry model usage")
        if self.model_usage_status == "recorded" and any(field is None for field in usage_fields):
            raise ValueError("recorded model status requires model and token usage")
        if self.model_usage_status == "unavailable" and self.model_id is None:
            raise ValueError("unavailable model usage requires a model ID")
        object.__setattr__(self, "artifact_hashes", freeze_json(self.artifact_hashes))
        object.__setattr__(self, "gate_results", freeze_json(self.gate_results))
        return self

    @field_serializer("artifact_hashes", "gate_results")
    def serialize_provenance_mapping(self, value: object) -> object:
        return thaw_json(value)


class ReleaseIntent(_ReleaseProvenance):
    """Pre-deployment release request with a deterministic external receipt ID."""

    schema_id: Literal["steward-forge.release-intent"] = "steward-forge.release-intent"
    schema_version: Literal[1] = 1

    @model_validator(mode="before")
    @classmethod
    def verify_serialized_identity(cls, value: object) -> object:
        if not isinstance(value, Mapping):
            return value
        supplied_hash = value.get("request_hash")
        supplied_receipt_id = value.get("receipt_id")
        if supplied_hash is None and supplied_receipt_id is None:
            return value
        payload = dict(value)
        payload.pop("request_hash", None)
        payload.pop("receipt_id", None)
        candidate = cls.model_validate(payload)
        if supplied_hash != candidate.request_hash:
            raise ValueError("request_hash does not match intent")
        if supplied_receipt_id != candidate.receipt_id:
            raise ValueError("receipt_id does not match intent")
        return payload

    def _identity_payload(self) -> dict[str, JsonValue]:
        return {
            field_name: thaw_json(getattr(self, field_name))
            for field_name in type(self).model_fields
        }

    @computed_field(return_type=str)
    @cached_property
    def request_hash(self) -> str:
        """Full digest of only the fields known before deployment."""

        return hashlib.sha256(canonical_json_bytes(self._identity_payload())).hexdigest()

    @computed_field(return_type=str)
    @cached_property
    def receipt_id(self) -> str:
        """Broker-compatible 24-hex ID derived from the full request digest."""

        return _receipt_id_for_request_hash(self.request_hash)


class DeploymentObservation(_ImmutableContract):
    """Dynamic output observed after an external deployment call."""

    schema_id: Literal["steward-forge.deployment-observation"] = (
        "steward-forge.deployment-observation"
    )
    schema_version: Literal[1] = 1
    receipt_id: ReceiptId
    request_hash: Sha256Hex
    observed_at: datetime
    lease_owner: str | None = Field(default=None, min_length=1)
    lease_epoch: int | None = Field(default=None, gt=0)
    status: Literal["succeeded", "failed"]
    workspace_ids: dict[str, str] = Field(default_factory=dict)
    rollback_state: dict[str, JsonValue] = Field(default_factory=dict)
    deployment_output: dict[str, JsonValue] = Field(default_factory=dict)

    @field_validator("observed_at")
    @classmethod
    def observed_at_is_timezone_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("observed_at must include a timezone")
        return value

    @model_validator(mode="after")
    def bind_to_release_request(self) -> DeploymentObservation:
        if self.receipt_id != _receipt_id_for_request_hash(self.request_hash):
            raise ValueError("receipt_id does not match request_hash")
        if (self.lease_owner is None) != (self.lease_epoch is None):
            raise ValueError("lease owner and epoch must be supplied together")
        for field_name in ("workspace_ids", "rollback_state", "deployment_output"):
            object.__setattr__(self, field_name, freeze_json(getattr(self, field_name)))
        return self

    @field_serializer("workspace_ids", "rollback_state", "deployment_output")
    def serialize_deployment_mapping(self, value: object) -> object:
        return thaw_json(value)


class GovernedReleaseReceipt(_ReleaseProvenance):
    """Complete release provenance plus independently observed deployment output."""

    schema_id: Literal["steward-forge.governed-release-receipt"] = (
        "steward-forge.governed-release-receipt"
    )
    schema_version: Literal[1] = 1
    receipt_id: ReceiptId
    request_hash: Sha256Hex
    deployment: DeploymentObservation
    evidence_chain_reference: EvidenceChainReference

    @classmethod
    def from_intent(
        cls,
        intent: ReleaseIntent,
        deployment: DeploymentObservation,
        *,
        evidence_chain_reference: EvidenceChainReference,
    ) -> GovernedReleaseReceipt:
        """Build a final receipt without manually copying intent-bound fields."""

        provenance = {
            field_name: getattr(intent, field_name)
            for field_name in _ReleaseProvenance.model_fields
        }
        return cls(
            **provenance,
            receipt_id=intent.receipt_id,
            request_hash=intent.request_hash,
            deployment=deployment,
            evidence_chain_reference=evidence_chain_reference,
        )

    @model_validator(mode="after")
    def bind_all_provenance_to_intent(self) -> GovernedReleaseReceipt:
        intent = ReleaseIntent(
            **{
                field_name: getattr(self, field_name)
                for field_name in _ReleaseProvenance.model_fields
            }
        )
        if self.request_hash != intent.request_hash:
            raise ValueError("request_hash does not match release provenance")
        if self.receipt_id != intent.receipt_id:
            raise ValueError("receipt_id does not match release provenance")
        if (
            self.deployment.receipt_id != self.receipt_id
            or self.deployment.request_hash != self.request_hash
        ):
            raise ValueError("deployment observation does not match release provenance")
        return self


class ReleaseEvidencePointer(_ImmutableContract):
    """Small cross-store pointer keyed by the governed receipt's stable ID."""

    schema_id: Literal["steward-forge.release-evidence-pointer"] = (
        "steward-forge.release-evidence-pointer"
    )
    schema_version: Literal[1] = 1
    receipt_id: ReceiptId
    request_hash: Sha256Hex
    receipt_location: str = Field(min_length=1)
    receipt_sha256: Sha256Hex
    evidence_chain_reference: EvidenceChainReference

    @model_validator(mode="after")
    def bind_to_release_request(self) -> ReleaseEvidencePointer:
        if self.receipt_id != _receipt_id_for_request_hash(self.request_hash):
            raise ValueError("receipt_id does not match request_hash")
        return self
