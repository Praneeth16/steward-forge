"""Deterministic redaction used before evidence or trace persistence."""

from __future__ import annotations

from typing import Any

from broker.security import contains_secret
from model_governance.contracts import DataClassification

SENSITIVE_KEY_PARTS = (
    "api_key",
    "credential",
    "password",
    "private_key",
    "secret",
    "token",
)


def redact_text(
    value: str,
    *,
    classification: DataClassification,
    classified_payload: bool,
) -> str:
    if classified_payload and classification != "public":
        return f"[REDACTED:{classification.upper()}]"
    if contains_secret(value):
        return "[REDACTED:SECRET]"
    return value


def redact_mapping(
    value: dict[str, Any],
    *,
    classification: DataClassification,
    classified_fields: frozenset[str] = frozenset({"prompt", "output"}),
) -> dict[str, Any]:
    return {
        key: _redact_value(
            item,
            key=key,
            classification=classification,
            classified_fields=classified_fields,
        )
        for key, item in value.items()
    }


def _redact_value(
    value: Any,
    *,
    key: str,
    classification: DataClassification,
    classified_fields: frozenset[str],
) -> Any:
    normalized = key.casefold()
    if any(part in normalized for part in SENSITIVE_KEY_PARTS):
        return "[REDACTED:SECRET]"
    if isinstance(value, str):
        return redact_text(
            value,
            classification=classification,
            classified_payload=key in classified_fields,
        )
    if isinstance(value, dict):
        return redact_mapping(
            value,
            classification=classification,
            classified_fields=classified_fields,
        )
    if isinstance(value, list | tuple):
        return [
            _redact_value(
                item,
                key=key,
                classification=classification,
                classified_fields=classified_fields,
            )
            for item in value
        ]
    return value
