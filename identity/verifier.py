"""Validate the Databricks Apps forwarded user token."""

from __future__ import annotations

import logging
import os
from typing import Protocol

from databricks.sdk import WorkspaceClient

from identity.context import ActorContext, Role

LOGGER = logging.getLogger(__name__)


class IdentityVerificationError(ValueError):
    """The forwarded token could not be validated."""


class IdentityVerifier(Protocol):
    def verify(self, token: str) -> ActorContext: ...


class StaticIdentityVerifier:
    """Explicit verifier used by deterministic tests and local development."""

    def __init__(self, actors_by_token: dict[str, ActorContext]) -> None:
        self._actors_by_token = actors_by_token

    def verify(self, token: str) -> ActorContext:
        try:
            return self._actors_by_token[token]
        except KeyError as error:
            raise IdentityVerificationError("user token is invalid") from error


class DatabricksIdentityVerifier:
    """Introspect a forwarded token through the target workspace SCIM API."""

    def __init__(self, role_groups: dict[Role, str] | None = None) -> None:
        self._role_groups = role_groups or {
            "submitter": os.getenv("STEWARD_FORGE_SUBMITTER_GROUP", "users"),
            "viewer": os.getenv("STEWARD_FORGE_VIEWER_GROUP", "users"),
            "approver": os.getenv("STEWARD_FORGE_APPROVER_GROUP", "users"),
            "operator": os.getenv("STEWARD_FORGE_OPERATOR_GROUP", "account admins"),
            "auditor": os.getenv("STEWARD_FORGE_AUDITOR_GROUP", "account admins"),
        }

    def verify(self, token: str) -> ActorContext:
        if not token:
            raise IdentityVerificationError("forwarded user token is required")
        token = token.removeprefix("Bearer ").strip()
        if not token:
            raise IdentityVerificationError("forwarded user token is required")
        host = os.environ.get("DATABRICKS_HOST", "")
        if host and not host.startswith("http"):
            host = f"https://{host}"
        try:
            user = WorkspaceClient(
                host=host or None,
                token=token,
                auth_type="pat",
            ).current_user.me()
        except Exception as error:
            LOGGER.warning(
                "Forwarded user token validation failed with %s",
                type(error).__name__,
            )
            raise IdentityVerificationError("user token is invalid") from error
        if not user.id:
            raise IdentityVerificationError("validated user has no stable subject")
        group_names = {
            str(group.display).casefold() for group in user.groups or [] if group.display
        }
        roles = {
            role
            for role, group_name in self._role_groups.items()
            if group_name.casefold() in group_names
        }
        if not roles:
            raise IdentityVerificationError("validated user has no Steward Forge role")
        return ActorContext(subject=str(user.id), roles=roles)
