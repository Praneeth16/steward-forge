"""Role, row, and separation-of-duties enforcement."""

from __future__ import annotations

from typing import Any

from identity.context import ActorContext, Role


class AccessDenied(PermissionError):
    """The validated actor is not authorized for this row or action."""


class AuthorizationPolicy:
    """Evaluate endpoint role and brief-row access from validated claims."""

    @staticmethod
    def require_role(actor: ActorContext, role: Role) -> None:
        if role not in actor.roles:
            raise AccessDenied(f"{role} role is required")

    def require_submit(self, actor: ActorContext) -> None:
        self.require_role(actor, "submitter")

    def require_view(self, actor: ActorContext, state: dict[str, Any]) -> None:
        if "operator" in actor.roles or "auditor" in actor.roles:
            return
        allowed_subjects = {
            str(state["submitted_by"]),
            str(state["brief"]["release_approver"]),
            *map(str, state["brief"].get("viewer_subjects", [])),
        }
        if "viewer" not in actor.roles or actor.subject not in allowed_subjects:
            raise AccessDenied("actor cannot view this brief")

    def require_approval(self, actor: ActorContext, state: dict[str, Any]) -> None:
        self.require_role(actor, "approver")
        if actor.subject != state["brief"]["release_approver"]:
            raise AccessDenied("only the named approver may decide this brief")

    def require_release(self, actor: ActorContext, state: dict[str, Any]) -> None:
        self.require_approval(actor, state)
        if actor.subject == state["submitted_by"]:
            raise AccessDenied("the submitter cannot approve release")
