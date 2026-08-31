"""HTTP interface for the first governed Steward Forge tracer."""

import os
from collections.abc import Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Any

from fastapi import Depends, FastAPI, HTTPException, Request, Response, status
from fastapi.responses import HTMLResponse

from identity import AccessDenied, ActorContext, DatabricksIdentityVerifier, IdentityVerifier
from identity.verifier import IdentityVerificationError, StaticIdentityVerifier
from ledger import InMemoryLedger, Ledger, LedgerConflict, LedgerNotFound
from ledger.postgres import PostgresLedger
from model_governance import (
    BriefBudgetSummary,
    GovernedModelGateway,
    ModelGovernanceError,
    load_model_governance_config,
    usd_to_minor_units,
)
from orchestrator.models import BriefSubmission, ReleaseDecision, ScopeDecision
from orchestrator.service import Orchestrator, WorkflowError

WORKBENCH_HTML = Path(__file__).with_name("index.html")


def get_orchestrator(request: Request) -> Orchestrator:
    return request.app.state.orchestrator


def get_actor(request: Request) -> ActorContext:
    token = request.headers.get("X-Forwarded-Access-Token", "")
    try:
        return request.app.state.identity_verifier.verify(token)
    except IdentityVerificationError as error:
        raise HTTPException(status_code=401, detail=str(error)) from error


def create_app(
    ledger: Ledger | None = None,
    identity_verifier: IdentityVerifier | None = None,
    model_gateway: GovernedModelGateway | None = None,
) -> FastAPI:
    selected_ledger = ledger or _default_ledger()
    selected_verifier = identity_verifier or _default_identity_verifier()
    _validate_model_runtime(model_gateway)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        if isinstance(selected_ledger, PostgresLedger):
            selected_ledger.open()
            selected_ledger.migrate()
        try:
            yield
        finally:
            if isinstance(selected_ledger, PostgresLedger):
                selected_ledger.close()

    app = FastAPI(title="Steward Forge", version="0.1.0", lifespan=lifespan)
    app.state.orchestrator = Orchestrator(selected_ledger)
    app.state.identity_verifier = selected_verifier
    app.state.model_gateway = model_gateway

    @app.get("/", response_class=HTMLResponse)
    def workbench() -> str:
        return WORKBENCH_HTML.read_text()

    @app.get("/api/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/me")
    def current_actor(
        actor: Annotated[ActorContext, Depends(get_actor)],
    ) -> dict[str, object]:
        return actor.model_dump(mode="json")

    @app.post("/api/briefs", status_code=status.HTTP_201_CREATED)
    def submit_brief(
        submission: BriefSubmission,
        response: Response,
        actor: Annotated[ActorContext, Depends(get_actor)],
        orchestrator: Annotated[Orchestrator, Depends(get_orchestrator)],
    ) -> dict[str, object]:
        brief, created = _call(orchestrator.submit, submission, actor)
        rendered = _with_model_budget(brief, model_gateway)
        response.status_code = status.HTTP_201_CREATED if created else status.HTTP_200_OK
        return rendered

    @app.get("/api/briefs/{brief_id}")
    def get_brief(
        brief_id: str,
        actor: Annotated[ActorContext, Depends(get_actor)],
        orchestrator: Annotated[Orchestrator, Depends(get_orchestrator)],
    ) -> dict[str, object]:
        brief = _call(orchestrator.get, brief_id, actor)
        return _with_model_budget(brief, model_gateway)

    @app.get("/api/briefs/{brief_id}/model-budget")
    def get_model_budget(
        brief_id: str,
        actor: Annotated[ActorContext, Depends(get_actor)],
        orchestrator: Annotated[Orchestrator, Depends(get_orchestrator)],
    ) -> dict[str, object]:
        brief = _call(orchestrator.get, brief_id, actor)
        return _model_budget(brief, model_gateway).model_dump(mode="json")

    @app.get("/api/briefs/{brief_id}/model-traces")
    def list_model_traces(
        brief_id: str,
        actor: Annotated[ActorContext, Depends(get_actor)],
        orchestrator: Annotated[Orchestrator, Depends(get_orchestrator)],
    ) -> list[dict[str, object]]:
        brief = _call(orchestrator.get, brief_id, actor)
        if model_gateway is None:
            return []
        _register_model_brief(brief, model_gateway)
        summaries = _call(model_gateway.read_trace_summaries, brief_id, actor)
        return [summary.model_dump(mode="json") for summary in summaries]

    @app.post("/api/briefs/{brief_id}/scope-decisions")
    def decide_scope(
        brief_id: str,
        decision: ScopeDecision,
        actor: Annotated[ActorContext, Depends(get_actor)],
        orchestrator: Annotated[Orchestrator, Depends(get_orchestrator)],
    ) -> dict[str, object]:
        brief = _call(orchestrator.decide_scope, brief_id, decision, actor)
        return _with_model_budget(brief, model_gateway)

    @app.post("/api/briefs/{brief_id}/release-decisions")
    def decide_release(
        brief_id: str,
        decision: ReleaseDecision,
        actor: Annotated[ActorContext, Depends(get_actor)],
        orchestrator: Annotated[Orchestrator, Depends(get_orchestrator)],
    ) -> dict[str, object]:
        brief = _call(orchestrator.decide_release, brief_id, decision, actor)
        return _with_model_budget(brief, model_gateway)

    return app


def _default_ledger() -> Ledger:
    if os.getenv("PGHOST"):
        return PostgresLedger.from_environment()
    return InMemoryLedger()


def _default_identity_verifier() -> IdentityVerifier:
    if os.getenv("DATABRICKS_APP_NAME"):
        return DatabricksIdentityVerifier()
    return StaticIdentityVerifier(
        {
            "local-submitter": ActorContext(
                subject="local-submitter", roles={"submitter", "viewer"}
            ),
            "local-approver": ActorContext(subject="local-approver", roles={"approver", "viewer"}),
        }
    )


def _validate_model_runtime(model_gateway: GovernedModelGateway | None) -> None:
    if "STEWARD_FORGE_MODEL_CALLS_ENABLED" not in os.environ:
        return
    config = load_model_governance_config()
    if config.enabled and model_gateway is None:
        raise RuntimeError(
            "model calls are enabled but no governed provider/trace adapter was injected"
        )


def _with_model_budget(
    brief: dict[str, Any], model_gateway: GovernedModelGateway | None
) -> dict[str, Any]:
    return brief | {"model_budget": _model_budget(brief, model_gateway).model_dump(mode="json")}


def _model_budget(
    brief: dict[str, Any], model_gateway: GovernedModelGateway | None
) -> BriefBudgetSummary:
    ceiling = usd_to_minor_units(brief["brief"]["cost_ceiling_usd"])
    if model_gateway is None:
        return BriefBudgetSummary(
            brief_id=str(brief["id"]),
            authorized_ceiling_minor_units=ceiling,
            budget_committed_minor_units=0,
            metered_actual_minor_units=0,
            remaining_authorization_minor_units=ceiling,
            request_count=0,
            throttle_count=0,
            incomplete_usage_count=0,
            reconciliation_failure_count=0,
            usage_status="not_used",
        )
    _register_model_brief(brief, model_gateway)
    return model_gateway._budget_summary(str(brief["id"]))


def _register_model_brief(brief: dict[str, Any], model_gateway: GovernedModelGateway) -> None:
    brief_input = brief["brief"]
    model_gateway.register_brief(
        brief_id=str(brief["id"]),
        run_id=f"workbench-{brief['id']}",
        owner_subject=str(brief["submitted_by"]),
        viewer_subjects=tuple(
            dict.fromkeys(
                (
                    str(brief_input["release_approver"]),
                    *map(str, brief_input.get("viewer_subjects", [])),
                )
            )
        ),
        authorized_ceiling_minor_units=usd_to_minor_units(brief_input["cost_ceiling_usd"]),
    )


def _call(function: Callable[..., Any], *args: object) -> Any:
    try:
        return function(*args)
    except LedgerNotFound as error:
        raise HTTPException(status_code=404, detail="brief not found") from error
    except LedgerConflict as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    except AccessDenied as error:
        raise HTTPException(status_code=403, detail=str(error)) from error
    except WorkflowError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    except ModelGovernanceError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
