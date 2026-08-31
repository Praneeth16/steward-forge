"""HTTP interface for the first governed Steward Forge tracer."""

from collections.abc import Callable
from pathlib import Path
from typing import Annotated, Any

from fastapi import Depends, FastAPI, HTTPException, Request, Response, status
from fastapi.responses import HTMLResponse

from ledger import LedgerNotFound
from orchestrator.models import BriefSubmission, ReleaseDecision, ScopeDecision
from orchestrator.service import Orchestrator, WorkflowError

WORKBENCH_HTML = Path(__file__).with_name("index.html")


def get_orchestrator(request: Request) -> Orchestrator:
    return request.app.state.orchestrator


def create_app() -> FastAPI:
    app = FastAPI(title="Steward Forge", version="0.1.0")
    app.state.orchestrator = Orchestrator()

    @app.get("/", response_class=HTMLResponse)
    def workbench() -> str:
        return WORKBENCH_HTML.read_text()

    @app.get("/api/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/api/briefs", status_code=status.HTTP_201_CREATED)
    def submit_brief(
        submission: BriefSubmission,
        response: Response,
        orchestrator: Annotated[Orchestrator, Depends(get_orchestrator)],
    ) -> dict[str, object]:
        brief, created = orchestrator.submit(submission)
        response.status_code = status.HTTP_201_CREATED if created else status.HTTP_200_OK
        return brief

    @app.get("/api/briefs/{brief_id}")
    def get_brief(
        brief_id: str,
        orchestrator: Annotated[Orchestrator, Depends(get_orchestrator)],
    ) -> dict[str, object]:
        return _call(orchestrator.get, brief_id)

    @app.post("/api/briefs/{brief_id}/scope-decisions")
    def decide_scope(
        brief_id: str,
        decision: ScopeDecision,
        orchestrator: Annotated[Orchestrator, Depends(get_orchestrator)],
    ) -> dict[str, object]:
        return _call(orchestrator.decide_scope, brief_id, decision)

    @app.post("/api/briefs/{brief_id}/release-decisions")
    def decide_release(
        brief_id: str,
        decision: ReleaseDecision,
        orchestrator: Annotated[Orchestrator, Depends(get_orchestrator)],
    ) -> dict[str, object]:
        return _call(orchestrator.decide_release, brief_id, decision)

    return app


def _call(
    function: Callable[..., dict[str, Any]], *args: object
) -> dict[str, object]:
    try:
        return function(*args)
    except LedgerNotFound as error:
        raise HTTPException(status_code=404, detail="brief not found") from error
    except WorkflowError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
