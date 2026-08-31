"""Deterministic generators for the Steward Forge engineering scenario."""

from .backlog import generate_backlog
from .common import build_namespace, canonical_jsonl, write_bundle
from .costs import generate_costs
from .pipelines import generate_pipeline_runs


def generate_all(seed: int, brief_id: str, run_id: str) -> dict[str, list[dict[str, object]]]:
    """Generate every synthetic table for one brief run."""

    namespace = build_namespace(brief_id, run_id)
    return {
        "backlog": generate_backlog(seed, namespace),
        "pipeline_runs": generate_pipeline_runs(seed, namespace),
        "platform_costs": generate_costs(seed, namespace),
    }


__all__ = [
    "build_namespace",
    "canonical_jsonl",
    "generate_all",
    "generate_backlog",
    "generate_costs",
    "generate_pipeline_runs",
    "write_bundle",
]
