"""Deterministic Product Manager proposal with no approval authority."""

from __future__ import annotations

from orchestrator.delivery_models import ProductScope
from orchestrator.models import BriefSubmission


class ProductManagerWorker:
    """Translate a submitted brief into a versioned scope proposal."""

    worker_id = "product-manager"
    contract_id = "product-manager-scope-proposal"
    contract_version = 1

    def propose(self, brief_id: str, brief: BriefSubmission) -> ProductScope:
        return ProductScope(
            brief_id=brief_id,
            outcome=brief.business_question,
            scope=(
                "Publish deterministic synthetic delivery-health data in the configured sandbox.",
                "Release a governed dashboard over backlog, reliability, and platform cost.",
            ),
            assumptions=(
                "All source records are deterministic synthetic demonstration data.",
                "A named human approves scope and the exact release candidate.",
                "Workers cannot make final regulated decisions.",
            ),
            acceptance_tests=tuple(brief.acceptance_tests),
        )
