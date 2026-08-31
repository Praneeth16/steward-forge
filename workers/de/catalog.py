"""Injectable catalog adapter used by the governed Data Engineer contract."""

from __future__ import annotations

import hashlib
from copy import deepcopy

from broker.contracts import SyntheticTableWriteArgs
from data.generators.common import canonical_jsonl
from workers.de.models import CatalogTableOutput


class InMemoryCatalogAdapter:
    """Local proof adapter; it does not claim or attempt Unity Catalog writes."""

    def __init__(self) -> None:
        self.tables: dict[str, tuple[dict[str, object], ...]] = {}
        self.write_events: list[CatalogTableOutput] = []

    def write(self, arguments: SyntheticTableWriteArgs) -> dict[str, object]:
        table_name = f"{arguments.namespace}__{arguments.dataset}"
        relation = ".".join((arguments.catalog, arguments.schema_name, table_name))
        rows = deepcopy(arguments.rows)
        output = CatalogTableOutput(
            dataset=arguments.dataset,
            relation=relation,
            row_count=len(rows),
            data_sha256=hashlib.sha256(canonical_jsonl(rows)).hexdigest(),
        )
        stored_rows = tuple(rows)
        existing = self.tables.get(relation)
        if existing is not None:
            if existing != stored_rows:
                raise ValueError("catalog relation is already bound to different rows")
        else:
            self.tables[relation] = stored_rows
            self.write_events.append(output)
        return output.model_dump(mode="json")
