"""Generate one deterministic synthetic GCC delivery dataset bundle."""

from __future__ import annotations

import argparse
from pathlib import Path

from . import generate_all
from .common import write_bundle


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--brief-id", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True, help="DAB-owned sandbox schema")
    parser.add_argument("--output", type=Path, default=Path("generated-data"))
    args = parser.parse_args()
    tables = generate_all(args.seed, args.brief_id, args.run_id)
    written = write_bundle(args.output, tables, args.catalog, args.schema)
    for table_name, path in written.items():
        print(f"{table_name}: {path}")


if __name__ == "__main__":
    main()
