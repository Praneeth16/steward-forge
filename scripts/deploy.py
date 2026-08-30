"""Run one portable Databricks Asset Bundle deployment.

Lakebase creates an OAuth role for the project owner when a project is created. The
database resource needs that role ID during the same deployment. This helper derives
the deterministic role ID from the authenticated Databricks user and supplies it as a
bundle variable, avoiding a personal identifier in source control.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from collections.abc import Sequence


def lakebase_owner_role_id(user_name: str) -> str:
    """Convert a Databricks user name to Lakebase's generated owner role ID."""
    local_part = user_name.split("@", maxsplit=1)[0].lower()
    role_id = re.sub(r"[^a-z0-9]+", "-", local_part).strip("-")
    if len(role_id) < 4:
        raise ValueError("The derived Lakebase owner role ID must contain at least 4 characters")
    return role_id[:63].rstrip("-")


def _profile_args(profile: str | None) -> list[str]:
    return ["--profile", profile] if profile else []


def authenticated_user(profile: str | None) -> str:
    command = ["databricks", "current-user", "me", *_profile_args(profile), "--output", "json"]
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    return str(json.loads(result.stdout)["userName"])


def deployment_command(
    *, profile: str | None, target: str, catalog: str, owner_role_id: str
) -> list[str]:
    return [
        "databricks",
        "bundle",
        "deploy",
        "--target",
        target,
        *_profile_args(profile),
        f"--var=catalog_name={catalog}",
        f"--var=lakebase_owner_role_id={owner_role_id}",
    ]


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True, help="Existing writable Unity Catalog catalog")
    parser.add_argument("--profile", help="Local Databricks CLI profile")
    parser.add_argument("--target", default="dev", choices=("dev", "prod"))
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    user_name = authenticated_user(args.profile)
    role_id = lakebase_owner_role_id(user_name)
    subprocess.run(
        deployment_command(
            profile=args.profile,
            target=args.target,
            catalog=args.catalog,
            owner_role_id=role_id,
        ),
        check=True,
    )


if __name__ == "__main__":
    main()

