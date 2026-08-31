from pathlib import Path

import yaml

ROOT = Path(__file__).parents[1]
BUNDLE_FILES = [ROOT / "databricks.yml", *sorted((ROOT / "resources").glob("*.yml"))]
FORBIDDEN_PUBLIC_VALUES = (
    "personal-workspace.example",
    "user@example.corp",
    "legacy-project-id",
    "personal-profile-name",
)


def test_bundle_files_are_valid_yaml() -> None:
    for path in BUNDLE_FILES:
        parsed = yaml.safe_load(path.read_text())
        assert isinstance(parsed, dict), path


def test_bundle_has_required_portable_variables() -> None:
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    assert set(bundle["variables"]) >= {
        "lakebase_project_id",
        "lakebase_database",
        "lakebase_owner_role_id",
        "catalog_name",
    }


def test_bundle_does_not_embed_personal_or_prior_project_identifiers() -> None:
    bundle_text = "\n".join(path.read_text().lower() for path in BUNDLE_FILES)
    for forbidden in FORBIDDEN_PUBLIC_VALUES:
        assert forbidden not in bundle_text
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    for target in bundle["targets"].values():
        assert set(target.get("workspace", {})) <= {"root_path"}


def test_lakebase_resources_are_project_owned() -> None:
    resources = yaml.safe_load((ROOT / "resources" / "lakebase.yml").read_text())["resources"]
    assert resources["postgres_projects"]["lakebase_project"]["project_id"] == (
        "${var.lakebase_project_id}"
    )
    assert resources["postgres_databases"]["ledger"]["postgres_database"] == (
        "${var.lakebase_database}"
    )
    assert resources["postgres_databases"]["ledger"]["role"].endswith(
        "/roles/${var.lakebase_owner_role_id}"
    )
    assert resources["postgres_branches"]["production"]["replace_existing"] is True
    assert resources["postgres_endpoints"]["primary"]["replace_existing"] is True


def test_catalog_is_configurable_and_dedicated() -> None:
    resources = yaml.safe_load((ROOT / "resources" / "catalog.yml").read_text())["resources"]
    assert {"sandbox", "evidence"} <= set(resources["schemas"])
    assert {schema["catalog_name"] for schema in resources["schemas"].values()} == {
        "${var.catalog_name}"
    }


def test_workbench_app_uses_bundle_owned_lakebase_resources() -> None:
    document = yaml.safe_load((ROOT / "resources" / "app.yml").read_text())
    app = document["resources"]["apps"]["workbench"]

    assert app["name"] == "${var.app_name}"
    assert app["source_code_path"] == "."
    assert app["lifecycle"]["started"] is True
    postgres = next(resource["postgres"] for resource in app["resources"] if "postgres" in resource)
    assert postgres == {
        "branch": "${resources.postgres_branches.production.name}",
        "database": "${var.lakebase_database}",
        "permission": "CAN_CONNECT_AND_CREATE",
    }
    assert app["config"]["env"] == [
        {
            "name": "ENDPOINT_NAME",
            "value": "${resources.postgres_endpoints.primary.name}",
        }
    ]
