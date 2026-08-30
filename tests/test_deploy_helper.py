import pytest

from scripts.deploy import deployment_command, lakebase_owner_role_id


@pytest.mark.parametrize(
    ("user_name", "expected"),
    [
        ("alex@example.com", "alex"),
        ("alex.smith@example.com", "alex-smith"),
        ("ALEX_SMITH+demo@example.com", "alex-smith-demo"),
    ],
)
def test_lakebase_owner_role_id_matches_lakebase_normalization(
    user_name: str, expected: str
) -> None:
    assert lakebase_owner_role_id(user_name) == expected


def test_lakebase_owner_role_id_rejects_too_short_names() -> None:
    with pytest.raises(ValueError):
        lakebase_owner_role_id("a@example.com")


def test_deployment_command_supplies_environment_inputs() -> None:
    command = deployment_command(
        profile="demo", target="dev", catalog="demo_catalog", owner_role_id="alex-smith"
    )
    assert command[:4] == ["databricks", "bundle", "deploy", "--target"]
    assert "--profile" in command
    assert "--var=catalog_name=demo_catalog" in command
    assert "--var=lakebase_owner_role_id=alex-smith" in command

