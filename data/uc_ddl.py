"""Unity Catalog DDL for generated GCC delivery tables."""

from __future__ import annotations

from .generators.common import TABLE_SCHEMAS, validate_namespace


def _quote_identifier(value: str) -> str:
    if not value.strip() or "`" in value or any(ord(character) < 32 for character in value):
        raise ValueError(
            "catalog identifiers must be non-empty and cannot contain controls or backticks"
        )
    return f"`{value}`"


def render_table_ddl(
    catalog: str,
    sandbox_schema: str,
    namespace: str,
    table_name: str,
) -> str:
    """Render one Delta table plus its Unity Catalog synthetic-data tag."""

    validate_namespace(namespace)
    if table_name not in TABLE_SCHEMAS:
        raise ValueError(f"unknown table: {table_name}")
    namespaced_table = f"{namespace}__{table_name}"
    relation = ".".join(
        (
            _quote_identifier(catalog),
            _quote_identifier(sandbox_schema),
            _quote_identifier(namespaced_table),
        )
    )
    definitions = []
    for column in TABLE_SCHEMAS[table_name]:
        nullability = "" if column.nullable else " NOT NULL"
        definitions.append(f"  `{column.name}` {column.sql_type}{nullability}")
    columns = ",\n".join(definitions)
    return (
        f"CREATE TABLE IF NOT EXISTS {relation} (\n{columns}\n) USING DELTA\n"
        "TBLPROPERTIES (\n"
        "  'steward_forge.data_classification' = 'SYNTHETIC',\n"
        "  'steward_forge.generator_version' = '1'\n"
        ");\n"
        f"ALTER TABLE {relation} SET TAGS ('data_classification' = 'SYNTHETIC');"
    )


def render_uc_ddl(catalog: str, sandbox_schema: str, namespace: str) -> str:
    """Render namespaced tables inside the DAB-owned sandbox schema."""

    validate_namespace(namespace)
    statements = [
        render_table_ddl(catalog, sandbox_schema, namespace, table_name)
        for table_name in TABLE_SCHEMAS
    ]
    return "\n\n".join(statements) + "\n"
