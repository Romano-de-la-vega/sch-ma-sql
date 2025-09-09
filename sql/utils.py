"""SQL helper utilities."""
import re
from schema_loader import load_schema, _sqltype_kind


def coerce_numeric_string_literals(sql: str, schema: dict | None = None) -> str:
    """Quote numeric constants when compared to text/boolean columns.

    Some legacy schemas store 0/1 flags in text fields. If the generated SQL uses
    a numeric literal (e.g. `col = 1`), PostgreSQL raises
    `operator does not exist: character varying = integer`. This function wraps
    such literals in quotes based on column types.
    """
    schema = schema or load_schema()
    for tinfo in schema.get("tables", {}).values():
        for col, meta in tinfo.get("columns", {}).items():
            if _sqltype_kind(meta.get("type")) in {"text", "bool"}:
                pattern = rf"(?i)((?:\w+\.)?\"?{col}\"?\s*=\s*)(\d+)"
                sql = re.sub(pattern, lambda m: f"{m.group(1)}'{m.group(2)}'", sql)
    return sql
