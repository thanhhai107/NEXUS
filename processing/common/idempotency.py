from __future__ import annotations

import re
from typing import Iterable, Sequence


def parse_key_list(value: str | Iterable[str] | None) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    return [str(item).strip() for item in value if str(item).strip()]


def existing_columns(data_frame, keys: Sequence[str]) -> list[str]:
    columns = set(data_frame.columns)
    return [key for key in keys if key in columns]


def resolve_dedup_keys(data_frame, preferred_keys: Sequence[str]) -> list[str]:
    preferred = existing_columns(data_frame, preferred_keys)
    if preferred:
        return preferred
    fallback = existing_columns(data_frame, ["_nexus_record_id"])
    if fallback:
        return fallback
    return []


def deduplicate_dataframe(data_frame, preferred_keys: Sequence[str]):
    keys = resolve_dedup_keys(data_frame, preferred_keys)
    if keys:
        return data_frame.dropDuplicates(keys), keys
    return data_frame.dropDuplicates(), []


def safe_identifier(value: str) -> str:
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", value):
        raise ValueError(f"Unsafe SQL identifier: {value}")
    return f"`{value}`"


def safe_temp_view(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_]", "_", value)
    if not cleaned or cleaned[0].isdigit():
        cleaned = f"tmp_{cleaned}"
    return cleaned


def merge_condition(keys: Sequence[str], target_alias: str = "target", source_alias: str = "source") -> str:
    if not keys:
        raise ValueError("MERGE requires at least one key")
    return " AND ".join(
        f"{target_alias}.{safe_identifier(key)} <=> {source_alias}.{safe_identifier(key)}"
        for key in keys
    )


def merge_assignments(columns: Sequence[str], source_alias: str = "source") -> str:
    return ", ".join(
        f"{safe_identifier(column)} = {source_alias}.{safe_identifier(column)}"
        for column in columns
    )


def insert_columns(columns: Sequence[str]) -> str:
    return ", ".join(safe_identifier(column) for column in columns)


def insert_values(columns: Sequence[str], source_alias: str = "source") -> str:
    return ", ".join(f"{source_alias}.{safe_identifier(column)}" for column in columns)


def table_exists(spark, table_name: str) -> bool:
    try:
        return bool(spark.catalog.tableExists(table_name))
    except Exception:
        try:
            spark.table(table_name).limit(1)
            return True
        except Exception:
            return False


def ensure_namespace(spark, table_name: str) -> None:
    parts = table_name.split(".")
    if len(parts) <= 1:
        return
    namespace = ".".join(parts[:-1])
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")


def write_idempotent_iceberg(
    spark,
    data_frame,
    table_name: str,
    *,
    preferred_keys: Sequence[str],
    mode: str = "merge",
) -> list[str]:
    deduped, keys = deduplicate_dataframe(data_frame, preferred_keys)
    resolved_mode = mode.lower()
    ensure_namespace(spark, table_name)
    if resolved_mode == "replace":
        deduped.writeTo(table_name).using("iceberg").createOrReplace()
        return keys

    if not table_exists(spark, table_name):
        deduped.writeTo(table_name).using("iceberg").create()
        return keys

    if not keys:
        deduped.writeTo(table_name).using("iceberg").append()
        return keys

    temp_view = safe_temp_view(f"merge_{table_name}")
    deduped.createOrReplaceTempView(temp_view)
    columns = deduped.columns
    spark.sql(
        f"""
        MERGE INTO {table_name} AS target
        USING {temp_view} AS source
        ON {merge_condition(keys)}
        WHEN MATCHED THEN UPDATE SET {merge_assignments(columns)}
        WHEN NOT MATCHED THEN INSERT ({insert_columns(columns)})
        VALUES ({insert_values(columns)})
        """
    )
    return keys
``