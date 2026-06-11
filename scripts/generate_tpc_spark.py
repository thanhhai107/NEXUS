"""Generate TPC-DI CSV data using Spark for high performance.

Run: docker exec nexus-spark spark-submit /opt/nexus/scripts/generate_tpc_spark.py --scale 10 --output /opt/nexus/runtime/datasets/tpcdi_sf10 --error-profile moderate
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from typing import Any

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import (
    col, lit, expr, concat, when, rand, floor, round as spark_round,
    monotonically_increasing_id, array, element_at, randn, format_string,
)

BATCH_SIZE = 500000
SCHEMAS_DIR = "/opt/nexus/domains/tpc/schemas"

ERROR_PRESETS = {
    "none": {"null": 0.0, "duplicate": 0.0, "type_error": 0.0, "overflow": 0.0, "schema_drift": 0.0},
    "low": {"null": 0.01, "duplicate": 0.005, "type_error": 0.005, "overflow": 0.002, "schema_drift": 0.001},
    "moderate": {"null": 0.05, "duplicate": 0.02, "type_error": 0.02, "overflow": 0.01, "schema_drift": 0.005},
    "high": {"null": 0.15, "duplicate": 0.05, "type_error": 0.05, "overflow": 0.03, "schema_drift": 0.02},
    "extreme": {"null": 0.30, "duplicate": 0.10, "type_error": 0.10, "overflow": 0.05, "schema_drift": 0.05},
}

RECORD_COUNTS_SF1 = {
    "tpcdi_dim_account": 50000,
    "tpcdi_dim_broker": 10,
    "tpcdi_dim_company": 5000,
    "tpcdi_dim_customer": 50000,
    "tpcdi_dim_date": 2557,
    "tpcdi_dim_security": 6850,
    "tpcdi_dim_time": 86400,
    "tpcdi_dim_trade": 865000,
    "tpcdi_fact_cash_balances": 250000,
    "tpcdi_fact_holdings": 500000,
    "tpcdi_fact_market_history": 34250,
    "tpcdi_fact_watches": 500,
    "tpcdi_industry": 100,
    "tpcdi_prospect": 1000,
    "tpcdi_status_type": 10,
    "tpcdi_tax_rate": 15,
    "tpcdi_trade_type": 5,
}


def load_schema(dataset: str) -> dict:
    path = f"{SCHEMAS_DIR}/{dataset}.schema.json"
    with open(path) as f:
        return json.load(f)


def inject_errors(df, error_profile: dict, fields: list):
    """Inject errors independently without cascading effects."""
    r = rand()
    total_rows = df.count()

    # 1. Null injection: set random cells to empty string (not NULL — CSV-safe)
    if error_profile["null"] > 0:
        null_cols = []
        for c in fields:
            new_c = when(rand() < error_profile["null"], lit("")).otherwise(col(c)).alias(c)
            null_cols.append(new_c)
        df = df.select(*null_cols)

    # 2. Type error: put text values in numeric columns (e.g. "N/A" instead of number)
    if error_profile["type_error"] > 0:
        type_cols = []
        for c in fields:
            new_c = when(rand() < error_profile["type_error"],
                concat(lit("ERR_"), col(c).cast("string"))
            ).otherwise(col(c)).alias(c)
            type_cols.append(new_c)
        df = df.select(*type_cols)

    # 3. Overflow: put extreme values in numeric columns
    if error_profile["overflow"] > 0:
        ov_cols = []
        for c in fields:
            new_c = when(rand() < error_profile["overflow"],
                lit("999999999999999")
            ).otherwise(col(c)).alias(c)
            ov_cols.append(new_c)
        df = df.select(*ov_cols)

    # 4. Duplicate: copy a random sample of rows (single pass)
    if error_profile["duplicate"] > 0:
        dup_rows = int(total_rows * error_profile["duplicate"])
        if dup_rows > 0:
            dup = df.limit(dup_rows)
            df = df.union(dup)

    # 5. Schema drift: ADD extra column (don't rename — that breaks schema too hard)
    if error_profile["schema_drift"] > 0:
        df = df.withColumn("_unexpected_field",
            when(rand() < error_profile["schema_drift"], lit("drift_value")).otherwise(lit(None)))

    return df


def generate_dataset(spark: SparkSession, dataset: str, scale: int, output_dir: str, error_profile: dict = None) -> int:
    schema = load_schema(dataset)
    properties = schema.get("properties", {})
    fields = list(properties.keys())
    num_records = RECORD_COUNTS_SF1.get(dataset, 100) * scale

    num_batches = math.ceil(num_records / BATCH_SIZE)
    total_written = 0

    for batch in range(num_batches):
        batch_size = min(BATCH_SIZE, num_records - total_written)
        offset = batch * BATCH_SIZE + 1

        cols = []
        for field in fields:
            prop = properties.get(field, {})
            field_type = prop.get("type", "string")
            if isinstance(field_type, list):
                field_type = [t for t in field_type if t != "null"][0]

            col_expr = _gen_column(field, field_type, offset)
            cols.append(col_expr.alias(field))

        df = spark.range(offset, offset + batch_size).select(*cols)

        if error_profile and error_profile.get("null", 0) + error_profile.get("duplicate", 0) + \
           error_profile.get("type_error", 0) + error_profile.get("overflow", 0) + \
           error_profile.get("schema_drift", 0) > 0:
            df = inject_errors(df, error_profile, fields)

        out_path = f"{output_dir}/{dataset}"
        df.coalesce(1).write.mode("append" if batch > 0 else "overwrite").option("header", "true").csv(out_path)
        total_written += batch_size

    print(f"  {dataset}: {total_written} records -> {output_dir}/{dataset}/")
    return total_written


def _gen_column(name: str, ftype: str, offset: int) -> Any:
    r = rand()

    if name.endswith("_id") or name.startswith("sk_"):
        return floor(col("id") * 100 + 1).cast("string")

    if "date" in name and "dts" not in name:
        return format_string("%04d-%02d-%02d",
            floor(2015 + r * 7),
            floor(1 + r * 12),
            floor(1 + r * 28)
        )

    if "dts" in name or "timestamp" in name:
        return concat(
            format_string("202%01d-", floor(1 + r * 2)),
            format_string("%02d-", floor(1 + r * 12)),
            format_string("%02d ", floor(1 + r * 28)),
            format_string("%02d:", floor(r * 24)),
            format_string("%02d:", floor(r * 60)),
            lit("00.000")
        )

    if "price" in name or "amount" in name or "dividend" in name:
        return spark_round(r * 1000, 2).cast("string")

    if "quantity" in name or "volume" in name:
        return floor(1 + r * 100000).cast("string")

    if "cash" in name or "balance" in name:
        return spark_round((r - 0.5) * 20000000, 2).cast("string")

    if "status" in name:
        statuses = ["ACTIVE", "CLOSED", "PENDING", "COMPLETED"]
        return element_at(array([lit(s) for s in statuses]), floor(1 + r * 4).cast("int"))

    if "flag" in name or "iscurrent" in name:
        return when(r > 0.5, "true").otherwise("false")

    if "ratio" in name:
        return spark_round(r, 4).cast("string")

    if "score" in name or "rating" in name:
        return floor(300 + r * 550).cast("string")

    if "effectivedate" in name:
        return lit("2015-01-01")

    if "enddate" in name:
        return lit("9999-12-31")

    if name == "gender":
        return when(r > 0.5, "M").otherwise("F")

    if name == "country":
        return when(r > 0.6, "USA").otherwise("CAN")

    if name in ("state_prov", "exchangeid"):
        return element_at(array([lit(v) for v in ["NYSE", "NASDAQ", "AMEX", "LSE"]]), floor(1 + r * 4).cast("int"))

    if name == "email":
        return concat(lit("user"), (col("id") - offset + 1).cast("string"), lit("@example.com"))

    if name == "postalcode":
        return format_string("%05d", floor(r * 99999))

    if name == "batchid":
        return lit("1")

    if name == "taxid":
        return concat(format_string("%03d-", floor(100 + r * 900)), format_string("%02d-", floor(10 + r * 90)), format_string("%04d", floor(1000 + r * 9000)))

    if name == "symbol":
        return expr("substring(md5(cast(id as string)), 1, cast(3 + rand(1) * 3 as int))")

    if ftype == "integer":
        return floor(1 + r * 999999).cast("string")

    if ftype == "number":
        return spark_round(r * 10000, 2).cast("string")

    if ftype == "boolean":
        return when(r > 0.5, "true").otherwise("false")

    rand_letters = [chr(65 + i) for i in range(26)]
    return element_at(array([lit(c) for c in rand_letters]), floor(1 + r * 26).cast("int"))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scale", type=int, default=3, choices=[3, 10, 50])
    parser.add_argument("--output", type=str, required=True)
    parser.add_argument("--datasets", type=str, default="")
    parser.add_argument("--error-profile", type=str, default="none",
                        choices=["none", "low", "moderate", "high", "extreme"])
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    error_profile = ERROR_PRESETS.get(args.error_profile, ERROR_PRESETS["none"])

    if args.datasets:
        datasets = [d.strip() for d in args.datasets.split(",")]
    else:
        datasets = sorted(RECORD_COUNTS_SF1.keys())

    print(f"Generating TPC-DI SF={args.scale}, error={args.error_profile}")
    total = 0
    for ds in datasets:
        cnt = generate_dataset(spark, ds, args.scale, args.output, error_profile)
        total += cnt

    print(f"\nTotal: {total} records")
    spark.stop()


if __name__ == "__main__":
    main()
