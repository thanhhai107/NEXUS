# TPC-specific data generators for NEXUS data-caterer integration.
# Provides Python-based generator functions that produce TPC benchmark
# compliant data patterns when Data Caterer native generators are
# insufficient or unavailable.

from __future__ import annotations

import random
from datetime import date, timedelta
from typing import Any


def tpch_mktsegment() -> str:
    return random.choice(["AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD"])


def tpch_order_priority() -> str:
    return random.choice(["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"])


def tpch_ship_mode() -> str:
    return random.choice(["REG AIR", "AIR", "RAIL", "TRUCK", "MAIL", "FOB", "SHIP"])


def tpch_order_status() -> str:
    return random.choice(["O", "F", "P"])


def tpch_return_flag() -> str:
    weights = [0.25, 0.05, 0.70]
    return random.choices(["R", "A", "N"], weights=weights, k=1)[0]


def tpch_line_status() -> str:
    weights = [0.50, 0.50]
    return random.choices(["O", "F"], weights=weights, k=1)[0]


def tpch_part_type() -> str:
    sizes = ["STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"]
    materials = ["ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED"]
    types = ["TIN", "NICKEL", "BRASS", "STEEL", "COPPER"]
    return f"{random.choice(sizes)} {random.choice(materials)} {random.choice(types)}"


def tpch_part_container() -> str:
    return random.choice([
        "SM CASE", "SM BOX", "SM BAG", "SM PACK", "SM PKG",
        "MED CASE", "MED BOX", "MED BAG", "MED PACK", "MED PKG",
        "LG CASE", "LG BOX", "LG BAG", "LG PACK", "LG PKG",
        "JUMBO CASE", "JUMBO BOX", "JUMBO BAG", "JUMBO PACK", "JUMBO PKG",
        "WRAP CASE", "WRAP BOX", "WRAP BAG", "WRAP PACK", "WRAP PKG",
    ])


def tpcc_credit() -> str:
    return random.choice(["BC", "GC"])


def tpcds_category() -> str:
    return random.choice([
        "Sports", "Books", "Home", "Electronics", "Women", "Men",
        "Children", "Shoes", "Music", "Jewelry",
    ])


def tpcds_education() -> str:
    return random.choice([
        "Primary", "Secondary", "2 yr Degree", "4 yr Degree",
        "Advanced Degree", "College", "High School",
    ])


def tpcds_marital_status() -> str:
    return random.choice(["M", "S", "W", "D", "U"])


def tpcds_gender() -> str:
    return random.choice(["M", "F"])


def random_date(start_year: int = 1992, end_year: int = 1998) -> str:
    start = date(start_year, 1, 1)
    end = date(end_year, 12, 31)
    delta = (end - start).days
    result = start + timedelta(days=random.randint(0, delta))
    return result.isoformat()


def random_phone() -> str:
    return f"{random.randint(10, 34)}-{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"


def random_clerk() -> str:
    return f"Clerk#{random.randint(1, 9999):04d}"


def random_amount(min_val: int = -99999, max_val: int = 999999) -> float:
    return round(random.uniform(min_val, max_val), 2)


def random_discount() -> float:
    return round(random.uniform(0.0, 0.10), 2)


def random_tax() -> float:
    return round(random.uniform(0.0, 0.08), 2)


def random_quantity(min_val: int = 1, max_val: int = 50) -> int:
    return random.randint(min_val, max_val)


_TPC_GENERATORS: dict[str, Any] = {
    "tpch_mktsegment": tpch_mktsegment,
    "tpch_order_priority": tpch_order_priority,
    "tpch_ship_mode": tpch_ship_mode,
    "tpch_order_status": tpch_order_status,
    "tpch_return_flag": tpch_return_flag,
    "tpch_line_status": tpch_line_status,
    "tpch_part_type": tpch_part_type,
    "tpch_part_container": tpch_part_container,
    "tpcc_credit": tpcc_credit,
    "tpcds_category": tpcds_category,
    "tpcds_education": tpcds_education,
    "tpcds_marital_status": tpcds_marital_status,
    "tpcds_gender": tpcds_gender,
    "random_date": random_date,
    "random_phone": random_phone,
    "random_clerk": random_clerk,
    "random_amount": random_amount,
    "random_discount": random_discount,
    "random_tax": random_tax,
    "random_quantity": random_quantity,
}


def get_generator(name: str) -> Any:
    return _TPC_GENERATORS.get(name)
