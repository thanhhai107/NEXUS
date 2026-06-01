# TPC-DI data generators for NEXUS data-caterer integration.
# Provides Python-based generator functions that produce TPC-DI
# compliant data patterns when Data Caterer native generators are
# insufficient or unavailable.

from __future__ import annotations

import random
from datetime import date, timedelta
from typing import Any


def tpcdi_sp_rating() -> str:
    return random.choice([
        "AAA", "AA+", "AA", "AA-", "A+", "A", "A-",
        "BBB+", "BBB", "BBB-", "BB+", "BB", "BB-",
    ])


def tpcdi_status() -> str:
    return random.choice(["ACTIVE", "INACTIVE", "CLOSED", "PENDING", "SUSPENDED"])


def tpcdi_issue_type() -> str:
    return random.choice(["COMMON", "PREFERRED", "BOND", "ETF"])


def random_date(start_year: int = 1992, end_year: int = 1998) -> str:
    start = date(start_year, 1, 1)
    end = date(end_year, 12, 31)
    delta = (end - start).days
    result = start + timedelta(days=random.randint(0, delta))
    return result.isoformat()


def random_phone() -> str:
    return f"{random.randint(10, 34)}-{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"


def random_amount(min_val: int = -99999, max_val: int = 999999) -> float:
    return round(random.uniform(min_val, max_val), 2)


def random_discount() -> float:
    return round(random.uniform(0.0, 0.10), 2)


def random_tax() -> float:
    return round(random.uniform(0.0, 0.08), 2)


def random_quantity(min_val: int = 1, max_val: int = 50) -> int:
    return random.randint(min_val, max_val)


_TPC_GENERATORS: dict[str, Any] = {
    "tpcdi_sp_rating": tpcdi_sp_rating,
    "tpcdi_status": tpcdi_status,
    "tpcdi_issue_type": tpcdi_issue_type,
    "random_date": random_date,
    "random_phone": random_phone,
    "random_amount": random_amount,
    "random_discount": random_discount,
    "random_tax": random_tax,
    "random_quantity": random_quantity,
}


def get_generator(name: str) -> Any:
    return _TPC_GENERATORS.get(name)
