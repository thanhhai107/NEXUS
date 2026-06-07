"""I/O utilities for loading TPC-DI benchmark data.

Deprecated: Use ``common.tpcdi_io`` for streaming access to DIGen sources.
"""

from __future__ import annotations

import warnings
from pathlib import Path
from typing import Any

from common.tpcdi_io import (
    iter_tpcdi_records,
    iter_tpcdi_chunks,
    count_tpcdi_records,
    read_tpcdi_dataframe,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
TPCDI_RUNTIME_DIR = PROJECT_ROOT / "runtime" / "datasets" / "tpcdi"
BENCHMARK_DIR = PROJECT_ROOT / "benchmark"
REPORTS_DIR = BENCHMARK_DIR / "reports"


SOURCE_MAP = {
    "tpcdi_dim_account": "customer_mgmt",
    "tpcdi_dim_broker": "hr",
    "tpcdi_dim_company": "finwire",
    "tpcdi_dim_customer": "customer_mgmt",
    "tpcdi_dim_date": "date",
    "tpcdi_dim_security": "finwire",
    "tpcdi_dim_time": "time",
    "tpcdi_dim_trade": "trade",
    "tpcdi_fact_cash_balances": "cash_transaction",
    "tpcdi_fact_holdings": "holding_history",
    "tpcdi_fact_market_history": "daily_market",
    "tpcdi_fact_watches": "watch_history",
    "tpcdi_industry": "industry",
    "tpcdi_prospect": "prospect",
    "tpcdi_status_type": "status_type",
    "tpcdi_tax_rate": "tax_rate",
    "tpcdi_trade_type": "trade_type",
}


def load_tpcdi_data(table: str, base_dir: Path | None = None) -> list[dict[str, Any]]:
    """Load all records for a table into memory.

    Deprecated: use ``iter_tpcdi_records`` (streaming) or
    ``iter_tpcdi_chunks`` (chunked) from ``common.tpcdi_io`` instead.
    """
    warnings.warn(
        "load_tpcdi_data is deprecated. Use iter_tpcdi_records or "
        "iter_tpcdi_chunks from common.tpcdi_io for SF>1.",
        DeprecationWarning,
        stacklevel=2,
    )
    source_name = SOURCE_MAP.get(table, table)
    return list(iter_tpcdi_records(source_name, "batch1"))
