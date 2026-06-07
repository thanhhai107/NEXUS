"""
TPC-DI source file parsers.

Each parser reads from ``iter_tpcdi_records`` and applies type coercion.
"""

from ingestion.tpcdi.parsers.reference import parse_reference
from ingestion.tpcdi.parsers.datetime_dim import parse_date_dim, parse_time_dim
from ingestion.tpcdi.parsers.csv_pipe import (
    parse_hr,
    parse_daily_market,
    parse_prospect,
    parse_trade,
    parse_cash_transaction,
    parse_holding_history,
    parse_watch_history,
)
from ingestion.tpcdi.parsers.complex import (
    parse_customer_mgmt,
    parse_finwire,
    parse_customer_update,
    parse_account_update,
)

__all__ = [
    "parse_reference",
    "parse_date_dim",
    "parse_time_dim",
    "parse_hr",
    "parse_daily_market",
    "parse_prospect",
    "parse_trade",
    "parse_cash_transaction",
    "parse_holding_history",
    "parse_watch_history",
    "parse_customer_mgmt",
    "parse_finwire",
    "parse_customer_update",
    "parse_account_update",
]
