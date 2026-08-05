"""Refresh the `symbols` table from the NASDAQ Trader symbol directory file.

Downloads the full listing of NASDAQ/NYSE/AMEX/ARCA-listed tickers, upserts
name/security_type for anything present, marks previously-active symbols
that dropped out of the file as delisted, and inserts brand new tickers
as active.
"""
import io
import logging
import re

import psycopg2.extras

import config
from http_utils import request_with_retry

logger = logging.getLogger("chartist.symbols")

# Plain common-stock / ETF tickers only -- letters and numbers, no $, ., -,
# or other special characters. Excludes preferred shares (e.g. "CMS$C"),
# units/warrants (e.g. "COPL.U", "COPL.W"), and similar special issue types.
_PLAIN_TICKER_RE = re.compile(r"^[A-Za-z0-9]+$")


def _parse_nasdaq_file(text: str):
    """Parse the pipe-delimited NASDAQ symbol directory into row dicts.

    Returns a list of {ticker, exchange_code, name, security_type}. Tickers
    with special characters (preferred shares, units, warrants, etc.) are
    filtered out here so they never reach the insert/upsert logic.
    """
    lines = text.splitlines()
    if not lines:
        return []

    header = lines[0].split("|")
    col_idx = {name: i for i, name in enumerate(header)}

    rows = []
    skipped_special = 0
    for line in lines[1:]:
        if not line or line.startswith("File Creation Time"):
            continue
        fields = line.split("|")
        if len(fields) != len(header):
            continue

        if fields[col_idx["Test Issue"]] == "Y":
            continue

        exchange_char = fields[col_idx["Listing Exchange"]]
        exchange_code = config.EXCHANGE_CODE_MAP.get(exchange_char)
        if exchange_code is None:
            continue

        ticker = fields[col_idx["Symbol"]].strip()
        if not ticker:
            continue
        if not _PLAIN_TICKER_RE.match(ticker):
            skipped_special += 1
            continue

        security_type = "etf" if fields[col_idx["ETF"]] == "Y" else "stock"
        name = fields[col_idx["Security Name"]].strip()

        rows.append(
            {
                "ticker": ticker,
                "exchange_code": exchange_code,
                "name": name,
                "security_type": security_type,
            }
        )
    if skipped_special:
        logger.info(
            "Skipped %d ticker(s) with special characters (preferred/units/warrants/etc.)",
            skipped_special,
        )
    return rows


def _ensure_exchanges(conn):
    """Make sure the exchanges we need exist, returning {code: exchange_id}."""
    with conn.cursor() as cur:
        for code, name in (
            ("NASDAQ", "Nasdaq"),
            ("NYSE", "New York Stock Exchange"),
            ("AMEX", "NYSE American"),
            ("ARCA", "NYSE Arca"),
        ):
            cur.execute(
                """
                INSERT INTO exchanges (code, name)
                VALUES (%s, %s)
                ON CONFLICT (code) DO NOTHING
                """,
                (code, name),
            )
        cur.execute("SELECT code, exchange_id FROM exchanges")
        return {code: exchange_id for code, exchange_id in cur.fetchall()}


def refresh_symbols(conn) -> dict:
    """Download the NASDAQ symbol directory and sync it into `symbols`.

    Returns a summary dict: {added, delisted, updated, total_in_file}.
    """
    logger.info("Downloading NASDAQ symbol directory from %s", config.NASDAQ_SYMBOL_DIR_URL)
    response = request_with_retry("GET", config.NASDAQ_SYMBOL_DIR_URL)
    response.raise_for_status()

    rows = _parse_nasdaq_file(response.text)
    logger.info("Parsed %d symbols from NASDAQ/NYSE/AMEX/ARCA after filtering", len(rows))

    exchange_ids = _ensure_exchanges(conn)

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT symbol_id, ticker, exchange_id FROM symbols WHERE status = 'active'")
        currently_active = {(r["ticker"], r["exchange_id"]): r["symbol_id"] for r in cur.fetchall()}

    file_keys = set()
    upsert_rows = []
    for row in rows:
        exchange_id = exchange_ids.get(row["exchange_code"])
        if exchange_id is None:
            continue
        file_keys.add((row["ticker"], exchange_id))
        upsert_rows.append((row["ticker"], exchange_id, row["name"], row["security_type"]))

    added = 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO symbols (ticker, exchange_id, name, security_type, status, created_at, updated_at)
            VALUES %s
            ON CONFLICT (ticker, exchange_id) DO UPDATE SET
                name = EXCLUDED.name,
                security_type = EXCLUDED.security_type,
                status = 'active',
                updated_at = now()
            """,
            [(ticker, exchange_id, name, security_type, "active") for ticker, exchange_id, name, security_type in upsert_rows],
            template="(%s, %s, %s, %s, %s, now(), now())",
        )
        added = len(file_keys - set(currently_active.keys()))

    delisted_keys = set(currently_active.keys()) - file_keys
    delisted = 0
    if delisted_keys:
        with conn.cursor() as cur:
            symbol_ids = [currently_active[key] for key in delisted_keys]
            cur.execute(
                """
                UPDATE symbols
                SET status = 'delisted', updated_at = now()
                WHERE symbol_id = ANY(%s) AND status = 'active'
                """,
                (symbol_ids,),
            )
            delisted = cur.rowcount

    summary = {
        "added": added,
        "delisted": delisted,
        "total_in_file": len(file_keys),
    }
    logger.info(
        "Symbol refresh complete: %d added, %d delisted, %d total active in file",
        summary["added"], summary["delisted"], summary["total_in_file"],
    )
    return summary
