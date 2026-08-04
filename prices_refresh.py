"""Refresh `daily_prices` with the latest daily bar for all active symbols.

Primary path: EODHD's Bulk API (`/api/eod-bulk-last-day/US`), which returns
the entire US market for the most recent trading day in a single request.

Fallback path: if the Bulk API isn't activated on this account yet (a 403 or
other plan-related error), loop over active symbols individually against
`/api/eod/{ticker}.US`, scoped to the last few days (a nightly top-up, not a
backfill), using a small thread pool. Whichever path runs is logged clearly.

Stores raw open/high/low/close/volume -- NOT `adjusted_close`. This project
stores raw prices and adjusts for splits at read time elsewhere; it never
rewrites stored history.
"""
import concurrent.futures
import datetime
import logging

import psycopg2.extras

import config
from http_utils import request_with_retry
from trading_day import eastern_today

logger = logging.getLogger("chartist.prices")


def _fetch_bulk_prices():
    """Fetch the whole-market bulk EOD file. Returns a list of raw dicts, or
    None if the Bulk API isn't available (not activated, plan error, etc.)."""
    response = request_with_retry(
        "GET", config.EODHD_BULK_URL,
        params={"api_token": config.EODHD_API_KEY, "fmt": "json"},
    )
    if response.status_code != 200:
        logger.warning(
            "Bulk prices endpoint returned %d, treating Bulk API as unavailable: %s",
            response.status_code, response.text[:200],
        )
        return None
    return response.json()


def _fetch_prices_for_symbol(ticker, from_date_str):
    """Fetch the last few days of EOD bars for one ticker. Returns a list of raw dicts."""
    url = config.EODHD_EOD_URL_TEMPLATE.format(ticker=ticker)
    response = request_with_retry(
        "GET", url,
        params={
            "api_token": config.EODHD_API_KEY,
            "fmt": "json",
            "period": "d",
            "from": from_date_str,
        },
    )
    if response.status_code != 200:
        logger.warning(
            "EOD request failed (%d) for %s: %s", response.status_code, ticker, response.text[:200]
        )
        raise RuntimeError(f"EOD request failed ({response.status_code}) for {ticker}")
    return response.json() or []


def refresh_daily_prices(conn, active_symbols, trade_date: datetime.date = None) -> dict:
    """Fetch the latest bar(s) for all active symbols and insert into daily_prices.

    active_symbols: list of {symbol_id, ticker, exchange_id} dicts.
    Returns summary dict: {bars_inserted, errored_tickers}.
    """
    trade_date = trade_date or eastern_today()

    ticker_to_symbol_id = {}
    for row in active_symbols:
        ticker = row["ticker"]
        if ticker in ticker_to_symbol_id:
            continue
        ticker_to_symbol_id[ticker] = row["symbol_id"]

    errored_tickers = []
    price_rows = []

    bulk_records = _fetch_bulk_prices()

    if bulk_records is not None:
        logger.info(
            "Daily prices: using EODHD Bulk API (whole US market, 1 request); "
            "%d active tickers to match against %d returned records",
            len(ticker_to_symbol_id), len(bulk_records),
        )
        matched = 0
        for rec in bulk_records:
            symbol_id = ticker_to_symbol_id.get(rec.get("code"))
            if symbol_id is None:
                continue
            trade_date_str = rec.get("date")
            close = rec.get("close")
            if not trade_date_str or close is None:
                continue
            price_rows.append(
                (symbol_id, trade_date_str, rec.get("open"), rec.get("high"), rec.get("low"), close, rec.get("volume"))
            )
            matched += 1
        logger.info("Daily prices: matched %d of %d active tickers in the bulk response", matched, len(ticker_to_symbol_id))
    else:
        tickers = sorted(ticker_to_symbol_id.keys())
        from_date_str = (trade_date - datetime.timedelta(days=config.PRICE_LOOKBACK_DAYS)).isoformat()
        logger.warning(
            "Daily prices: Bulk API unavailable, falling back to per-symbol requests "
            "(%d active tickers, %d workers, window since %s)",
            len(tickers), config.FALLBACK_MAX_WORKERS, from_date_str,
        )

        with concurrent.futures.ThreadPoolExecutor(max_workers=config.FALLBACK_MAX_WORKERS) as pool:
            futures = {
                pool.submit(_fetch_prices_for_symbol, ticker, from_date_str): ticker
                for ticker in tickers
            }
            for future in concurrent.futures.as_completed(futures):
                ticker = futures[future]
                try:
                    bars = future.result()
                except Exception:
                    logger.exception("Daily prices fetch failed for %s", ticker)
                    errored_tickers.append(ticker)
                    continue
                symbol_id = ticker_to_symbol_id[ticker]
                for bar in bars:
                    bar_date = bar.get("date")
                    close = bar.get("close")
                    if not bar_date or close is None:
                        continue
                    price_rows.append(
                        (symbol_id, bar_date, bar.get("open"), bar.get("high"), bar.get("low"), close, bar.get("volume"))
                    )

    bars_inserted = 0
    if price_rows:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO daily_prices (symbol_id, trade_date, open, high, low, close, volume)
                VALUES %s
                ON CONFLICT (symbol_id, trade_date) DO NOTHING
                """,
                price_rows,
            )
            bars_inserted = cur.rowcount

    summary = {
        "bars_inserted": bars_inserted,
        "errored_tickers": sorted(set(errored_tickers)),
    }
    logger.info(
        "Daily prices refresh complete: %d bars inserted, %d tickers errored",
        bars_inserted, len(summary["errored_tickers"]),
    )
    return summary
