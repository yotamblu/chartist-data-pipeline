"""Refresh `daily_prices` with today's daily bar for all active symbols.

Fetches from Alpaca's multi-symbol bars endpoint, batched the same way as
corporate actions, and applies the same bisect-on-400 handling for tickers
with special characters.
"""
import datetime
import logging
import time

import psycopg2.extras

import config
from http_utils import request_with_retry
from trading_day import eastern_today

logger = logging.getLogger("chartist.prices")

_HEADERS = {
    "APCA-API-KEY-ID": config.ALPACA_API_KEY_ID,
    "APCA-API-SECRET-KEY": config.ALPACA_API_SECRET_KEY,
}


def _chunk(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def _fetch_bars_page(tickers, trade_date_str, page_token=None):
    params = {
        "symbols": ",".join(tickers),
        "timeframe": "1Day",
        "start": trade_date_str,
        "end": trade_date_str,
        "feed": "iex",
        "adjustment": "raw",
        "limit": 10000,
    }
    if page_token:
        params["page_token"] = page_token
    return request_with_retry("GET", config.ALPACA_BARS_URL, headers=_HEADERS, params=params)


def _fetch_bars_for_batch(tickers, trade_date_str, errored_tickers):
    """Fetch bars for a batch of tickers, bisecting on 400 to isolate bad tickers.

    Returns {ticker: bar_dict}.
    """
    if not tickers:
        return {}

    bars_by_ticker = {}
    page_token = None
    while True:
        response = _fetch_bars_page(tickers, trade_date_str, page_token)

        if response.status_code == 400:
            if len(tickers) == 1:
                logger.warning(
                    "Bars request failed (400) for ticker %s, skipping: %s",
                    tickers[0], response.text[:200],
                )
                errored_tickers.append(tickers[0])
                return bars_by_ticker
            mid = len(tickers) // 2
            logger.info("Bars batch of %d returned 400, bisecting to isolate bad ticker(s)", len(tickers))
            bars_by_ticker.update(_fetch_bars_for_batch(tickers[:mid], trade_date_str, errored_tickers))
            bars_by_ticker.update(_fetch_bars_for_batch(tickers[mid:], trade_date_str, errored_tickers))
            return bars_by_ticker

        response.raise_for_status()
        payload = response.json()
        for ticker, bars in (payload.get("bars") or {}).items():
            if bars:
                bars_by_ticker[ticker] = bars[0]

        page_token = payload.get("next_page_token")
        if not page_token:
            break
        time.sleep(config.REQUEST_DELAY_SECONDS)

    return bars_by_ticker


def refresh_daily_prices(conn, active_symbols, trade_date: datetime.date = None) -> dict:
    """Fetch today's bar for all active symbols and insert into daily_prices.

    active_symbols: list of {symbol_id, ticker, exchange_id} dicts.
    Returns summary dict: {bars_inserted, errored_tickers}.
    """
    trade_date = trade_date or eastern_today()
    trade_date_str = trade_date.isoformat()

    ticker_to_symbol_id = {}
    for row in active_symbols:
        ticker = row["ticker"]
        if ticker in ticker_to_symbol_id:
            continue
        ticker_to_symbol_id[ticker] = row["symbol_id"]

    tickers = sorted(ticker_to_symbol_id.keys())
    batches = list(_chunk(tickers, config.BATCH_SIZE))
    logger.info(
        "Fetching daily bars for %d active tickers in %d batch(es) for %s",
        len(tickers), len(batches), trade_date_str,
    )

    errored_tickers = []
    price_rows = []

    for batch_num, batch in enumerate(batches, start=1):
        logger.info("Daily prices batch %d/%d (%d tickers)", batch_num, len(batches), len(batch))
        bars = _fetch_bars_for_batch(batch, trade_date_str, errored_tickers)
        for ticker, bar in bars.items():
            symbol_id = ticker_to_symbol_id.get(ticker)
            if symbol_id is None:
                continue
            price_rows.append(
                (
                    symbol_id,
                    trade_date_str,
                    bar.get("o"),
                    bar.get("h"),
                    bar.get("l"),
                    bar.get("c"),
                    bar.get("v"),
                )
            )
        time.sleep(config.REQUEST_DELAY_SECONDS)

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
        "errored_tickers": errored_tickers,
    }
    logger.info(
        "Daily prices refresh complete: %d bars inserted, %d tickers errored",
        bars_inserted, len(errored_tickers),
    )
    return summary
