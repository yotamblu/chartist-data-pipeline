"""Full price-history backfill for brand-new symbols.

Runs as its own explicit step between the splits/dividends refresh and the
regular daily prices top-up, so it's clearly visible in the logs as a
distinct phase rather than folded silently into the top-up.

New symbols are detected via `prices_loaded_at IS NULL` on `symbols`
(see `db.get_symbols_needing_price_backfill`) -- a symbol just inserted by
the symbols refresh step naturally starts out this way, so no separate
"is this new" tracking is needed. The regular daily prices step only ever
fetches a short recent window, which is fine for symbols that already have
history but leaves a brand-new symbol with no rows at all -- this step
exists to fill that gap once, in full.

Always goes per-symbol -- the Bulk API only ever returns the most recent
single day and can't backfill history. Reuses the same thread-pool +
jittered-backoff approach as the other per-symbol EODHD fallback paths,
since a full-history payload per symbol is large and slow (~7s per request
is normal, not a bug). In practice very few symbols need this on any given
night (just new listings), so overall runtime stays low even though each
individual request is slow.

IMPORTANT -- chunked commits: inserts commit in small chunks (~150 rows),
never as one multi-year transaction per symbol. A full history spans many
TimescaleDB hypertable chunks, and holding locks across all of them in a
single uncommitted transaction causes "out of shared memory" errors on
Aiven. Do not collapse this into one big INSERT/commit per symbol, even in
the name of "simplifying" it.

Stores raw open/high/low/close/volume -- NOT `adjusted_close`, same as
everywhere else in this project.
"""
import concurrent.futures
import logging

import psycopg2.extras

import config
import db
from http_utils import request_with_retry

logger = logging.getLogger("chartist.price_backfill")

# EODHD has no data before this for any US-listed symbol we'd care about;
# a wide-open start date just means "give me everything you have."
BACKFILL_START_DATE = "2000-01-01"

# Small on purpose -- see the chunked-commits note in the module docstring.
INSERT_CHUNK_SIZE = 150


def _chunk(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def _fetch_full_history(ticker):
    """Fetch a symbol's full daily price history from EODHD. Returns a list of raw dicts."""
    url = config.EODHD_EOD_URL_TEMPLATE.format(ticker=ticker)
    response = request_with_retry(
        "GET", url,
        params={
            "api_token": config.EODHD_API_KEY,
            "fmt": "json",
            "period": "d",
            "from": BACKFILL_START_DATE,
        },
    )
    if response.status_code != 200:
        logger.warning(
            "Full history request failed (%d) for %s: %s", response.status_code, ticker, response.text[:200]
        )
        raise RuntimeError(f"Full history request failed ({response.status_code}) for {ticker}")
    return response.json() or []


def _store_history(conn, symbol_id, bars) -> int:
    """Insert a symbol's full history in small chunks, committing after each
    one -- see the chunked-commits note in the module docstring. Returns
    the number of rows actually inserted."""
    rows = []
    for bar in bars:
        bar_date = bar.get("date")
        close = bar.get("close")
        if not bar_date or close is None:
            continue
        rows.append(
            (symbol_id, bar_date, bar.get("open"), bar.get("high"), bar.get("low"), close, bar.get("volume"))
        )

    inserted = 0
    for rows_chunk in _chunk(rows, INSERT_CHUNK_SIZE):
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO daily_prices (symbol_id, trade_date, open, high, low, close, volume)
                VALUES %s
                ON CONFLICT (symbol_id, trade_date) DO NOTHING
                """,
                rows_chunk,
            )
            inserted += cur.rowcount
        conn.commit()
    return inserted


def backfill_new_symbols(conn, new_symbols) -> dict:
    """Backfill full price history for symbols that have never been loaded.

    new_symbols: list of {symbol_id, ticker, exchange_id} dicts -- rows from
    `symbols` where prices_loaded_at IS NULL (db.get_symbols_needing_price_backfill).
    Returns summary dict: {symbols_backfilled, bars_inserted, errored_tickers}.
    """
    if not new_symbols:
        logger.info("New symbol backfill: no new symbols found, nothing to do")
        return {"symbols_backfilled": 0, "bars_inserted": 0, "errored_tickers": []}

    logger.info(
        "New symbol backfill: %d new symbol(s) found (prices_loaded_at IS NULL), "
        "fetching full history with %d workers",
        len(new_symbols), config.FALLBACK_MAX_WORKERS,
    )

    errored_tickers = []
    symbols_backfilled = 0
    bars_inserted = 0

    # Fetching (slow, ~7s/symbol for a full-history payload) happens
    # concurrently across a thread pool. Storing happens sequentially on
    # this single connection as each fetch completes -- psycopg2
    # connections aren't safe to share across threads, and per-symbol
    # inserts are cheap compared to the fetch, so this isn't a bottleneck.
    with concurrent.futures.ThreadPoolExecutor(max_workers=config.FALLBACK_MAX_WORKERS) as pool:
        futures = {pool.submit(_fetch_full_history, row["ticker"]): row for row in new_symbols}
        for future in concurrent.futures.as_completed(futures):
            row = futures[future]
            ticker, symbol_id = row["ticker"], row["symbol_id"]
            try:
                bars = future.result()
            except Exception:
                logger.exception("Full history fetch failed for new symbol %s", ticker)
                errored_tickers.append(ticker)
                continue

            try:
                inserted = _store_history(conn, symbol_id, bars)
                db.mark_prices_loaded(conn, symbol_id)
                conn.commit()
            except Exception:
                logger.exception("Full history store failed for new symbol %s", ticker)
                conn.rollback()
                errored_tickers.append(ticker)
                continue

            symbols_backfilled += 1
            bars_inserted += inserted
            logger.info("New symbol backfill: %s done (%d bars)", ticker, inserted)

    summary = {
        "symbols_backfilled": symbols_backfilled,
        "bars_inserted": bars_inserted,
        "errored_tickers": sorted(set(errored_tickers)),
    }
    logger.info(
        "New symbol backfill complete: %d/%d symbols backfilled, %d bars inserted, %d errored",
        symbols_backfilled, len(new_symbols), bars_inserted, len(summary["errored_tickers"]),
    )
    return summary
