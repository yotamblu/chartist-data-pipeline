"""Refresh `splits` and `dividends` from Alpaca's corporate actions endpoint.

Queries a short recent window across all active symbols, batched to stay
under request size limits. Some tickers with special characters (e.g.
"CMS$C") make Alpaca return 400 for the whole batch -- when that happens we
bisect the batch and retry each half, isolating down to the single bad
ticker if needed, logging it, and continuing with everything else.
"""
import datetime
import logging
import time

import psycopg2.extras

import config
from http_utils import request_with_retry

logger = logging.getLogger("chartist.corporate_actions")

_HEADERS = {
    "APCA-API-KEY-ID": config.ALPACA_API_KEY_ID,
    "APCA-API-SECRET-KEY": config.ALPACA_API_SECRET_KEY,
}


def _chunk(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def _fetch_corporate_actions_page(tickers, types, start, end, page_token=None):
    params = {
        "symbols": ",".join(tickers),
        "types": types,
        "start": start,
        "end": end,
    }
    if page_token:
        params["page_token"] = page_token
    return request_with_retry(
        "GET", config.ALPACA_CORPORATE_ACTIONS_URL, headers=_HEADERS, params=params
    )


def _fetch_corporate_actions_for_batch(tickers, types, start, end, errored_tickers):
    """Fetch all pages of corporate actions for a batch of tickers.

    On a 400, bisects the batch and retries each half recursively, isolating
    single bad tickers (which get logged into errored_tickers and skipped).
    Returns a list of raw record dicts merged across all action-type buckets.
    """
    if not tickers:
        return []

    records = []
    page_token = None
    while True:
        response = _fetch_corporate_actions_page(tickers, types, start, end, page_token)

        if response.status_code == 400:
            if len(tickers) == 1:
                logger.warning(
                    "Corporate actions request failed (400) for ticker %s, skipping: %s",
                    tickers[0], response.text[:200],
                )
                errored_tickers.append(tickers[0])
                return records
            mid = len(tickers) // 2
            logger.info(
                "Corporate actions batch of %d returned 400, bisecting to isolate bad ticker(s)",
                len(tickers),
            )
            records.extend(
                _fetch_corporate_actions_for_batch(tickers[:mid], types, start, end, errored_tickers)
            )
            records.extend(
                _fetch_corporate_actions_for_batch(tickers[mid:], types, start, end, errored_tickers)
            )
            return records

        response.raise_for_status()
        payload = response.json()
        actions = payload.get("corporate_actions", {})
        for bucket in actions.values():
            records.extend(bucket)

        page_token = payload.get("next_page_token")
        if not page_token:
            break
        time.sleep(config.REQUEST_DELAY_SECONDS)

    return records


def refresh_corporate_actions(conn, active_symbols) -> dict:
    """Fetch recent splits/dividends for all active symbols and upsert them.

    active_symbols: list of {symbol_id, ticker, exchange_id} dicts.
    Returns summary dict: {splits_upserted, dividends_upserted, errored_tickers}.
    """
    ticker_to_symbol_id = {}
    for row in active_symbols:
        ticker = row["ticker"]
        if ticker in ticker_to_symbol_id:
            logger.warning(
                "Duplicate ticker %s across exchanges; corporate actions will map to symbol_id %d",
                ticker, ticker_to_symbol_id[ticker],
            )
            continue
        ticker_to_symbol_id[ticker] = row["symbol_id"]

    tickers = sorted(ticker_to_symbol_id.keys())
    end = datetime.date.today()
    start = end - datetime.timedelta(days=config.CORPORATE_ACTIONS_LOOKBACK_DAYS)
    start_str, end_str = start.isoformat(), end.isoformat()

    batches = list(_chunk(tickers, config.BATCH_SIZE))
    logger.info(
        "Fetching splits/dividends for %d active tickers in %d batch(es) (window %s to %s)",
        len(tickers), len(batches), start_str, end_str,
    )

    errored_tickers = []
    split_rows = []
    dividend_rows = []

    for batch_num, batch in enumerate(batches, start=1):
        logger.info("Corporate actions batch %d/%d (%d tickers)", batch_num, len(batches), len(batch))
        split_records = _fetch_corporate_actions_for_batch(
            batch, "forward_split,reverse_split", start_str, end_str, errored_tickers
        )
        for rec in split_records:
            symbol_id = ticker_to_symbol_id.get(rec.get("symbol"))
            if symbol_id is None:
                continue
            old_rate = rec.get("old_rate")
            new_rate = rec.get("new_rate")
            ex_date = rec.get("ex_date")
            if not old_rate or not new_rate or not ex_date:
                continue
            ratio = float(new_rate) / float(old_rate)
            split_rows.append((symbol_id, ex_date, ratio))
        time.sleep(config.REQUEST_DELAY_SECONDS)

        dividend_records = _fetch_corporate_actions_for_batch(
            batch, "cash_dividend", start_str, end_str, errored_tickers
        )
        for rec in dividend_records:
            symbol_id = ticker_to_symbol_id.get(rec.get("symbol"))
            if symbol_id is None:
                continue
            rate = rec.get("rate")
            ex_date = rec.get("ex_date")
            if rate is None or not ex_date:
                continue
            dividend_rows.append((symbol_id, ex_date, float(rate)))
        time.sleep(config.REQUEST_DELAY_SECONDS)

    # Alpaca can return the same corporate action more than once (e.g. across
    # overlapping pages), and ON CONFLICT DO UPDATE can't touch the same row
    # twice within a single statement -- dedupe on the same key as the DB's
    # unique constraint, keeping the last-seen record for each.
    deduped_splits = {(symbol_id, ex_date): (symbol_id, ex_date, ratio) for symbol_id, ex_date, ratio in split_rows}
    deduped_dividends = {(symbol_id, ex_date): (symbol_id, ex_date, amount) for symbol_id, ex_date, amount in dividend_rows}

    splits_upserted = 0
    if deduped_splits:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO splits (symbol_id, ex_date, ratio, created_at)
                VALUES %s
                ON CONFLICT (symbol_id, ex_date) DO UPDATE SET
                    ratio = EXCLUDED.ratio
                """,
                list(deduped_splits.values()),
                template="(%s, %s, %s, now())",
            )
            splits_upserted = len(deduped_splits)

    dividends_upserted = 0
    if deduped_dividends:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO dividends (symbol_id, ex_date, amount, created_at)
                VALUES %s
                ON CONFLICT (symbol_id, ex_date) DO UPDATE SET
                    amount = EXCLUDED.amount
                """,
                list(deduped_dividends.values()),
                template="(%s, %s, %s, now())",
            )
            dividends_upserted = len(deduped_dividends)

    summary = {
        "splits_upserted": splits_upserted,
        "dividends_upserted": dividends_upserted,
        "errored_tickers": errored_tickers,
    }
    logger.info(
        "Corporate actions refresh complete: %d splits, %d dividends, %d tickers errored",
        splits_upserted, dividends_upserted, len(errored_tickers),
    )
    return summary
