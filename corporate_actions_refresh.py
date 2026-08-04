"""Refresh `splits` and `dividends` from EODHD.

Primary path: EODHD's Bulk API (`/api/eod-bulk-last-day/US`) with
`type=splits` and `type=dividends`, covering the whole US market in two
requests.

Fallback path: if the Bulk API isn't activated on this account yet (a 403 or
other plan-related error), loop over active symbols individually against
`/api/splits/{ticker}.US` and `/api/div/{ticker}.US`, scoped to a short
recent window, using a small thread pool. Whichever path runs is logged
clearly.

Dividends are stored using EODHD's `unadjustedValue` (falling back to
`dividend`/`value` if that's ever missing), consistent with this project's
raw-data-as-source-of-truth principle -- prices are stored raw and adjusted
for splits at read time elsewhere, never by rewriting stored history.
"""
import concurrent.futures
import datetime
import logging

import psycopg2.extras

import config
from http_utils import request_with_retry

logger = logging.getLogger("chartist.corporate_actions")


def _parse_split_ratio(split_str):
    """Parse EODHD's "new/old" split string (e.g. "4.000000/1.000000") into a
    numeric ratio: 2.0 for a 2-for-1 split, 0.5 for a 1-for-2 reverse split."""
    if not split_str:
        return None
    try:
        new, old = split_str.split("/")
        return float(new) / float(old)
    except (ValueError, ZeroDivisionError):
        return None


def _fetch_bulk(action_type):
    """Fetch a bulk splits/dividends file. Returns a list of raw dicts, or
    None if the Bulk API isn't available (not activated, plan error, etc.)."""
    response = request_with_retry(
        "GET", config.EODHD_BULK_URL,
        params={"api_token": config.EODHD_API_KEY, "fmt": "json", "type": action_type},
    )
    if response.status_code != 200:
        logger.warning(
            "Bulk %s endpoint returned %d, treating Bulk API as unavailable: %s",
            action_type, response.status_code, response.text[:200],
        )
        return None
    return response.json()


def _fetch_splits_for_symbol(ticker, from_date_str):
    url = config.EODHD_SPLITS_URL_TEMPLATE.format(ticker=ticker)
    response = request_with_retry(
        "GET", url, params={"api_token": config.EODHD_API_KEY, "fmt": "json", "from": from_date_str}
    )
    if response.status_code != 200:
        logger.warning("Splits request failed (%d) for %s: %s", response.status_code, ticker, response.text[:200])
        raise RuntimeError(f"Splits request failed ({response.status_code}) for {ticker}")
    return response.json() or []


def _fetch_dividends_for_symbol(ticker, from_date_str):
    url = config.EODHD_DIVIDENDS_URL_TEMPLATE.format(ticker=ticker)
    response = request_with_retry(
        "GET", url, params={"api_token": config.EODHD_API_KEY, "fmt": "json", "from": from_date_str}
    )
    if response.status_code != 200:
        logger.warning("Dividends request failed (%d) for %s: %s", response.status_code, ticker, response.text[:200])
        raise RuntimeError(f"Dividends request failed ({response.status_code}) for {ticker}")
    return response.json() or []


def _unadjusted_dividend_amount(rec):
    amount = rec.get("unadjustedValue")
    if amount is None:
        amount = rec.get("dividend", rec.get("value"))
    return amount


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

    errored_tickers = []
    split_rows = []
    dividend_rows = []

    bulk_splits = _fetch_bulk("splits")
    bulk_dividends = _fetch_bulk("dividends")

    if bulk_splits is not None and bulk_dividends is not None:
        logger.info(
            "Splits/dividends: using EODHD Bulk API (whole US market, 2 requests); "
            "%d active tickers to match against %d split record(s) and %d dividend record(s)",
            len(ticker_to_symbol_id), len(bulk_splits), len(bulk_dividends),
        )

        for rec in bulk_splits:
            symbol_id = ticker_to_symbol_id.get(rec.get("code"))
            if symbol_id is None:
                continue
            ratio = _parse_split_ratio(rec.get("split"))
            ex_date = rec.get("date")
            if ratio is None or not ex_date:
                continue
            split_rows.append((symbol_id, ex_date, ratio))

        for rec in bulk_dividends:
            symbol_id = ticker_to_symbol_id.get(rec.get("code"))
            if symbol_id is None:
                continue
            amount = _unadjusted_dividend_amount(rec)
            ex_date = rec.get("date")
            if amount is None or not ex_date:
                continue
            dividend_rows.append((symbol_id, ex_date, float(amount)))
    else:
        tickers = sorted(ticker_to_symbol_id.keys())
        end = datetime.date.today()
        from_date_str = (end - datetime.timedelta(days=config.CORPORATE_ACTIONS_LOOKBACK_DAYS)).isoformat()
        logger.warning(
            "Splits/dividends: Bulk API unavailable, falling back to per-symbol requests "
            "(%d active tickers, %d workers, window since %s)",
            len(tickers), config.FALLBACK_MAX_WORKERS, from_date_str,
        )

        with concurrent.futures.ThreadPoolExecutor(max_workers=config.FALLBACK_MAX_WORKERS) as pool:
            futures = {
                pool.submit(_fetch_splits_for_symbol, ticker, from_date_str): ticker
                for ticker in tickers
            }
            for future in concurrent.futures.as_completed(futures):
                ticker = futures[future]
                try:
                    records = future.result()
                except Exception:
                    logger.exception("Splits fetch failed for %s", ticker)
                    errored_tickers.append(ticker)
                    continue
                symbol_id = ticker_to_symbol_id[ticker]
                for rec in records:
                    ratio = _parse_split_ratio(rec.get("split"))
                    ex_date = rec.get("date")
                    if ratio is None or not ex_date:
                        continue
                    split_rows.append((symbol_id, ex_date, ratio))

        with concurrent.futures.ThreadPoolExecutor(max_workers=config.FALLBACK_MAX_WORKERS) as pool:
            futures = {
                pool.submit(_fetch_dividends_for_symbol, ticker, from_date_str): ticker
                for ticker in tickers
            }
            for future in concurrent.futures.as_completed(futures):
                ticker = futures[future]
                try:
                    records = future.result()
                except Exception:
                    logger.exception("Dividends fetch failed for %s", ticker)
                    errored_tickers.append(ticker)
                    continue
                symbol_id = ticker_to_symbol_id[ticker]
                for rec in records:
                    amount = _unadjusted_dividend_amount(rec)
                    ex_date = rec.get("date")
                    if amount is None or not ex_date:
                        continue
                    dividend_rows.append((symbol_id, ex_date, float(amount)))

    # EODHD can return the same corporate action more than once (e.g. if a
    # symbol appears twice, or bulk + per-symbol overlap during testing), and
    # ON CONFLICT DO UPDATE can't touch the same row twice within a single
    # statement -- dedupe on the same key as the DB's unique constraint,
    # keeping the last-seen record for each.
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
        "errored_tickers": sorted(set(errored_tickers)),
    }
    logger.info(
        "Corporate actions refresh complete: %d splits, %d dividends, %d tickers errored",
        splits_upserted, dividends_upserted, len(summary["errored_tickers"]),
    )
    return summary
