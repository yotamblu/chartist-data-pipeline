"""Nightly ingestion job for Chartist.

Order of operations:
1. Check today is a NYSE trading day (exit 0, cleanly, if not -- unless
   --force is passed).
2. Refresh `symbols` from the NASDAQ symbol directory.
3. Refresh `splits` / `dividends` from EODHD (bulk API, per-symbol fallback).
4. Refresh `daily_prices` with today's bar from EODHD (bulk API, per-symbol fallback).
5. Log a summary and exit non-zero only if an entire step failed outright.
"""
import argparse
import sys
import time

import config
import db
from corporate_actions_refresh import refresh_corporate_actions
from prices_refresh import refresh_daily_prices
from symbols_refresh import refresh_symbols
from trading_day import eastern_today, is_trading_day

logger = config.setup_logging()


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Chartist nightly ingestion job")
    parser.add_argument(
        "--force",
        "-f",
        action="store_true",
        help=(
            "Skip the NYSE trading-day check and run the refresh steps "
            "regardless of what day it is. Useful for manual/local testing."
        ),
    )
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    start_time = time.monotonic()
    today = eastern_today()

    if not is_trading_day(today):
        if not args.force:
            logger.info("%s is not a NYSE trading day. Nothing to do, exiting cleanly.", today)
            return 0
        logger.warning(
            "%s is not a NYSE trading day, but --force was passed -- running the refresh anyway.",
            today,
        )

    try:
        config.require_env()
    except RuntimeError as exc:
        logger.error(str(exc))
        return 1

    symbol_summary = {"added": 0, "delisted": 0, "total_in_file": 0}
    corp_actions_summary = {"splits_upserted": 0, "dividends_upserted": 0, "errored_tickers": []}
    prices_summary = {"bars_inserted": 0, "errored_tickers": []}
    failed_steps = []

    logger.info("Step 1/3: refreshing symbols...")
    try:
        with db.get_connection() as conn:
            symbol_summary = refresh_symbols(conn)
    except Exception:
        logger.exception("Symbol refresh step failed outright")
        failed_steps.append("symbols")

    logger.info("Step 2/3: refreshing splits/dividends...")
    try:
        with db.get_connection() as conn:
            active_symbols = db.get_active_symbols(conn)
            corp_actions_summary = refresh_corporate_actions(conn, active_symbols)
    except Exception:
        logger.exception("Corporate actions refresh step failed outright")
        failed_steps.append("corporate_actions")

    logger.info("Step 3/3: refreshing daily prices...")
    try:
        with db.get_connection() as conn:
            active_symbols = db.get_active_symbols(conn)
            prices_summary = refresh_daily_prices(conn, active_symbols, today)
    except Exception:
        logger.exception("Daily prices refresh step failed outright")
        failed_steps.append("daily_prices")

    elapsed = time.monotonic() - start_time
    all_errored_tickers = sorted(
        set(corp_actions_summary.get("errored_tickers", []))
        | set(prices_summary.get("errored_tickers", []))
    )

    logger.info(
        "=== Nightly ingestion summary ===\n"
        "  Symbols added:      %d\n"
        "  Symbols delisted:   %d\n"
        "  Splits found:       %d\n"
        "  Dividends found:    %d\n"
        "  Price bars inserted:%d\n"
        "  Tickers errored:    %d%s\n"
        "  Steps failed:       %s\n"
        "  Runtime:            %.1fs",
        symbol_summary.get("added", 0),
        symbol_summary.get("delisted", 0),
        corp_actions_summary.get("splits_upserted", 0),
        corp_actions_summary.get("dividends_upserted", 0),
        prices_summary.get("bars_inserted", 0),
        len(all_errored_tickers),
        f" ({', '.join(all_errored_tickers)})" if all_errored_tickers else "",
        ", ".join(failed_steps) if failed_steps else "none",
        elapsed,
    )

    return 1 if failed_steps else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
