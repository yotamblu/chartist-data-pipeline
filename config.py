"""Shared configuration and logging setup for the ingestion job."""
import logging
import os
import sys

from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL")
EODHD_API_KEY = os.environ.get("EODHD_API_KEY")

NASDAQ_SYMBOL_DIR_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt"

# EODHD Bulk API -- one request returns the whole US market for the most
# recent trading day. Primary path for prices/splits/dividends; requires
# Bulk API activation on the account (requested from EODHD support). Falls
# back to the per-symbol endpoints below if it isn't active yet.
EODHD_BULK_URL = "https://eodhd.com/api/eod-bulk-last-day/US"

# Per-symbol fallback endpoints, ".US" suffix required by EODHD for US-listed tickers.
EODHD_EOD_URL_TEMPLATE = "https://eodhd.com/api/eod/{ticker}.US"
EODHD_SPLITS_URL_TEMPLATE = "https://eodhd.com/api/splits/{ticker}.US"
EODHD_DIVIDENDS_URL_TEMPLATE = "https://eodhd.com/api/div/{ticker}.US"

# Exchange codes we care about, mapped from the NASDAQ file's "Listing Exchange" column.
EXCHANGE_CODE_MAP = {
    "Q": "NASDAQ",
    "N": "NYSE",
    "A": "AMEX",
}

MAX_RETRIES = 5
CORPORATE_ACTIONS_LOOKBACK_DAYS = 7
# Per-symbol fallback for prices is a nightly top-up, not a backfill --
# a few days of overlap is enough to self-heal a missed run or two, since
# writes are idempotent (ON CONFLICT DO NOTHING).
PRICE_LOOKBACK_DAYS = 5
# Concurrency for the per-symbol fallback paths.
FALLBACK_MAX_WORKERS = 6


def setup_logging() -> logging.Logger:
    logger = logging.getLogger("chartist")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def require_env() -> None:
    missing = [
        name
        for name, value in (
            ("DATABASE_URL", DATABASE_URL),
            ("EODHD_API_KEY", EODHD_API_KEY),
        )
        if not value
    ]
    if missing:
        raise RuntimeError(
            f"Missing required environment variables: {', '.join(missing)}. "
            "Copy .env.example to .env and fill in real values."
        )
