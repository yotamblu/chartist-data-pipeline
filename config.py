"""Shared configuration and logging setup for the ingestion job."""
import logging
import os
import sys

from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL")
ALPACA_API_KEY_ID = os.environ.get("ALPACA_API_KEY_ID")
ALPACA_API_SECRET_KEY = os.environ.get("ALPACA_API_SECRET_KEY")

NASDAQ_SYMBOL_DIR_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt"
ALPACA_BARS_URL = "https://data.alpaca.markets/v2/stocks/bars"
ALPACA_CORPORATE_ACTIONS_URL = "https://data.alpaca.markets/v1/corporate-actions"

# Exchange codes we care about, mapped from the NASDAQ file's "Listing Exchange" column.
EXCHANGE_CODE_MAP = {
    "Q": "NASDAQ",
    "N": "NYSE",
    "A": "AMEX",
}

BATCH_SIZE = 100
REQUEST_DELAY_SECONDS = 0.3
MAX_RETRIES = 5
CORPORATE_ACTIONS_LOOKBACK_DAYS = 7


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
            ("ALPACA_API_KEY_ID", ALPACA_API_KEY_ID),
            ("ALPACA_API_SECRET_KEY", ALPACA_API_SECRET_KEY),
        )
        if not value
    ]
    if missing:
        raise RuntimeError(
            f"Missing required environment variables: {', '.join(missing)}. "
            "Copy .env.example to .env and fill in real values."
        )
