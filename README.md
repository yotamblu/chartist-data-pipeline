# Chartist Nightly Ingestion

Nightly data ingestion job for the Chartist stock analysis platform. Runs once
per US trading day, after market close, and keeps the Postgres/TimescaleDB
database current: symbol list, splits, dividends, and daily price bars.

## What it does

Each run, in order:

1. **Trading day check** — uses `pandas_market_calendars` to confirm today is
   a NYSE trading day. If not (weekend/holiday), logs that and exits `0`.
2. **Symbol refresh** — downloads the NASDAQ Trader symbol directory,
   upserts ticker/name/security_type for NASDAQ/NYSE/AMEX-listed stocks and
   ETFs, marks symbols that dropped out of the file as `delisted`, and
   inserts brand-new tickers as `active`.
3. **Splits & dividends refresh** — pulls the last 7 days of corporate
   actions from Alpaca for all active symbols (batched 100 tickers/request)
   and upserts into `splits` / `dividends`.
4. **Daily prices refresh** — pulls today's daily bar from Alpaca for all
   active symbols and inserts into `daily_prices`.
5. **Summary log** — symbols added/delisted, splits/dividends found, bars
   inserted, any tickers that errored out, and total runtime. Exits non-zero
   only if an entire step failed outright (individual skipped/errored
   tickers are expected and don't fail the run).

## Project layout

```
config.py                    env loading + logging setup
db.py                        Postgres connection helper
http_utils.py                HTTP retry/backoff (429s, 5xx)
trading_day.py                NYSE trading-day check
symbols_refresh.py           step 2
corporate_actions_refresh.py step 3
prices_refresh.py            step 4
main.py                      orchestrates all of the above
.github/workflows/nightly-ingestion.yml
```

## Local setup

1. Install dependencies (Python 3.11+ recommended):

   ```
   pip install -r requirements.txt
   ```

2. Copy `.env.example` to `.env` and fill in real values:

   ```
   cp .env.example .env
   ```

   - `DATABASE_URL` — Postgres connection string for the existing Chartist DB.
   - `ALPACA_API_KEY_ID` / `ALPACA_API_SECRET_KEY` — Alpaca Market Data API
     credentials.

   `.env` is loaded automatically via `python-dotenv` and is gitignored — never
   commit real secrets.

## Running it manually

```
python main.py
```

Logs go to stdout with timestamps. On a non-trading day it logs that and
exits `0` without touching anything else. On a trading day it runs all four
steps and prints a summary at the end.

To run the refresh regardless of whether today is a NYSE trading day
(useful for local testing on a weekend/holiday), pass `--force` / `-f`:

```
python main.py --force
```

This only bypasses the trading-day check — it logs a warning that it's
running out-of-band, then runs the same four steps as normal.

## Why it's safe to re-run

Every write is an upsert or a conditional update, so re-running after a
partial failure never creates duplicates or corrupts data:

- **Symbols**: `ON CONFLICT (ticker, exchange_id) DO UPDATE` — re-running
  just re-applies the same name/security_type/status. Delisting only flips
  symbols still marked `active` that are missing from the current file, so
  it's stable across runs.
- **Splits / dividends**: unique on `(symbol_id, ex_date)`, `ON CONFLICT DO
  UPDATE` — re-fetching the same 7-day window just overwrites the same rows
  with the same values.
- **Daily prices**: unique on `(symbol_id, trade_date)`, `ON CONFLICT DO
  NOTHING` — a second run for the same day is a no-op once the bar exists.
- Each of the three refresh steps runs in its own DB transaction/connection,
  so a failure in one step (e.g. prices) doesn't roll back or block the
  others, and the next run picks up cleanly.
- Ticker-level errors (e.g. Alpaca 400s on tickers with special characters
  like `CMS$C`) are isolated via batch-bisection, logged, and skipped —
  they don't fail the whole step or the run.

## GitHub Actions

`.github/workflows/nightly-ingestion.yml` runs on a cron schedule at
**02:00 UTC, Tuesday–Saturday**. That's always after 9pm US/Eastern for the
preceding weekday, whether Eastern is on EST (9pm ET = 02:00 UTC) or EDT
(9pm ET = 01:00 UTC) — 02:00 UTC is safely past market close either way.
`main.py` itself still checks the NYSE calendar and exits cleanly on
holidays, so the workflow doesn't need to special-case those. You can also
trigger it manually from the Actions tab (`workflow_dispatch`).

### Configuring secrets

In the GitHub repo, go to **Settings → Secrets and variables → Actions** and
add three repository secrets with the same names used locally in `.env`:

- `DATABASE_URL`
- `ALPACA_API_KEY_ID`
- `ALPACA_API_SECRET_KEY`

The workflow passes these through as environment variables to `main.py`,
exactly like `.env` does locally.
