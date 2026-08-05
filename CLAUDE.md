# CLAUDE.md

Guidance for working in this repo: Chartist's nightly data ingestion job.

## What this is

A standalone Python job (not a web service) that runs once per NYSE trading
day, after market close, and keeps the shared Postgres/TimescaleDB database
current: symbol list, splits, dividends, and daily price bars. See
`README.md` for the full step-by-step and local setup instructions.

## Data provider: EODHD

Prices, splits, and dividends come from **EODHD** (paid "All World EOD"
plan, $19.99/month), via a single `EODHD_API_KEY`. This project previously
used Alpaca; that integration has been fully removed.

### Bulk-primary, per-symbol-fallback design

Both the splits/dividends step (`corporate_actions_refresh.py`) and the
daily prices step (`prices_refresh.py`) follow the same pattern:

1. **Primary: Bulk API.** `GET /api/eod-bulk-last-day/US` returns the
   entire US market for the most recent trading day in one request (add
   `&type=splits` / `&type=dividends` for corporate actions). This requires
   Bulk API activation on the EODHD account, requested from EODHD support
   separately from the plan purchase.
2. **Fallback: per-symbol calls.** If the bulk call comes back non-200
   (e.g. a 403 because Bulk API activation hasn't landed yet, or any other
   error), the code falls back to looping over active symbols individually
   (`/api/eod/{ticker}.US`, `/api/splits/{ticker}.US`, `/api/div/{ticker}.US`),
   scoped to a short recent window (5 days for prices, 7 for corporate
   actions — a nightly top-up, not a backfill), using a small thread pool
   (`config.FALLBACK_MAX_WORKERS`) with jittered backoff on 429s and a
   capped retry count (`config.MAX_RETRIES`, via `http_utils.request_with_retry`).

Whichever path actually ran is logged clearly (`"using EODHD Bulk API..."`
vs `"Bulk API unavailable, falling back to per-symbol requests..."`) so
it's visible in the job's output. Once EODHD confirms Bulk API activation,
no code change is needed — the primary path just starts succeeding instead
of falling through.

### Raw prices, not adjusted

Daily prices store the raw `close` field from EODHD, **never**
`adjusted_close`. This project stores raw OHLCV history and applies split
adjustments at read time elsewhere (in the `chartist-api` backend) — it
never rewrites stored history. The same principle applies to dividends:
store EODHD's `unadjustedValue`, not the split-adjusted `dividend`/`value`.

### Splits field format

EODHD's `split` field is a string `"new/old"` (e.g. `"4.000000/1.000000"`
for a 4-for-1 split, `"1.000000/10.000000"` for a 1-for-10 reverse split).
Parse it as `float(new) / float(old)` to get the numeric ratio the DB
expects (2.0 = 2-for-1, 0.5 = 1-for-2 reverse) — see
`corporate_actions_refresh._parse_split_ratio`.

### No fundamentals from EODHD

EODHD's lower/mid tiers (including the All World EOD plan used here) don't
include full company fundamentals access — confirmed via a 403 on this
plan. Company profile data (name, sector, description, etc.) comes from
Finnhub/FMP in the separate `chartist-api` backend, not from EODHD. Don't
add fundamentals calls here expecting them to work on this plan.

## New-symbol full history backfill (`price_backfill.py`)

The regular daily prices step (`prices_refresh.py`) only ever fetches a
short recent window (bulk = latest day, fallback = last 5 days) — fine for
symbols that already have price history, but useless for a symbol that was
just added and has zero rows in `daily_prices`. `price_backfill.py` exists
to close that gap, and runs as its own explicit step between the
splits/dividends refresh and the regular daily prices top-up, so it shows
up as a distinct phase in the logs rather than being folded silently into
the top-up.

- **Detecting "new"**: the `symbols.prices_loaded_at` column already means
  exactly "has this symbol's price history ever been loaded." A symbol
  just inserted by the symbols refresh step naturally has
  `prices_loaded_at = NULL`, so `db.get_symbols_needing_price_backfill`
  (`WHERE status = 'active' AND prices_loaded_at IS NULL`) is the only
  signal needed — there's no separate "is this new" tracking to maintain.
- **Always per-symbol, never bulk**: the Bulk API only ever returns the
  most recent single day and structurally cannot backfill history, so this
  step always calls `/api/eod/{ticker}.US` with `from=2000-01-01`. It reuses
  the same thread pool + jittered backoff as the other per-symbol EODHD
  fallback paths — full-history payloads are slow (~7s/symbol is normal,
  not a bug), so fetches run concurrently.
- **Chunked commits are load-bearing, not an optional nicety.** Inserts for
  each symbol's full history commit in small chunks (`INSERT_CHUNK_SIZE =
  150` rows), never as one multi-year transaction. A multi-year history
  spans many TimescaleDB hypertable chunks, and accumulating locks across
  all of them in a single uncommitted transaction caused "out of shared
  memory" errors on Aiven in an earlier version of this backfill. **If a
  future change "simplifies" this back into one big INSERT per symbol, it
  will reintroduce that bug** — don't do it, even though it looks like dead
  weight for a symbol with only a few years of history.
- **On failure, `prices_loaded_at` stays NULL** — no separate retry
  tracking. The symbol just gets picked up again by the same query on the
  next nightly run. Because chunk inserts use `ON CONFLICT DO NOTHING`,
  re-running a partially-backfilled symbol is safe and idempotent (some
  chunks re-insert as no-ops, the rest fill in).
- `prices_loaded_at` is only set (`db.mark_prices_loaded`, `now()`) after
  all chunks for a symbol have been stored successfully.

## What doesn't change often

- **Trading-day check** (`trading_day.py`) uses `pandas_market_calendars`
  and `America/New_York` for "today" — deliberately not UTC or the runner's
  system clock, since a UTC-clocked runner's date can be a day ahead of
  the US/Eastern trading day. Don't reintroduce a UTC-based date check here.
- **Symbols refresh** (`symbols_refresh.py`) has nothing to do with the
  price data provider — it's driven entirely by the NASDAQ Trader symbol
  directory file, independent of Alpaca/EODHD/whatever else prices come
  from. It filters to Listing Exchange Q/N/A/P (NASDAQ/NYSE/AMEX/ARCA),
  skips test issues, and skips tickers with special characters (preferred
  shares, warrants, units are intentionally excluded from this project's
  universe). `config.EXCHANGE_CODE_MAP` is the single source of truth for
  which Listing Exchange codes are in scope -- a code missing from that map
  silently drops every ticker on that exchange out of the file on the very
  next run, which reads as a mass "delisting" even though nothing actually
  changed on the exchange (this happened for real with ARCA/"P" -- it was
  never in the map from the initial commit onward, so every ARCA-listed
  symbol looked delisted until the map was fixed). When adding a new
  exchange to the universe, update both `EXCHANGE_CODE_MAP` and
  `_ensure_exchanges` in `symbols_refresh.py` together.
- **Idempotent writes everywhere.** Every write is an upsert or a
  conditional update (`ON CONFLICT ... DO UPDATE` / `DO NOTHING`) because
  this job sometimes fails partway through and gets re-run. Any new write
  path must preserve this — a second run for the same data must never
  create duplicates or corrupt existing rows.
- **Database schema** is out of scope for this job — it reads/writes
  against tables owned elsewhere; don't alter table definitions here.
