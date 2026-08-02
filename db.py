"""Postgres connection helpers."""
import logging
from contextlib import contextmanager

import psycopg2
import psycopg2.extras

import config

logger = logging.getLogger("chartist.db")


@contextmanager
def get_connection():
    """Yield a psycopg2 connection, committing on success and rolling back on error."""
    conn = psycopg2.connect(config.DATABASE_URL)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_active_symbols(conn):
    """Return a list of dicts: {symbol_id, ticker, exchange_id} for all active symbols."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT symbol_id, ticker, exchange_id FROM symbols WHERE status = 'active'"
        )
        return cur.fetchall()


def get_exchange_id_map(conn):
    """Return {exchange_code: exchange_id} for exchanges already present in the DB."""
    with conn.cursor() as cur:
        cur.execute("SELECT code, exchange_id FROM exchanges")
        return {code: exchange_id for code, exchange_id in cur.fetchall()}
