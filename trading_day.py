"""Determine whether today is a NYSE trading day."""
import datetime
import logging
from typing import Optional
from zoneinfo import ZoneInfo

import pandas_market_calendars as mcal

logger = logging.getLogger("chartist.trading_day")

EASTERN = ZoneInfo("America/New_York")


def eastern_today() -> datetime.date:
    """Today's date in US/Eastern -- the timezone the trading day is defined in.

    Deliberately not datetime.date.today(), which is the runner's local/system
    date. On a UTC-clocked runner, 02:00 UTC is still the previous evening in
    US/Eastern, so using the system date would check/process the wrong day.
    """
    return datetime.datetime.now(EASTERN).date()


def is_trading_day(check_date: Optional[datetime.date] = None) -> bool:
    check_date = check_date or eastern_today()
    nyse = mcal.get_calendar("NYSE")
    schedule = nyse.schedule(start_date=check_date, end_date=check_date)
    return not schedule.empty
