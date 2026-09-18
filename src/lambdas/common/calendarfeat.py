"""Calendar features -- pipelines.md step 4.6, shared by BOTH pipelines.

The training path (Phase 4, running on EMR Serverless) and the inference path
(Pipeline 2, running in a Lambda) import this same module. That is a
requirement, not a convenience: "Calendar features come from shared code with
4.6" (README section 5). Two implementations of "is this a holiday" drift, and
the drift is invisible offline.

US federal holidays are computed here rather than taken from the `holidays`
package for the same reason. The package is not in the EMR Serverless image and
not in the Lambda runtime, so using it would mean two installs that can pin
different versions -- reintroducing exactly the skew this module exists to
prevent. The federal rules are fixed law and fit in fifty lines.

Hour and weekday are encoded CYCLICALLY. Without it hour 23 is maximally
distant from hour 0, which is the opposite of the truth and costs the temporal
arm real signal.
"""

from __future__ import annotations

import math
from datetime import date, timedelta

_TAU = 2.0 * math.pi


# --------------------------------------------------------------------------
# US federal holidays
# --------------------------------------------------------------------------
def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    """The nth `weekday` (Mon=0) of a month. n=-1 means the last one."""
    if n > 0:
        first = date(year, month, 1)
        offset = (weekday - first.weekday()) % 7
        return first + timedelta(days=offset + 7 * (n - 1))

    next_month = date(year + (month == 12), (month % 12) + 1, 1)
    last = next_month - timedelta(days=1)
    return last - timedelta(days=(last.weekday() - weekday) % 7)


def _observed(d: date) -> list[date]:
    """Fixed-date holidays shift when they land on a weekend.

    Both the actual date and the observed date are returned: transit demand
    responds to the observed day off, but the calendar date still reads as a
    holiday to riders.
    """
    if d.weekday() == 5:
        return [d, d - timedelta(days=1)]
    if d.weekday() == 6:
        return [d, d + timedelta(days=1)]
    return [d]


def us_federal_holidays(year: int) -> set[date]:
    days: set[date] = set()

    for fixed in (date(year, 1, 1), date(year, 6, 19), date(year, 7, 4),
                  date(year, 11, 11), date(year, 12, 25)):
        days.update(_observed(fixed))

    days.add(_nth_weekday(year, 1, 0, 3))    # MLK Day, 3rd Monday of January
    days.add(_nth_weekday(year, 2, 0, 3))    # Washington's Birthday
    days.add(_nth_weekday(year, 5, 0, -1))   # Memorial Day, last Monday of May
    days.add(_nth_weekday(year, 9, 0, 1))    # Labor Day
    days.add(_nth_weekday(year, 10, 0, 2))   # Columbus Day
    days.add(_nth_weekday(year, 11, 3, 4))   # Thanksgiving, 4th Thursday

    return days


_HOLIDAY_CACHE: dict[int, set[date]] = {}


def is_holiday(d: date) -> bool:
    year = d.year
    if year not in _HOLIDAY_CACHE:
        _HOLIDAY_CACHE[year] = us_federal_holidays(year)
    return d in _HOLIDAY_CACHE[year]


# --------------------------------------------------------------------------
# Feature derivation
# --------------------------------------------------------------------------
def calendar_features(local_dt) -> dict:
    """Derive every `temporal` feature in features.yaml from a LOCAL timestamp.

    The input must already be localised to the project timezone. Passing a UTC
    timestamp here silently shifts every hour-of-day feature by 4-5 hours,
    which the model will happily learn around and the inference path will not.
    """
    hour = local_dt.hour
    dow = local_dt.weekday()
    day = local_dt.date() if hasattr(local_dt, "date") else local_dt

    return {
        "hour_sin": math.sin(_TAU * hour / 24.0),
        "hour_cos": math.cos(_TAU * hour / 24.0),
        "dow_sin": math.sin(_TAU * dow / 7.0),
        "dow_cos": math.cos(_TAU * dow / 7.0),
        "is_weekend": 1.0 if dow >= 5 else 0.0,
        "is_holiday": 1.0 if is_holiday(day) else 0.0,
        "month": float(local_dt.month),
    }


FEATURE_NAMES = [
    "hour_sin", "hour_cos", "dow_sin", "dow_cos",
    "is_weekend", "is_holiday", "month",
]
