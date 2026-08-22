"""Observation dates for a panel.

A ``Calendar`` in the spec says "start here, produce this many cut-offs, at this
frequency". This module turns that into the actual list of dates and into the
filename each one is written under.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:  # pragma: no cover
    # Imported for typing only. At runtime this module is reached from
    # `sdd.generate.book`, and importing the schema here would close a loop:
    # calendar -> spec -> loader -> generate -> book -> calendar. Nothing hit it
    # because every caller happened to import `sdd.generate` first, so the loop
    # was already broken by the time calendar was reached — but
    # `from sdd.calendar import period_dates` in a fresh interpreter failed.
    from sdd.spec.schema import Calendar

_OFFSETS = {
    # Weekly cadences anchor on Sunday, which is the usual collection week end
    # for instalment lending. A fortnight is two of them rather than a separate
    # offset, because pandas has no native one.
    "week_end": pd.offsets.Week,
    "month_end": pd.offsets.MonthEnd,
    "month_start": pd.offsets.MonthBegin,
    "quarter_end": pd.offsets.QuarterEnd,
    "year_end": pd.offsets.YearEnd,
}


def period_dates(cal: Calendar) -> list[pd.Timestamp]:
    """Every cut-off date, first one included.

    The start date is snapped to the frequency's boundary — a calendar starting
    ``2024-01-15`` with ``month_end`` produces ``2024-01-31`` first, matching how
    loan tapes are actually cut.
    """
    base = pd.Timestamp(cal.start)
    if cal.freq == "day":
        return [base + pd.Timedelta(days=i) for i in range(cal.periods)]
    if cal.freq == "fortnight_end":
        first = base + pd.offsets.Week(weekday=6)
        return [first + pd.Timedelta(weeks=2 * i) for i in range(cal.periods)]

    offset_cls = _OFFSETS[cal.freq]
    first = base + (offset_cls(weekday=6) if cal.freq == "week_end" else offset_cls(0))
    dates = [first]
    step = offset_cls(weekday=6) if cal.freq == "week_end" else offset_cls(1)
    for _ in range(1, cal.periods):
        dates.append(dates[-1] + step)
    return dates


def format_filename(template: str, date: pd.Timestamp, period: int, name: str) -> str:
    """Fill a per-period filename template.

    Placeholders: ``{name} {yyyy} {mm} {dd} {yyyymm} {yyyy_mm_dd} {period}``.
    """
    return template.format(
        name=name,
        yyyy=f"{date.year:04d}",
        mm=f"{date.month:02d}",
        dd=f"{date.day:02d}",
        yyyymm=f"{date.year:04d}{date.month:02d}",
        yyyy_mm_dd=date.strftime("%Y-%m-%d"),
        period=period,
    )


def first_business_day(year: int, month: int = 1) -> pd.Timestamp:
    """First weekday of a month — the ESMA convention for a deal closing date."""
    d = pd.Timestamp(year=year, month=month, day=1)
    while d.weekday() >= 5:  # Saturday=5, Sunday=6
        d += pd.Timedelta(days=1)
    return d
