"""Weekly and fortnightly cut-offs.

Instalment lending does not run on months. Buy-now-pay-later is four fortnightly
payments, payday lending is weekly or monthly against a pay cycle, and
group-collected microfinance is weekly. Modelled on the nearest supported
cadence — daily — a four-instalment plan carries 84 rows instead of 4, which is
twenty times the data describing the same product.
"""

from __future__ import annotations

from itertools import pairwise

import pandas as pd
import pytest

# `sdd` first: importing `sdd.calendar` on its own leaves the package partially
# initialised and the generate subpackage cannot then find it.
from sdd import api
from sdd.calendar import period_dates
from sdd.spec.schema import Calendar


@pytest.mark.parametrize(
    ("freq", "gap_days", "per_year"),
    [("week_end", 7, 52.18), ("fortnight_end", 14, 26.09)],
)
def test_the_cut_offs_are_evenly_spaced(freq, gap_days, per_year):
    dates = period_dates(Calendar(start="2026-01-05", periods=10, freq=freq))
    gaps = {(b - a).days for a, b in pairwise(dates)}
    assert gaps == {gap_days}
    assert Calendar(start="2026-01-05", periods=2, freq=freq).periods_per_year == pytest.approx(
        per_year, abs=0.01
    )


@pytest.mark.parametrize("freq", ["week_end", "fortnight_end"])
def test_every_cut_off_lands_on_the_same_weekday(freq):
    """Anchored on Sunday, the usual collection week end for instalment lending."""
    dates = period_dates(Calendar(start="2026-01-05", periods=8, freq=freq))
    assert {d.weekday() for d in dates} == {6}


def test_a_year_is_365_25_days_not_52_weeks():
    """Rounding to 52 and 26 would bias every annual-to-period rate conversion.

    A hazard given as 12% a year would convert slightly wrong every period and
    compound across the panel — small, invisible, and in one direction.
    """
    assert Calendar(start="2026-01-05", periods=2, freq="week_end").periods_per_year > 52.0
    assert Calendar(start="2026-01-05", periods=2, freq="fortnight_end").periods_per_year > 26.0


def test_a_fortnight_is_two_weeks_of_the_same_anchor():
    weekly = period_dates(Calendar(start="2026-01-05", periods=9, freq="week_end"))
    fortnightly = period_dates(Calendar(start="2026-01-05", periods=5, freq="fortnight_end"))
    assert fortnightly == weekly[::2]


def test_the_existing_cadences_are_unchanged():
    """Month-end anchoring is what every shipped pack relies on."""
    dates = period_dates(Calendar(start="2024-01-15", periods=3, freq="month_end"))
    assert [d.strftime("%Y-%m-%d") for d in dates] == ["2024-01-31", "2024-02-29", "2024-03-31"]

    quarterly = period_dates(Calendar(start="2024-01-15", periods=3, freq="quarter_end"))
    assert [d.strftime("%Y-%m-%d") for d in quarterly] == [
        "2024-03-31",
        "2024-06-30",
        "2024-09-30",
    ]


def test_a_fortnightly_product_runs_end_to_end(tmp_path):
    """The cadence has to reach a whole run, not just the date list.

    Four instalments, a plan that settles when the last is paid, and a write-off
    path for the ones that do not.
    """
    spec = {
        "spec_version": 1,
        "meta": {"name": "bnpl", "title": "BNPL", "asset_class": "bnpl"},
        "entity": {
            "id_column": "plan_id",
            "time_column": "reporting_date",
            "calendar": {"start": "2026-01-05", "periods": 8, "freq": "fortnight_end"},
        },
        "columns": [
            {
                "name": "plan_id",
                "role": "static",
                "dtype": "str",
                "generator": {"kind": "sequence", "prefix": "P", "width": 8},
            },
            {
                "name": "reporting_date",
                "role": "dynamic",
                "dtype": "str",
                "generator": {"kind": "constant", "value": "2026-01-11"},
            },
            {
                "name": "order_value",
                "role": "static",
                "dtype": "float",
                "min": 0.0,
                "generator": {
                    "kind": "scipy",
                    "dist": "lognorm",
                    "params": {"s": 0.6, "loc": 0.0, "scale": 90.0},
                    "decimals": 2,
                    "clip_min": 15.0,
                },
            },
            {
                "name": "instalments_paid",
                "role": "dynamic",
                "dtype": "int",
                "generator": {"kind": "constant", "value": 0},
            },
            {
                "name": "instalments_remaining",
                "role": "dynamic",
                "dtype": "int",
                "generator": {"kind": "constant", "value": 4},
            },
            {
                "name": "plan_status",
                "role": "dynamic",
                "dtype": "category",
                "domain": ["Current", "Late", "Settled"],
                "generator": {
                    "kind": "categorical",
                    "values": ["Current", "Late"],
                    "weights": [0.95, 0.05],
                },
            },
        ],
        "lifecycle": {
            "state_column": "plan_status",
            "states": ["Current", "Late", "Settled"],
            "terminal": ["Settled"],
            "transitions": [[0.93, 0.07], [0.60, 0.40]],
            "hazards": [
                {
                    "kind": "condition",
                    "name": "settled",
                    "when": "instalments_remaining <= 0",
                    "to_state": "Settled",
                }
            ],
        },
        "dynamics": {
            "counters": [
                {
                    "column": "instalments_paid",
                    "expr": "min(instalments_paid + 1, 4)",
                    "clip_max": 4,
                    "dtype": "int",
                },
                {
                    "column": "instalments_remaining",
                    "expr": "max(4 - instalments_paid, 0)",
                    "clip_min": 0,
                    "dtype": "int",
                },
            ]
        },
        "emit": {"filename": "bnpl_{yyyy_mm_dd}.csv", "write_panel": True},
        "validation": {"checks": {"closed_pool": True}},
    }

    result = api.run(spec, 500, tmp_path, seed=9)
    assert result["validation"]["passed"], [
        c["name"] for c in result["validation"]["checks"] if not c["passed"]
    ]

    panel = pd.read_parquet(result["panel"])
    cutoffs = sorted(pd.to_datetime(panel["reporting_date"]).unique())
    assert (cutoffs[1] - cutoffs[0]).days == 14

    settled = panel[panel["plan_status"] == "Settled"]
    assert not settled.empty, "no plan ever completed its four instalments"
    assert (settled["instalments_paid"] == 4).all()
