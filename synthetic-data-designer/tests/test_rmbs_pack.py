"""End-to-end on the RMBS pack — the proof the generalisation is lossless.

The pack is a declarative re-expression of upstream deeploans' hardcoded Python.
If the spec-driven engine can reproduce its schema, its arithmetic, and its
lifecycle behaviour, then nothing deal-specific is left in code.

The 71-column header is asserted literally rather than by reading upstream, so
this suite is self-contained and CI does not need the deeploans checkout.
``scripts/parity_check.py`` does the live comparison against upstream.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from sdd.age.panel import run_ageing
from sdd.generate import build_book
from sdd.spec import load_spec

# The exact Hypoport / ESMA Annex 2 header, in order.
HYPOPORT_COLUMNS = [
    "loan_id",
    "transaction_name",
    "esma_transaction_identifier",
    "reporting_date",
    "closing_date",
    "originator_name",
    "servicer_name",
    "currency",
    "country",
    "origination_year",
    "maturity_date_proxy",
    "original_balance",
    "current_balance",
    "repayment_type",
    "interest_only_flag",
    "current_interest_rate_pct",
    "rate_type",
    "remaining_interest_fixed_period_months",
    "fixed_interest_period_end_in_months",
    "seasoning_months",
    "remaining_term_months",
    "legal_maturity_months",
    "loan_part_count",
    "debtor_count",
    "property_type",
    "province",
    "economic_region_nuts3",
    "construction_year",
    "occupancy",
    "property_usage",
    "employment_status",
    "self_employed_flag",
    "borrower_type",
    "loan_purpose",
    "buy_to_let_flag",
    "nhg_flag",
    "guarantee_type",
    "oltomv_original",
    "cltomv_current",
    "cltimv_current",
    "original_market_value_at_origination",
    "current_original_market_value",
    "indexed_market_value",
    "property_valuation_type",
    "loan_to_income",
    "payment_due_to_income_pct",
    "borrower_annual_income",
    "scheduled_monthly_payment",
    "arrears_bucket",
    "arrears_amount",
    "days_past_due",
    "default_crr_flag",
    "performing_status",
    "foreclosure_flag",
    "forbearance_flag",
    "restructuring_flag",
    "epc_label",
    "epc_issue_year",
    "primary_energy_demand_kwh_m2",
    "construction_deposit_flag",
    "construction_deposit_pct",
    "construction_deposit_amount",
    "interest_payment_frequency",
    "principal_payment_frequency",
    "balance_bucket",
    "cltomv_current_bucket",
    "cltimv_current_bucket",
    "oltomv_original_bucket",
    "loan_to_income_bucket",
    "payment_due_to_income_pct_bucket",
    "construction_year_bucket",
]

N = 3000
SEED = 42


@pytest.fixture(scope="module")
def spec(rmbs_spec_path_module):
    return load_spec(rmbs_spec_path_module)


@pytest.fixture(scope="module")
def rmbs_spec_path_module():
    from tests.conftest import PACKS

    return PACKS / "rmbs_nl_green_lion.yaml"


@pytest.fixture(scope="module")
def book(spec) -> pd.DataFrame:
    return build_book(spec, N, seed=SEED)


@pytest.fixture(scope="module")
def panel(spec, book, tmp_path_factory) -> tuple[pd.DataFrame, dict]:
    out = tmp_path_factory.mktemp("rmbs")
    result = run_ageing(spec, book, out, seed=SEED)
    return pd.read_parquet(out / spec.emit.panel_filename), result


# ---------------------------------------------------------------------------
# schema parity — exact
# ---------------------------------------------------------------------------


def test_schema_matches_upstream_exactly(spec):
    assert spec.output_columns() == HYPOPORT_COLUMNS
    assert len(HYPOPORT_COLUMNS) == 71


def test_book_emits_those_columns_in_that_order(spec, book):
    assert list(book[spec.output_columns()].columns) == HYPOPORT_COLUMNS


def test_filenames_follow_the_hypoport_convention(panel):
    _, result = panel
    names = [f.rsplit("/", 1)[-1] for f in result["files"]]
    assert names[0] == "green_lion_202401_1_synthetic_loan_tape.csv"
    assert names[-1] == "green_lion_202512_1_synthetic_loan_tape.csv"
    assert len(names) == 24


# ---------------------------------------------------------------------------
# period-0 arithmetic
# ---------------------------------------------------------------------------


def test_ids_are_sequential_and_unique(book):
    assert book["loan_id"].is_unique
    assert book["loan_id"].iloc[0] == "GL2024_000001"
    assert book["loan_id"].iloc[-1] == f"GL2024_{N:06d}"


def test_market_value_inverts_the_original_ltv(book):
    expected = book["original_balance"] / (book["oltomv_original"] / 100.0)
    np.testing.assert_allclose(
        book["original_market_value_at_origination"], expected.round(2), atol=0.01
    )


def test_nhg_respects_the_balance_cap(book):
    """No loan above the 2024 NHG limit may carry the guarantee, whatever the
    underlying coin flip said."""
    over_cap = book[book["original_balance"] > 435_000]
    assert (over_cap["nhg_flag"] == "N").all()
    assert (book.loc[book["nhg_flag"] == "Y", "guarantee_type"] == "NHG").all()
    assert (book.loc[book["nhg_flag"] == "N", "guarantee_type"] == "None").all()


def test_interest_only_loans_are_not_amortised_at_period_zero(book):
    io = book[book["interest_only_flag"] == "Y"]
    assert len(io) > 0
    np.testing.assert_allclose(io["current_balance"], io["original_balance"])


def test_amortising_loans_have_paid_something_down(book):
    amort = book[book["interest_only_flag"] == "N"]
    assert (amort["current_balance"] < amort["original_balance"]).all()


def test_interest_only_flag_follows_repayment_type(book):
    io_types = book["repayment_type"].isin(["InterestOnly", "Bullet"])
    assert (book.loc[io_types, "interest_only_flag"] == "Y").all()
    assert (book.loc[~io_types, "interest_only_flag"] == "N").all()


def test_nuts3_region_is_nested_inside_its_province(book):
    assert (book.loc[book["province"] == "Utrecht", "economic_region_nuts3"] == "NL310").all()
    zh = book.loc[book["province"] == "Zuid-Holland", "economic_region_nuts3"]
    assert set(zh) <= {"NL331", "NL332", "NL333", "NL337", "NL33A", "NL33B", "NL33C"}


def test_energy_demand_maps_from_the_epc_label(book):
    assert (book.loc[book["epc_label"] == "A+++", "primary_energy_demand_kwh_m2"] == 30).all()
    assert (book.loc[book["epc_label"] == "G", "primary_energy_demand_kwh_m2"] == 420).all()


def test_maturity_date_proxy_is_iso_formatted(book):
    assert book["maturity_date_proxy"].str.match(r"^\d{4}-\d{2}-28$").all()


def test_construction_deposit_only_on_building_purposes(book):
    flagged = book[book["construction_deposit_flag"] == "Y"]
    assert set(flagged["loan_purpose"]) <= {"Renovation", "Construction"}
    assert (flagged["construction_deposit_pct"].between(10, 25)).all()
    unflagged = book[book["construction_deposit_flag"] == "N"]
    assert (unflagged["construction_deposit_amount"] == 0).all()


def test_deal_level_constants_are_single_valued(book):
    for col in (
        "currency",
        "country",
        "originator_name",
        "closing_date",
        "transaction_name",
        "esma_transaction_identifier",
    ):
        assert book[col].nunique() == 1, col
    assert book["closing_date"].iloc[0] == "2024-01-01"


# ---------------------------------------------------------------------------
# panel behaviour
# ---------------------------------------------------------------------------


def test_panel_has_one_row_per_loan_per_period(panel):
    df, _ = panel
    assert (
        df.groupby("reporting_date")["loan_id"]
        .nunique()
        .equals(df.groupby("reporting_date").size())
    )


def test_reporting_dates_are_month_ends_in_order(panel):
    df, _ = panel
    dates = sorted(df["reporting_date"].unique())
    assert dates[0] == "2024-01-31"
    assert dates[-1] == "2025-12-31"
    assert len(dates) == 24


def test_pool_is_closed_and_shrinks(panel):
    """No loan may appear that was not in the first cut-off, and attrition must
    actually happen."""
    df, _ = panel
    first = set(df.loc[df["reporting_date"] == "2024-01-31", "loan_id"])
    assert set(df["loan_id"]) == first
    counts = df.groupby("reporting_date").size()
    assert counts.iloc[-1] < counts.iloc[0]


def test_static_columns_never_change_for_a_loan(panel):
    df, _ = panel
    static = [
        "origination_year",
        "original_balance",
        "province",
        "nhg_flag",
        "legal_maturity_months",
        "interest_only_flag",
        "scheduled_monthly_payment",
        "economic_region_nuts3",
        "borrower_annual_income",
        "epc_label",
        "construction_year",
    ]
    changing = df.groupby("loan_id")[static].nunique().max()
    assert (changing == 1).all(), f"these drifted: {changing[changing > 1].index.tolist()}"


def test_seasoning_advances_by_exactly_one_period(panel):
    df, _ = panel
    ordered = df.sort_values(["loan_id", "reporting_date"])
    delta = ordered.groupby("loan_id")["seasoning_months"].diff().dropna()
    assert (delta == 1).all()


def test_remaining_term_falls_by_exactly_one_period(panel):
    df, _ = panel
    ordered = df.sort_values(["loan_id", "reporting_date"])
    delta = ordered.groupby("loan_id")["remaining_term_months"].diff().dropna()
    assert delta.isin([-1, 0]).all()  # 0 only once the term has bottomed out


def test_terminal_states_end_the_loans_life(panel):
    """A redeemed or charged-off loan is written once, then leaves the pool."""
    df, _ = panel
    terminal = df[df["arrears_bucket"].isin(["Redeemed", "Charged-Off"])]
    assert len(terminal) > 0
    last_seen = df.groupby("loan_id")["reporting_date"].max()
    for loan_id, date in terminal[["loan_id", "reporting_date"]].itertuples(index=False):
        assert last_seen[loan_id] == date


def test_terminal_loans_show_a_zero_balance(panel):
    df, _ = panel
    terminal = df[df["arrears_bucket"].isin(["Redeemed", "Charged-Off"])]
    assert (terminal["current_balance"] == 0).all()
    assert (terminal["arrears_amount"] == 0).all()
    assert (terminal["cltomv_current"] == 0).all()


def test_delinquent_balances_are_frozen(panel):
    """No payment received means no principal retired."""
    df, _ = panel
    ordered = df.sort_values(["loan_id", "reporting_date"])
    prev_balance = ordered.groupby("loan_id")["current_balance"].shift()
    delinquent = ordered["arrears_bucket"].isin(
        ["1-29 DPD", "30-59 DPD", "60-89 DPD", "90+ DPD", "Defaulted"]
    )
    frozen = ordered[delinquent & prev_balance.notna()]
    np.testing.assert_allclose(frozen["current_balance"], prev_balance[frozen.index], atol=0.01)


def test_performing_balances_fall(panel):
    df, _ = panel
    ordered = df.sort_values(["loan_id", "reporting_date"])
    prev = ordered.groupby("loan_id")["current_balance"].shift()
    perf = ordered[
        (ordered["arrears_bucket"] == "Performing")
        & (ordered["interest_only_flag"] == "N")
        & prev.notna()
    ]
    assert (perf["current_balance"] <= prev[perf.index] + 0.01).all()


def test_days_past_due_matches_the_arrears_bucket(panel):
    df, _ = panel
    expected = {
        "Performing": 0,
        "1-29 DPD": 15,
        "30-59 DPD": 45,
        "60-89 DPD": 75,
        "90+ DPD": 120,
        "Defaulted": 200,
        "Charged-Off": 200,
        "Redeemed": 0,
    }
    for bucket, dpd in expected.items():
        rows = df[df["arrears_bucket"] == bucket]
        if len(rows):
            assert (rows["days_past_due"] == dpd).all(), bucket


def test_default_and_foreclosure_flags_track_the_state(panel):
    df, _ = panel
    defaulted = df[df["arrears_bucket"].isin(["Defaulted", "Charged-Off"])]
    assert (defaulted["default_crr_flag"] == "Y").all()
    assert (defaulted["foreclosure_flag"] == "Y").all()
    performing = df[df["arrears_bucket"] == "Performing"]
    assert (performing["default_crr_flag"] == "N").all()


def test_charge_off_happens_only_after_nine_months_in_default(panel):
    df, _ = panel
    ordered = df.sort_values(["loan_id", "reporting_date"])
    charged = ordered[ordered["arrears_bucket"] == "Charged-Off"]["loan_id"].unique()
    assert len(charged) > 0
    for loan_id in charged[:20]:
        path = ordered[ordered["loan_id"] == loan_id]["arrears_bucket"].tolist()
        run = 0
        for state in path:
            if state == "Defaulted":
                run += 1
            elif state == "Charged-Off":
                assert run == 8, f"{loan_id} charged off after {run} months in default"
                break
            else:
                run = 0


def test_arrears_accrue_one_payment_per_delinquent_period(panel):
    df, _ = panel
    ordered = df.sort_values(["loan_id", "reporting_date"])
    sample_id = ordered[ordered["arrears_bucket"] == "Defaulted"]["loan_id"].iloc[0]
    path = ordered[ordered["loan_id"] == sample_id]
    payment = path["scheduled_monthly_payment"].iloc[0]
    delinquent = path[~path["arrears_bucket"].isin(["Performing", "Redeemed", "Charged-Off"])]
    ratios = delinquent["arrears_amount"] / payment
    # Each qualifying period adds exactly one whole scheduled payment.
    np.testing.assert_allclose(ratios, ratios.round(), atol=0.01)


def test_ltv_ratios_agree_with_the_printed_balance(panel):
    df, _ = panel
    active = df[df["current_balance"] > 0]
    expected = active["current_balance"] / active["current_original_market_value"] * 100
    np.testing.assert_allclose(active["cltomv_current"], expected.round(2), atol=0.02)


def test_buckets_track_the_values_they_bin(panel):
    df, _ = panel
    low = df[df["current_balance"].between(100_001, 149_999)]
    assert (low["balance_bucket"] == "100k-150k").all()


def test_first_cutoff_is_overwhelmingly_performing(panel):
    df, _ = panel
    first = df[df["reporting_date"] == "2024-01-31"]
    assert (first["performing_status"] == "Non-defaulted").mean() > 0.90


def test_defaults_appear_by_the_final_cutoff(panel):
    df, _ = panel
    last = df[df["reporting_date"] == "2025-12-31"]
    assert (last["arrears_bucket"] == "Defaulted").sum() > 0


def test_no_negative_balances_anywhere(panel):
    df, _ = panel
    for col in ("current_balance", "arrears_amount", "original_balance", "days_past_due"):
        assert (df[col] >= 0).all(), col


# ---------------------------------------------------------------------------
# reproducibility
# ---------------------------------------------------------------------------


def test_same_seed_reproduces_the_book(spec):
    a = build_book(spec, 500, seed=11)
    b = build_book(spec, 500, seed=11)
    pd.testing.assert_frame_equal(a, b)


def test_different_seed_changes_the_book(spec):
    a = build_book(spec, 500, seed=11)
    b = build_book(spec, 500, seed=12)
    assert not a["original_balance"].equals(b["original_balance"])


def test_same_seed_reproduces_the_panel(spec, tmp_path):
    small = build_book(spec, 400, seed=5)
    a = run_ageing(spec, small, tmp_path / "a", seed=5)
    b = run_ageing(spec, small, tmp_path / "b", seed=5)
    left = pd.read_parquet(tmp_path / "a" / spec.emit.panel_filename)
    right = pd.read_parquet(tmp_path / "b" / spec.emit.panel_filename)
    pd.testing.assert_frame_equal(left, right)
    assert a["final_rows"] == b["final_rows"]
