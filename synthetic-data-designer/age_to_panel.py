"""
Ageing pass: take the origination loan book (one snapshot at month 0) and walk
it forward month by month, writing one CSV per cutoff in Hypoport's naming
convention.

Per-cutoff state evolution (vectorised over all loans simultaneously):

    1. Apply prepayment hazard      -> drop redeemed loans from future cutoffs
    2. Apply Markov delinquency      -> update arrears_bucket, dpd, performing_status,
                                        default/foreclosure flags
    3. Increment seasoning, decrement remaining term
    4. Amortise current_balance      -> closed-form annuity, IO unchanged
    5. Update market values via HPI  -> current_original_market_value, indexed_market_value
    6. Recompute dynamic LTV ratios  -> cltomv_current, cltimv_current
    7. Recompute arrears_amount      -> scheduled_payment * dpd / 30
    8. Recompute dynamic buckets     -> balance_bucket, cltomv_bucket, cltimv_bucket
    9. Set reporting_date            -> month-end of cutoff
    10. Write green_lion_<yyyymm>_1_synthetic_loan_tape.csv

Calibration anchors (Dutch prime RMBS — Moody's / DBRS / S&P Dutch series):
    - Annualised prepayment hazard 6-8%; ~0.55% monthly
    - Monthly default rate 0.05-0.10% performing -> 30 DPD
    - Cure rate from 30 DPD back to performing ~50% / month
    - Charge-off / foreclosure absorbs out of 120+ DPD after several months

The HPI overlay is a slow positive drift plus a small noise shock per month
that you can swap for an actual NL HPI series.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import numpy as np
import pandas as pd

from data_designer_loan_book import (
    HYPOPORT_COLUMNS,
    _balance_bucket,
    _ltv_bucket,
)


# --------------------------------------------------------------------------
# Markov delinquency chain
# --------------------------------------------------------------------------

DELINQ_STATES = ["Performing", "1-29 DPD", "30-59 DPD", "60-89 DPD", "90+ DPD",
                 "Defaulted", "Charged-Off", "Redeemed"]
DPD_LOOKUP    = np.array([0, 15, 45, 75, 120, 200, 200, 0], dtype=np.int16)

# Indices into DELINQ_STATES for terminal/state checks
IDX_PERFORMING = 0
IDX_DEFAULTED  = 5
IDX_CHARGEOFF  = 6
IDX_REDEEMED   = 7

# Monthly Markov transitions over the *active* 6-state chain (Performing..Defaulted).
# Defaulted is absorbing within the chain itself; Charged-Off and Redeemed are
# terminal labels emitted by separate logic (see step_one_month).
# Row = from, col = to. Calibrated for ~0.7% annualised default rate
# (Dutch prime range; tune in calibration task #9).
TRANS_MATRIX = np.array([
    # Perf   1-29   30-59  60-89  90+    Def
    [0.9925, 0.0060, 0.0010, 0.0003, 0.0001, 0.0001],  # from Performing
    [0.6500, 0.1500, 0.1800, 0.0150, 0.0040, 0.0010],  # from 1-29
    [0.3000, 0.1200, 0.1500, 0.4000, 0.0250, 0.0050],  # from 30-59
    [0.1200, 0.0500, 0.0800, 0.1500, 0.5800, 0.0200],  # from 60-89
    [0.0400, 0.0100, 0.0200, 0.0500, 0.4800, 0.4000],  # from 90+
    [0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 1.0000],  # Defaulted absorbing
])
_CUMPROBS = np.cumsum(TRANS_MATRIX, axis=1)

# After this many consecutive months in Defaulted, the loan is charged off.
# Aligns with DBRS/Moody's NL recovery-timeline assumptions (~9 months).
MONTHS_IN_DEFAULT_TO_CHARGEOFF = 9


def step_delinquency(state_idx: np.ndarray, rng: np.random.Generator) -> np.ndarray:
    """Vectorised Markov step. state_idx shape (n,) int8; returns next state."""
    rows = _CUMPROBS[state_idx]            # (n, 6)
    u = rng.random(state_idx.shape)[:, None]
    return (u > rows).sum(axis=1).astype(np.int8)


def state_str_to_idx(s: pd.Series) -> np.ndarray:
    mapping = {st: i for i, st in enumerate(DELINQ_STATES)}
    return s.map(mapping).fillna(0).astype(np.int8).to_numpy()


def state_idx_to_str(idx: np.ndarray) -> np.ndarray:
    return np.asarray(DELINQ_STATES)[idx]


# --------------------------------------------------------------------------
# Prepayment hazard
# --------------------------------------------------------------------------

ANNUAL_PREPAYMENT_HAZARD = 0.07       # 7% annualised — Dutch prime mid-range
MONTHLY_PREPAYMENT_HAZARD = 1 - (1 - ANNUAL_PREPAYMENT_HAZARD) ** (1.0 / 12)  # ~0.605%


def step_prepayment(n: int, rng: np.random.Generator) -> np.ndarray:
    """Bernoulli mask: True = loan redeemed this month."""
    return rng.random(n) < MONTHLY_PREPAYMENT_HAZARD


# --------------------------------------------------------------------------
# HPI overlay (NL house price index proxy)
# --------------------------------------------------------------------------

def hpi_monthly_drift(month_index: int, rng: np.random.Generator | None = None) -> float:
    """Monthly HPI multiplier. ~3% annualised drift plus optional noise."""
    base = 1.03 ** (1.0 / 12)            # ~0.00247 per month
    return base


# --------------------------------------------------------------------------
# Single-cutoff update
# --------------------------------------------------------------------------

def step_one_month(
    df: pd.DataFrame,
    month_offset: int,
    reporting_date: pd.Timestamp,
    rng: np.random.Generator,
) -> tuple[pd.DataFrame, np.ndarray]:
    """Apply one month of ageing and return (this_month_df, terminal_mask).

    `df` is the *previous* month's state (only loans that were still active).
    The returned `this_month_df` is the state to write to the cutoff CSV.
    `terminal_mask` flags rows whose status went terminal this month
    (Redeemed or Charged-Off) so the orchestrator drops them next iteration.
    """
    n = len(df)
    if n == 0:
        return df, np.zeros(0, dtype=bool)

    # Re-index 0..n-1 so positional masks line up with df.iloc accesses
    df = df.reset_index(drop=True)
    state_idx = state_str_to_idx(df["arrears_bucket"])

    # --- Step 1: decide which loans prepay THIS month ---
    # Eligible: only loans that are not yet in a terminal absorbing state.
    eligible = (state_idx != IDX_DEFAULTED)
    prepay_mask = step_prepayment(n, rng) & eligible

    # --- Step 2: Markov delinquency transition for non-prepaid loans ---
    new_state = state_idx.copy()
    keep_mask = ~prepay_mask
    if keep_mask.any():
        new_state[keep_mask] = step_delinquency(state_idx[keep_mask], rng)

    # Track months-in-default counter for charge-off transition
    if "_months_in_default" not in df.columns:
        df["_months_in_default"] = 0
    md = df["_months_in_default"].astype(int).to_numpy()
    md = np.where(new_state == IDX_DEFAULTED, md + 1, 0)
    chargeoff_mask = (md >= MONTHS_IN_DEFAULT_TO_CHARGEOFF) & (new_state == IDX_DEFAULTED)
    # Force charged-off state for those that hit the threshold
    new_state = np.where(chargeoff_mask, IDX_CHARGEOFF, new_state)
    # Force redeemed state for prepaid loans
    new_state = np.where(prepay_mask, IDX_REDEEMED, new_state)
    df["_months_in_default"] = md

    # --- Step 3: update delinquency / status fields ---
    df["arrears_bucket"]    = state_idx_to_str(new_state)
    df["days_past_due"]     = DPD_LOOKUP[new_state]
    df["performing_status"] = np.select(
        [new_state == IDX_DEFAULTED, new_state == IDX_CHARGEOFF, new_state == IDX_REDEEMED],
        ["Defaulted", "Charged-Off", "Redeemed"],
        default="Non-defaulted",
    )
    df["default_crr_flag"]  = np.where(
        np.isin(new_state, [4, IDX_DEFAULTED, IDX_CHARGEOFF]), "Y", "N"
    )
    df["foreclosure_flag"]  = np.where(
        np.isin(new_state, [IDX_DEFAULTED, IDX_CHARGEOFF]), "Y", "N"
    )

    # --- Step 4: seasoning / term advance (all loans, including terminal) ---
    df["seasoning_months"]      = df["seasoning_months"].astype(int) + 1
    df["remaining_term_months"] = (
        df["legal_maturity_months"].astype(int) - df["seasoning_months"]
    ).clip(lower=0).astype(int)
    fp = df["remaining_interest_fixed_period_months"].astype(int) - 1
    df["remaining_interest_fixed_period_months"] = fp.clip(lower=0)
    df["fixed_interest_period_end_in_months"]    = fp.clip(lower=0)

    # --- Step 5: amortise current_balance (Performing only; frozen otherwise) ---
    # In real RMBS:
    #   - Performing → borrower paid this month: balance reduces by principal portion
    #     of the annuity payment (or stays flat for interest-only loans).
    #   - Any DPD state or Defaulted → no payment received: balance is frozen.
    #     (We don't accrue interest into the principal balance here; that is a
    #     servicing-accounting choice. ESMA RREL52 (current_balance) is meant
    #     to be the outstanding principal balance, which under "no payment"
    #     stays flat. Accrued unpaid interest sits in arrears_amount.)
    #   - Redeemed / Charged-Off → terminal, balance = 0.
    active_mask     = ~np.isin(new_state, [IDX_REDEEMED, IDX_CHARGEOFF])
    performing_mask = (new_state == IDX_PERFORMING)
    r           = (df["current_interest_rate_pct"].astype(float) / 100.0 / 12.0).to_numpy()
    prev_balance    = df["current_balance"].astype(float).to_numpy()
    monthly_payment = df["scheduled_monthly_payment"].astype(float).to_numpy()
    is_io           = (df["interest_only_flag"].to_numpy() == "Y")
    # One-step amortisation (Performing only):
    #   Amortising loan:   B_{t+1} = B_t * (1+r) - M
    #   Interest-only:     B_{t+1} = B_t              (interest paid, no principal)
    one_step = np.where(
        is_io,
        prev_balance,
        np.maximum(prev_balance * (1.0 + r) - monthly_payment, 0.0),
    )
    balance = np.where(performing_mask, one_step, prev_balance)   # arrears/default → frozen
    df["current_balance"] = np.where(active_mask, balance, 0.0).round(2)

    # --- Step 6: HPI updates (active loans only — terminal loans don't matter) ---
    drift = hpi_monthly_drift(month_offset)
    df["current_original_market_value"] = (
        df["current_original_market_value"].astype(float) * drift
    ).round(2)
    df["indexed_market_value"] = (
        df["indexed_market_value"].astype(float) * drift
    ).round(2)

    # --- Step 7: LTV ratios (terminal -> 0) ---
    # Pre-zero the output array so unmasked positions (den == 0) are
    # explicitly 0.0 rather than uninitialised memory (numpy >=1.25
    # warns when `np.divide(..., where=...)` is used without `out=`).
    def safe_div(num: np.ndarray, den: np.ndarray) -> np.ndarray:
        num_f = num.astype(np.float64, copy=False)
        den_f = den.astype(np.float64, copy=False)
        out = np.zeros_like(num_f)
        np.divide(num_f, den_f, out=out, where=(den_f != 0))
        return np.where(active_mask, out * 100.0, 0.0)

    df["cltomv_current"] = safe_div(df["current_balance"].to_numpy(),
                                    df["current_original_market_value"].to_numpy()).round(2)
    df["cltimv_current"] = safe_div(df["current_balance"].to_numpy(),
                                    df["indexed_market_value"].to_numpy()).round(2)

    # --- Step 8: arrears amount — accumulate consecutive missed payments ---
    # Carry a per-loan counter `_consec_arrears` across cutoffs. Each month a
    # loan is non-Performing (and not terminal), one more scheduled payment
    # has been missed. Reset on cure or terminal.
    if "_consec_arrears" not in df.columns:
        df["_consec_arrears"] = 0
    prev_consec = df["_consec_arrears"].astype(int).to_numpy()
    in_arrears  = active_mask & ~performing_mask
    new_consec  = np.where(performing_mask, 0,
                  np.where(in_arrears,    prev_consec + 1,
                                          0))           # terminal → 0
    df["_consec_arrears"] = new_consec.astype(int)
    df["arrears_amount"] = np.where(
        in_arrears,
        monthly_payment * new_consec,
        0.0,
    ).round(2)

    # --- Step 9: dynamic buckets ---
    df["balance_bucket"]        = _balance_bucket(df["current_balance"])
    df["cltomv_current_bucket"] = _ltv_bucket(df["cltomv_current"])
    df["cltimv_current_bucket"] = _ltv_bucket(df["cltimv_current"])

    # --- Step 10: reporting date ---
    df["reporting_date"] = reporting_date.strftime("%Y-%m-%d")

    terminal_mask = np.isin(new_state, [IDX_REDEEMED, IDX_CHARGEOFF])
    return df, terminal_mask


# --------------------------------------------------------------------------
# Driver: 24-cutoff panel
# --------------------------------------------------------------------------

def month_ends_between(start: str, n_months: int) -> list[pd.Timestamp]:
    """Return n_months month-end timestamps starting at `start` (inclusive)."""
    base = pd.Timestamp(start)
    dates = [base + pd.offsets.MonthEnd(0)]   # first cutoff
    for i in range(1, n_months):
        dates.append(dates[-1] + pd.offsets.MonthEnd(1))
    return dates


def run_ageing(
    loan_book: pd.DataFrame,
    n_cutoffs: int = 24,
    first_cutoff: str = "2024-01-31",
    out_dir: str = "./out/cutoffs",
    seed: int = 42,
) -> None:
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(seed)
    dates = month_ends_between(first_cutoff, n_cutoffs)
    current = loan_book.copy()
    current["reporting_date"] = dates[0].strftime("%Y-%m-%d")
    current["_months_in_default"] = 0
    # Seed consecutive-arrears counter from the initial DPD bucket:
    #   Performing → 0
    #   Any DPD bucket → ceil(dpd / 30) months of missed payments
    dpd0 = current["days_past_due"].astype(int).to_numpy()
    current["_consec_arrears"] = np.where(dpd0 == 0, 0, np.maximum((dpd0 + 29) // 30, 1))
    terminal_mask = np.zeros(len(current), dtype=bool)

    for m, date in enumerate(dates):
        if m > 0:
            current, terminal_mask = step_one_month(current, m, date, rng)
        out = current[HYPOPORT_COLUMNS].copy()
        fname = f"green_lion_{date.year:04d}{date.month:02d}_1_synthetic_loan_tape.csv"
        path = os.path.join(out_dir, fname)
        out.to_csv(path, index=False)
        # Per-cutoff status mix (informative — shows the new terminal states)
        vc = current["performing_status"].value_counts()
        mix = " ".join(f"{k[:4]}={v}" for k, v in vc.items())
        print(f"[ageing] cutoff={date.date()} rows={len(current):>7,}  [{mix}] -> {fname}")
        # Drop terminal rows (Redeemed / Charged-Off) before next iteration
        if terminal_mask.any():
            current = current.loc[~terminal_mask].reset_index(drop=True)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--loan-book", default="./out/loan_book.parquet")
    p.add_argument("--n-cutoffs", type=int, default=24)
    p.add_argument("--first-cutoff", default="2024-01-31")
    p.add_argument("--out-dir", default="./out/cutoffs")
    p.add_argument("--seed", type=int, default=42)
    args = p.parse_args()
    df = pd.read_parquet(args.loan_book)
    run_ageing(df, n_cutoffs=args.n_cutoffs, first_cutoff=args.first_cutoff,
               out_dir=args.out_dir, seed=args.seed)


if __name__ == "__main__":
    main()
