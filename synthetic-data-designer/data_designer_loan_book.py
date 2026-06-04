"""
NeMo Data Designer config that produces the *origination loan book* — a single
snapshot of ~500k Dutch RMBS mortgages observed at the first cutoff
(2024-01-31), aligned to the Hypoport / ESMA Annex 2 schema.

Architecture
------------
NeMo Data Designer is per-row.  We use it for what it's best at: realistic,
correlated *primitive* fields drawn from named distributions (province,
NUTS-3, EPC label, original balance, OLTV, employment status, …).  Anything
that is a deterministic numeric derivation (monthly payment, current
balance, LTV, bucket columns) is computed in a vectorised pandas pass in
`derive_static_fields()` — both because Jinja's parser dislikes long
arithmetic expressions and because pandas is ~100x faster per row at scale.

The output of this module is the starting point.  `age_to_panel.py` then ages
the loan book month-by-month across the remaining 23 cutoffs (Feb 2024 to
Dec 2025), writing one CSV per cutoff in Hypoport's naming convention.

No LLM calls are made — every column is a sampler or a tiny expression, so
API cost is zero and runtime is bound by samplers + pandas IO.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import numpy as np
import pandas as pd

import data_designer.config as dd
from data_designer.interface import DataDesigner


# --------------------------------------------------------------------------
# Domain calibration (Dutch prime RMBS — Green Lion / ING)
# --------------------------------------------------------------------------

PROVINCE_WEIGHTS = {
    "Zuid-Holland":   0.230,
    "Noord-Holland":  0.205,
    "Noord-Brabant":  0.135,
    "Gelderland":     0.115,
    "Utrecht":        0.085,
    "Limburg":        0.060,
    "Overijssel":     0.055,
    "Groningen":      0.035,
    "Friesland":      0.030,
    "Drenthe":        0.025,
    "Flevoland":      0.020,
    "Zeeland":        0.005,
}

NUTS3_BY_PROVINCE = {
    "Zuid-Holland":   ["NL331","NL332","NL333","NL337","NL33A","NL33B","NL33C"],
    "Noord-Holland":  ["NL321","NL322","NL323","NL324","NL325","NL326","NL327","NL329","NL32B"],
    "Noord-Brabant":  ["NL411","NL412","NL413","NL414","NL415"],
    "Gelderland":     ["NL221","NL224","NL225","NL226","NL227","NL228"],
    "Utrecht":        ["NL310"],
    "Limburg":        ["NL421","NL422","NL423"],
    "Overijssel":     ["NL211","NL212","NL213"],
    "Groningen":      ["NL111","NL112","NL113"],
    "Friesland":      ["NL121","NL122","NL123"],
    "Drenthe":        ["NL131","NL132","NL133"],
    "Flevoland":      ["NL230"],
    "Zeeland":        ["NL341","NL342"],
}

EPC_KWH = {  # mid-point of typical Dutch EPC kWh/m²/yr ranges
    "A+++": 30, "A++": 60, "A+": 90, "A": 130, "B": 170,
    "C": 210, "D": 260, "E": 310, "F": 360, "G": 420,
}

# Final Hypoport-parity column order (71 columns)
HYPOPORT_COLUMNS = [
    "loan_id","transaction_name","esma_transaction_identifier","reporting_date","closing_date",
    "originator_name","servicer_name","currency","country","origination_year","maturity_date_proxy",
    "original_balance","current_balance","repayment_type","interest_only_flag",
    "current_interest_rate_pct","rate_type","remaining_interest_fixed_period_months",
    "fixed_interest_period_end_in_months","seasoning_months","remaining_term_months",
    "legal_maturity_months","loan_part_count","debtor_count","property_type","province",
    "economic_region_nuts3","construction_year","occupancy","property_usage","employment_status",
    "self_employed_flag","borrower_type","loan_purpose","buy_to_let_flag","nhg_flag","guarantee_type",
    "oltomv_original","cltomv_current","cltimv_current","original_market_value_at_origination",
    "current_original_market_value","indexed_market_value","property_valuation_type","loan_to_income",
    "payment_due_to_income_pct","borrower_annual_income","scheduled_monthly_payment","arrears_bucket",
    "arrears_amount","days_past_due","default_crr_flag","performing_status","foreclosure_flag",
    "forbearance_flag","restructuring_flag","epc_label","epc_issue_year",
    "primary_energy_demand_kwh_m2","construction_deposit_flag","construction_deposit_pct",
    "construction_deposit_amount","interest_payment_frequency","principal_payment_frequency",
    "balance_bucket","cltomv_current_bucket","cltimv_current_bucket","oltomv_original_bucket",
    "loan_to_income_bucket","payment_due_to_income_pct_bucket","construction_year_bucket",
]


# --------------------------------------------------------------------------
# Data Designer config — primitive samplers only
# --------------------------------------------------------------------------

def build_loan_book_config() -> dd.DataDesignerConfigBuilder:
    b = dd.DataDesignerConfigBuilder()

    # IDs — NeMo Data Designer's UUID sampler with short_form=True uses only
    # 8 hex chars (16^8 = 4.3B possible values), which has a birthday-paradox
    # collision probability of ~29 collisions per 500k draws.  We use a
    # placeholder here (any column) and assign collision-free sequential IDs
    # (GL<deal_year>_000001 style — matches Hypoport's Green Lion convention)
    # in derive_static_fields.  Switching to short_form=False would give 32
    # hex chars and effectively zero collisions, but the sequential form is
    # cleaner and matches the reference data exactly.
    b.add_column(dd.SamplerColumnConfig(
        name="loan_id",
        sampler_type=dd.SamplerType.UUID,
        params=dd.UUIDSamplerParams(prefix="TMP_", short_form=True, uppercase=True),
    ))

    # Categorical primitives
    cat = dd.SamplerType.CATEGORY

    b.add_column(dd.SamplerColumnConfig(
        name="origination_year", sampler_type=cat,
        params=dd.CategorySamplerParams(
            values=list(range(2008, 2024)),
            weights=[0.02,0.02,0.02,0.03,0.03,0.04,0.05,0.06,
                     0.07,0.08,0.09,0.10,0.11,0.10,0.10,0.08],
        ),
    ))
    b.add_column(dd.SamplerColumnConfig(
        name="repayment_type", sampler_type=cat,
        params=dd.CategorySamplerParams(
            values=["Annuity","Linear","InterestOnly","Bullet","Savings"],
            weights=[0.80,0.06,0.08,0.03,0.03],
        ),
    ))
    b.add_column(dd.SamplerColumnConfig(
        name="rate_type", sampler_type=cat,
        params=dd.CategorySamplerParams(
            values=["Fixed","Variable","Hybrid"],
            weights=[0.90,0.07,0.03],
        ),
    ))
    b.add_column(dd.SamplerColumnConfig(
        name="remaining_interest_fixed_period_months", sampler_type=cat,
        params=dd.CategorySamplerParams(
            values=[12,24,36,60,84,108,120,180,240],
            weights=[0.06,0.07,0.09,0.18,0.20,0.18,0.10,0.08,0.04],
        ),
    ))
    b.add_column(dd.SamplerColumnConfig(
        name="legal_maturity_months", sampler_type=cat,
        params=dd.CategorySamplerParams(
            values=[240,300,330,360],
            weights=[0.08,0.30,0.12,0.50],
        ),
    ))
    b.add_column(dd.SamplerColumnConfig(
        name="loan_part_count", sampler_type=cat,
        params=dd.CategorySamplerParams(values=[1,2,3,4], weights=[0.55,0.30,0.10,0.05]),
    ))
    b.add_column(dd.SamplerColumnConfig(
        name="debtor_count", sampler_type=cat,
        params=dd.CategorySamplerParams(values=[1,2,3], weights=[0.30,0.66,0.04]),
    ))
    b.add_column(dd.SamplerColumnConfig(
        name="property_type", sampler_type=cat,
        params=dd.CategorySamplerParams(
            values=["House","Apartment","Townhouse","Detached","SemiDetached"],
            weights=[0.45,0.30,0.13,0.07,0.05],
        ),
    ))
    province_values  = list(PROVINCE_WEIGHTS.keys())
    province_weights = list(PROVINCE_WEIGHTS.values())
    b.add_column(dd.SamplerColumnConfig(
        name="province", sampler_type=cat,
        params=dd.CategorySamplerParams(values=province_values, weights=province_weights),
    ))
    b.add_column(dd.SamplerColumnConfig(
        name="economic_region_nuts3", sampler_type=dd.SamplerType.SUBCATEGORY,
        params=dd.SubcategorySamplerParams(category="province", values=NUTS3_BY_PROVINCE),
    ))
    b.add_column(dd.SamplerColumnConfig(
        name="construction_year", sampler_type=cat,
        params=dd.CategorySamplerParams(
            values=[1900,1920,1945,1960,1970,1980,1990,2000,2010,2018,2022],
            weights=[0.02,0.04,0.08,0.09,0.11,0.13,0.15,0.18,0.10,0.07,0.03],
        ),
    ))
    b.add_column(dd.SamplerColumnConfig(
        name="occupancy", sampler_type=cat,
        params=dd.CategorySamplerParams(
            values=["OwnerOccupied","TenantOccupied","Vacant","PartiallyOccupied"],
            weights=[0.88,0.08,0.02,0.02],
        ),
    ))
    b.add_column(dd.SamplerColumnConfig(
        name="employment_status", sampler_type=cat,
        params=dd.CategorySamplerParams(
            values=["Employed","SelfEmployed","Retired","Unemployed","Student","Other"],
            weights=[0.72,0.14,0.08,0.02,0.01,0.03],
        ),
    ))
    b.add_column(dd.SamplerColumnConfig(
        name="borrower_type", sampler_type=cat,
        params=dd.CategorySamplerParams(values=["Consumer","Corporate"], weights=[0.985,0.015]),
    ))
    b.add_column(dd.SamplerColumnConfig(
        name="loan_purpose", sampler_type=cat,
        params=dd.CategorySamplerParams(
            values=["Purchase/Refinance","Construction","Renovation","Other"],
            weights=[0.86,0.04,0.07,0.03],
        ),
    ))
    b.add_column(dd.SamplerColumnConfig(
        name="epc_label", sampler_type=cat,
        params=dd.CategorySamplerParams(
            values=["A+++","A++","A+","A","B","C","D","E","F","G"],
            weights=[0.02,0.04,0.06,0.18,0.21,0.20,0.13,0.08,0.05,0.03],
        ),
    ))
    b.add_column(dd.SamplerColumnConfig(
        name="forbearance_flag", sampler_type=cat,
        params=dd.CategorySamplerParams(values=["N","Y"], weights=[0.985,0.015]),
    ))
    b.add_column(dd.SamplerColumnConfig(
        name="restructuring_flag", sampler_type=cat,
        params=dd.CategorySamplerParams(values=["N","Y"], weights=[0.99,0.01]),
    ))
    b.add_column(dd.SamplerColumnConfig(
        name="_arrears_state", sampler_type=cat,
        params=dd.CategorySamplerParams(
            values=["Performing","1-29 DPD","30-59 DPD","60-89 DPD","90+ DPD","Defaulted"],
            weights=[0.965,0.018,0.008,0.004,0.003,0.002],
        ),
    ))

    # Numeric primitives (scipy + gaussian)
    b.add_column(dd.SamplerColumnConfig(
        name="borrower_annual_income", sampler_type=dd.SamplerType.SCIPY,
        params=dd.ScipySamplerParams(
            dist_name="lognorm",
            dist_params={"s":0.45, "scale":65000.0},
            decimal_places=2,
        ),
    ))
    b.add_column(dd.SamplerColumnConfig(
        name="original_balance", sampler_type=dd.SamplerType.SCIPY,
        params=dd.ScipySamplerParams(
            dist_name="lognorm",
            dist_params={"s":0.40, "scale":300000.0},
            decimal_places=2,
        ),
    ))
    b.add_column(dd.SamplerColumnConfig(
        name="oltomv_original", sampler_type=dd.SamplerType.SCIPY,
        params=dd.ScipySamplerParams(
            dist_name="truncnorm",
            dist_params={"a":-2.5,"b":1.5,"loc":85.0,"scale":12.0},
            decimal_places=2,
        ),
    ))
    b.add_column(dd.SamplerColumnConfig(
        name="current_interest_rate_pct", sampler_type=dd.SamplerType.GAUSSIAN,
        params=dd.GaussianSamplerParams(mean=3.10, stddev=0.65, decimal_places=2),
    ))
    # Bernoulli helpers
    b.add_column(dd.SamplerColumnConfig(
        name="_nhg_pre", sampler_type=dd.SamplerType.BERNOULLI,
        params=dd.BernoulliSamplerParams(p=0.45),
    ))
    b.add_column(dd.SamplerColumnConfig(
        name="_construction_deposit_pre", sampler_type=dd.SamplerType.BERNOULLI,
        params=dd.BernoulliSamplerParams(p=0.04),
    ))

    # Tiny expressions: simple flag derivations (no arithmetic)
    b.add_column(dd.ExpressionColumnConfig(
        name="interest_only_flag",
        expr="{{ 'Y' if repayment_type in ['InterestOnly','Bullet'] else 'N' }}",
    ))
    b.add_column(dd.ExpressionColumnConfig(
        name="self_employed_flag",
        expr="{{ 'Y' if employment_status == 'SelfEmployed' else 'N' }}",
    ))
    b.add_column(dd.ExpressionColumnConfig(
        name="property_usage",
        expr="{{ 'OwnerOccupied' if occupancy == 'OwnerOccupied' else 'BuyToLet' }}",
    ))
    b.add_column(dd.ExpressionColumnConfig(
        name="buy_to_let_flag",
        expr="{{ 'Y' if property_usage == 'BuyToLet' else 'N' }}",
    ))

    return b


# --------------------------------------------------------------------------
# Vectorised pandas post-processor — derived fields & constants
# --------------------------------------------------------------------------

def _first_business_day(year: int, month: int = 1) -> pd.Timestamp:
    """First weekday of the given month/year (ESMA convention for deal
    closing date: first business day of January of the deal year, ISO 8601)."""
    d = pd.Timestamp(year=year, month=month, day=1)
    while d.weekday() >= 5:    # 5=Sat, 6=Sun
        d += pd.Timedelta(days=1)
    return d


def derive_static_fields(
    df: pd.DataFrame,
    first_cutoff: str = "2024-01-31",
    deal_year: int | None = None,
    closing_date: str | None = None,
    transaction_name: str | None = None,
    esma_identifier: str | None = None,
) -> pd.DataFrame:
    """Compute all deterministic derived columns and constants.

    Deal-level metadata (transaction_name, esma_transaction_identifier,
    closing_date) is derived from `deal_year` when not given explicitly,
    following ESMA Annex 2 conventions:
      - closing_date: first business day of January of the deal year
        (ISO 8601 YYYY-MM-DD).
      - esma_transaction_identifier: 18-char LEI of the SPV + YYYYMM of
        deal closing (e.g. 3TK20IVIUJ8J3ZU0QE75N202401).
      - transaction_name: '<Issuer Name> YYYY-<series> B.V.'.
    """
    n = len(df)
    cutoff = pd.Timestamp(first_cutoff)
    cutoff_year = cutoff.year
    cutoff_month = cutoff.month

    # Deal-level metadata — derived from deal_year (defaults to cutoff year)
    if deal_year is None:
        deal_year = cutoff_year

    # Collision-free sequential loan_id (overwrites the placeholder UUID from
    # the Data Designer sampler).  Format matches Hypoport's reference data:
    # GL<YYYY>_<6-digit zero-padded sequence>, e.g. GL2024_000001.
    df = df.reset_index(drop=True)
    df["loan_id"] = [f"GL{deal_year}_{i:06d}" for i in range(1, n + 1)]
    if closing_date is None:
        closing_date = _first_business_day(deal_year, 1).strftime("%Y-%m-%d")
    if transaction_name is None:
        transaction_name = f"Green Lion {deal_year}-1 B.V."
    if esma_identifier is None:
        # ESMA transaction identifier convention: LEI of SPV + YYYYMM of close
        # (using ING LEI prefix 3TK20IVIUJ8J3ZU0QE75 as placeholder)
        esma_identifier = f"3TK20IVIUJ8J3ZU0QE75N{deal_year}{_first_business_day(deal_year,1).month:02d}"

    # Constants
    df["transaction_name"]           = transaction_name
    df["esma_transaction_identifier"] = esma_identifier
    df["reporting_date"]             = first_cutoff
    df["closing_date"]               = closing_date
    df["originator_name"]            = "ING"
    df["servicer_name"]              = "ING"
    df["currency"]                   = "EUR"
    df["country"]                    = "NL"
    df["interest_payment_frequency"]  = "Monthly"
    df["principal_payment_frequency"] = "Monthly"
    df["property_valuation_type"]    = "Indexed/Origination Proxy"
    df["fixed_interest_period_end_in_months"] = df["remaining_interest_fixed_period_months"]

    # NHG: reset to 'N' if balance > €435k (2024 cap), else use Bernoulli
    nhg_pre = df["_nhg_pre"].astype(int)
    df["nhg_flag"] = np.where((df["original_balance"] <= 435000) & (nhg_pre == 1), "Y", "N")
    df["guarantee_type"] = pd.Series(
        np.where(df["nhg_flag"].to_numpy() == "Y", "NHG", "None"),
        index=df.index, dtype="object",
    )

    # Maturity date
    maturity_year  = df["origination_year"].astype(int) + df["legal_maturity_months"].astype(int) // 12
    maturity_month = (df["legal_maturity_months"].astype(int) % 12) + 1
    df["maturity_date_proxy"] = [
        f"{y:04d}-{m:02d}-28" for y, m in zip(maturity_year.tolist(), maturity_month.tolist())
    ]

    # Original market value = balance / (oltv / 100)
    df["original_market_value_at_origination"] = (
        df["original_balance"] / (df["oltomv_original"] / 100.0)
    ).round(2)

    # Seasoning at first cutoff (approx: month-July of origination_year)
    df["seasoning_months"] = (
        (cutoff_year - df["origination_year"].astype(int)) * 12 + (cutoff_month - 7)
    ).clip(lower=1).astype(int)
    df["remaining_term_months"] = (
        df["legal_maturity_months"].astype(int) - df["seasoning_months"]
    ).clip(lower=1).astype(int)

    # HPI-style uplift since origination
    years_since_origination = cutoff_year - df["origination_year"].astype(int)
    hpi_market   = 1.030 ** years_since_origination
    hpi_indexed  = 1.045 ** years_since_origination
    df["current_original_market_value"] = (df["original_market_value_at_origination"] * hpi_market).round(2)
    df["indexed_market_value"]          = (df["original_market_value_at_origination"] * hpi_indexed).round(2)

    # Current balance at first cutoff — annuity-style amortisation for non-IO,
    # full balance for IO. Closed-form: B(t) = P * ((1+r)^n - (1+r)^t) / ((1+r)^n - 1)
    r = df["current_interest_rate_pct"] / 100.0 / 12.0
    n_months = df["legal_maturity_months"].astype(int)
    t = df["seasoning_months"]
    one_plus_r = 1.0 + r
    pow_n = np.power(one_plus_r, n_months)
    pow_t = np.power(one_plus_r, t)
    amort_balance = df["original_balance"] * (pow_n - pow_t) / (pow_n - 1.0)
    df["current_balance"] = np.where(
        df["interest_only_flag"] == "Y",
        df["original_balance"],
        amort_balance,
    ).round(2)

    # LTVs at first cutoff
    df["cltomv_current"] = (df["current_balance"] / df["current_original_market_value"] * 100).round(2)
    df["cltimv_current"] = (df["current_balance"] / df["indexed_market_value"] * 100).round(2)

    # Affordability
    df["loan_to_income"] = (df["original_balance"] / df["borrower_annual_income"]).round(2)
    # Scheduled monthly payment (annuity for non-IO, interest only for IO)
    annuity_pay = df["original_balance"] * r * pow_n / (pow_n - 1.0)
    io_pay      = df["original_balance"] * r
    df["scheduled_monthly_payment"] = np.where(
        df["interest_only_flag"] == "Y", io_pay, annuity_pay
    ).round(2)
    df["payment_due_to_income_pct"] = (
        df["scheduled_monthly_payment"] * 12 / df["borrower_annual_income"] * 100
    ).round(2)

    # Arrears unpacking
    a = df["_arrears_state"]
    dpd_map = {"Performing":0, "1-29 DPD":15, "30-59 DPD":45, "60-89 DPD":75, "90+ DPD":120, "Defaulted":200}
    df["arrears_bucket"]    = a
    df["days_past_due"]     = a.map(dpd_map).astype(int)
    # arrears_amount = scheduled_monthly_payment × ceil(dpd / 30)
    # i.e. one missed scheduled payment per ~30 DPD.  This matches the
    # counter-based accrual used in the ageing pass, so the first delta after
    # month-0 is exactly one scheduled payment (no boundary discontinuity).
    consec_arrears0 = np.where(
        df["days_past_due"].to_numpy() == 0,
        0,
        np.maximum((df["days_past_due"].to_numpy() + 29) // 30, 1),
    )
    df["arrears_amount"] = (df["scheduled_monthly_payment"] * consec_arrears0).round(2)
    df["default_crr_flag"]  = np.where(a.isin(["90+ DPD","Defaulted"]), "Y", "N")
    df["performing_status"] = np.where(a == "Defaulted", "Defaulted", "Non-defaulted")
    df["foreclosure_flag"]  = np.where(a == "Defaulted", "Y", "N")

    # Energy
    df["epc_issue_year"] = df["origination_year"].astype(int)
    df["primary_energy_demand_kwh_m2"] = df["epc_label"].map(EPC_KWH).astype(int)

    # Construction deposit
    cd_pre = df["_construction_deposit_pre"].astype(int)
    in_renovation = df["loan_purpose"].isin(["Renovation","Construction"])
    df["construction_deposit_flag"] = np.where((cd_pre == 1) & in_renovation, "Y", "N")
    # 10–24% deterministic spread based on loan_id hash
    # 10-24% deterministic spread based on row index (loan_id is now a
    # constant-length sequential string, so we can't use len() for variability).
    cd_pct = (np.arange(len(df), dtype="int64") % 15) + 10
    df["construction_deposit_pct"] = np.where(
        df["construction_deposit_flag"] == "Y",
        cd_pct,
        0,
    ).astype(int)
    df["construction_deposit_amount"] = (
        df["original_balance"] * df["construction_deposit_pct"] / 100.0
    ).round(2)

    # Bucket columns
    df["balance_bucket"]    = _balance_bucket(df["current_balance"])
    df["cltomv_current_bucket"] = _ltv_bucket(df["cltomv_current"])
    df["cltimv_current_bucket"] = _ltv_bucket(df["cltimv_current"])
    df["oltomv_original_bucket"] = _ltv_bucket(df["oltomv_original"])
    df["loan_to_income_bucket"]  = _lti_bucket(df["loan_to_income"])
    df["payment_due_to_income_pct_bucket"] = _payment_bucket(df["payment_due_to_income_pct"])
    df["construction_year_bucket"] = _construction_year_bucket(df["construction_year"])

    # Drop helpers
    df = df.drop(columns=[c for c in ["_nhg_pre","_construction_deposit_pre","_arrears_state"] if c in df.columns])

    # Reorder to canonical Hypoport 71-column layout
    return df[HYPOPORT_COLUMNS]


def _balance_bucket(s: pd.Series) -> pd.Series:
    bins   = [0, 100_000, 150_000, 200_000, 250_000, 300_000, 350_000, 400_000, 450_000, 500_000, 550_000, 600_000, 1e12]
    labels = ["<100k","100k-150k","150k-200k","200k-250k","250k-300k","300k-350k","350k-400k","400k-450k","450k-500k","500k-550k","550k-600k","600k+"]
    return pd.cut(s, bins=bins, labels=labels, include_lowest=True).astype(str)

def _ltv_bucket(s: pd.Series) -> pd.Series:
    bins   = [-1, 20, 30, 40, 50, 60, 70, 80, 90, 100, 1e6]
    labels = ["<20","20-30","30-40","40-50","50-60","60-70","70-80","80-90","90-100","100+"]
    return pd.cut(s, bins=bins, labels=labels).astype(str)

def _lti_bucket(s: pd.Series) -> pd.Series:
    bins   = [-1, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0, 5.5, 6.0, 1e6]
    labels = ["<1.5","1.5-2.0","2.0-2.5","2.5-3.0","3.0-3.5","3.5-4.0","4.0-4.5","4.5-5.0","5.0-5.5","5.5-6.0","6.0+"]
    return pd.cut(s, bins=bins, labels=labels).astype(str)

def _payment_bucket(s: pd.Series) -> pd.Series:
    bins   = [-1, 5, 10, 15, 20, 25, 30, 35, 1e6]
    labels = ["<5","5-10","10-15","15-20","20-25","25-30","30-35","35+"]
    return pd.cut(s, bins=bins, labels=labels).astype(str)

def _construction_year_bucket(s: pd.Series) -> pd.Series:
    bins   = [-1, 1945, 1960, 1970, 1980, 1985, 1990, 1995, 2000, 2005, 2010, 2015, 9999]
    labels = ["<1945","1945-1960","1960-1970","1970-1980","1980-1985","1985-1990","1990-1995","1995-2000","2000-2005","2005-2010","2010-2015","2015+"]
    return pd.cut(s, bins=bins, labels=labels).astype(str)


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------

def generate_loan_book(
    num_records: int,
    out_path: str,
    first_cutoff: str = "2024-01-31",
    deal_year: int | None = None,
) -> pd.DataFrame:
    builder = build_loan_book_config()
    designer = DataDesigner()
    print(f"[loan-book] NeMo DataDesigner generating {num_records:,} records...")
    result = designer.create(
        config_builder=builder,
        num_records=num_records,
        dataset_name="rmbs_loan_book",
    )
    if hasattr(result, "load_dataset"):
        df = result.load_dataset().copy()
    else:
        df = result.dataset.copy()
    print(f"[loan-book] Deriving static fields ({len(df):,} rows)...")
    df = derive_static_fields(df, first_cutoff=first_cutoff, deal_year=deal_year)
    Path(os.path.dirname(out_path) or ".").mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)
    print(f"[loan-book] Wrote {len(df):,} rows × {len(df.columns)} cols → {out_path}")
    return df


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--num-records", type=int, default=500_000)
    p.add_argument("--first-cutoff", default="2024-01-31")
    p.add_argument("--deal-year", type=int, default=None,
                   help="Deal closing year (defaults to first-cutoff year). "
                        "Derives ESMA closing_date as first business day of January of deal_year.")
    p.add_argument("--out", default="./out/loan_book.parquet")
    args = p.parse_args()
    generate_loan_book(args.num_records, args.out,
                       first_cutoff=args.first_cutoff, deal_year=args.deal_year)


if __name__ == "__main__":
    main()
