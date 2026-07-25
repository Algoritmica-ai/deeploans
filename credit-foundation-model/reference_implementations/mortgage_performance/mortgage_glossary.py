# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Field-by-field glossary for the single-family mortgage performance dataset.

Condensed from the official *Single-Family Loan Performance Dataset and Credit Risk Transfer —
Glossary and File Layout* (© 2026 mortgage performance data, revised 5/26/2026). Keys are the snake_case column
names used in this repo (see ``configs/mortgage_performance/raw_schema.yaml``); the ``position`` matches the
1-based Field Position in the official layout.

Two blocks:
  * ``RAW_FIELDS``      — the 113 published source fields.
  * ``DERIVED_FIELDS``  — the 6 columns ``scripts/ingest_mortgage_performance.py`` adds/rewrites so the rest
                          of the pipeline stays asset-generic.

Each entry: ``(position, plain_name, dtype, description, enumerations|None)``. ``enumerations`` is a
plain string when the field is coded (kept verbatim-ish from the glossary) else ``None``.
"""

from __future__ import annotations

# name -> (position, plain_name, dtype, description, enumerations)
RAW_FIELDS: dict[str, tuple[int, str, str, str, str | None]] = {
    "reference_pool_id": (1, "Reference Pool ID", "str",
        "A unique identifier for the reference pool.", None),
    "loan_identifier": (2, "Loan Identifier", "str",
        "A unique identifier for the mortgage loan (does not correspond to loan identifiers in "
        "other mortgage performance data disclosures). Renamed to loan_id in this repo.", None),
    "monthly_reporting_period": (3, "Monthly Reporting Period", "date(MMYYYY)",
        "The month/year of the servicer's cut-off period for the loan information (the observation "
        "month of each row).", None),
    "channel": (4, "Channel", "str",
        "The origination channel used by the party that delivered the loan to the issuer.",
        "R = Retail; C = Correspondent; B = Broker"),
    "seller_name": (5, "Seller Name", "str",
        "The entity that delivered the mortgage loan to mortgage performance data. Sellers under ~1% of volume "
        "are shown as 'Other'.", None),
    "servicer_name": (6, "Servicer Name", "str",
        "The entity that serves as the primary servicer of the loan. Blank before Dec-2001; small "
        "servicers shown as 'Other'.", None),
    "master_servicer": (7, "Master Servicer", "str", "mortgage performance data.", None),
    "original_interest_rate": (8, "Original Interest Rate", "float",
        "The original interest rate on the loan as identified in the original mortgage note.", None),
    "current_interest_rate": (9, "Current Interest Rate", "float",
        "The rate of interest in effect for the periodic installment due (updated for modified "
        "loans).", None),
    "original_upb": (10, "Original UPB", "float",
        "The dollar amount of the loan as stated on the note at origination (rounded).", None),
    "upb_at_issuance": (11, "UPB at Issuance", "float",
        "The unpaid principal balance of the loan as of the cut-off date of the reference pool.",
        None),
    "current_actual_upb": (12, "Current Actual UPB", "float",
        "The current actual outstanding unpaid principal balance, reflecting payments received. "
        "Masked for the first six months of a loan's life; reduced to zero when removed.", None),
    "original_loan_term": (13, "Original Loan Term", "int",
        "The number of months of regularly scheduled borrower payments at origination.", None),
    "origination_date": (14, "Origination Date", "date(MMYYYY)",
        "The date of the individual note (loan origination). Reparsed to an ISO date in this repo "
        "and used as the temporal split key.", None),
    "first_payment_date": (15, "First Payment Date", "date(MMYYYY)",
        "The date of the first scheduled loan payment by the borrower.", None),
    "loan_age": (16, "Loan Age", "int",
        "The number of calendar months since origination (from the first full month interest "
        "accrues).", None),
    "remaining_months_to_legal_maturity": (17, "Remaining Months to Legal Maturity", "int",
        "Calendar months remaining until the loan is due to be paid in full per the maturity "
        "date.", None),
    "remaining_months_to_maturity": (18, "Remaining Months to Maturity", "int",
        "Calendar months remaining until the UPB amortizes to zero given prepayments (blank for "
        "modified loans).", None),
    "maturity_date": (19, "Maturity Date", "date(MMYYYY)",
        "The month/year the loan is scheduled to be paid in full per the loan documents.", None),
    "original_ltv": (20, "Original Loan-to-Value (LTV)", "int",
        "Loan amount at origination divided by the property value, as a percentage. Null if LTV "
        ">97% (or >200% for HARP) or unknown.", None),
    "original_cltv": (21, "Original Combined LTV (CLTV)", "int",
        "All known outstanding loans at origination divided by property value, as a percentage.",
        None),
    "number_of_borrowers": (22, "Number of Borrowers", "int",
        "The number of individuals obligated to repay the loan.", None),
    "dti": (23, "Debt-to-Income (DTI)", "int",
        "Total monthly debt expense divided by total monthly income at origination. Blank if "
        "out-of-range, unknown, or a HARP refinance.", None),
    "borrower_credit_score_at_origination": (24, "Borrower Credit Score at Origination", "int",
        "Representative Classic FICO of the primary borrower at acquisition. No longer populated "
        "from Mar-2026 (see origination_classic_fico). Blank if unknown.", None),
    "co_borrower_credit_score_at_origination": (25, "Co-Borrower Credit Score at Origination", "int",
        "Representative Classic FICO of the co-borrower at acquisition. Blank if unknown.", None),
    "first_time_home_buyer_indicator": (26, "First-Time Home Buyer Indicator", "str",
        "Whether the borrower/co-borrower qualifies as a first-time homebuyer.",
        "Y = Yes; N = No; Null = Unknown"),
    "loan_purpose": (27, "Loan Purpose", "str",
        "Whether the loan is a purchase or a refinance.",
        "C = Cash-Out Refinance; R = Refinance; P = Purchase; U = Refinance-Not Specified"),
    "property_type": (28, "Property Type", "str",
        "The type of property securing the loan.",
        "CO = Condominium; CP = Co-Operative; PU = Planned Urban Development; "
        "MH = Manufactured Home; SF = Single-Family Home"),
    "number_of_units": (29, "Number of Units", "int",
        "The number of units in the mortgaged property (1-4).", None),
    "occupancy_status": (30, "Occupancy Status", "str",
        "Property occupancy status at origination.",
        "P = Principal; S = Second; I = Investor; U = Unknown"),
    "property_state": (31, "Property State", "str",
        "Two-letter abbreviation of the state/territory of the property.", None),
    "metropolitan_statistical_area": (32, "Metropolitan Statistical Area (MSA)", "str",
        "Numeric MSA/MSDA code for the property; '00000' if not in a designated area. "
        "High-cardinality geo — excluded from features.", None),
    "zip_code_short": (33, "Zip Code Short", "str",
        "First three digits of the property ZIP code. High-cardinality geo — excluded from "
        "features.", None),
    "mortgage_insurance_percentage": (34, "Mortgage Insurance Percentage", "float",
        "Original percentage of MI coverage used to compute the insurance benefit on default.",
        None),
    "amortization_type": (35, "Amortization Type", "str",
        "Whether the loan is fixed- or adjustable-rate at origination.",
        "FRM = Fixed Rate Mortgage; ARM = Adjustable Rate Mortgage"),
    "prepayment_penalty_indicator": (36, "Prepayment Penalty Indicator", "str",
        "Whether the borrower is subject to a penalty for early principal payment.", "Y = Yes; N = No"),
    "interest_only_loan_indicator": (37, "Interest Only Loan Indicator", "str",
        "Whether the loan requires only interest payments for a specified initial period.",
        "Y = Yes; N = No"),
    "interest_only_first_principal_and_interest_payment_date": (38,
        "Interest Only First P&I Payment Date", "date(MMYYYY)",
        "For interest-only loans, the month/year the first fully amortizing P&I payment is due.",
        None),
    "months_to_amortization": (39, "Months to Amortization", "int",
        "For interest-only loans, months from the current month to the first P&I payment date.",
        None),
    "current_loan_delinquency_status": (40, "Current Loan Delinquency Status", "str",
        "Number of months the obligor is delinquent per the governing documents. Blank after "
        "removal; 'XX' if unknown; '99' if >=99 months. LEAKAGE — the outcome column.",
        "0 = current; 1 = 30-59d; 2 = 60-89d; ... 6 = 180-209d (D180); XX = unknown"),
    "loan_payment_history": (41, "Loan Payment History", "str",
        "Coded 24-month string of monthly delinquency codes (most recent on the right). "
        "Non-tabular — excluded.",
        "00 = Current; 01 = 30-59d; 02 = 60-89d; ...; XX = unknown"),
    "modification_flag": (42, "Modification Flag", "str",
        "Whether the loan has been modified (set to Y from the first modification onward). "
        "Post-default servicing — leakage.", "Y = Yes; N = No"),
    "mortgage_insurance_cancellation_indicator": (43, "MI Cancellation Indicator", "str",
        "Whether mortgage insurance has been cancelled since origination.",
        "Y = cancelled; M = not cancelled; NA = never had MI"),
    "zero_balance_code": (44, "Zero Balance Code", "str",
        "Reason the loan balance was reduced to zero or experienced a credit event. Drives the "
        "default/prepay labels; LEAKAGE as a raw feature.",
        "01 = Prepaid/Matured; 02 = Third-Party Sale; 03 = Short Sale; 06 = Repurchased; "
        "09 = Deed-in-Lieu/REO; 15 = Non-Performing Note Sale; 16 = Reperforming Note Sale; "
        "96 = Removal (non-credit); 97 = D180 credit event; 98 = Other credit event"),
    "zero_balance_effective_date": (45, "Zero Balance Effective Date", "date(MMYYYY)",
        "Date the loan balance was reduced to zero. Leakage (termination timing).", None),
    "upb_at_the_time_of_removal": (46, "UPB at the Time of Removal", "float",
        "The unpaid principal balance at the time of removal/liquidation. Leakage.", None),
    "repurchase_date": (47, "Repurchase Date", "date(MMYYYY)",
        "Date a Reversed Credit Event Reference Obligation occurs. Leakage.", None),
    "scheduled_principal_current": (48, "Scheduled Principal Current", "float",
        "Minimum principal payment the borrower is obligated to pay for the reporting period.",
        None),
    "total_principal_current": (49, "Total Principal Current", "float",
        "Change between prior and current reporting period's Current Actual UPB.", None),
    "unscheduled_principal_current": (50, "Unscheduled Principal Current", "float",
        "Principal received in excess of the scheduled payment (prepayments).", None),
    "last_paid_installment_date": (51, "Last Paid Installment Date", "date(MMYYYY)",
        "Due date of the last paid installment collected. Post-default servicing — leakage.", None),
    "foreclosure_date": (52, "Foreclosure Date", "date(MMYYYY)",
        "Date the legal action of foreclosure completed. Outcome — leakage.", None),
    "disposition_date": (53, "Disposition Date", "date(MMYYYY)",
        "Date mortgage performance data's interest in the property ends. Outcome — leakage.", None),
    "foreclosure_costs": (54, "Foreclosure Costs", "float",
        "Expenses to obtain title, value, and maintain the property. Post-disposition — leakage.",
        None),
    "property_preservation_and_repair_costs": (55, "Property Preservation and Repair Costs", "float",
        "Expenses to secure/preserve the property (maintenance and repairs). Leakage.", None),
    "asset_recovery_costs": (56, "Asset Recovery Costs", "float",
        "Expenses to remove occupants/personal property post-foreclosure. Leakage.", None),
    "miscellaneous_holding_expenses_and_credits": (57, "Miscellaneous Holding Expenses and Credits",
        "float", "Expenses/credits associated with preserving the property (HOA dues, premiums, "
        "rental income, title). Leakage.", None),
    "associated_taxes_for_holding_property": (58, "Associated Taxes for Holding Property", "float",
        "Payment of taxes associated with holding the property. Leakage.", None),
    "net_sales_proceeds": (59, "Net Sales Proceeds", "float",
        "Total cash from the property sale net of selling expenses. Leakage.", None),
    "credit_enhancement_proceeds": (60, "Credit Enhancement Proceeds", "float",
        "Proceeds from MI claims and recourse/indemnification payments. Leakage.", None),
    "repurchase_make_whole_proceeds": (61, "Repurchase Make Whole Proceeds", "float",
        "Amounts received under rep-and-warranty arrangements for repurchase/loss reimbursement. "
        "Leakage.", None),
    "other_foreclosure_proceeds": (62, "Other Foreclosure Proceeds", "float",
        "Amounts other than sale proceeds received following foreclosure. Leakage.", None),
    "modification_related_non_interest_bearing_upb": (63,
        "Modification-Related Non-Interest Bearing UPB", "float",
        "Portion of UPB that will not accrue interest due to an eligible modification. Leakage.",
        None),
    "principal_forgiveness_amount": (64, "Principal Forgiveness Amount", "float",
        "Reduction of UPB formally agreed by lender and borrower, usually with a modification. "
        "Leakage.", None),
    "original_list_start_date": (65, "Original List Start Date", "date(MMYYYY)",
        "Date authorizing a broker to begin procuring a buyer for the property. REO listing — "
        "populated only heading to disposition; excluded.", None),
    "original_list_price": (66, "Original List Price", "float",
        "Initial price at which the property is offered for sale. Excluded (REO listing).", None),
    "current_list_start_date": (67, "Current List Start Date", "date(MMYYYY)",
        "Later date authorizing a broker to procure a buyer. Excluded (REO listing).", None),
    "current_list_price": (68, "Current List Price", "float",
        "Current price at which the property is offered for sale. Excluded (REO listing).", None),
    "borrower_credit_score_at_issuance": (69, "Borrower Credit Score at Issuance", "int",
        "Most recent borrower FICO (Equifax FICO 5) as of CRT deal issuance. Blank if unknown.",
        None),
    "co_borrower_credit_score_at_issuance": (70, "Co-Borrower Credit Score at Issuance", "int",
        "Most recent co-borrower FICO as of CRT deal issuance. Blank if unknown.", None),
    "borrower_credit_score_current": (71, "Borrower Credit Score Current", "int",
        "Most recent available borrower FICO for the loan. Blank if unknown.", None),
    "co_borrower_credit_score_current": (72, "Co-Borrower Credit Score Current", "int",
        "Most recent available co-borrower FICO for the loan. Blank if unknown.", None),
    "mortgage_insurance_type": (73, "Mortgage Insurance Type", "str",
        "The entity responsible for the MI premium payment.",
        "1 = Borrower Paid; 2 = Lender Paid; 3 = Enterprise/Investor Paid; Null = No MI"),
    "servicing_activity_indicator": (74, "Servicing Activity Indicator", "str",
        "Whether servicing activity changed during the reporting period. Leakage (contemporaneous "
        "servicing).", "Y = Yes; N = No"),
    "current_period_modification_loss_amount": (75, "Current Period Modification Loss Amount",
        "float", "Loss for the loan from a modification event this reporting period. Leakage.",
        None),
    "cumulative_modification_loss_amount": (76, "Cumulative Modification Loss Amount", "float",
        "Cumulative loss for the loan from modification events. Leakage.", None),
    "current_period_credit_event_net_gain_or_loss": (77, "Current Period Credit Event Net Gain/Loss",
        "float", "Net realized gain/loss from a credit event this period (positive = loss). "
        "Leakage.", None),
    "cumulative_credit_event_net_gain_or_loss": (78, "Cumulative Credit Event Net Gain/Loss", "float",
        "Cumulative net realized gain/loss from credit events. Leakage.", None),
    "special_eligibility_program": (79, "Special Eligibility Program", "str",
        "Expanded-eligibility program designed to increase/maintain home ownership.",
        "F = HFA Preferred; H = HomeReady/Home Possible; R = RefiNow; O = Other; "
        "7 = Not Applicable; 9 = Not Available"),
    "foreclosure_principal_write_off_amount": (80, "Foreclosure Principal Write-off Amount", "float",
        "Amounts determined uncollectable under foreclosure statute of limitations. Leakage.",
        None),
    "relocation_mortgage_indicator": (81, "Relocation Mortgage Indicator", "str",
        "Whether the loan is a relocation mortgage (employer-relocation borrower).", "Y = Yes; N = No"),
    "zero_balance_code_change_date": (82, "Zero Balance Code Change Date", "date(MMYYYY)",
        "Most recent date a loan-status change resulted in a Zero Balance Code change. Leakage.",
        None),
    "loan_holdback_indicator": (83, "Loan Holdback Indicator", "str",
        "Whether the loan is temporarily on 'hold' while mortgage performance data evaluates a unique situation. "
        "Excluded.", "Y = Yes (current); N = No (previously held); Null = not classified"),
    "loan_holdback_effective_date": (84, "Loan Holdback Effective Date", "date(MMYYYY)",
        "Date of the latest Loan Holdback indicator change. Excluded.", None),
    "delinquent_accrued_interest": (85, "Delinquent Accrued Interest", "float",
        "Lost accrued interest for a loan subject to a credit event. Leakage.", None),
    "property_valuation_method": (86, "Property Valuation Method", "str",
        "The method by which the property value was obtained. Null before 2017 acquisitions.",
        "A = Appraisal; C = Waiver + Data Collection (Condition); P = Waiver + Data Collection "
        "(Value); R = GSE Targeted Refinance; W = Appraisal Waiver; O = Other"),
    "high_balance_loan_indicator": (87, "High Balance Loan Indicator", "str",
        "Whether the original balance exceeds the general conforming limit up to the high-cost "
        "limit.", "Y = Yes; N = No"),
    "arm_initial_fixed_rate_period_le_5_yr_indicator": (88,
        "ARM Initial Fixed-Rate Period <= 5yr Indicator", "str",
        "For an ARM, whether the initial fixed-rate period is <=5 years.", "Y = Yes; N = No"),
    "arm_product_type": (89, "ARM Product Type", "str",
        "For an ARM, a string denoting initial fixed period, adjustment frequency, and loan term.",
        None),
    "initial_fixed_rate_period": (90, "Initial Fixed-Rate Period", "int",
        "For an ARM, months between the first accrual month and the initial rate change date.",
        None),
    "interest_rate_adjustment_frequency": (91, "Interest Rate Adjustment Frequency", "int",
        "For an ARM, months between scheduled rate changes.", None),
    "next_interest_rate_adjustment_date": (92, "Next Interest Rate Adjustment Date", "date(MMYYYY)",
        "For an ARM, the month/year the interest rate is next subject to change. Excluded.", None),
    "next_payment_change_date": (93, "Next Payment Change Date", "date(MMYYYY)",
        "For an ARM, the next date the borrower's payment amount is subject to change. Excluded.",
        None),
    "arm_index": (94, "ARM Index", "str",
        "For an ARM, the index on which rate adjustments are based.", None),
    "arm_cap_structure": (95, "ARM Cap Structure", "str",
        "For an ARM, a numeric string of the initial/periodic/lifetime interest-rate up caps.",
        None),
    "initial_interest_rate_cap_up_percent": (96, "Initial Interest Rate Cap Up Percent", "float",
        "For an ARM, max points the rate can adjust up at the initial change date.", None),
    "periodic_interest_rate_cap_up_percent": (97, "Periodic Interest Rate Cap Up Percent", "float",
        "For an ARM, max points the rate can adjust up at each subsequent change date.", None),
    "lifetime_interest_rate_cap_up_percent": (98, "Lifetime Interest Rate Cap Up Percent", "float",
        "For an ARM, max points the rate can adjust up over the life of the loan.", None),
    "mortgage_margin": (99, "Mortgage Margin", "float",
        "For an ARM, the rate added to the index value to set the new rate at each change date.",
        None),
    "arm_balloon_indicator": (100, "ARM Balloon Indicator", "str",
        "For an ARM, whether the loan has a balloon feature.", "Y = Yes; N = No"),
    "arm_plan_number": (101, "ARM Plan Number", "int",
        "For an ARM, the standardized plan code under which the loan was delivered.", None),
    "borrower_assistance_plan": (102, "Borrower Assistance Plan", "str",
        "The type of assistance plan providing temporary payment relief. Contemporaneous "
        "loss-mitigation — leakage.",
        "F = Forbearance; R = Repayment; T = Trial Period; O = Other Workout; N = No Workout; "
        "7 = Not Applicable; 9 = Not Available"),
    "high_loan_to_value_hltv_refinance_option_indicator": (103,
        "High LTV (HLTV) Refinance Option Indicator", "str",
        "Whether an eligible loan is refinanced under mortgage performance data's HLTV refinance option.",
        "Y = Yes; N = No"),
    "deal_name": (104, "Deal Name", "str",
        "The title of the series issuance. Identifier metadata — excluded.", None),
    "repurchase_make_whole_proceeds_flag": (105, "Repurchase Make Whole Proceeds Flag", "str",
        "Whether mortgage performance data received rep-and-warranty repurchase proceeds. Leakage.",
        "Y = Yes; N = No"),
    "alternative_delinquency_resolution": (106, "Alternative Delinquency Resolution", "str",
        "Loss-mitigation solution to resolve delinquencies while keeping the loan in the security. "
        "Contemporaneous — leakage.",
        "P = Payment Deferral; C = Payment Deferral (COVID-19); D = Payment Deferral (Disaster); "
        "7 = Not Applicable; 9 = Not Available"),
    "alternative_delinquency_resolution_count": (107, "Alternative Delinquency Resolution Count",
        "int", "Total number of alternative delinquency resolutions reported for the loan. "
        "Leakage.", None),
    "total_deferral_amount": (108, "Total Deferral Amount", "float",
        "Total non-interest-bearing deferral amount from alternative delinquency resolutions. "
        "Leakage.", None),
    "payment_deferral_modification_event_indicator": (109,
        "Payment Deferral Modification Event Indicator", "str",
        "Whether a payment deferral contributes to a modification event. Leakage.",
        "Y = Yes; N = No; 7 = Not Applicable"),
    "interest_bearing_upb": (110, "Interest Bearing UPB", "float",
        "Current actual UPB less any non-interest-bearing UPB from an eligible modification. "
        "Masked for the first six months.", None),
    "origination_classic_fico": (111, "Origination Classic FICO", "int",
        "Standardized Classic FICO used at origination (lowest representative score across "
        "borrowers). Populated from Dec-2025.", None),
    "issuance_classic_fico": (112, "Issuance Classic FICO", "int",
        "Most recent Classic FICO as of CRT deal issuance. Populated from Dec-2025.", None),
    "current_classic_fico": (113, "Current Classic FICO", "int",
        "Most recent available Classic FICO for the loan. Populated from Dec-2025.", None),
}

# Columns scripts/ingest_mortgage_performance.py adds or rewrites (position=None: not in the source layout).
DERIVED_FIELDS: dict[str, tuple[None, str, str, str, str | None]] = {
    "loan_id": (None, "Loan ID", "str",
        "Renamed from loan_identifier (field 2). The entity key — splits are by loan_id, never by "
        "row.", None),
    "reporting_date": (None, "Reporting Date (ISO)", "date(ISO)",
        "monthly_reporting_period (MMYYYY) parsed to an ISO 'YYYY-MM-DD' month-end string — the "
        "chronologically sortable observation time column.", None),
    "dlq_num": (None, "Delinquency Months (numeric)", "Int64",
        "current_loan_delinquency_status cast to a number; 'XX'/blank -> <NA>. LEAKAGE (the "
        "outcome).", None),
    "default_event": (None, "Default Event (label)", "boolean",
        "TRUE if dlq_num >= 6 (D180, 180+ days delinquent) OR zero_balance_code is a credit event "
        "(02/03/09/15). This is the model LABEL.", "True = default; False = not; <NA> = unknown dlq"),
    "prepay_event": (None, "Prepay Event", "bool",
        "TRUE if zero_balance_code == '01' (prepaid or matured). A clean, non-credit termination.",
        "True = prepaid/matured; False = not"),
    "is_performing": (None, "Is Performing (gate)", "boolean",
        "TRUE if the loan is current (dlq_num == 0) and not yet terminated (no credit event, no "
        "prepay). Used as the 'currently performing' gate so the task predicts NEW defaults.",
        "True = performing; False = not; <NA> = unknown dlq"),
}

# Convenience: everything, in one dict.
ALL_FIELDS = {**RAW_FIELDS, **DERIVED_FIELDS}


def describe(name: str) -> str:
    """Return a one-line 'plain_name — description' for a column name (or the name if unknown)."""
    entry = ALL_FIELDS.get(name)
    return f"{entry[1]} — {entry[3]}" if entry else name
