# Column Glossary — Synthetic Dutch RMBS Panel

Definitions for all 71 columns of the Hypoport / ESMA Annex 2 schema produced by
this pipeline. For each column you get its ESMA reference code (where it maps
cleanly), its data type, whether it is **static** (set at origination, identical
for the same `loan_id` across all 24 cutoffs) or **dynamic** (recomputed per
cutoff), the allowed values or range, and a one-paragraph definition.

**ESMA reference:** Annex 2 of Commission Delegated Regulation (EU) 2020/1224,
"Underlying exposures information — residential real estate". Codes use the
form **RREL\<n\>** for residential real-estate loans. Where the Hypoport
column has no direct ESMA equivalent (e.g. internal bucket columns), the ESMA
code is shown as `n/a`.

**Legend**

| Marker | Meaning |
|---|---|
| **S** | **Static** — set at origination, identical across all cutoffs of the same `loan_id`. |
| **D** | **Dynamic** — evolves per cutoff via the ageing pass. |
| **C** | **Constant** — single value across the entire panel (deal-level metadata). |

---

## 1. Identifiers & transaction (9 columns)

### `loan_id` — **S**
- **ESMA:** RREL1 (*Unique identifier*).
- **Type:** string, format `GL<YYYY>_<6-digit zero-padded sequence>` (e.g. `GL2024_000001`).
- **Definition:** Stable per-loan key. Identical for the same loan across all
  24 cutoffs. Sequential and collision-free by construction.

### `transaction_name` — **C**
- **ESMA:** Deal-level (no per-loan ESMA code).
- **Type:** string, e.g. `"Green Lion 2024-1 B.V."`.
- **Definition:** Legal name of the securitisation Special Purpose Vehicle that
  holds the pool. Single value across the entire panel; derived from
  `--deal-year`.

### `esma_transaction_identifier` — **C**
- **ESMA:** Securitisation Repository transaction identifier.
- **Type:** string, 24 chars: 20-char LEI prefix of the SPV + `YYYYMM` of deal
  close (e.g. `3TK20IVIUJ8J3ZU0QE75N202401`).
- **Definition:** Unique identifier under the EU Securitisation Regulation
  reporting regime. Single value per deal.

### `reporting_date` — **D**
- **ESMA:** RREL5 / pool cut-off date.
- **Type:** date, ISO 8601 `YYYY-MM-DD`. Always a month-end.
- **Definition:** Pool observation date. The value at which all dynamic
  fields (balance, arrears, LTVs, etc.) are measured. Strictly increasing
  per `loan_id`.

### `closing_date` — **C**
- **ESMA:** Deal closing date.
- **Type:** date, ISO 8601 `YYYY-MM-DD`. First business day of January of
  the deal year (e.g. `2024-01-01`).
- **Definition:** Date when the notes were issued and the deal closed.
  Single value across the entire panel.

### `originator_name` — **C**
- **ESMA:** RREL4 (*Originator*).
- **Type:** string (`"ING"`).
- **Definition:** Institution that originated the underlying loans.

### `servicer_name` — **C**
- **ESMA:** Servicer / cash manager (RREL64 family).
- **Type:** string (`"ING"`).
- **Definition:** Institution servicing the loans (collecting payments,
  managing arrears).

### `currency` — **C**
- **ESMA:** RREL36 (*Denomination of the underlying exposure*).
- **Type:** ISO 4217 string (`"EUR"`).
- **Definition:** Currency in which the loan is denominated.

### `country` — **C**
- **ESMA:** Country of the obligor (RREL11).
- **Type:** ISO 3166-1 alpha-2 (`"NL"`).
- **Definition:** Country of the obligor — Netherlands for the entire Green
  Lion pool.

---

## 2. Loan economics & terms (14 columns)

### `origination_year` — **S**
- **ESMA:** Year component of RREL15 (*Date of origination*).
- **Type:** integer, 2008–2023.
- **Definition:** Calendar year when the mortgage was originated.

### `maturity_date_proxy` — **S**
- **ESMA:** RREL16 (*Date of maturity*).
- **Type:** date string, `YYYY-MM-28` format.
- **Definition:** Contractual loan maturity date, computed as
  `origination_year + (legal_maturity_months / 12)`. The "proxy" suffix
  reflects that day-of-month is fixed to 28 for synthesis convenience.

### `original_balance` — **S**
- **ESMA:** RREL42 (*Original balance*).
- **Type:** float, EUR. Plausible range €500 – €5,000,000.
- **Definition:** Loan principal at origination, in EUR. Drawn from a
  log-normal with parameters `s=0.40, scale=300000` to anchor at the
  Dutch prime ~€300k median.

### `current_balance` — **D**
- **ESMA:** RREL52 (*Current balance*).
- **Type:** float, EUR. Always `≤ original_balance`.
- **Definition:** Outstanding principal at the reporting date. Evolution is
  state-dependent: Performing amortising loans drop by one annuity step
  per month (`B_{t+1} = B_t·(1+r) − M`); interest-only Performing loans
  stay flat; loans in any DPD bucket or Defaulted have a **frozen**
  balance (no principal change while no payments are coming in); terminal
  states (Redeemed, Charged-Off) carry zero.

### `repayment_type` — **S**
- **ESMA:** RREL49 (*Type of repayment*).
- **Type:** enum string. Values: `Annuity`, `Linear`, `InterestOnly`,
  `Bullet`, `Savings`. Default mix 80 / 6 / 8 / 3 / 3 %.
- **Definition:** Amortisation schedule type. Annuity dominates Dutch
  prime; Interest-only and Bullet carry no principal reduction during
  the loan term.

### `interest_only_flag` — **S**
- **ESMA:** Implied by RREL49.
- **Type:** `Y` / `N`.
- **Definition:** Derived: `Y` iff `repayment_type ∈ {InterestOnly,
  Bullet}`. Drives the amortisation branch in the ageing pass.

### `current_interest_rate_pct` — **S**
- **ESMA:** RREL57 (*Current interest rate*).
- **Type:** float, percentage. Typically 1.5 – 5.0 %.
- **Definition:** APR currently applied to the loan. Synthesised from
  `Normal(3.10, 0.65)` clipped to plausible NL prime range.

### `rate_type` — **S**
- **ESMA:** RREL55 (*Interest rate type*).
- **Type:** enum string. `Fixed`, `Variable`, `Hybrid`. Mix 90 / 7 / 3 %.
- **Definition:** Fixed-vs-floating classification of the interest rate.

### `remaining_interest_fixed_period_months` — **D**
- **ESMA:** Derived from RREL60 (*Interest rate reset period*).
- **Type:** integer months, ≥ 0.
- **Definition:** Months remaining until the next interest-rate reset.
  Decrements by 1 each cutoff and floors at 0.

### `fixed_interest_period_end_in_months` — **D**
- **ESMA:** Same family as above.
- **Type:** integer months, ≥ 0.
- **Definition:** Synonym carried for Hypoport parity; equal to
  `remaining_interest_fixed_period_months` in our pipeline.

### `seasoning_months` — **D**
- **ESMA:** RREL14 (*Account seasoning*).
- **Type:** integer months, ≥ 1.
- **Definition:** Calendar months elapsed since origination (regardless of
  whether the loan is Performing or in arrears). Increments by exactly +1
  between consecutive cutoffs.

### `remaining_term_months` — **D**
- **ESMA:** RREL61 (*Maturity of the underlying exposure* − seasoning).
- **Type:** integer months, ≥ 0.
- **Definition:** Months remaining until maturity. Computed as
  `legal_maturity_months − seasoning_months`, floored at 0.

### `legal_maturity_months` — **S**
- **ESMA:** RREL16 (term in months).
- **Type:** integer. Discrete values `{240, 300, 330, 360}` (i.e. 20-, 25-,
  27.5- and 30-year terms).
- **Definition:** Contractual loan term at origination, in months.

### `loan_part_count` — **S**
- **ESMA:** Implied by Dutch RMBS reporting (multiple legal sub-loans per
  obligor are common).
- **Type:** integer `1`–`4`. Mix 55 / 30 / 10 / 5 %.
- **Definition:** Number of legal loan parts the obligor has at this
  property. In the current panel we emit one row per `loan_id` regardless;
  this column is descriptive metadata only.

---

## 3. Borrower & property (14 columns)

### `debtor_count` — **S**
- **ESMA:** RREL10 (*Number of obligors*).
- **Type:** integer `1`–`3`. Mix 30 / 66 / 4 %.
- **Definition:** Number of natural persons jointly liable for the loan.

### `property_type` — **S**
- **ESMA:** RREL75 (*Property type*).
- **Type:** enum. `House`, `Apartment`, `Townhouse`, `Detached`,
  `SemiDetached`. Mix 45 / 30 / 13 / 7 / 5 %.
- **Definition:** Built form of the financed property.

### `province` — **S**
- **ESMA:** RREL81 (*Region*).
- **Type:** enum, Dutch province name (Zuid-Holland, Noord-Holland, …).
- **Definition:** Dutch province (NUTS-2 equivalent) in which the property
  is located. Distribution weighted toward Hypoport empirical.

### `economic_region_nuts3` — **S**
- **ESMA:** RREL82 (*Region — NUTS-3*).
- **Type:** string, NUTS-3 code (e.g. `NL331`).
- **Definition:** Eurostat NUTS-3 region code. Conditional on `province`
  via the Data Designer `SUBCATEGORY` sampler.

### `construction_year` — **S**
- **ESMA:** RREL76 (*Year of construction*).
- **Type:** integer year. Sampled bins from 1900 to 2022.
- **Definition:** Year the property was built.

### `occupancy` — **S**
- **ESMA:** RREL78 (*Occupancy type*).
- **Type:** enum. `OwnerOccupied`, `TenantOccupied`, `Vacant`,
  `PartiallyOccupied`. Mix 88 / 8 / 2 / 2 %.
- **Definition:** Occupancy status of the property.

### `property_usage` — **S**
- **ESMA:** Derived from RREL78.
- **Type:** enum. `OwnerOccupied`, `BuyToLet`.
- **Definition:** Derived: `BuyToLet` if `occupancy != OwnerOccupied`,
  else `OwnerOccupied`. Drives `buy_to_let_flag`.

### `employment_status` — **S**
- **ESMA:** RREL19 (*Employment status of the obligor*).
- **Type:** enum. `Employed`, `SelfEmployed`, `Retired`, `Unemployed`,
  `Student`, `Other`. Mix 72 / 14 / 8 / 2 / 1 / 3 %.
- **Definition:** Primary obligor's employment status at origination.

### `self_employed_flag` — **S**
- **ESMA:** Implied by RREL19.
- **Type:** `Y` / `N`.
- **Definition:** Derived: `Y` iff `employment_status == SelfEmployed`.

### `borrower_type` — **S**
- **ESMA:** RREL12 (*Obligor type*).
- **Type:** enum. `Consumer`, `Corporate`. Mix 98.5 / 1.5 %.
- **Definition:** Legal classification of the obligor.

### `loan_purpose` — **S**
- **ESMA:** RREL26 (*Purpose*).
- **Type:** enum. `Purchase/Refinance`, `Construction`, `Renovation`,
  `Other`. Mix 86 / 4 / 7 / 3 %.
- **Definition:** Stated purpose of the loan at origination.

### `buy_to_let_flag` — **S**
- **ESMA:** Implied by RREL78.
- **Type:** `Y` / `N`.
- **Definition:** Derived: `Y` iff `property_usage == BuyToLet`.

### `nhg_flag` — **S**
- **ESMA:** NL-specific (NHG = Nationale Hypotheek Garantie).
- **Type:** `Y` / `N`.
- **Definition:** Whether the loan is insured by the NHG. Bernoulli prior
  ~45% with the 2024 NHG cap of €435,000 enforced — any loan above the
  cap is automatically `N`.

### `guarantee_type` — **S**
- **ESMA:** RREL27 (*Type of guarantee*).
- **Type:** enum. `NHG`, `None`.
- **Definition:** Derived: `"NHG"` if `nhg_flag == Y`, else literal string
  `"None"` (matches Hypoport convention — not SQL NULL).

---

## 4. LTV / valuation (7 columns)

### `oltomv_original` — **S**
- **ESMA:** RREL47 (*Original loan-to-value*).
- **Type:** float, percentage. Typically 50 – 110.
- **Definition:** Loan-to-Market-Value at origination:
  `original_balance / original_market_value × 100`. Drawn from a
  truncated normal centred on 85.

### `cltomv_current` — **D**
- **ESMA:** RREL97 (*Current loan-to-value*).
- **Type:** float, percentage. 0 (terminal loans) up to ~110.
- **Definition:** Current Loan-to-Market-Value, computed from
  `current_balance / current_original_market_value × 100` for active
  loans, 0 for terminal.

### `cltimv_current` — **D**
- **ESMA:** RREL98 (*Current loan-to-indexed-value*).
- **Type:** float, percentage.
- **Definition:** Current Loan-to-Indexed-Market-Value using the indexed
  property value. Typically lower than `cltomv_current` in a rising HPI
  environment.

### `original_market_value_at_origination` — **S**
- **ESMA:** RREL43 (*Original valuation amount*).
- **Type:** float, EUR.
- **Definition:** Appraised market value at the time the loan was
  originated. Derived: `original_balance / (oltomv_original / 100)`.

### `current_original_market_value` — **D**
- **ESMA:** RREL96 (*Current valuation amount*).
- **Type:** float, EUR.
- **Definition:** Market value rolled forward from origination using the
  HPI drift. Equal to `original_market_value_at_origination ×
  hpi_growth_factor` for active loans.

### `indexed_market_value` — **D**
- **ESMA:** RREL95 (*Indexed valuation amount*).
- **Type:** float, EUR.
- **Definition:** Market value indexed by the chosen HPI series. In the
  default pipeline this is also driven by a 3 % p.a. drift; swap for a
  real NL CBS HPI series for stress-test realism.

### `property_valuation_type` — **S**
- **ESMA:** RREL98 (*Valuation type*).
- **Type:** enum string. Currently constant `"Indexed/Origination Proxy"`.
- **Definition:** Source / type of the valuation. Constant in the synthesis;
  real data flips after re-valuations.

---

## 5. Affordability (4 columns)

### `loan_to_income` — **S**
- **ESMA:** RREL106 (*Loan to income ratio*).
- **Type:** float. Typically 1.5 – 6.0.
- **Definition:** Derived: `original_balance / borrower_annual_income`.

### `payment_due_to_income_pct` — **S**
- **ESMA:** RREL107 (*Debt-service-to-income*).
- **Type:** float, percentage.
- **Definition:** Derived: `scheduled_monthly_payment × 12 /
  borrower_annual_income × 100`.

### `borrower_annual_income` — **S**
- **ESMA:** RREL21 (*Primary income*).
- **Type:** float, EUR. Drawn from `LogNormal(s=0.45, scale=65000)`.
- **Definition:** Gross primary income of the obligor at origination.

### `scheduled_monthly_payment` — **S**
- **ESMA:** RREL59 (*Periodic payment*).
- **Type:** float, EUR.
- **Definition:** Contractual monthly payment. For amortising loans:
  `original_balance × r × (1+r)^n / ((1+r)^n − 1)` (standard annuity).
  For interest-only: `original_balance × r`. Identical per `loan_id`
  across all cutoffs (validated by SQL test D07).

---

## 6. Performance (8 columns)

### `arrears_bucket` — **D**
- **ESMA:** RREL130 (*Account status*).
- **Type:** enum. `Performing`, `1-29 DPD`, `30-59 DPD`, `60-89 DPD`,
  `90+ DPD`, `Defaulted`, `Charged-Off`, `Redeemed`.
- **Definition:** Delinquency bucket of the loan at this cutoff. Driven
  by the Markov delinquency chain in `age_to_panel.py`.

### `arrears_amount` — **D**
- **ESMA:** RREL128 (*Arrears amount*).
- **Type:** float, EUR. Always `≥ 0`. Zero for Performing and terminal.
- **Definition:** Cumulative missed scheduled payments. Accrues by
  exactly +1 `scheduled_monthly_payment` for each consecutive
  non-Performing month, resets to 0 on cure or terminal.

### `days_past_due` — **D**
- **ESMA:** RREL133 (*Number of days in arrears*).
- **Type:** integer days. Bucket midpoints `{0, 15, 45, 75, 120, 200}`.
- **Definition:** Days past due, reported at the midpoint of the arrears
  bucket: 0 for Performing/terminal, 15 for 1-29 DPD, 45 for 30-59,
  75 for 60-89, 120 for 90+, 200 for Defaulted/Charged-Off.

### `default_crr_flag` — **D**
- **ESMA:** RREL135 (*Default flag — CRR Article 178*).
- **Type:** `Y` / `N`.
- **Definition:** `Y` when `arrears_bucket ∈ {90+ DPD, Defaulted,
  Charged-Off}`, else `N`. Corresponds to the CRR 90-day default
  definition.

### `performing_status` — **D**
- **ESMA:** RREL134 (*Performing status*).
- **Type:** enum. `Non-defaulted`, `Defaulted`, `Charged-Off`, `Redeemed`.
- **Definition:** Top-level performance label. Non-defaulted covers
  Performing through 90+ DPD; Defaulted is the absorbing default state;
  Charged-Off and Redeemed are terminal cutoff-only labels (the loan
  drops out next cutoff).

### `foreclosure_flag` — **D**
- **ESMA:** RREL137 (*Foreclosure flag*).
- **Type:** `Y` / `N`.
- **Definition:** `Y` when the loan is Defaulted or Charged-Off (i.e.
  foreclosure proceedings are open or have concluded).

### `forbearance_flag` — **S**
- **ESMA:** RREL142 (*Forbearance flag*).
- **Type:** `Y` / `N`. Mix 98.5 / 1.5 %.
- **Definition:** Whether the loan has been forborne (servicer concession
  due to financial difficulty).

### `restructuring_flag` — **S**
- **ESMA:** RREL143 (*Restructuring flag*).
- **Type:** `Y` / `N`. Mix 99 / 1 %.
- **Definition:** Whether the loan has been formally restructured.

---

## 7. Energy / ESG (3 columns)

### `epc_label` — **S**
- **ESMA:** RREL90 (*Energy Performance Certificate value*).
- **Type:** enum. `A+++, A++, A+, A, B, C, D, E, F, G`.
- **Definition:** Dutch EPC label of the property. Weights skew toward
  modern ratings post-2015 (A/B), with a long tail of older D-G stock.

### `epc_issue_year` — **S**
- **ESMA:** RREL91 (*EPC issue date* — year part).
- **Type:** integer year.
- **Definition:** Year the EPC was issued. Pegged to `origination_year` in
  the synthesis (EPCs are typically refreshed at sale/refinance).

### `primary_energy_demand_kwh_m2` — **S**
- **ESMA:** RREL93 (*Primary energy demand*).
- **Type:** integer kWh/m²/yr. Range 30 – 420.
- **Definition:** Mid-point of the EPC band's kWh/m²/yr range. Deterministic
  mapping from `epc_label`.

---

## 8. Construction deposit (3 columns)

Dutch-specific: a "bouwdepot" earmarks part of the loan for renovation /
construction works.

### `construction_deposit_flag` — **S**
- **ESMA:** NL-specific.
- **Type:** `Y` / `N`. Y only when `loan_purpose ∈ {Renovation, Construction}`.
- **Definition:** Whether a construction deposit was set up at origination.

### `construction_deposit_pct` — **S**
- **ESMA:** NL-specific.
- **Type:** integer percentage. `0` when flag is N, else 10 – 24.
- **Definition:** Construction deposit as a percentage of the original loan.

### `construction_deposit_amount` — **S**
- **ESMA:** NL-specific.
- **Type:** float, EUR.
- **Definition:** `original_balance × construction_deposit_pct / 100`.

---

## 9. Payment frequencies (2 columns)

### `interest_payment_frequency` — **S**
- **ESMA:** RREL56 (*Interest payment frequency*).
- **Type:** string. Currently constant `"Monthly"`.
- **Definition:** Periodicity at which interest is charged.

### `principal_payment_frequency` — **S**
- **ESMA:** RREL58 (*Principal repayment frequency*).
- **Type:** string. Currently constant `"Monthly"`.
- **Definition:** Periodicity at which principal is amortised.

---

## 10. Pre-computed bucket columns (7 columns)

These are deterministic discretisations of underlying continuous fields,
included for parity with Hypoport's format and for analytics convenience.

### `balance_bucket` — **D**
- **ESMA:** n/a — internal bucket.
- **Type:** enum string. Edges `<100k, 100k-150k, 150k-200k, …, 600k+`.
- **Definition:** Bucketed `current_balance`. Re-computed each cutoff.

### `cltomv_current_bucket` — **D**
- **ESMA:** n/a — internal bucket.
- **Type:** enum string. Edges `<20, 20-30, 30-40, …, 90-100, 100+`.
- **Definition:** Bucketed `cltomv_current`. Re-computed each cutoff.

### `cltimv_current_bucket` — **D**
- **ESMA:** n/a — internal bucket.
- **Type:** enum string. Same edges as `cltomv_current_bucket`.
- **Definition:** Bucketed `cltimv_current`. Re-computed each cutoff.

### `oltomv_original_bucket` — **S**
- **ESMA:** n/a — internal bucket.
- **Type:** enum string. Same edges as the LTV buckets above.
- **Definition:** Bucketed `oltomv_original`. Static (origination value).

### `loan_to_income_bucket` — **S**
- **ESMA:** n/a — internal bucket.
- **Type:** enum string. Edges `<1.5, 1.5-2.0, 2.0-2.5, …, 5.5-6.0, 6.0+`.
- **Definition:** Bucketed `loan_to_income`. Static.

### `payment_due_to_income_pct_bucket` — **S**
- **ESMA:** n/a — internal bucket.
- **Type:** enum string. Edges `<5, 5-10, 10-15, …, 30-35, 35+`.
- **Definition:** Bucketed `payment_due_to_income_pct`. Static.

### `construction_year_bucket` — **S**
- **ESMA:** n/a — internal bucket.
- **Type:** enum string. Edges `<1945, 1945-1960, 1960-1970, …, 2010-2015, 2015+`.
- **Definition:** Bucketed `construction_year`. Static.

---

## Quick reference — column count by category

| Category | Columns | Count |
|---|---|---|
| Identifiers & transaction | loan_id, transaction_name, esma_transaction_identifier, reporting_date, closing_date, originator_name, servicer_name, currency, country | **9** |
| Loan economics & terms | origination_year, maturity_date_proxy, original_balance, current_balance, repayment_type, interest_only_flag, current_interest_rate_pct, rate_type, remaining_interest_fixed_period_months, fixed_interest_period_end_in_months, seasoning_months, remaining_term_months, legal_maturity_months, loan_part_count | **14** |
| Borrower & property | debtor_count, property_type, province, economic_region_nuts3, construction_year, occupancy, property_usage, employment_status, self_employed_flag, borrower_type, loan_purpose, buy_to_let_flag, nhg_flag, guarantee_type | **14** |
| LTV / valuation | oltomv_original, cltomv_current, cltimv_current, original_market_value_at_origination, current_original_market_value, indexed_market_value, property_valuation_type | **7** |
| Affordability | loan_to_income, payment_due_to_income_pct, borrower_annual_income, scheduled_monthly_payment | **4** |
| Performance | arrears_bucket, arrears_amount, days_past_due, default_crr_flag, performing_status, foreclosure_flag, forbearance_flag, restructuring_flag | **8** |
| Energy / ESG | epc_label, epc_issue_year, primary_energy_demand_kwh_m2 | **3** |
| Construction deposit | construction_deposit_flag, construction_deposit_pct, construction_deposit_amount | **3** |
| Payment frequencies | interest_payment_frequency, principal_payment_frequency | **2** |
| Pre-computed buckets | balance_bucket, cltomv_current_bucket, cltimv_current_bucket, oltomv_original_bucket, loan_to_income_bucket, payment_due_to_income_pct_bucket, construction_year_bucket | **7** |
| **Total** | | **71** |
