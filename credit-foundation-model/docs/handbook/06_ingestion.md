# Part 6 — Ingestion

> **You are here:**  raw ─▶ [INGEST] ─▶ validate ─▶ split ─▶ tokenize ─▶ encode ─▶ pretrain ─▶ fine-tune ─▶ score ─▶ calibrate ─▶ serve


> Files: `scripts/ingest.py` (asset-blind driver) · `reference_implementations/mortgage_performance/adapter.py`
> (all Mortgage knowledge) · recipe `configs/mortgage_performance/ingest_2000_2024.yaml`.
> Historical note: this stage began life as a single `scripts/ingest_mortgage_performance.py`; v1.1 split it
> into driver + adapter (a thin compatibility shim keeps the old command working).

## 6.1 Purpose

Turn the raw, quarterly, cryptically-coded source into the **contract panel** — one clean
parquet dataset, one row per loan-month, with ISO dates, string ids, and the derived label
columns — written **shard-by-shard so a killed run resumes**, and optionally down-sampled to a
deterministic N% of loans.

```
INPUT   gs://…/raw_by_reporting/reporting_year=YYYY/reporting_quarter=Q#/*.parquet  (~4B rows raw)
CONFIG  sources (root + quarter list) · sample_pct · workers · sharded/combine · out
OUTPUT  <out>/panel_2000_2024/part-<YYYYQ#>.parquet   one per quarter
        <out>/panel_2000_2024/_meta-<YYYYQ#>.json     completion sidecars (the resume mechanism)
        <out>/panel_2000_2024/_ingest.meta.json       manifest with per-shard stats + full config
```

## 6.2 The split of responsibilities (why two files)

```
scripts/ingest.py (driver — knows NOTHING about Mortgage)      reference_implementations/mortgage_performance/adapter.py
─ reads the recipe + dataset.yaml contract                    ─ knows the hive layout
─ resolves the adapter by name from the registry              ─ knows MMYYYY dates, ZBC codes, D180
─ orchestrates: pending sources → thread pool → shards        ─ derives the contract columns
─ skip-if-complete resume; manifest; combine option           ─ applies the loan-hash sample
```

This is the G1 framework seam: to onboard a new dataset you replace the right column, never the
left. The driver works for any adapter that can `load_source()`.

## 6.3 The driver's functions (`scripts/ingest.py`)

**`ingest_sharded(adapter, shard_dir, *, workers, log)` — the heart.**
- *Inputs:* an adapter; the output directory; thread count.
- *Logic:* list `adapter.sources()` → compute a unique **tag** per source (`2016Q1`; via
  `adapter.source_tag` or a sanitized-basename fallback; duplicate tags are rejected) → for each
  source whose sidecar `_meta-<tag>.json` does **not** exist: `load_source()` → write
  `part-<tag>.parquet` → *then* write the sidecar. A thread pool runs `workers` sources at once.
- *Output:* summary dict `{shards, rows, written, skipped, per_shard}` assembled from **all**
  sidecars (so totals are right even when everything was skipped).
- *The resume invariant (memorize this):* **the sidecar is written strictly after its shard.**
  A kill during the parquet write leaves shard-without-sidecar → that source is redone next run
  (`write_parquet` truncates, so the partial file is cleanly overwritten). A kill between shard
  and sidecar merely re-reads one source. There is no state file to corrupt.
- *Example:* the design's acceptance test kills after 3 of 4 quarters; the rerun reads exactly
  one (`tests/test_ingest_sharded.py::test_kill_after_three_quarters_then_rerun_reads_only_the_rest`).

**`main()`** — config plumbing: refuses `adapter: generic` (a conforming panel needs no ingest —
point `prepare_data` at it), falls back to the legacy whole-panel single-file path when
`sharded: false`, and with `combine: true` additionally concatenates shards into the v1.0 single
file (RAM-bound — never at 100%).

## 6.4 The adapter's functions (`MortgagePerformanceAdapter`)

**`_iso_month_end(series)`** — `"042020"` → `"2020-04-30"`.
- *Why month-end strings, not timestamps:* ISO strings sort chronologically, survive parquet
  round-trips identically, and are what every comparison downstream (`<= cutoff`) expects.
- *Edge handled:* leap years (Feb 2016 → 02-29), blanks/garbage → `NA`, plus a permissive
  fallback parse for odd mirrors of the dataset.

**`_derive(df)`** — the business logic (§5.5's table in code):
- Renames `loan_identifier → loan_id` (kept as **string** — these ids look numeric, and a CSV
  round-trip that coerces them to int has caused real mismatch bugs).
- `dlq_num` = numeric delinquency (`"XX"`/blank → NA).
- `default_event` = `dlq_num >= 6` (D180) OR ZBC ∈ {02,03,09,15};
  `prepay_event` = ZBC == "01"; `is_performing` = current and not terminated.
- Missing any of the 5 required raw columns → hard `SystemExit` naming them.
- *Dtype subtlety you will eventually hit:* `default_event`/`is_performing` are **nullable**
  booleans (NA propagates from unknown delinquency); `prepay_event` is plain bool. Every
  consumer does `.fillna(False)`. A test locks this contract.

**`load_source(src)`** — read one quarter (file or hive dir) → `_derive` → hash sample:

```python
keep = pd.util.hash_pandas_object(df["loan_id"], index=False) % 100 < sample_pct
```

- *Why hashing instead of `df.sample()`:* determinism across runs AND across quarters — a loan
  is in the 10% sample in *every* quarter of its life or in none. Random sampling per quarter
  would shred histories.

**`source_tag(src)`** — `…/reporting_year=2016/reporting_quarter=Q1` → `"2016Q1"` (the shard name).

**`load_panel()`** — legacy: all sources through a thread pool, one concatenated frame.

## 6.5 Example run

```bash
python scripts/ingest.py -c configs/mortgage_performance/ingest_2000_2024.yaml \
    --sample_pct 10 --combined_name panel_2000_2024_10pct.parquet
# ...
#   skip part-2000Q1.parquet (already complete)          ← resumed run
#   done gs://…/reporting_quarter=Q3: 3,412,887 rows  228,145 loans  reporting 2000-07-31..2000-09-30
#   wrote part-2000Q3.parquet  (3,412,887 rows, 228,145 loans)
# Wrote 100 shard(s) -> …/panel_2000_2024_10pct: 331,208,554 rows (97 written, 3 skipped as complete)
# Next: python scripts/prepare_data.py … --input …/panel_2000_2024_10pct
```

Note the honesty in the summary: per-shard *loan* counts are printed but never summed — a loan
reports in many quarters, so loan counts don't add.

## 6.6 Common errors & debugging

| Symptom | Cause → fix |
|---|---|
| `Missing expected source columns [...]` | Source isn't the published layout (wrong root / mirror with renamed cols) → check `sources.root`, inspect one file's columns |
| `ArrowNotImplementedError` reading `gs://` | Container's pyarrow has no native GCS → always go through `storage.read_parquet` (gcsfs), never `pd.read_parquet("gs://…")` directly |
| Hangs then dies hours in with SSL/OAuth errors | Transient cloud failures → `storage.retry()` already handles the known markers; if a new marker appears, add it to `_TRANSIENT_MARKERS` |
| Rerun re-reads a quarter you thought was done | Its sidecar is missing — the previous run died mid-write there. That's the mechanism working, not a bug |
| Reading the shard DIR dies with `Unsupported cast from string to null` | A column entirely empty in some quarters (REO/modification fields in year-2000 shards) is arrow `null`-typed there and `string` elsewhere → `storage.read_parquet`/`streaming` unify fragment schemas automatically (fix #111); if you scan shard dirs with raw pyarrow, unify first |
| `source tags collide` | Two sources map to one shard name (e.g. same basename in different dirs) → give the adapter a disambiguating `source_tag` |

**Performance notes:** `workers` is a *thread* pool (I/O-bound reads; pandas releases the GIL
enough). The derivation is vectorized pandas — the wall clock is dominated by network reads, so
more workers helps until the NIC saturates (~8 on the reference box). Memory high-water mark is
`workers × largest-quarter`, not the whole panel — that's the point of sharding.

### Things to remember

1. The driver (`ingest.py`) is asset-blind; every source quirk lives in `MortgagePerformanceAdapter._derive`.
2. Sidecar-written-after-shard IS the resume mechanism: rerun the same command, finished quarters skip.
3. Loan-hash sampling keeps whole loans, deterministically, across every quarter.
4. loan_ids stay strings; `default_event`/`is_performing` are nullable booleans → consumers `.fillna(False)`.

---
*Next: [Part 7 — Validation](07_validation.md): proving the panel we just wrote is actually right.*
