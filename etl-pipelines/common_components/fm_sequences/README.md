# FM Sequences: SME + Residential Mortgage (first pass)

## Inputs
- `sme_snapshots`: list of dict-like rows from SME asset-level ETL outputs (expects columns such as `AS3`, `AS1`, `AS27`, `AS30`, etc.).
- `rmb_snapshots`: list of dict-like rows from residential mortgage ETL outputs (expects columns such as `AR3`, `AR1`, `AR19`, `AR24`, etc.).
- Optional config reference: `configs/sme_rmb_token_mapping.sample.json`.

## Outputs
- `SequenceEvent` objects with:
  - `entity_type`, `entity_id`, `as_of_date`
  - `sequence_index` (global deterministic order)
  - `event_kind` (`entity_initialized` or `attribute_updated`)
  - `token`, `value`, `source_schema`
- Optional rendered strings via `render_for_tokenization(...)`.

## Assumptions
- Input rows are already available in memory as dictionaries.
- This is schema-first and non-invasive: no ETL pipeline wiring is changed.
- Missing feature values are emitted as `__MISSING__` by default (can be disabled).
- If no explicit date exists, `pcd_year`/`pcd_month` are used; fallback is `1970-01-01`.
