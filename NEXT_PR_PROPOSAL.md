# Next PR Proposal: Unify API query features across all credit types

## Recommended PR title
`api: unify route query behavior (columns + detailed) across credit types and remove route duplication`

## Why this should be the next PR

The backend currently implements six very similar route modules (`aut`, `cmr`, `cre`, `les`, `rmb`, `sme`) with mostly duplicated `get_result` logic.

- `sme` supports both `columns` selection and a `detailed` switch in its endpoint handler.
- Other credit types only support `filter`, `limit`, and `offset`.

This creates avoidable API inconsistency and maintenance risk.

## Evidence in the codebase

- `sme` route supports `columns` and `detailed`:
  - `api/api-backend-main/backend/app/routes/sme.py`
- Other route handlers do not expose those query params:
  - `api/api-backend-main/backend/app/routes/aut.py`
  - `api/api-backend-main/backend/app/routes/cmr.py`
  - `api/api-backend-main/backend/app/routes/cre.py`
  - `api/api-backend-main/backend/app/routes/les.py`
  - `api/api-backend-main/backend/app/routes/rmb.py`
- Existing tests are focused on SME and parser/validator, so cross-credit-type behavior is currently under-tested:
  - `api/api-backend-main/backend/tests/test_sme_endpoints.py`
  - `api/api-backend-main/backend/tests/test_filter_parser.py`
  - `api/api-backend-main/backend/tests/test_validator.py`

## Scope for the PR

1. Introduce a shared route factory/helper that can register endpoints for any `credit_type` with one implementation.
2. Standardize query parameters for all credit types:
   - `filter`
   - `limit`
   - `offset`
   - `columns`
   - `detailed`
3. Keep backwards compatibility:
   - Existing endpoint paths stay unchanged.
   - Existing query patterns remain valid.
4. Add tests for at least one non-SME credit type (e.g., `aut`) to verify:
   - route registration
   - filter parsing passthrough
   - columns parsing passthrough
   - detailed/list mode behavior
5. Update API README with examples for the new shared query behavior.

## Acceptance criteria

- All credit types accept the same core query params.
- OpenAPI docs show consistent endpoint query options.
- Existing SME tests still pass.
- New tests cover at least one additional credit type.

## Why this is higher leverage than smaller fixes

Smaller improvements (for example, case-insensitive `columns` parsing noted in parser TODOs) are useful, but this PR removes a repeated code pattern and prevents feature drift across 6 route modules. That gives faster long-term iteration and lowers risk of inconsistent behavior.

## Follow-up PR after this

Once route behavior is unified, the next natural PR is:

`api: make columns parsing case-insensitive and add regression tests`

This directly addresses the parser TODO while relying on the now-shared route pathway.
