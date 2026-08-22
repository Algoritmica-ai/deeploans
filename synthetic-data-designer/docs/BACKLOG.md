# Backlog

What is knowingly left undone, and why. Written at the close of the CLO v1 work
so that whoever picks this up next — including a later me — starts from what was
decided rather than re-deriving it.

Everything the CLO Pack v1 Specification asks for is delivered: §29's twenty-four
definition-of-done items, §31's six release tests, and both halves of §21 that
can be done without a licensed dataset. See `docs/clo/` for the reports.

---

## Blocked on a decision or an asset

### Custom CLO calibration (§21, second half)

Marginals, rank correlations, rating migration, default rates, prepayment,
recovery and turnover measured against a **real vendor dataset**.

**Blocked, correctly.** §21 makes it conditional on *"where an appropriately
licensed real vendor dataset becomes available"*, and says in the next sentence
that vendor-derived parameters must not reach the public pack. There is no such
dataset here.

**Not blocked on code.** `sdd.validate.fidelity`, `api.fidelity` and
`sdd fidelity` already do this against any reference tape. A licensed user runs
it today. What is missing is the licence, not the feature.

### Moody's WARF and diversity score under their own names

The measures ship as `wa_credit_factor` and `effective_obligors`, computed from
this pack's own rating chain and from an inverse Herfindahl respectively. If a
licence to the published factor tables is obtained, both become substitutions in
the pack YAML — nine rules for the factors — with no code change.
See `docs/clo/GENERIC-CREDIT-MEASURES.md`.

---

## Worth doing, nobody is waiting

### Correlation between a group attribute and an entity column

Group attributes now correlate with **each other**, and entity columns correlate
with each other. Nothing connects the two — an obligor's leverage does not
influence the spread on its facilities, though on a real book it plainly would.

Harder than it looks. The entity correlation is imposed by reordering, and a
group attribute cannot be reordered once it is attached to members without
breaking the agreement across an obligor's facilities. It needs a conditional
draw rather than a permutation. **~1 week**, and only worth it if someone asks
for the joint structure specifically.

### Plausibility bands on the other two packs

Only the CLO pack declares them. The machinery is generic and the other two
packs would benefit — an auto book of €400 loans would pass every invariant
today. **~half a day per pack**, mostly research into defensible ranges.

### A functional derivation the profiler cannot see

`ebitda_eur` is `revenue × margin` in the CLO pack, and the profiler relearns it
as an independent column with a 0.99 correlation rather than as a derivation.
The round trip reproduces it well enough that nothing breaks, and a correlation
of 0.99 is a fair description of a functional identity. Detecting
multiplicative relationships between numeric columns would be the honest fix.
**~3 days**, low value.

### Release suite in CI on a schedule

`pytest -m release --release-target=<url>` is run by hand before a release.
Running it nightly against the Space would catch a deployment that drifted
without anyone touching the repo. Held back because it writes into a **public
shared workspace** and would leave a run's artefacts there every night.
**~half a day**, once someone decides that is acceptable.

---

## Explicitly out of scope for v1

Per §28 and §32, the entire liability side belongs to the **CLO Laboratory**, a
separate project after the collateral generator is validated:

tranches (AAA/AA/A/BBB/BB/equity) · interest waterfall · principal waterfall ·
OC test diversion · IC test diversion · management-fee waterfall · tranche
pricing · tranche cash-flow projections · equity IRR · call and refinancing
economics · rating-agency model replication

Not a backlog. A different product.

---

## Judgement calls that are open to revision

Recorded because each was a decision rather than a fact, and a later reader may
disagree.

| decision | reasoning | where |
|---|---|---|
| §31 Test B's "lower recoveries" reads as the **rate**, not the amount | adverse produces ~7× the defaults, so gross recoveries rise (€30.6m → €155.1m) while each default returns less (0.554 → 0.401). The amount reading is close to unsatisfiable, and the rate is what the pack declares | `tests/release/test_release_31.py` |
| §31 Test C's "materially greater" is a **1.5× floor** | a bare `>` would be satisfied by a rounding error | same |
| A group key must explain **two** attributes, not one | one is too easily a coarsening of the key — a NUTS3 region rolls up to a province and is not a parent record | `src/sdd/profile/groups.py` |
| Group members capped at **25** on average | past roughly two dozen a column is classifying entities, not identifying a parent. The one genuinely arbitrary threshold in the group detector | same |
| Plausibility ranges are **market-derived, not fitted** | a band fitted to the generator's output can never fail | `packs/clo_eu_leveraged_loans.yaml` |
| Loan price bounded to **40–101** | a leveraged loan is callable at par, so it cannot sustain a price much above 100 | same |
