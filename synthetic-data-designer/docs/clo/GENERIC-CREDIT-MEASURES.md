# Generic credit measures: what we ship, and why

**Short version:** renaming the measures is not what solves the licensing question. Not shipping
anyone else's numbers is. This note says exactly what we compute, where every number comes from,
and what a licensed user should do instead.

*Not legal advice.* This is engineering risk reduction — it removes the specific thing that creates
exposure. Whether that is sufficient for a given commercial arrangement is a question for a lawyer,
and the final call is not mine.

---

## What the exposure actually is

Three things get conflated. Only one of them matters.

| | Concern? | Why |
|---|---|---|
| **The name** — "WARF", "diversity score" | Low | These are industry vocabulary. "Weighted average rating factor" describes an arithmetic operation, and descriptions of methods are not what copyright protects. |
| **The idea** — average credit quality weighted by size; adjust a name count for concentration | **No** | Ideas and methods are not copyrightable. Everyone in structured credit computes both. |
| **The tables** — the specific factor per rating grade; the diversity-score lookup and its industry-correlation assumptions | **Yes** | These are compilations of numbers published in an agency's methodology documents. Copying them is copying their work product, and this is where the exposure sits. |

So a rename alone would have changed nothing: the same table under a different heading is the same
table. What we have done is **not use their tables at all**, and rename as a consequence — so nobody
reads our number as theirs.

---

## What we actually compute

### `wa_credit_factor` — average credit quality as one number

Ratings are labels. There is no midpoint between B+ and CCC, so a portfolio needs a numeric
stand-in before it can have an average credit quality at all. That number is `credit_factor`, and
the average is its par-weighted mean.

**Where the numbers come from.** Each factor is the probability that *this pack's own rating chain*
reaches D within five years from that grade, multiplied by 10,000 and rounded. Take the
`transitions` matrix under `secondary_chains` in `packs/clo_eu_leveraged_loans.yaml`, raise it to
the 60th power, read the column for D:

| grade | factor | | grade | factor |
|---|---:|---|---|---:|
| BB | 618 | | CCC+ | 3,562 |
| BB- | 854 | | CCC | 4,575 |
| B+ | 1,195 | | CCC- | 5,611 |
| B | 1,717 | | D | 10,000 |
| B- | 2,522 | | | |

Anyone can reproduce these from the pack file and nothing else. A test does exactly that on every
run, so if someone quietly pastes an agency's table in, the suite fails.

**Two properties this buys, beyond the licensing point.**

*Internal consistency.* The factor and the migration behaviour are the same model. A book that
downgrades faster genuinely reads worse, because the same matrix drives both. An imported external
table would sit alongside our migration model with no relationship to it, and the two could disagree
without anything noticing.

*Substitutability.* This is ordinary pack data — a `when` derivation with nine rules. Anyone holding
a licence to an agency's published factors pastes them into the YAML and changes nothing else. No
code knows what the numbers mean.

**The ×10,000 scaling is arithmetic, not expression.** Both our factors and an agency's are
default probabilities scaled by 10,000, so they land in the same range. That is what happens when
two people scale probabilities by the same round number — it is not derivation.

### `effective_obligors` — diversity as a count

The inverse Herfindahl: **1 ÷ Σ(each obligor's squared share of par)**.

This is ordinary statistics. The Herfindahl-Hirschman index is used by competition regulators
worldwide and its inverse — the "effective number" — is standard in ecology, political science and
portfolio theory. No agency is involved at any point.

It deliberately reads as a **count**, which is the useful part: a hundred equal obligors score
100.0, and moving half the money into two of them drops it to about 8. It is directly comparable to
the plain obligor number the report already carries, and the gap between the two *is* the
concentration.

What it does not reproduce: an agency diversity score also folds in assumptions about which
industries are correlated with which. Those assumptions are the agency's model and we do not have
them. Our measure answers "how concentrated is the money" and not "how correlated are these
businesses" — a narrower question, honestly scoped, and the industry concentration metrics sit
beside it for the part it does not cover.

### `portfolio_turnover` — how much of the book changed hands

Par that left the pool since the last cut-off, as a share of the par that was there. No agency
equivalent; it is just a measurement.

---

## The names

| was | is | why |
|---|---|---|
| WARF | `wa_credit_factor` | Says what it is — a weighted average of a factor — without claiming to be a particular agency's factor. |
| Diversity score | `effective_obligors` | Says what it counts. "Score" implies a scale someone owns; "effective obligors" is a number you can check against the obligor count. |

The renames follow from the substance rather than standing in for it.

---

## For a licensed user

The pack is designed to be swapped, not patched:

1. **Factors** — edit the `credit_factor` derivation in the pack YAML. Nine rules, one per grade.
   Nothing in the code cares where the numbers came from, and both invariants
   (`credit_factor_matches_the_rating`, `credit_factor_is_monotone_in_the_grade`) keep working.
2. **Diversity** — a full agency diversity score needs the industry-correlation table too, which is
   a larger change than a substitution. `effective_obligors` stays useful alongside it either way.
3. **Naming** — if you are licensed and want the conventional headings, rename the metrics in the
   YAML. The charts read the metric names, so the interface follows.

---

## What is checked, and where

`tests/test_generic_credit_measures.py`:

- the factors are recomputed from the pack's own transition matrix and must match
- the factor ordering is monotone in the grade — a downgrade can never improve reported quality
- `effective_obligors` is recomputed by hand as an inverse Herfindahl on the opening cut-off
- it never exceeds the plain obligor count, and falls when money concentrates (negative control on
  constructed frames with a known answer)
- both invariants are shown to **fail** when the mapping is deliberately broken

The two invariants also run on every generation, so a pack edited into an inconsistent state is
caught at run time rather than at review time.
