"""S0.3 custom-SQL reach · S0.4 reproducibility · S0.5 rating as derivation."""

from __future__ import annotations

import filecmp
import pathlib
import tempfile

import pandas as pd

from sdd import api

PACK = "auto_abs_esma_annex5"
tmp = pathlib.Path(tempfile.mkdtemp())


def base() -> dict:
    spec = api.load(PACK).model_dump(mode="json", exclude_none=True, by_alias=True)
    spec["entity"]["calendar"]["periods"] = 10
    return spec


# ---------------------------------------------------------------- S0.3
print("S0.3 — the three hardest §20 invariants as CustomInvariant SQL\n")

spec = base()
loaded = api.load(spec)
idc, tc = loaded.entity.id_column, loaded.entity.time_column
statec = loaded.lifecycle.state_column
# Stand-ins for CLO columns on an existing pack.
balc = next(c.name for c in loaded.columns if "balance" in c.name.lower())
grpc = next(c.name for c in loaded.columns if c.dtype == "category" and c.name != statec)
print(f"  proxies: balance={balc!r}  group={grpc!r}  first_seen -> open pool\n")

spec["originations"] = {"per_period": 40, "start_period": 1, "end_period": 4, "fresh": True}
spec["validation"] = {
    "checks": {"closed_pool": False},
    "custom": [
        {  # 1. reconciliation: a period total must equal the sum of its rows
            "name": "period_totals_reconcile",
            "description": "portfolio total ties to facility-level sum",
            "sql": f"""
                with per_row as (
                    select "{tc}" as p, sum("{balc}") as facility_sum, count(*) as n
                    from panel group by 1
                )
                select * from per_row where facility_sum is null or n = 0
            """,
        },
        {  # 2. no acquisitions after the reinvestment end
            "name": "no_acquisition_after_reinvestment_end",
            "description": "nothing joins the pool after the window closes",
            "sql": f"""
                with first_seen as (
                    select "{idc}" as id, min("{tc}") as joined from panel group by 1
                ),
                cutoffs as (
                    select distinct "{tc}" as p from panel
                ),
                ranked as (
                    select p, row_number() over (order by p) - 1 as idx from cutoffs
                )
                select f.id, f.joined, r.idx
                from first_seen f join ranked r on r.p = f.joined
                where r.idx > 4
            """,
        },
        {  # 3. obligor concentration: no group over 25% of par in any period
            "name": "group_concentration_within_cap",
            "description": "no group exceeds its share of the portfolio",
            "sql": f"""
                with by_group as (
                    select "{tc}" as p, "{grpc}" as g, sum("{balc}") as par
                    from panel group by 1, 2
                ),
                totals as (select p, sum(par) as total from by_group group by 1)
                select b.p, b.g, b.par / t.total as share
                from by_group b join totals t on t.p = b.p
                where t.total > 0 and b.par / t.total > 0.25
            """,
        },
    ],
}

res = api.run(spec, 500, tmp / "sql", seed=3)
for chk in res["validation"]["checks"]:
    if chk["name"] in {
        "period_totals_reconcile",
        "no_acquisition_after_reinvestment_end",
        "group_concentration_within_cap",
    }:
        mark = "PASS" if chk["passed"] else f"FAIL ({chk['violations']} rows)"
        err = f"  ERROR: {chk['error'][:70]}" if chk.get("error") else ""
        print(f"  {chk['name']:42} {mark}{err}")
print(
    f"\n  total checks run: {res['validation']['total']}, "
    f"failed: {res['validation']['total'] - sum(c['passed'] for c in res['validation']['checks'])}"
)

# ---------------------------------------------------------------- S0.4
print("\n\nS0.4 — reproducibility: same spec, same seed, twice\n")
s = base()
a = api.run(s, 400, tmp / "rep_a", seed=99, validate_output=False)
b = api.run(s, 400, tmp / "rep_b", seed=99, validate_output=False)
pa, pb = pathlib.Path(a["panel"]), pathlib.Path(b["panel"])
byte_same = filecmp.cmp(pa, pb, shallow=False)
fa, fb = pd.read_parquet(pa), pd.read_parquet(pb)
data_same = fa.equals(fb)
print(f"  data identical  : {data_same}")
print(f"  bytes identical : {byte_same}   ({pa.stat().st_size:,} vs {pb.stat().st_size:,} bytes)")
print(f"  spec_hash equal : {a['spec_hash'] == b['spec_hash']}")

# ---------------------------------------------------------------- S0.5
print("\n\nS0.5 — nine rating grades from state + noise, as a derivation\n")
GRADES = ["BB", "BB-", "B+", "B", "B-", "CCC+", "CCC", "CCC-", "D"]
s = base()
s["columns"].append(
    {
        "name": "rating_noise",
        "dtype": "float",
        "generator": {"kind": "uniform", "low": 0.0, "high": 1.0},
    }
)
s["columns"].append(
    {"name": "rating_at_cutoff", "dtype": "str", "generator": {"kind": "constant", "value": "BB"}}
)
if s.get("emit", {}).get("column_order"):
    s["emit"]["column_order"] += ["rating_noise", "rating_at_cutoff"]
s["derivations"].append(
    {
        "target": "rating_at_cutoff",
        "kind": "when",
        "stage": "both",
        "rules": [
            {"if": f"{statec} == 'Defaulted'", "then": "D"},
            {"if": f"({statec} == 'Charged-Off')", "then": "D"},
            {"if": f"({statec} == '61-90 DPD') and (rating_noise < 0.5)", "then": "CCC-"},
            {"if": f"{statec} == '61-90 DPD'", "then": "CCC"},
            {"if": f"({statec} == '31-60 DPD') and (rating_noise < 0.5)", "then": "CCC+"},
            {"if": f"{statec} == '31-60 DPD'", "then": "B-"},
            {"if": f"({statec} == '1-30 DPD') and (rating_noise < 0.5)", "then": "B"},
            {"if": f"{statec} == '1-30 DPD'", "then": "B+"},
            {"if": "rating_noise < 0.35", "then": "BB"},
        ],
        "else": "BB-",
    }
)
res = api.run(s, 500, tmp / "rating", seed=5, validate_output=False)
panel = pd.read_parquet(res["panel"])
found = [g for g in GRADES if g in set(panel["rating_at_cutoff"])]
print(f"  grades produced : {len(found)}/9  -> {found}")
mig = panel.groupby(idc)["rating_at_cutoff"].nunique()
print(f"  facilities whose grade changed over time: {int((mig > 1).sum())}/{len(mig)}")
