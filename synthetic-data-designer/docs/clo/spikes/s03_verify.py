"""S0.3 verification — did the custom SQL checks really run?

A check that passes because its SQL silently failed is worse than no check. So
each one is run twice: once as written, and once with the threshold moved so it
*must* fail. A check that cannot be made to fail is not a check.
"""

from __future__ import annotations

import pathlib
import tempfile

from sdd import api

PACK = "auto_abs_esma_annex5"
tmp = pathlib.Path(tempfile.mkdtemp())

loaded = api.load(PACK)
idc, tc = loaded.entity.id_column, loaded.entity.time_column
balc = "original_principal_balance"
grpc = "geographic_region"


def spec_with(cap: float, window_end: int) -> dict:
    s = api.load(PACK).model_dump(mode="json", exclude_none=True, by_alias=True)
    s["entity"]["calendar"]["periods"] = 10
    s["originations"] = {"per_period": 40, "start_period": 1, "end_period": 4, "fresh": True}
    s["validation"] = {
        "checks": {"closed_pool": False},
        "custom": [
            {
                "name": "reconcile",
                "sql": f"""
                with per_row as (select "{tc}" as p, sum("{balc}") as s, count(*) as n from panel group by 1)
                select * from per_row where s is null or n = 0""",
            },
            {
                "name": "no_late_acquisition",
                "sql": f"""
                with first_seen as (select "{idc}" as id, min("{tc}") as joined from panel group by 1),
                     ranked as (select p, row_number() over (order by p) - 1 as idx
                                from (select distinct "{tc}" as p from panel) c)
                select f.id from first_seen f join ranked r on r.p = f.joined
                where r.idx > {window_end}""",
            },
            {
                "name": "concentration",
                "sql": f"""
                with g as (select "{tc}" as p, "{grpc}" as k, sum("{balc}") as par from panel group by 1,2),
                     t as (select p, sum(par) as total from g group by 1)
                select g.p, g.k, g.par/t.total as share from g join t on t.p = g.p
                where t.total > 0 and g.par/t.total > {cap}""",
            },
        ],
    }
    return s


def run(label: str, cap: float, window_end: int) -> None:
    res = api.run(spec_with(cap, window_end), 500, tmp / label, seed=3)
    wanted = {"custom::reconcile", "custom::no_late_acquisition", "custom::concentration"}
    print(f"\n  {label}")
    for c in res["validation"]["checks"]:
        if c["name"] in wanted:
            state = "pass" if c["passed"] else f"FAIL {c['violations']} rows"
            err = f"  [sql error: {c['error'][:50]}]" if c.get("error") else ""
            print(f"    {c['name']:22} {state}{err}")


print("S0.3 — do the custom SQL invariants actually execute?")
run("as written (cap 25%, window 4)", 0.25, 4)
run("negative control (cap 0.1%, window 0)", 0.001, 0)
