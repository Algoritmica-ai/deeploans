"""S0.1 — can a derivation stamp a date once and never overwrite it?

Derivations recompute every period. A naive rule rewrites the date each cut-off,
so a loan that defaulted in March claims December by December. The question is
whether the expression language can express "write only if empty".

The column has to be *declared* as well as derived: `output_columns()` reads the
declared column list, so a derivation target that is not a column never reaches
disk.
"""

from __future__ import annotations

import pathlib
import tempfile

import pandas as pd

from sdd import api

PACK = "auto_abs_esma_annex5"


def build(expr_template: str) -> tuple[dict, str, str]:
    spec = api.load(PACK).model_dump(mode="json", exclude_none=True, by_alias=True)
    spec["entity"]["calendar"]["periods"] = 8
    time_col = spec["entity"]["time_column"]

    lc = spec["lifecycle"]
    state_col = lc["state_column"]
    terminal = set(lc.get("terminal") or [])
    absorbing = set(lc.get("absorbing") or [])
    # A state an entity lands in and stays in, so drift is visible if it happens.
    watch = (
        next(s for s in lc["states"] if s in absorbing)
        if absorbing
        else [s for s in lc["states"] if s not in terminal][-1]
    )

    spec["columns"].append(
        {
            "name": "spike_event_date",
            "dtype": "str",
            "generator": {"kind": "constant", "value": None},
        }
    )
    if spec.get("emit", {}).get("column_order"):
        spec["emit"]["column_order"].append("spike_event_date")
    spec["derivations"].append(
        {
            "target": "spike_event_date",
            "kind": "expr",
            "stage": "period",
            "expr": expr_template.format(s=state_col, w=watch, t=time_col),
        }
    )
    return spec, state_col, watch


def verdict(label: str, panel: pd.DataFrame, id_col: str) -> None:
    stamped = panel[panel["spike_event_date"].notna() & (panel["spike_event_date"] != "None")]
    if stamped.empty:
        print(f"  {label:20} NO ROWS STAMPED — inconclusive")
        return
    per_entity = stamped.groupby(id_col)["spike_event_date"].nunique()
    drifted = int((per_entity > 1).sum())
    mark = "STICKY  ✓" if drifted == 0 else f"DRIFTED ✗  {drifted}/{len(per_entity)}"
    print(f"  {label:20} {mark}   entities stamped: {len(per_entity)}")


tmp = pathlib.Path(tempfile.mkdtemp())
print("S0.1 — sticky exit dates\n")

for label, expr in [
    ("naive where()", "where({s} == '{w}', {t}, None)"),
    ("coalesce()", "coalesce(spike_event_date, where({s} == '{w}', {t}, None))"),
]:
    spec, state_col, watch = build(expr)
    loaded = api.load(spec)
    res = api.run(
        spec,
        400,
        tmp / label.replace("(", "").replace(")", "").replace(" ", "_"),
        seed=7,
        validate_output=False,
    )
    panel = pd.read_parquet(res["panel"])
    if label.startswith("naive"):
        print(f"  (trigger state: {watch!r}, time column: {loaded.entity.time_column!r})\n")
    verdict(label, panel, loaded.entity.id_column)
