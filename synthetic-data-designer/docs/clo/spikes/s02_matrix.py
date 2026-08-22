"""S0.2 — does an 8-state model with 4 terminal states work?

Both shipped packs use 2 terminal states reached by hazards, never by the matrix.
The CLO model needs 4. This builds the real shape and runs it.
"""

from __future__ import annotations

import pathlib
import tempfile

import pandas as pd

from sdd import api
from sdd.spec import SpecError

PACK = "auto_abs_esma_annex5"

STATES = [
    "Performing",
    "Watchlist",
    "Distressed",
    "Defaulted",
    "Recovered",
    "Prepaid",
    "Sold",
    "Matured",
]
TERMINAL = ["Recovered", "Prepaid", "Sold", "Matured"]

# Rows over the 4 non-terminal states, in order. Directional only (§10).
MATRIX = [
    [0.960, 0.030, 0.008, 0.002],  # Performing
    [0.180, 0.700, 0.100, 0.020],  # Watchlist
    [0.030, 0.150, 0.740, 0.080],  # Distressed
    [0.000, 0.000, 0.000, 1.000],  # Defaulted — absorbing; leaves via dwell hazard
]


def clo_spec(extra_hazards: list | None = None, *, with_matured: bool = True) -> dict:
    spec = api.load(PACK).model_dump(mode="json", exclude_none=True, by_alias=True)
    spec["entity"]["calendar"]["periods"] = 24
    old_state = spec["lifecycle"]["state_column"]

    spec["lifecycle"] = {
        "state_column": old_state,
        "states": STATES if with_matured else [s for s in STATES if s != "Matured"],
        "terminal": TERMINAL if with_matured else [s for s in TERMINAL if s != "Matured"],
        "absorbing": ["Defaulted"],
        "transitions": MATRIX,
        "initial_distribution": {"Performing": 0.94, "Watchlist": 0.05, "Distressed": 0.01},
        "hazards": [
            {
                "kind": "bernoulli",
                "name": "prepayment",
                "annual_rate": 0.20,
                "to_state": "Prepaid",
                "excluded_states": ["Defaulted"],
            },
            {"kind": "bernoulli", "name": "trading", "annual_rate": 0.12, "to_state": "Sold"},
            {
                "kind": "dwell_time",
                "name": "recovery_lag",
                "from_state": "Defaulted",
                "periods": 6,
                "to_state": "Recovered",
            },
        ]
        + (extra_hazards or []),
    }
    # The old pack's per-state forced values name states that no longer exist.
    spec["lifecycle"].pop("state_fields", None)
    am = spec.get("dynamics", {}).get("amortisation")
    if am:
        am["only_when_state"] = ["Performing", "Watchlist", "Distressed"]
    for key in ("dynamics", "validation"):
        spec.get(key, {}).pop("recovery", None)
    spec["validation"] = {"checks": {"closed_pool": False}}
    spec.pop("scenarios", None)
    return spec, old_state


tmp = pathlib.Path(tempfile.mkdtemp())
print("S0.2 — 8 states, 4 terminal\n")

# --- A. does the shape load and run? -------------------------------------
spec, state_col = clo_spec(with_matured=False)
loaded = api.load(spec)
n = len(loaded.lifecycle.transition_states or [])
print(f"  transition_states inferred : {loaded.lifecycle.transition_states}")
print(f"  matrix                     : {n} x {n}   (terminal excluded automatically)")

res = api.run(spec, 600, tmp / "clo", seed=11, validate_output=False)
panel = pd.read_parquet(res["panel"])
reached = panel[state_col].value_counts()
print("\n  states actually reached over 24 periods:")
for s in STATES:
    hits = int(reached.get(s, 0))
    route = "matrix" if s not in TERMINAL else "hazard"
    flag = "" if hits else "   <-- never reached"
    print(f"    {s:12} {hits:7,}  via {route}{flag}")

# --- B. can maturity be expressed as a hazard? ----------------------------
print("\n  maturity as a condition on a column:")
try:
    spec2, _ = clo_spec(
        extra_hazards=[
            {
                "kind": "condition",
                "name": "maturity",
                "when": "months_to_maturity <= 0",
                "to_state": "Matured",
            }
        ]
    )
    api.load(spec2)
    print("    condition hazard: ACCEPTED")
except SpecError as exc:
    first = str(exc).strip().splitlines()[-1][:110]
    print(f"    condition hazard: REJECTED — {first}")
