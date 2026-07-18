# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 finevals.ai and contributors.
"""Score calibration — turn rank scores into usable probabilities of default (v1.1 G6.1).

A fine-tuned scorer is trained on a **rebalanced** sample (negatives downsampled, positives
upweighted), so its softmax output ranks loans correctly but is NOT the probability of default:
its mean sits far above the true base rate. Calibration fixes the *level* without touching the
*ranking*: fit a monotone mapping raw-score → PD on a held-out, labeled calibration window, and
apply it at scoring time.

Two methods, both serialized to plain JSON (no pickle — auditable, backend-agnostic):

* ``isotonic`` (default) — non-parametric monotone step fit (sklearn ``IsotonicRegression``),
  stored as breakpoints and applied with linear interpolation. Monotone ⇒ ROC/rank metrics are
  unchanged by construction (ties aside).
* ``platt`` — a logistic curve ``sigmoid(a·s + b)``; 2 parameters, better for tiny calibration
  sets, also strictly monotone.

The **embargo rule** (enforced by ``scripts/calibrate.py``): the calibration window must never
be a test window — calibrating on the window you report metrics on silently flatters Brier the
same way peeking flatters ROC.
"""

from __future__ import annotations

import numpy as np


def brier(labels: np.ndarray, probs: np.ndarray) -> float:
    """Mean squared error between outcomes (0/1) and predicted probabilities — lower is better."""
    labels = np.asarray(labels, dtype=float)
    probs = np.asarray(probs, dtype=float)
    return float(np.mean((probs - labels) ** 2))


def fit_calibrator(scores: np.ndarray, labels: np.ndarray, method: str = "isotonic") -> dict:
    """Fit a raw-score → PD mapping on a labeled calibration window; return a JSON-able dict.

    The dict carries the mapping plus fit metadata (n, base rate, Brier before/after) so the
    calibrator file is self-describing.
    """
    s = np.asarray(scores, dtype=float)
    y = np.asarray(labels, dtype=float)
    if len(s) != len(y) or len(s) == 0:
        raise ValueError(f"scores/labels must be equal-length and non-empty, got {len(s)}/{len(y)}")
    if not (0 < y.sum() < len(y)):
        raise ValueError("calibration window needs BOTH outcomes "
                         f"(got {int(y.sum())} positives in {len(y)})")

    if method == "isotonic":
        from sklearn.isotonic import IsotonicRegression
        iso = IsotonicRegression(y_min=0.0, y_max=1.0, out_of_bounds="clip").fit(s, y)
        mapping = {"x": [float(v) for v in iso.X_thresholds_],
                   "y": [float(v) for v in iso.y_thresholds_]}
    elif method == "platt":
        from sklearn.linear_model import LogisticRegression
        lr = LogisticRegression(C=1e10, solver="lbfgs").fit(s.reshape(-1, 1), y.astype(int))
        mapping = {"a": float(lr.coef_[0][0]), "b": float(lr.intercept_[0])}
    else:
        raise ValueError(f"unknown calibration method '{method}' (use 'isotonic' or 'platt')")

    cal = {"method": method, **mapping}
    cal["meta"] = {"n": int(len(y)), "base_rate": float(y.mean()),
                   "brier_before": brier(y, s), "brier_after": brier(y, apply_calibrator(cal, s))}
    return cal


def apply_calibrator(cal: dict, scores) -> np.ndarray:
    """Map raw scores to calibrated PDs using a dict from :func:`fit_calibrator` (or its JSON)."""
    s = np.asarray(scores, dtype=float)
    if cal["method"] == "isotonic":
        x, y = np.asarray(cal["x"], dtype=float), np.asarray(cal["y"], dtype=float)
        return np.interp(s, x, y)                  # linear between breakpoints, clipped at the ends
    if cal["method"] == "platt":
        return 1.0 / (1.0 + np.exp(-(cal["a"] * s + cal["b"])))
    raise ValueError(f"unknown calibration method '{cal.get('method')}'")


def reliability_table(labels: np.ndarray, probs: np.ndarray, bins: int = 10) -> list[dict]:
    """Equal-width probability bins → predicted vs realized rate (the calibration-curve numbers)."""
    y = np.asarray(labels, dtype=float)
    p = np.asarray(probs, dtype=float)
    edges = np.linspace(0.0, 1.0, bins + 1)
    idx = np.clip(np.digitize(p, edges[1:-1]), 0, bins - 1)
    out = []
    for b in range(bins):
        m = idx == b
        if not m.any():
            continue
        out.append({"bin": f"[{edges[b]:.2f},{edges[b + 1]:.2f})", "n": int(m.sum()),
                    "mean_pd": float(p[m].mean()), "realized": float(y[m].mean())})
    return out


def save_calibrator(cal: dict, url: str) -> None:
    """Write the calibrator JSON to ``url`` (local/gs:///s3://)."""
    import json

    from credit_fm.utils import storage
    storage.write_text(json.dumps(cal, indent=2), url)


def load_calibrator(url: str, key=None) -> dict:
    """Read a calibrator JSON from ``url`` and sanity-check its shape."""
    import json

    from credit_fm.utils import storage
    storage.ensure_auth(url, key)
    cal = json.loads(storage.read_text(url))
    if cal.get("method") not in ("isotonic", "platt"):
        raise ValueError(f"{url}: not a calibrator file (method={cal.get('method')!r})")
    return cal
