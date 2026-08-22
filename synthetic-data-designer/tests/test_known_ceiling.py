"""The ceiling: the best score any model could achieve on generated data.

Every other test here asks whether the data is internally consistent or looks
plausible. These ask whether the *measurement* is sound, because the whole claim
rests on it: if the ceiling can be beaten, it is not a ceiling, and the number
is worse than useless — it is a number people would quote.

So the tests that matter are the ones that could fail. A strong model is trained
and must land below the ceiling; a model that cheats must be caught; and a
generating process that cannot be inverted must be refused rather than
approximated.
"""

from __future__ import annotations

import copy
import pathlib
import tempfile

import numpy as np
import pandas as pd
import pytest

from sdd import api, benchmark

PACK = "credit_benchmark_known_ceiling"
ENTITIES = 12_000
SEED = 7


@pytest.fixture(scope="module")
def generated():
    tmp = pathlib.Path(tempfile.mkdtemp())
    result = api.run(PACK, ENTITIES, tmp, seed=SEED, validate_output=False)
    return api.load(PACK), pd.read_parquet(result["panel"])


@pytest.fixture(scope="module")
def known(generated):
    spec, panel = generated
    return benchmark.ceiling(spec, panel)


# ---------------------------------------------------------------------------
# the hidden driver must actually be hidden
# ---------------------------------------------------------------------------


def test_an_arrow_backed_panel_can_still_be_split(generated):
    """A panel read back under pandas 3, handed to scikit-learn.

    Parquet returns identifiers as Arrow-backed strings there, and
    `train_test_split` indexes positionally with an integer array — which an
    Arrow-backed index refuses outright. The same code passed on pandas 2 and
    failed on all three Python versions in CI, so the dtype is forced here to
    keep the regression reproducible on either version.
    """
    from sklearn.model_selection import train_test_split

    spec, panel = generated
    arrow = panel.copy()
    arrow[spec.entity.id_column] = arrow[spec.entity.id_column].astype("string[pyarrow]")

    features = benchmark.observables(spec, arrow)
    labels = benchmark.label_outcome(spec, arrow)
    train, test = train_test_split(features.index, test_size=0.3, random_state=0, stratify=labels)

    assert len(train) and len(test)
    assert not set(train) & set(test)
    assert benchmark.ceiling(spec, arrow).ceiling > 0.5


def test_the_driver_never_reaches_the_output(generated):
    """The asymmetry the whole instrument rests on.

    If the driver leaked, a model could read the answer and the ceiling would
    describe nothing.
    """
    spec, panel = generated
    assert spec.benchmark.latent not in panel.columns

    hidden = [c.name for c in spec.columns if c.role == "helper"]
    assert hidden, "nothing is hidden, so there is no latent to infer"
    for name in hidden:
        assert name not in panel.columns, f"{name} leaked into the panel"


def test_a_visible_driver_is_refused(generated):
    """Loud, not lenient. A ceiling computed against a readable driver would be
    arithmetically fine and completely meaningless."""
    spec, panel = generated
    exposed = spec.model_dump(mode="json", exclude_none=True, by_alias=True)
    next(c for c in exposed["columns"] if c["name"] == "risk_tier")["role"] = "static"

    with pytest.raises(benchmark.BenchmarkError, match=r"not hidden|reaches the output"):
        benchmark.ceiling(exposed, panel)


def test_a_spec_with_no_benchmark_block_says_so(tmp_path):
    """Most packs are not instruments, and asking one for a ceiling should
    explain that rather than fail somewhere in the arithmetic."""
    result = api.run("auto_abs_esma_annex5", 200, tmp_path, seed=1, validate_output=False)
    panel = pd.read_parquet(result["panel"])

    with pytest.raises(benchmark.BenchmarkError, match="declares no `benchmark` block"):
        benchmark.ceiling("auto_abs_esma_annex5", panel)


# ---------------------------------------------------------------------------
# the ceiling has to bind
# ---------------------------------------------------------------------------


def test_the_ceiling_sits_between_chance_and_the_oracle(known):
    """The three numbers have a required order, and it is not decorative.

    A ceiling below chance means the inversion has the sign wrong. A ceiling
    above the oracle means the observables carry more than the truth they are
    readings of, which is impossible.
    """
    assert 0.5 < known.ceiling <= known.oracle + benchmark.CEILING_TOLERANCE
    assert known.oracle <= 1.0
    assert known.observable_cost >= -benchmark.CEILING_TOLERANCE


def test_a_trained_model_does_not_beat_it(generated, known):
    """The test that decides whether any of this is worth having.

    A gradient booster with four hundred trees is given every observable and a
    proper out-of-sample split. It should get close — the ceiling is meant to be
    approachable — and it must not exceed it.
    """
    from sklearn.ensemble import HistGradientBoostingClassifier
    from sklearn.model_selection import train_test_split

    spec, panel = generated
    features = benchmark.observables(spec, panel)
    labels = benchmark.label_outcome(spec, panel)

    train, test = train_test_split(features.index, test_size=0.4, random_state=0, stratify=labels)
    held_out = panel[panel[spec.entity.id_column].isin(set(test))]

    model = HistGradientBoostingClassifier(max_iter=400, random_state=0)
    model.fit(features.loc[train], labels.loc[train])
    scores = pd.Series(model.predict_proba(features.loc[test])[:, 1], index=test)

    report = benchmark.compare(spec, held_out, scores, name="gbm")
    assert not report["beat_the_ceiling"], report
    assert report["achieved"] > 0.75, "the benchmark should be learnable, not hopeless"
    assert report["captured"] > 0.85, report


def test_a_single_feature_falls_short(generated, known):
    """If one observable were enough, combining them would be pointless and the
    ceiling would be a restatement of the best column."""
    spec, panel = generated
    features = benchmark.observables(spec, panel)
    labels = benchmark.label_outcome(spec, panel)

    from sklearn.metrics import roc_auc_score

    best_single = max(roc_auc_score(labels, -features[column]) for column in features.columns)
    assert best_single < known.ceiling - 0.01, (
        f"a single feature reaches {best_single:.4f} against a ceiling of {known.ceiling:.4f}; "
        "the observables are not carrying independent information"
    )


# ---------------------------------------------------------------------------
# negative controls
# ---------------------------------------------------------------------------


def test_cheating_is_caught(generated):
    """The control that proves the check can fail.

    Scored on the rows it was fitted to, a flexible model memorises and clears
    the ceiling. That is impossible, so it must be reported — this is the exact
    mistake the check caught during development, reproduced on purpose.
    """
    from sklearn.ensemble import HistGradientBoostingClassifier

    spec, panel = generated
    features = benchmark.observables(spec, panel)
    labels = benchmark.label_outcome(spec, panel)

    leaky = HistGradientBoostingClassifier(max_iter=400, random_state=0)
    leaky.fit(features, labels)
    in_sample = pd.Series(leaky.predict_proba(features)[:, 1], index=features.index)

    report = benchmark.compare(spec, panel, in_sample, name="in-sample")
    assert report["beat_the_ceiling"], (
        "a model scored on its own training rows cleared the ceiling and was not "
        "flagged, which is the one failure this check exists to prevent"
    )


def test_noisier_observables_lower_the_ceiling(generated, tmp_path):
    """The difficulty dial, which is what makes this an instrument.

    Doubling the measurement error must lower what is achievable while leaving
    the oracle alone — the hidden driver has not changed, only how well it can
    be read.
    """
    spec, _ = generated
    noisier = copy.deepcopy(spec.model_dump(mode="json", exclude_none=True, by_alias=True))
    for column in noisier["columns"]:
        if column["name"].startswith("_noise_"):
            column["generator"]["stddev"] *= 2.5

    panel = pd.read_parquet(
        api.run(noisier, ENTITIES, tmp_path / "noisy", seed=SEED, validate_output=False)["panel"]
    )
    degraded = benchmark.ceiling(noisier, panel)
    baseline = benchmark.ceiling(spec, _panel(tmp_path / "base"))

    assert degraded.ceiling < baseline.ceiling - 0.02, (
        f"noise 2.5x left the ceiling at {degraded.ceiling:.4f} against "
        f"{baseline.ceiling:.4f}; the dial does nothing"
    )
    assert abs(degraded.oracle - baseline.oracle) < 0.03, "the oracle should barely move"


def _panel(out) -> pd.DataFrame:
    return pd.read_parquet(api.run(PACK, ENTITIES, out, seed=SEED, validate_output=False)["panel"])


# ---------------------------------------------------------------------------
# the declared model has to be readable
# ---------------------------------------------------------------------------


def test_the_emission_model_is_read_from_the_spec(generated, known):
    """The centres and widths come out of the YAML, not out of the code.

    That is what lets someone build their own instrument, and it is what makes
    the difficulty dial work at all.
    """
    spec, _ = generated
    tiers = next(c for c in spec.columns if c.name == spec.benchmark.latent).domain
    assert set(known.latent_risk) == set(map(str, tiers))

    risks = [known.latent_risk[str(t)] for t in tiers]
    assert risks == sorted(risks), f"risk should rise across the tiers in order: {risks}"


def test_a_missing_centre_is_refused(generated):
    """Half a description of the generating process cannot be inverted, and
    guessing the rest would produce a plausible wrong answer."""
    spec, panel = generated
    broken = spec.model_dump(mode="json", exclude_none=True, by_alias=True)
    rules = next(d for d in broken["derivations"] if d["target"] == "_centre_score")["rules"]
    rules.pop()

    with pytest.raises(benchmark.BenchmarkError, match="centres for"):
        benchmark.ceiling(broken, panel)


# ---------------------------------------------------------------------------
# the mark scheme: evaluation + expected_behaviour
# ---------------------------------------------------------------------------


def test_the_mark_scheme_changes_no_data(tmp_path):
    """The claim that matters most, because it is the one that sounds wrong.

    `evaluation` and `expected_behaviour` describe how a *model* should be
    judged. They say nothing about generation, and a panel produced with them is
    identical to one produced without — same seed, same rows, same values.

    Checked by hashing both, because "it should not change anything" is exactly
    the sort of claim that quietly stops being true.
    """
    import hashlib

    spec = api.load(PACK).model_dump(mode="json", exclude_none=True, by_alias=True)
    stripped = copy.deepcopy(spec)
    stripped["benchmark"].pop("expected_behaviour", None)
    stripped["benchmark"].pop("evaluation", None)

    def digest(variant, tag):
        result = api.run(variant, 4_000, tmp_path / tag, seed=42, validate_output=False)
        frame = pd.read_parquet(result["panel"])
        return hashlib.sha256(
            pd.util.hash_pandas_object(frame, index=False).values.tobytes()
        ).hexdigest()

    assert digest(spec, "with") == digest(stripped, "without")


def test_the_declared_metrics_are_all_computed(generated):
    """Whatever the pack lists is what gets reported, and nothing silently drops."""
    spec, panel = generated
    labels = benchmark.label_outcome(spec, panel)
    features = benchmark.observables(spec, panel)
    scores = pd.Series(1.0 - features["bureau_score"].rank(pct=True), index=features.index)

    report = benchmark.compare(spec, panel, scores, name="ranker")
    assert set(report["metrics"]) == set(spec.benchmark.evaluation.metrics)
    assert 0.5 < report["metrics"]["roc_auc"] < 1.0
    assert 0.0 < report["metrics"]["pr_auc"] <= 1.0
    assert 0.0 < report["metrics"]["ks"] <= 1.0
    assert labels.sum() > 0


def test_a_ranking_score_reports_no_brier(generated):
    """A model emitting 0-to-1000 is not claiming to be calibrated.

    Scoring it against a 0/1 outcome would report a terrible Brier for a model
    that never promised one, so those two come back as nan rather than as a bad
    number someone would quote.
    """
    spec, panel = generated
    features = benchmark.observables(spec, panel)
    raw = pd.Series(-features["bureau_score"].to_numpy(), index=features.index)

    report = benchmark.compare(spec, panel, raw, name="unscaled")
    assert np.isnan(report["metrics"]["brier"])
    assert np.isnan(report["metrics"]["calibration_error"])
    assert report["metrics"]["roc_auc"] > 0.5


def test_an_honest_model_passes_every_check(generated):
    """The control in the other direction. A mark scheme that fails everything
    is as useless as one that passes everything."""
    spec, panel = generated
    features = benchmark.observables(spec, panel)
    honest = pd.Series(-features["bureau_score"].to_numpy(), index=features.index)

    report = benchmark.compare(spec, panel, honest, name="honest")
    assert report["passed"], [c for c in report["behaviour"] if not c["passed"]]


def test_a_backwards_relationship_is_caught(generated):
    """A model can score respectably with one driver inverted, and an AUC will
    never show it. This is the check that does."""
    spec, panel = generated
    features = benchmark.observables(spec, panel)
    inverted = pd.Series(features["bureau_score"].to_numpy(), index=features.index)

    report = benchmark.compare(spec, panel, inverted, name="inverted")
    failed = {c["subject"] for c in report["behaviour"] if not c["passed"]}
    assert "bureau_score" in failed
    assert not report["passed"]


def test_leaning_on_a_categorical_decoy_is_caught(generated):
    """`region` is noise by construction, so a model whose output tracks it has
    found something that is not there."""
    spec, panel = generated
    features = benchmark.observables(spec, panel)
    region = benchmark.observables_extra(spec, panel, "region").reindex(features.index)
    superstitious = pd.Series((region == "North").astype(float).to_numpy(), index=features.index)

    report = benchmark.compare(spec, panel, superstitious, name="region-reader")
    failed = {c["subject"] for c in report["behaviour"] if not c["passed"]}
    assert "region" in failed


def test_leaning_on_a_continuous_decoy_is_caught(generated):
    """The same question for `current_balance`, which needs a different measure.

    Grouping a continuous column gives one group per row, so a swing-between-
    groups test reports the model's whole output range and fails everything.
    That happened: a model ignoring `current_balance` entirely scored 4.4
    standard deviations of apparent influence. Rank correlation is the measure
    that fits the shape.
    """
    spec, panel = generated
    features = benchmark.observables(spec, panel)
    balance = benchmark.observables_extra(spec, panel, "current_balance")
    tracks_it = pd.Series(
        balance.reindex(features.index).to_numpy(dtype=float), index=features.index
    )

    report = benchmark.compare(spec, panel, tracks_it, name="balance-reader")
    failed = {c["subject"] for c in report["behaviour"] if not c["passed"]}
    assert "current_balance" in failed

    honest = pd.Series(-features["bureau_score"].to_numpy(), index=features.index)
    clean = benchmark.compare(spec, panel, honest, name="honest")
    balance_check = next(c for c in clean["behaviour"] if c["subject"] == "current_balance")
    assert balance_check["passed"], (
        f"a model ignoring the decoy was flagged anyway: {balance_check}"
    )


def test_too_little_signal_is_caught(generated):
    """`min_signal_captured` is a bar in units that compare across datasets.

    A raw AUC cannot be compared between portfolios with different ceilings;
    share-of-available-signal can, which is why the bar is set in those terms.
    """
    spec, panel = generated
    features = benchmark.observables(spec, panel)
    rng = np.random.default_rng(0)
    weak = pd.Series(
        -features["bureau_score"].to_numpy() + rng.normal(0, 300, len(features)),
        index=features.index,
    )

    report = benchmark.compare(spec, panel, weak, name="weak")
    captured = next(c for c in report["behaviour"] if c["check"] == "signal_captured")
    assert not captured["passed"]
    assert report["captured"] < spec.benchmark.expected_behaviour.min_signal_captured


def test_the_primary_metric_must_have_a_ceiling():
    """A Brier score cannot be compared against a ranking bound, so the spec
    refuses one as primary rather than reporting a meaningless comparison."""
    from sdd.spec.schema import Evaluation

    with pytest.raises(ValueError, match="cannot be the primary metric"):
        Evaluation(metrics=["roc_auc", "brier"], primary="brier")

    with pytest.raises(ValueError, match="not in `metrics`"):
        Evaluation(metrics=["roc_auc"], primary="pr_auc")
