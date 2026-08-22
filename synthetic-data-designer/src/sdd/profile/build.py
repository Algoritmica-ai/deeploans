"""Turn a profile (and optionally a structure) into a runnable design spec.

This is where the two inputs meet. The **profile** knows how the data is
distributed and how it behaves; the **structure** knows which fields must exist,
in what order, and with what declared type. Where they disagree the structure
wins on schema — it is the contract — and the profile wins on distribution — it
is the evidence.

The output is a spec you can run immediately and edit afterwards. Anything
inferred with low confidence carries a ``review`` note naming what to check,
because a spec that hides its guesses is worse than one that admits them.
"""

from __future__ import annotations

from typing import Any

from sdd.profile.profiler import DatasetProfile
from sdd.profile.template import Template
from sdd.spec.schema import (
    Accrual,
    Amortisation,
    Bucket,
    Calendar,
    ChainCoupling,
    ChartSpec,
    Column,
    CorrelationTarget,
    Counter,
    Derivation,
    DesignSpec,
    Dynamics,
    Emit,
    Entity,
    Generation,
    Group,
    GroupSize,
    Index,
    Lifecycle,
    Meta,
    Metric,
    Originations,
    Results,
    SecondaryChain,
    Target,
    Validation,
)

# Below this, an inference is flagged for a human to look at.
REVIEW_BELOW = 0.5


def spec_from_profile(
    profile: DatasetProfile,
    *,
    template: Template | None = None,
    name: str = "profiled",
    asset_class: str = "generic",
    periods: int | None = None,
    freq: str = "month_end",
    start: str | None = None,
    data: Any = None,
) -> DesignSpec:
    """Build a design spec from a profile, optionally constrained by a structure.

    ``data`` is the sample the profile was taken from, and only group detection
    needs it: how many facilities each obligor carries, and how many of the
    obligors arriving in month twenty were already on the book, are counts over
    rows rather than summaries of columns. Without it everything else is built as
    before and the group structure is simply not looked for.
    """
    if not profile.id_column:
        raise ValueError(
            "cannot build a spec without an entity identifier. Pass id_column= to "
            "profile_dataset(), or add an id column to the sample."
        )

    time_column = profile.time_column or "as_of_date"
    columns, constants, review = _build_columns(profile, template)

    # A snapshot has no cut-off column, but every spec needs one — the panel is
    # written per period and the engine stamps the date into it. Invent the
    # column rather than merely naming it, or the spec fails its own validation.
    if not profile.time_column:
        from sdd.spec.schema import ConstantGen

        start = start or "2024-01-31"
        columns.insert(
            1,
            Column(
                name=time_column,
                role="dynamic",
                dtype="str",
                generator=ConstantGen(value=start),
                description="Cut-off date. Added because the sample was a single snapshot "
                "with no time column of its own.",
                confidence=1.0,
            ),
        )
    buckets, derivations = _build_buckets(profile)
    lifecycle = _build_lifecycle(profile)
    dynamics = _build_dynamics(profile, lifecycle)

    ordered = _output_order(profile, template, columns)

    spec = DesignSpec(
        meta=Meta(
            name=name,
            asset_class=(
                template.asset_class if template and template.asset_class else asset_class
            ),
            regulatory_template=template.name if template else None,
            description=(
                f"Profiled from a {'panel' if profile.is_panel else 'snapshot'} of "
                f"{profile.rows:,} rows across {len(profile.columns)} columns."
            ),
            source=(template.source if template else None),
        ),
        entity=Entity(
            id_column=profile.id_column,
            time_column=time_column,
            calendar=Calendar(
                start=start or _first_period(profile),
                periods=periods or max(profile.periods, 1),
                freq=freq,  # type: ignore[arg-type]
            ),
        ),
        constants=constants,
        columns=columns,
        buckets=buckets,
        derivations=derivations,
        lifecycle=lifecycle,
        dynamics=dynamics,
        originations=_build_originations(profile, lifecycle),
        generation=_build_generation(profile, columns),
        emit=Emit(
            filename=f"{name}_{{yyyymm}}.csv",
            column_order=ordered,
            formats=["csv"],
        ),
        validation=Validation(non_negative_columns=_non_negative(profile)),
    )

    # One of these is measured and two are proposed, and the difference matters
    # enough that each function says which it is.
    spec.groups = _build_groups(data, profile, spec)
    spec.secondary_chains = _build_secondary_chains(profile)
    spec.entity.targets = _build_targets(profile, spec)
    spec.metrics = _build_metrics(profile, spec)
    spec.results = _build_results(spec)

    if review:
        spec.meta.description = (
            f"{spec.meta.description} {len(review)} column(s) need review: "
            f"{', '.join(review[:8])}{'…' if len(review) > 8 else ''}"
        )
    return spec


# ---------------------------------------------------------------------------
# columns
# ---------------------------------------------------------------------------


def _build_columns(
    profile: DatasetProfile, template: Template | None
) -> tuple[list[Column], dict[str, Any], list[str]]:
    columns: list[Column] = []
    constants: dict[str, Any] = {}
    review: list[str] = []

    # Sampling order matters: a conditional generator must see its parent, and
    # the id and time columns anchor everything else.
    ordered = sorted(
        profile.columns,
        key=lambda c: (c.name != profile.id_column, c.name != profile.time_column),
    )

    for col in ordered:
        declared = template.field(col.name) if template else None

        # A constant is deal-level metadata, not a column to sample.
        if col.role == "constant" and col.name not in (profile.id_column, profile.time_column):
            constants[col.name] = col.examples[0] if col.examples else None
            continue

        dtype = declared.dtype if declared and declared.dtype else col.dtype
        note = None
        if col.confidence < REVIEW_BELOW:
            note = (col.fit.note if col.fit and col.fit.note else None) or (
                f"inferred with low confidence ({col.confidence:.2f}); check the generator"
            )
            review.append(col.name)

        columns.append(
            Column(
                name=col.name,
                role=col.role,
                dtype=dtype,
                generator=col.fit.generator if col.fit else None,
                description=(declared.description if declared else None),
                domain=col.domain if col.domain and len(col.domain) <= 50 else None,
                min=col.minimum,
                max=col.maximum,
                # A column the sample never left blank is treated as required.
                # The reverse is not assumed: an optional column is *allowed* to
                # be blank, and only becomes blank if someone asks for missing
                # values, so a profiled spec reproduces the sample by default.
                required=col.nulls == 0,
                confidence=round(col.confidence, 3),
                review=note,
            )
        )

    # A field the structure demands but the sample never showed still has to
    # exist in the output, or the tape does not match its own template.
    if template:
        seen = {c.name for c in columns} | set(constants)
        for f in template.fields:
            if f.name in seen:
                continue
            from sdd.spec.schema import ConstantGen

            columns.append(
                Column(
                    name=f.name,
                    role="static",
                    dtype=f.dtype or "str",
                    generator=ConstantGen(value=None),
                    description=f.description,
                    confidence=0.0,
                    review="required by the structure but absent from the sample; "
                    "emits null until a generator is chosen",
                )
            )
            review.append(f.name)

    return columns, constants, review


def _build_originations(
    profile: DatasetProfile, lifecycle: Lifecycle | None
) -> Originations | None:
    """Carry an observed open pool into the spec.

    Recorded as a *rate* rather than a count, so the setting still means the same
    thing when the spec is run at a different scale: a book taking on 2% of its
    opening size every month does that whether it opens with 5,000 loans or
    500,000.

    Skipped without a lifecycle, because there would be no ageing for the new
    entities to join — the spec would declare arrivals that never arrive.
    """
    learned = profile.dynamics.get("originations")
    if not learned or lifecycle is None or learned["rate"] <= 0:
        return None
    return Originations(
        rate=round(learned["rate"], 6),
        start_period=learned.get("start_period", 1),
        fresh=bool(learned.get("fresh", True)),
    )


def _build_generation(profile: DatasetProfile, columns: list[Column]) -> Generation:
    """Carry the sample's joint structure into the spec.

    The generators above are marginals — each column fitted on its own. Recording
    the observed rank correlation is what lets the engine put the joint structure
    back afterwards, and what makes the correlation control in the UI mean
    something measurable rather than decorative.
    """
    target = None
    measured = profile.correlation
    if measured:
        declared = {c.name for c in columns}
        keep = [i for i, name in enumerate(measured["columns"]) if name in declared]
        if len(keep) >= 2:
            target = CorrelationTarget(
                columns=[measured["columns"][i] for i in keep],
                matrix=[[measured["matrix"][i][j] for j in keep] for i in keep],
            )
    return Generation(method="distribution", correlation_target=target)


def _build_buckets(profile: DatasetProfile) -> tuple[dict[str, Bucket], list[Derivation]]:
    """Turn discovered binnings into reusable bucket rules plus derivations.

    A bucket column recovered this way is computed from its source every period,
    so a balance and its band can never disagree — which is exactly the failure
    independent sampling produces.
    """
    buckets: dict[str, Bucket] = {}
    derivations: list[Derivation] = []
    for found in profile.derived:
        key = f"{found.target}_bins"
        buckets[key] = Bucket(bins=found.bins, labels=found.labels)
        derivations.append(
            Derivation(
                target=found.target,
                kind="bucket",
                bucket=key,
                source=found.source,
                # Recompute every period: the source moves, so the band must too.
                stage="both",
            )
        )
    return buckets, derivations


def _output_order(
    profile: DatasetProfile, template: Template | None, columns: list[Column]
) -> list[str]:
    """The structure fixes column order when there is one; otherwise keep the
    sample's order, which is what a reader will expect."""
    if template and template.fields:
        return template.column_names
    known = {c.name for c in columns}
    order = [c.name for c in profile.columns]
    # A column the builder added (a synthesised cut-off date) still has to be
    # emitted, or the spec promises a column it never writes.
    return order + [c.name for c in columns if c.name not in order and c.name in known]


def _non_negative(profile: DatasetProfile) -> list[str]:
    """Numeric columns never observed below zero are asserted to stay that way."""
    return [
        c.name
        for c in profile.columns
        if c.dtype in ("int", "float")
        and c.minimum is not None
        and c.minimum >= 0
        and any(
            word in c.name.lower() for word in ("balance", "amount", "payment", "value", "days")
        )
    ]


def _first_period(profile: DatasetProfile) -> str:
    for col in profile.columns:
        if col.name == profile.time_column and col.examples:
            return str(col.examples[0])[:10]
    return "2024-01-31"


# ---------------------------------------------------------------------------
# lifecycle and dynamics
# ---------------------------------------------------------------------------


def _build_lifecycle(profile: DatasetProfile) -> Lifecycle | None:
    learned = profile.dynamics.get("lifecycle")
    if not learned:
        return None

    terminal = learned.get("terminal") or []
    hazards: list[Any] = []

    # One rule per terminal state, measured from the panel.
    #
    # This used to emit exactly two whatever the data held: a flat rate into the
    # first terminal state and a write-off delay fixed at nine periods. A book
    # with four ways out came back with two of them unreachable and the loader
    # refused the whole spec, so any tape where loans are sold, mature or recover
    # from default could not be profiled into anything runnable.
    for exit_rule in learned.get("exits") or []:
        rule = {k: v for k, v in exit_rule.items() if k != "evidence"}
        hazards.append(rule)

    # Nothing learned, but entities clearly leave: fall back to the attrition
    # rate rather than emit a lifecycle nothing can exit.
    attrition = profile.dynamics.get("attrition")
    if not hazards and attrition and terminal and attrition["annual_rate"] > 0:
        hazards.append(
            {
                "kind": "bernoulli",
                "name": "attrition",
                "annual_rate": min(attrition["annual_rate"], 0.99),
                "to_state": terminal[0],
                "excluded_states": list(learned.get("absorbing", [])),
            }
        )

    return Lifecycle(
        state_column=learned["state_column"],
        states=learned["states"],
        transition_states=learned["transition_states"],
        transitions=learned["transitions"],
        terminal=terminal,
        absorbing=learned.get("absorbing", []),
        hazards=hazards,
    )


def _build_groups(data: Any, profile: DatasetProfile, spec: DesignSpec) -> list[Group]:
    """Lift the parent record out of the entity's own columns.

    Detection is in `sdd.profile.groups`; this is the surgery that follows it,
    and the surgery is the point. Leaving the attributes where they are and
    merely noting the key would change nothing — the industry would still be
    drawn per facility, and the same obligor would still come out in four
    industries at once.

    So the shared columns are *moved*: out of `spec.columns`, where they are
    drawn once per entity, into `group.columns`, where they are drawn once per
    group and joined onto every member. The key column is dropped outright,
    since the generator mints it from `id_format`.

    The output order is left untouched. Group attributes reach the panel like any
    other column, so a relearned tape has the same columns in the same places —
    only their provenance changes.
    """
    import pandas as pd

    from sdd.profile.groups import learn_groups

    if not isinstance(data, pd.DataFrame):
        return []

    learned = learn_groups(data, profile)
    if not learned:
        return []

    groups: list[Group] = []
    for found in learned:
        by_name = {c.name: c for c in spec.columns}
        attributes = [by_name[name] for name in found["columns"] if name in by_name]
        if len(attributes) < 2:
            continue

        moved = {c.name for c in attributes} | {found["key"]}
        spec.columns = [c for c in spec.columns if c.name not in moved]
        spec.constants = {k: v for k, v in spec.constants.items() if k not in moved}
        _drop_from_correlation(spec, moved)

        size = found["size"]
        groups.append(
            Group(
                name=found["name"],
                key=found["key"],
                ratio=found["ratio"],
                id_format=found["id_format"],
                new_group_rate=found["new_group_rate"],
                size=GroupSize(
                    kind=size["kind"],
                    concentration=size.get("concentration", 1.4),
                    max_members=size.get("max_members"),
                ),
                columns=attributes,
                correlation_target=_group_correlation(data, found["key"], attributes),
            )
        )
    return groups


def _group_correlation(data: Any, key: str, attributes: list[Column]) -> Any:
    """Rank correlation between the group's own numeric attributes.

    **Measured one row per group, never one row per entity.** An obligor with six
    facilities would otherwise contribute six identical rows, and the resulting
    correlation would be weighted by how much each company happened to borrow —
    so the companies with the most facilities would decide what the relationship
    between revenue and leverage looks like.

    This is what closes the gap the group work opened. Moving the attributes onto
    the group made them mutually consistent *across an obligor's facilities*,
    which is the point; it also left them drawn one generator at a time, so
    revenue and leverage stopped moving together at all.
    """
    import pandas as pd

    from sdd.profile.profiler import measure_correlation

    numeric = [c.name for c in attributes if c.dtype in ("int", "float")]
    if len(numeric) < 2 or not isinstance(data, pd.DataFrame) or key not in data.columns:
        return None

    per_group = data.groupby(key, sort=False)[numeric].first()
    measured = measure_correlation(per_group.reset_index(drop=True), numeric)
    if not measured:
        return None
    return CorrelationTarget(columns=measured["columns"], matrix=measured["matrix"])


def _drop_from_correlation(spec: DesignSpec, moved: set[str]) -> None:
    """Take the moved columns out of the correlation target.

    The target is reimposed by reordering `spec.columns`, so naming a column that
    now lives on the group makes the spec invalid — which is how this surfaced,
    as a loader error rather than as anything subtle.

    The relationship itself is not lost — it moves with the columns. Measured
    across groups rather than across entities and attached to the group as its
    own `correlation_target`, so revenue and leverage go on moving together
    inside the obligor table. See `_group_correlation`.
    """
    target = spec.generation.correlation_target
    if target is None:
        return

    keep = [i for i, name in enumerate(target.columns) if name not in moved]
    if len(keep) < 2:
        spec.generation.correlation_target = None
        return

    target.columns = [target.columns[i] for i in keep]
    target.matrix = [[target.matrix[r][c] for c in keep] for r in keep]


def _build_secondary_chains(profile: DatasetProfile) -> list[SecondaryChain]:
    """Chains that migrate alongside the lifecycle.

    The chain's own `Lifecycle` is reused wholesale — same column, same states,
    same matrix — with two things stripped that the panel learner reports but a
    chain may not have: terminal states, which only the lifecycle may declare,
    and exit hazards, which are how an entity leaves the pool.
    """
    chains: list[SecondaryChain] = []
    for learned in profile.dynamics.get("secondary_chains") or []:
        inner = learned["lifecycle"]
        chains.append(
            SecondaryChain(
                name=learned["name"],
                lifecycle=Lifecycle(
                    state_column=inner["state_column"],
                    states=inner["states"],
                    transitions=inner["transitions"],
                    absorbing=inner.get("absorbing", []),
                    initial_distribution=inner.get("initial_distribution") or None,
                    terminal=[],
                    hazards=[],
                ),
                coupling=ChainCoupling(
                    forced_by=learned["coupling"]["forced_by"],
                    stress=learned["coupling"]["stress"],
                ),
            )
        )
    return chains


def _build_dynamics(profile: DatasetProfile, lifecycle: Lifecycle | None) -> Dynamics:
    learned = profile.dynamics
    amortisation = None

    am = learned.get("amortisation")
    if am and am.get("balance"):
        only_when = am.get("only_when_state") or None
        if only_when and lifecycle:
            only_when = [s for s in only_when if s in lifecycle.states] or None
        # An amortisation rule is only as good as the columns behind it, and a
        # tape that carries neither a payment nor a remaining term cannot support
        # one at all. Falling back through the kinds — annuity, then linear, then
        # a frozen balance — keeps the spec runnable instead of producing one that
        # fails its own validation on the first load.
        kind = am["kind"]
        payment, term = am.get("payment"), am.get("term")
        if kind == "annuity" and not (am.get("rate") and payment):
            kind = "linear"
        if kind == "linear" and not (payment or term):
            kind = "interest_only"

        amortisation = Amortisation(
            kind=kind,
            balance=am["balance"],
            rate=am.get("rate") if kind == "annuity" else None,
            payment=payment,
            term=term if kind in ("linear", "bullet") else None,
            only_when_state=only_when,
            **(
                {"rate_per_period": max(0.0, 1.0 - am.get("median_period_ratio", 1.0))}
                if kind in ("revolving", "depreciation")
                else {}
            ),
        )

    # A balance that fell by the same amount every period looks exactly like a
    # counter, and the panel learner reports it as one. Keeping both would make
    # the spec contradict itself: the ageing loop runs counters first and
    # amortisation second, so amortisation wins, and the invariant asserting the
    # counter's fixed step then fails against the engine's own output.
    # Amortisation owns the balance.
    owned = {amortisation.balance} if amortisation else set()
    counters = [
        Counter(column=c["column"], step=c["step"], clip_min=0 if c["step"] < 0 else None)
        for c in learned.get("counters", [])
        if c["column"] not in owned
    ]

    indices = [
        Index(
            name=i["name"],
            applies_to=i["applies_to"],
            kind="constant_drift",
            annual=i["annual"],
        )
        for i in learned.get("indices", [])
    ]

    accruals: list[Accrual] = []
    if am and am.get("payment") and lifecycle:
        arrears = next(
            (c.name for c in profile.columns if "arrears" in c.name.lower() and c.dtype == "float"),
            None,
        )
        if arrears:
            accruals.append(Accrual(column=arrears, add=am["payment"], when="not_performing"))

    return Dynamics(
        amortisation=amortisation, indices=indices, counters=counters, accruals=accruals
    )


# ---------------------------------------------------------------------------
# the one-call path
# ---------------------------------------------------------------------------


def build_spec(
    data: Any,
    *,
    structure: str | Template | None = None,
    name: str = "profiled",
    id_column: str | None = None,
    time_column: str | None = None,
    state_column: str | None = None,
    periods: int | None = None,
    max_rows: int | None = None,
    **kwargs: Any,
) -> tuple[DesignSpec, DatasetProfile]:
    """Profile a sample and build a spec from it in one call.

    Returns both, because the profile carries the evidence behind every choice
    the spec makes and a caller usually wants to show them together.

    ``max_rows`` caps how much of the sample is read. Distribution shapes settle
    long before a tape is exhausted, so a caller with someone waiting — the web
    UI — can bound the wait without meaningfully changing the answer.
    """
    import pandas as pd

    from sdd.profile.profiler import profile_dataset, read_sample
    from sdd.profile.template import load_template

    tmpl = load_template(structure) if isinstance(structure, str) else structure

    # Read once, here, rather than letting `profile_dataset` open the file
    # itself. Group detection counts rows — how many facilities each obligor
    # carries, how many arrive later belonging to a borrower already on the book
    # — so it needs the frame, not a path to one.
    #
    # Passing the caller's argument straight through looked equivalent and was
    # not: every test hands over a DataFrame, while the web and CLI paths hand
    # over a `Path`. Group detection skipped itself on exactly the two routes a
    # user takes, and did so silently, because a spec with no groups is a
    # perfectly valid spec. Caught on the deployed Space, against a tape the
    # detector handles correctly in memory.
    #
    # Reading here also means one read instead of two, and the same `max_rows`
    # cap over both — so the group shape is measured on the rows that were
    # profiled rather than on a longer tape nothing else saw.
    frame = data if isinstance(data, pd.DataFrame) else read_sample(data, max_rows=max_rows)

    profile = profile_dataset(
        frame,
        id_column=id_column,
        time_column=time_column,
        state_column=state_column,
        max_rows=max_rows,
    )
    spec = spec_from_profile(
        profile, template=tmpl, name=name, periods=periods, data=frame, **kwargs
    )
    return spec, profile


# ---------------------------------------------------------------------------
# targets, metrics and charts
# ---------------------------------------------------------------------------


def _build_targets(profile: DatasetProfile, spec: DesignSpec) -> list[Target]:
    """The size of the opening book. Measured.

    This one is a real observation. The tape says what the portfolio came to on
    day one; a spec without it regenerates to whatever the draws happen to sum
    to, and the deal size stops being something anyone chose.

    Two things make it more than bookkeeping. A distribution fit lands near the
    observed mean but rarely on it — a lognormal fitted to balances is a couple
    of per cent out in either direction — and the target corrects that drift
    back onto the number that was actually measured. And it turns portfolio size
    into a knob: edit `total` and the generator rescales, instead of hunting for
    the parameter that happens to control it.

    Only emitted where it would work. `apply_targets` scales a generator by its
    closed-form mean, and raises on a generator that has none; a target learned
    onto a resampled column would fail every regeneration rather than the run
    that wrote it, which is the worst place to put a failure.
    """
    from sdd.generate.targets import _expected_value

    balance = _balance_column(profile)
    if not balance:
        return []
    column = next((c for c in spec.columns if c.name == balance), None)
    stats = profile.column(balance)
    if column is None or stats is None or not stats.mean or stats.mean <= 0:
        return []
    if _expected_value(column.generator) is None:
        return []

    entities = int(profile.opening_entities or profile.entities or 0)
    if entities < 1:
        return []
    return [Target(column=balance, total=round(stats.mean * entities, 2), entities=entities)]


def _build_metrics(profile: DatasetProfile, spec: DesignSpec) -> list[Metric]:
    """A starting portfolio report. Proposed, not recovered.

    Which figures matter is a judgement, and no amount of looking at rows
    reveals it — a tape does not record that its owner cares about weighted
    average spread rather than weighted average life. Both are computable from
    the same columns and only one is the point.

    So this proposes the four that hold for any book with a balance: what it is
    worth, how many assets are in it, what it earns, and how much of it is in
    trouble. A reader who wants different ones gets a list to edit instead of an
    empty section to invent, which is the whole difference in practice.
    """
    balance = _balance_column(profile)
    if not balance:
        return []

    metrics = [
        Metric(
            name="total_balance",
            kind="sum",
            column=balance,
            decimals=2,
            description=f"{balance} summed across the book.",
        ),
        Metric(
            name="active_entities",
            kind="count",
            description="Entities reporting at this cut-off.",
        ),
    ]

    rate = _rate_column(profile)
    if rate:
        metrics.append(
            Metric(
                name="wa_rate",
                kind="weighted_mean",
                column=rate,
                weight=balance,
                decimals=4,
                description=f"{rate}, weighted by {balance}.",
            )
        )

    lifecycle = spec.lifecycle
    if lifecycle and len(lifecycle.states) > 1:
        healthy = lifecycle.states[0]
        metrics.append(
            Metric(
                name="non_performing_pct",
                kind="share_where",
                column=balance,
                where=f"{lifecycle.state_column} != {healthy!r}",
                decimals=6,
                description=f"Share of {balance} not in {healthy!r}.",
            )
        )
    return metrics


def _build_results(spec: DesignSpec) -> Results:
    """Charts for the metrics just proposed, on the same footing.

    Drawn from the metrics rather than from the panel, so the line on the screen
    is the number in the report and not a second calculation that might disagree.
    """
    names = {m.name for m in spec.metrics}
    charts: list[ChartSpec] = []

    if "total_balance" in names:
        charts.append(
            ChartSpec(
                kind="series",
                title="Portfolio balance",
                metric="total_balance",
                unit="money",
                description="What the book is worth at each cut-off.",
                explain="The sum of every reporting entity's balance. It falls as assets "
                "amortise, redeem or leave the pool, and steps up if new ones arrive.",
            )
        )

    lifecycle = spec.lifecycle
    if lifecycle and len(lifecycle.states) > 1:
        live = [s for s in lifecycle.states if s not in lifecycle.terminal]
        charts.append(
            ChartSpec(
                kind="stacked_series",
                title="State mix",
                column=lifecycle.state_column,
                states=live or lifecycle.states,
                unit="percent",
                description="Share of entities in each state, cut-off by cut-off.",
                explain="Every entity sits in exactly one state each period. States an "
                "entity cannot leave are left out, because they only accumulate and "
                "would swamp the picture.",
            )
        )

    if "non_performing_pct" in names:
        charts.append(
            ChartSpec(
                kind="series",
                title="Non-performing share",
                metric="non_performing_pct",
                unit="percent",
                description="Balance outside the healthy state, as a share of the book.",
                explain="Weighted by balance rather than counted by entity, so one large "
                "asset in trouble registers as more than one small one.",
            )
        )

    return Results(charts=charts)


def _balance_column(profile: DatasetProfile) -> str | None:
    """The column a portfolio's size is measured in.

    Named candidates first, because `current_balance` beats a column that merely
    contains the word; and numeric-only throughout, since a tape read from CSV
    can carry a `balance_type` column holding text.
    """
    named = {c.name.lower(): c for c in profile.columns}
    for hint in (
        "current_par",
        "current_balance",
        "outstanding_balance",
        "current_principal_balance",
        "principal_balance",
        "balance",
    ):
        candidate = named.get(hint)
        if candidate is not None and candidate.dtype in ("int", "float"):
            return candidate.name
    for column in profile.columns:
        if column.dtype in ("int", "float") and "balance" in column.name.lower():
            return column.name
    return None


def _rate_column(profile: DatasetProfile) -> str | None:
    """The column an average coupon would be taken over.

    Same trap as the balance column, and the one that produced a real bug: a
    tape carries both `interest_rate_type` holding "Fixed" and
    `current_interest_rate_pct` holding 4.2, and a match on the substring alone
    picks the wrong one.
    """
    for hint in ("current_interest_rate_pct", "coupon_pct", "interest_rate_pct", "rate_pct"):
        for column in profile.columns:
            if column.name.lower() == hint and column.dtype in ("int", "float"):
                return column.name
    for column in profile.columns:
        name = column.name.lower()
        if (
            column.dtype in ("int", "float")
            and any(hint in name for hint in ("coupon", "interest_rate", "rate"))
            and not any(bad in name for bad in ("type", "flag", "code", "index"))
        ):
            return column.name
    return None
