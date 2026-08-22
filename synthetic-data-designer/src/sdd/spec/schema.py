"""Pydantic models for the design spec.

A *design spec* is a single YAML/JSON document that fully determines a synthetic
dataset: what columns exist, how each is sampled, how rows are derived, how loans
move through a lifecycle over time, and how the result is written out.

The point of the spec is that nothing about a particular deal, asset class, or
jurisdiction lives in Python. Upstream deeploans hardcoded the Dutch RMBS facts
(province weights, a 6x6 delinquency matrix, annuity amortisation, a
``green_lion_*`` filename); here every one of those is a field below.

Vocabulary, in plain terms:

``entity``
    The thing being tracked over time — a loan, a lease, a facility. The
    ``id_column`` identifies it; the same id appears once per period.
``period``
    One observation date (a "cut-off"). A monthly panel over two years has 24.
``static`` vs ``dynamic``
    A static column has the same value for an entity in every period (the
    province a house is in). A dynamic column changes (the outstanding balance).
``lifecycle``
    A state machine: Performing -> 30 days late -> Defaulted -> Charged-Off.
``hazard``
    A per-period probability of an event, e.g. a 0.6%/month chance of prepaying.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Annotated, Any, Literal

import numpy as np
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

SPEC_VERSION = 1

# Tolerance when checking that a row of transition probabilities sums to 1.
_ROW_SUM_TOL = 1e-6


class _Base(BaseModel):
    """Common config: reject unknown keys so typos in a spec fail loudly."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)


# ---------------------------------------------------------------------------
# meta / entity / calendar
# ---------------------------------------------------------------------------


class Meta(_Base):
    name: str
    title: str | None = Field(
        default=None,
        description="Human-readable name, shown wherever a person picks this spec. `name` is "
        "the identifier — short, lowercase, used in filenames — and makes a poor label.",
    )
    asset_class: str = "generic"
    regulatory_template: str | None = None
    description: str | None = None
    source: str | None = Field(
        default=None,
        description="Where this spec came from — a pack name, or the sample file it was profiled from.",
    )
    entity_noun: str | None = Field(
        default=None,
        description="What one row of the opening book is called, singular — 'facility', "
        "'plan', 'invoice'. The interface uses it wherever it counts them, so a pack reads "
        "in its own vocabulary instead of everything being a loan.",
    )
    entity_noun_plural: str | None = Field(
        default=None,
        description="Plural of `entity_noun`, when adding an 's' is wrong. Defaults to a "
        "simple rule, which handles 'plans' and 'invoices' and not 'facilities'.",
    )
    display_order: int | None = Field(
        default=None,
        description="Where this pack sits in the picker. Lower comes first; packs without one "
        "follow, alphabetically. Alphabetical order is an accident of naming, and the first "
        "thing in a list is what most people click.",
    )
    featured: bool = Field(
        default=False,
        description="Mark this pack as the one to try first. At most one should carry it — "
        "highlighting everything highlights nothing.",
    )


def plural_of(noun: str) -> str:
    """Enough English to cover the nouns lending uses.

    Deliberately small: a pack whose plural this gets wrong says so explicitly
    rather than waiting for the rule to grow a special case.
    """
    if noun.endswith("y") and not noun.endswith(("ay", "ey", "iy", "oy", "uy")):
        return noun[:-1] + "ies"
    if noun.endswith(("s", "x", "z", "ch", "sh")):
        return noun + "es"
    return noun + "s"


class Calendar(_Base):
    """The observation dates the panel is emitted on."""

    start: str = Field(description="First cut-off date, ISO 8601 (YYYY-MM-DD).")
    periods: int = Field(ge=1, description="Number of cut-offs, including the first.")
    freq: Literal[
        "day",
        "week_end",
        "fortnight_end",
        "month_start",
        "month_end",
        "quarter_end",
        "year_end",
    ] = "month_end"

    @field_validator("start", mode="before")
    @classmethod
    def _iso_string(cls, v: Any) -> Any:
        # Unquoted YAML dates parse as datetime.date; accept them rather than
        # making every spec author remember the quotes.
        if isinstance(v, datetime):
            return v.date().isoformat()
        if isinstance(v, date):
            return v.isoformat()
        return v

    @property
    def periods_per_year(self) -> float:
        return {
            "day": 365.25,
            # 52.18 and 26.09, not 52 and 26: a year is 365.25 days, and rounding
            # here would bias every annual-to-period rate conversion — a hazard
            # given as 12% a year would come out slightly wrong every period, and
            # compound over a panel.
            "week_end": 365.25 / 7.0,
            "fortnight_end": 365.25 / 14.0,
            "month_start": 12.0,
            "month_end": 12.0,
            "quarter_end": 4.0,
            "year_end": 1.0,
        }[self.freq]


class Target(_Base):
    """An aggregate the portfolio should add up to, e.g. EUR 500m of collateral.

    Generators draw each entity independently, so a portfolio's total is whatever
    the draws happen to sum to. A deal has a *size*, and this is how a spec says
    so.

    It works by scaling the column's generator so the **expected** total matches,
    not by rescaling the values that were drawn. That distinction matters once a
    pool reinvests: rescaling the opening book alone would leave every facility
    acquired later drawn at the unscaled size, and a portfolio whose new assets
    are three times the size of its original ones is worse than one that misses
    its target by a few per cent. Changing the generator applies to every cohort
    for free, and shows up in the spec the user can download.

    The cost is that the realised total varies around the target by ordinary
    sampling error — a few per cent at a few hundred entities, less as the
    portfolio grows.
    """

    column: str = Field(description="Numeric column whose total is being aimed at.")
    total: float = Field(gt=0.0, description="What the column should sum to across the book.")
    entities: int | None = Field(
        default=None,
        ge=1,
        description="Entity count the total assumes. Defaults to the run's own count, so "
        "asking for more entities buys a bigger portfolio rather than smaller loans.",
    )


# ---------------------------------------------------------------------------
# groups — several entities sharing one parent
# ---------------------------------------------------------------------------


class GroupSize(_Base):
    """How members spread across groups.

    Real books are lumpy. A handful of borrowers carry several facilities while
    most carry one, and a rule that hands every group the same number of members
    produces a portfolio no concentration limit would ever bite on.
    """

    kind: Literal["zipf", "uniform", "fixed"] = "zipf"
    concentration: float = Field(
        default=1.4,
        gt=1.0,
        description="Zipf exponent. Higher is flatter; nearer 1.0 concentrates members "
        "into a few large groups.",
    )
    max_members: int | None = Field(
        default=None, ge=1, description="Cap on members per group. None means uncapped."
    )


class Group(_Base):
    """A parent record several entities share.

    The entity stays the unit of the panel — a facility, a loan, an account. A
    group is the thing behind several of them: the obligor behind three
    facilities, the household behind a mortgage and a buy-to-let, the dealer
    behind a month of car loans.

    What makes this more than a category column is that a group carries its
    **own** attributes, generated once and identical for every member. Three
    facilities lent to the same company must agree about that company's industry,
    country and revenue. Generated per facility they would disagree, and any
    analysis by obligor would be meaningless.

    **Shape.** Many entities to one group. One entity belonging to *several*
    groups — two named borrowers on a single mortgage — is a different shape and
    is not this: model those as columns of the entity, or make the household the
    group and let it hold several loans.
    """

    name: str = Field(description="What the group is, e.g. 'obligor'.")
    key: str = Field(description="Column holding the group identifier on each entity.")
    count: int | None = Field(
        default=None, ge=1, description="How many groups exist. Give this or `ratio`."
    )
    ratio: float | None = Field(
        default=None,
        gt=0.0,
        le=1.0,
        description="Groups as a share of entities, e.g. 0.45 for 225 groups per 500 "
        "entities. Survives a change in run size, which `count` does not.",
    )
    id_format: str = Field(
        default="G{seq:06d}",
        description="Format for the group identifier. '{seq}' is the 1-based group number.",
    )
    size: GroupSize = Field(default_factory=GroupSize)
    columns: list[Column] = Field(
        default_factory=list,
        description="Attributes of the group itself, generated once and shared by every "
        "member. These reach the output like any other column.",
    )
    correlation_target: CorrelationTarget | None = Field(
        default=None,
        description="Rank correlation between the group's *own* numeric attributes, "
        "reimposed by reordering the group table.\n\n"
        "Group attributes are drawn marginal by marginal, one generator each, so without "
        "this a company's revenue and its leverage vary independently — and a book where "
        "the most indebted borrowers are no larger or smaller than anyone else is not a "
        "book anyone has seen. Measured across *groups*, never across entities: an obligor "
        "with six facilities would otherwise count six times, and the correlation would be "
        "weighted by how much each company happened to borrow.",
    )
    new_group_rate: float = Field(
        default=0.5,
        ge=0.0,
        le=1.0,
        description="Share of entities joining an open pool that belong to a *new* group. "
        "The rest attach to a group that already exists — a lender lends again to a "
        "borrower it already has.",
    )

    @model_validator(mode="after")
    def _check(self) -> Group:
        if (self.count is None) == (self.ratio is None):
            raise ValueError(
                f"group {self.name!r} needs exactly one of `count` (a fixed number of "
                "groups) or `ratio` (a share of the entity count)"
            )
        names = [c.name for c in self.columns]
        if len(set(names)) != len(names):
            raise ValueError(f"group {self.name!r} declares duplicate columns")
        if self.key in names:
            raise ValueError(
                f"group {self.name!r} lists its own key {self.key!r} as an attribute; "
                "the key is generated from `id_format`"
            )
        return self

    def group_count(self, entities: int) -> int:
        """How many groups a book of ``entities`` entities has."""
        if self.count is not None:
            return min(self.count, entities)
        assert self.ratio is not None
        return max(1, min(round(entities * self.ratio), entities))


class Entity(_Base):
    id_column: str = Field(description="Column holding the per-entity identifier, e.g. loan_id.")
    id_format: str | None = Field(
        default=None,
        description=(
            "Python format string for generating ids, e.g. 'GL{deal_year}_{seq:06d}'. "
            "'{seq}' is the 1-based row number; other placeholders resolve against `constants` "
            "and `params`. When omitted, ids come from the id_column's own generator."
        ),
    )
    time_column: str = Field(description="Column holding the cut-off date of each row.")
    calendar: Calendar
    targets: list[Target] = Field(
        default_factory=list,
        description="Aggregate totals the opening portfolio should come to, e.g. a deal size.",
    )


# ---------------------------------------------------------------------------
# column generators
# ---------------------------------------------------------------------------


class CategoricalGen(_Base):
    kind: Literal["categorical"] = "categorical"
    values: list[Any]
    weights: list[float] | None = None

    @model_validator(mode="after")
    def _check(self) -> CategoricalGen:
        if not self.values:
            raise ValueError("categorical generator needs at least one value")
        if self.weights is not None:
            if len(self.weights) != len(self.values):
                raise ValueError(
                    f"categorical generator has {len(self.values)} values but "
                    f"{len(self.weights)} weights"
                )
            if any(w < 0 for w in self.weights):
                raise ValueError("categorical weights must be non-negative")
            if sum(self.weights) <= 0:
                raise ValueError("categorical weights must sum to a positive number")
        return self


class ConditionalCategoricalGen(_Base):
    """Pick a value from a pool chosen by another column's value.

    Upstream used this for province -> NUTS-3 region: each Dutch province has its
    own list of valid statistical regions.
    """

    kind: Literal["conditional_categorical"] = "conditional_categorical"
    parent: str = Field(description="Name of the column whose value selects the pool.")
    mapping: dict[str, list[Any]] = Field(description="parent value -> candidate values.")
    weights: dict[str, list[float]] | None = None
    default: list[Any] | None = Field(
        default=None,
        description="Pool to use when a parent value is missing from `mapping`. "
        "When omitted, an unmapped parent value is an error at generation time.",
    )

    @model_validator(mode="after")
    def _check(self) -> ConditionalCategoricalGen:
        for key, vals in self.mapping.items():
            if not vals:
                raise ValueError(f"conditional_categorical mapping for {key!r} is empty")
        if self.weights:
            for key, ws in self.weights.items():
                if key not in self.mapping:
                    raise ValueError(f"weights given for unmapped parent value {key!r}")
                if len(ws) != len(self.mapping[key]):
                    raise ValueError(f"weights for {key!r} do not match the number of values")
        return self


class ScipyGen(_Base):
    """Any continuous distribution from ``scipy.stats``, by name."""

    kind: Literal["scipy"] = "scipy"
    dist: str = Field(description="scipy.stats distribution name, e.g. 'lognorm', 'truncnorm'.")
    params: dict[str, float] = Field(default_factory=dict)
    decimals: int | None = None
    clip_min: float | None = None
    clip_max: float | None = None


class GaussianGen(_Base):
    kind: Literal["gaussian"] = "gaussian"
    mean: float
    stddev: float = Field(gt=0)
    decimals: int | None = None
    clip_min: float | None = None
    clip_max: float | None = None


class UniformGen(_Base):
    kind: Literal["uniform"] = "uniform"
    low: float
    high: float
    decimals: int | None = None

    @model_validator(mode="after")
    def _check(self) -> UniformGen:
        if self.high <= self.low:
            raise ValueError("uniform generator needs high > low")
        return self


class BernoulliGen(_Base):
    """A 0/1 coin flip. ``p`` is the probability of 1."""

    kind: Literal["bernoulli"] = "bernoulli"
    p: float = Field(ge=0.0, le=1.0)
    true_value: Any = 1
    false_value: Any = 0


class EmpiricalGen(_Base):
    """Resample from observed values — the profiler's fallback when no named
    distribution fits well. ``values`` are bin edges or raw observations."""

    kind: Literal["empirical"] = "empirical"
    values: list[float]
    weights: list[float] | None = None
    decimals: int | None = None

    @model_validator(mode="after")
    def _check(self) -> EmpiricalGen:
        if not self.values:
            raise ValueError("empirical generator needs at least one value")
        if self.weights is not None and len(self.weights) != len(self.values):
            raise ValueError("empirical weights must match values")
        return self


class SequenceGen(_Base):
    """Deterministic identifiers: ``prefix`` + zero-padded 1-based counter."""

    kind: Literal["sequence"] = "sequence"
    prefix: str = ""
    start: int = 1
    width: int = Field(default=6, ge=1)


class UUIDGen(_Base):
    kind: Literal["uuid"] = "uuid"
    prefix: str = ""
    short: bool = Field(default=False, description="8 hex chars instead of 32.")
    uppercase: bool = False


class ConstantGen(_Base):
    """The same value for every entity, including no value at all.

    ``value`` defaults to None rather than being required, and that is not
    cosmetic. A column seeded empty and filled later by a per-period derivation —
    an event date stamped when the event happens — is written ``value: null``.
    Required, the field is dropped by ``model_dump(exclude_none=True)``, which is
    exactly how the web layer hands a pack to the browser: the spec would come
    back missing the field and fail to validate, so a pack using this pattern
    could be loaded in the wizard and then refused when run.
    """

    kind: Literal["constant"] = "constant"
    value: Any = None


Generator = Annotated[
    CategoricalGen
    | ConditionalCategoricalGen
    | ScipyGen
    | GaussianGen
    | UniformGen
    | BernoulliGen
    | EmpiricalGen
    | SequenceGen
    | UUIDGen
    | ConstantGen,
    Field(discriminator="kind"),
]


ColumnRole = Literal["static", "dynamic", "derived", "constant", "helper"]
DType = Literal["int", "float", "str", "category", "bool", "date"]


class Column(_Base):
    """One output column.

    ``required`` and ``null_rate`` are the schema-review knobs: a required column
    is never blanked, an optional one may be, and ``null_rate`` overrides the
    global missing-value rate for this column alone.

    ``role`` drives both generation and validation:

    ``static``
        Sampled once at period 0 and carried unchanged. The invariant checker
        asserts it never changes for an entity.
    ``dynamic``
        Sampled at period 0, then updated each period by the ageing engine.
    ``derived``
        Not sampled at all — computed by a `derivations` entry or forced by the
        lifecycle's `state_fields`.
    ``constant``
        Same for every row in the whole dataset (deal-level facts).
    ``helper``
        An intermediate used by derivations and dropped before output.
    """

    name: str
    role: ColumnRole = "static"
    dtype: DType | None = None
    generator: Generator | None = None
    description: str | None = None
    domain: list[Any] | None = Field(
        default=None,
        description="Allowed values. Enforced by the invariant checker when set.",
    )
    min: float | None = None
    max: float | None = None
    required: bool = Field(
        default=True,
        description="A required column is never blanked by the missing-value setting.",
    )
    null_rate: float | None = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Share of rows blanked in this column, overriding generation.missing.",
    )
    confidence: float | None = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="How much to trust an inferred definition. Written by the profiler; "
        "anything below 0.5 is flagged for review.",
    )
    review: str | None = Field(
        default=None, description="Why this column needs a human look before it is trusted."
    )

    @model_validator(mode="after")
    def _check(self) -> Column:
        if self.role in ("static", "dynamic") and self.generator is None:
            raise ValueError(f"column {self.name!r} has role {self.role!r} but no generator")
        if self.role == "derived" and self.generator is not None:
            raise ValueError(
                f"column {self.name!r} is derived; its value comes from `derivations`, "
                "so it must not also declare a generator"
            )
        return self


# ---------------------------------------------------------------------------
# derivations
# ---------------------------------------------------------------------------


class WhenRule(_Base):
    if_: str = Field(alias="if")
    then: Any


class Derivation(_Base):
    """A deterministic column computed from other columns.

    Two forms:

    ``expr``
        A single vectorised expression, e.g.
        ``original_balance / (oltomv_original / 100)``.
    ``when``
        Ordered condition/value rules with a fallback, for categorical outputs:
        ``rules: [{if: "balance <= 435000", then: "Y"}], else: "N"``.

    Expressions are evaluated by a restricted AST walker (see
    :mod:`sdd.generate.deriver`) — a spec is data, never executable code.
    """

    target: str
    kind: Literal["expr", "when", "bucket", "format"] = "expr"
    expr: str | None = None
    rules: list[WhenRule] | None = None
    else_: Any = Field(default=None, alias="else")
    bucket: str | None = Field(default=None, description="Name of a `buckets` entry to apply.")
    source: str | None = Field(default=None, description="Input column for a bucket derivation.")
    template: str | None = Field(
        default=None,
        description="Python format string for kind 'format', e.g. '{y:04d}-{m:02d}-28'. "
        "Used for the date-proxy columns common in regulatory tapes.",
    )
    args: dict[str, str] = Field(
        default_factory=dict,
        description="Placeholder name -> expression, for kind 'format'.",
    )
    round: int | None = None
    dtype: DType | None = None
    stage: Literal["book", "period", "both"] = Field(
        default="book",
        description=(
            "When to run. 'book' = period 0 only (origination facts). "
            "'period' = every ageing period (recomputed as balances move). "
            "'both' = period 0 and every period after."
        ),
    )

    @model_validator(mode="after")
    def _check(self) -> Derivation:
        if self.kind == "expr" and not self.expr:
            raise ValueError(f"derivation for {self.target!r} is kind 'expr' but has no `expr`")
        if self.kind == "when" and not self.rules:
            raise ValueError(f"derivation for {self.target!r} is kind 'when' but has no `rules`")
        if self.kind == "bucket" and not (self.bucket and self.source):
            raise ValueError(
                f"derivation for {self.target!r} is kind 'bucket' and needs both "
                "`bucket` (which binning to use) and `source` (which column to bin)"
            )
        if self.kind == "format" and not (self.template and self.args):
            raise ValueError(
                f"derivation for {self.target!r} is kind 'format' and needs both "
                "`template` and `args`"
            )
        return self


class Bucket(_Base):
    """A reusable binning rule: turn a number into a labelled band.

    ``bins`` are the right-hand edges plus a leading lower bound, exactly as
    ``pandas.cut`` expects, so ``len(labels) == len(bins) - 1``.
    """

    bins: list[float]
    labels: list[str]
    right: bool = True
    include_lowest: bool = True

    @model_validator(mode="after")
    def _check(self) -> Bucket:
        if len(self.labels) != len(self.bins) - 1:
            raise ValueError(
                f"bucket has {len(self.bins)} edges so it needs {len(self.bins) - 1} labels, "
                f"got {len(self.labels)}"
            )
        if any(b <= a for a, b in zip(self.bins, self.bins[1:], strict=False)):
            raise ValueError("bucket edges must be strictly increasing")
        return self


# ---------------------------------------------------------------------------
# lifecycle
# ---------------------------------------------------------------------------


class BernoulliHazard(_Base):
    """A flat per-period probability of jumping to ``to_state``.

    Give either ``annual_rate`` (converted to a per-period rate using the
    calendar) or ``period_rate`` directly.
    """

    kind: Literal["bernoulli"] = "bernoulli"
    name: str
    to_state: str
    annual_rate: float | None = Field(default=None, ge=0.0, le=1.0)
    period_rate: float | None = Field(default=None, ge=0.0, le=1.0)
    from_states: list[str] | None = Field(
        default=None, description="Only these states are eligible. Default: all non-terminal."
    )
    excluded_states: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _check(self) -> BernoulliHazard:
        if (self.annual_rate is None) == (self.period_rate is None):
            raise ValueError(
                f"hazard {self.name!r} needs exactly one of `annual_rate` or `period_rate`"
            )
        return self

    def rate_per_period(self, periods_per_year: float) -> float:
        if self.period_rate is not None:
            return self.period_rate
        assert self.annual_rate is not None
        return 1.0 - (1.0 - self.annual_rate) ** (1.0 / periods_per_year)


class DwellTimeHazard(_Base):
    """Fires once an entity has spent ``periods`` consecutive periods in
    ``from_state`` — e.g. charge off after 9 months in default."""

    kind: Literal["dwell_time"] = "dwell_time"
    name: str
    from_state: str
    to_state: str
    periods: int = Field(ge=1)


class ConditionHazard(_Base):
    """Fires when an expression over the entity's own columns becomes true.

    The other two hazards are blind to the data: Bernoulli is a flat chance and
    dwell-time is a fixed count of periods, so both treat every entity alike.
    Maturity does not work that way — a loan matures when *its own* maturity date
    arrives, and a 24-month loan and a 72-month one written the same day mature
    four years apart. That is a condition on a column, and it is what this
    expresses.

    Deterministic, so it is evaluated before the probabilistic hazards and before
    the matrix: an entity that has reached its maturity date has matured, and
    cannot then be drawn into being prepaid or sold in the same period.
    """

    kind: Literal["condition"] = "condition"
    name: str
    when: str = Field(
        description="Expression over the entity's columns, e.g. 'months_to_maturity <= 0'."
    )
    to_state: str
    from_states: list[str] | None = Field(
        default=None, description="Only these states are eligible. Default: all non-terminal."
    )
    excluded_states: list[str] = Field(default_factory=list)


Hazard = Annotated[BernoulliHazard | DwellTimeHazard | ConditionHazard, Field(discriminator="kind")]


class Lifecycle(_Base):
    states: list[str]
    state_column: str = Field(description="Column that holds the state label.")
    transitions: list[list[float]] | None = Field(
        default=None,
        description=(
            "Square matrix over `transition_states` (defaults to all non-terminal states). "
            "Row = current state, column = next state; each row sums to 1."
        ),
    )
    transition_states: list[str] | None = Field(
        default=None,
        description="States the matrix covers. Defaults to `states` minus `terminal`.",
    )
    absorbing: list[str] = Field(
        default_factory=list,
        description="States an entity cannot leave, but which keep it in the pool (e.g. Defaulted).",
    )
    terminal: list[str] = Field(
        default_factory=list,
        description="States that end the entity's life — the row is written once, then dropped "
        "(e.g. Redeemed, Charged-Off).",
    )
    hazards: list[Hazard] = Field(default_factory=list)
    state_fields: dict[str, dict[str, Any]] = Field(
        default_factory=dict,
        description="state -> {column: forced value} applied after every transition.",
    )
    initial_distribution: dict[str, float] | None = Field(
        default=None,
        description="Optional state mix at period 0. When omitted the state_column's own "
        "generator supplies it.",
    )

    @model_validator(mode="after")
    def _check(self) -> Lifecycle:
        if len(set(self.states)) != len(self.states):
            raise ValueError("lifecycle states must be unique")
        known = set(self.states)

        for label, group in (("absorbing", self.absorbing), ("terminal", self.terminal)):
            unknown = set(group) - known
            if unknown:
                raise ValueError(f"lifecycle {label} names unknown states: {sorted(unknown)}")
        overlap = set(self.absorbing) & set(self.terminal)
        if overlap:
            raise ValueError(
                f"states cannot be both absorbing and terminal: {sorted(overlap)}. "
                "Absorbing means 'stuck but still in the pool'; terminal means 'leaves the pool'."
            )

        tstates = self.transition_states or [s for s in self.states if s not in self.terminal]
        unknown = set(tstates) - known
        if unknown:
            raise ValueError(f"transition_states names unknown states: {sorted(unknown)}")

        if self.transitions is not None:
            n = len(tstates)
            if len(self.transitions) != n:
                raise ValueError(
                    f"transition matrix has {len(self.transitions)} rows but there are "
                    f"{n} transition states {tstates}"
                )
            for i, row in enumerate(self.transitions):
                if len(row) != n:
                    raise ValueError(
                        f"transition matrix row {i} ({tstates[i]!r}) has {len(row)} entries, "
                        f"expected {n}"
                    )
                if any(p < 0 for p in row):
                    raise ValueError(
                        f"transition matrix row {i} ({tstates[i]!r}) has a negative probability"
                    )
                total = float(np.sum(row))
                if abs(total - 1.0) > _ROW_SUM_TOL:
                    raise ValueError(
                        f"transition matrix row {i} ({tstates[i]!r}) sums to {total:.6f}, "
                        "expected 1.0 — each row is a probability distribution over next states"
                    )
            # A state declared absorbing must actually be absorbing in the matrix,
            # or the two halves of the spec quietly contradict each other.
            for state in self.absorbing:
                if state not in tstates:
                    continue
                i = tstates.index(state)
                if abs(self.transitions[i][i] - 1.0) > _ROW_SUM_TOL:
                    raise ValueError(
                        f"state {state!r} is declared absorbing but its transition row gives it "
                        f"only {self.transitions[i][i]:.4f} probability of staying put; an "
                        "absorbing state's row must be 1.0 on its own diagonal"
                    )

        for hz in self.hazards:
            targets = [hz.to_state]
            sources = (
                [hz.from_state]
                if isinstance(hz, DwellTimeHazard)
                else list(hz.from_states or []) + list(hz.excluded_states)
            )
            unknown = (set(targets) | set(sources)) - known
            if unknown:
                raise ValueError(f"hazard {hz.name!r} names unknown states: {sorted(unknown)}")

        unknown = set(self.state_fields) - known
        if unknown:
            raise ValueError(f"state_fields names unknown states: {sorted(unknown)}")

        if self.initial_distribution:
            unknown = set(self.initial_distribution) - known
            if unknown:
                raise ValueError(f"initial_distribution names unknown states: {sorted(unknown)}")
            total = sum(self.initial_distribution.values())
            if abs(total - 1.0) > 1e-6:
                raise ValueError(f"initial_distribution sums to {total:.6f}, expected 1.0")
        return self

    @property
    def resolved_transition_states(self) -> list[str]:
        return self.transition_states or [s for s in self.states if s not in self.terminal]


# ---------------------------------------------------------------------------
# dynamics
# ---------------------------------------------------------------------------

AmortKind = Literal[
    "annuity",  # level payment; principal share grows over time (standard mortgage)
    "linear",  # equal principal each period, falling payment
    "bullet",  # no principal until maturity
    "interest_only",  # never amortises
    "revolving",  # balance drifts by a utilisation process (credit cards)
    "depreciation",  # value decays at a rate (auto residual value)
    "none",
]


class Amortisation(_Base):
    kind: AmortKind = "annuity"
    balance: str = Field(description="Column holding the outstanding balance.")
    rate: str | None = Field(
        default=None, description="Column holding the annual interest rate, in percent."
    )
    payment: str | None = Field(
        default=None, description="Column holding the scheduled payment per period."
    )
    term: str | None = Field(
        default=None, description="Column holding the remaining number of periods."
    )
    only_when_state: str | list[str] | None = Field(
        default=None,
        description="Amortise only in these lifecycle states. Anything else freezes the balance "
        "— a borrower who does not pay does not pay down principal.",
    )
    flat_when: str | None = Field(
        default=None,
        description="Expression selecting rows that never amortise, e.g. \"interest_only_flag == 'Y'\".",
    )
    rate_per_period: float | None = Field(
        default=None, description="Fixed decay rate for `revolving` / `depreciation`."
    )
    floor: float = 0.0

    @model_validator(mode="after")
    def _check(self) -> Amortisation:
        if self.kind == "annuity" and not (self.rate and self.payment):
            raise ValueError(
                "annuity amortisation needs both `rate` and `payment` columns: "
                "next balance = balance * (1 + rate) - payment"
            )
        if self.kind == "linear" and not (self.payment or self.term):
            raise ValueError("linear amortisation needs either `payment` or `term`")
        if self.kind in ("revolving", "depreciation") and self.rate_per_period is None:
            raise ValueError(f"{self.kind} amortisation needs `rate_per_period`")
        return self


class Index(_Base):
    """A multiplicative overlay applied to one or more columns each period.

    Used for house-price indices, car residual-value curves, CPI, and so on.
    """

    name: str
    applies_to: list[str]
    kind: Literal["constant_drift", "series"] = "constant_drift"
    annual: float | None = Field(default=None, description="Annualised growth, e.g. 0.03 for +3%.")
    series: list[float] | None = Field(
        default=None,
        description="Explicit per-period multipliers, one per period after the first. "
        "Shorter series repeat their last value.",
    )
    volatility: float = Field(
        default=0.0, ge=0.0, description="Per-period lognormal noise added to the drift."
    )
    clip_min: float | None = Field(
        default=None, description="Floor applied to the indexed column after each step."
    )
    clip_max: float | None = Field(
        default=None,
        description="Ceiling applied to the indexed column after each step.\n\n"
        "An index with volatility is a random walk, and a random walk with nothing to stop "
        "it wanders anywhere given enough periods. That is fine for a house-price index, "
        "which really can run away; it is wrong wherever the quantity has an economic "
        "bound. A leveraged loan is callable at par, so its price cannot sustain much "
        "above 100 no matter what the market does — the borrower simply refinances. Left "
        "unbounded, a 36-period run drifted the median price to 103.9, which no leveraged "
        "loan book has ever printed.",
    )

    @model_validator(mode="after")
    def _check(self) -> Index:
        if (
            self.clip_min is not None
            and self.clip_max is not None
            and self.clip_min > self.clip_max
        ):
            raise ValueError(f"index {self.name!r} has a floor above its ceiling")
        if self.kind == "constant_drift" and self.annual is None:
            raise ValueError(f"index {self.name!r} is constant_drift but has no `annual` rate")
        if self.kind == "series" and not self.series:
            raise ValueError(f"index {self.name!r} is kind 'series' but has no `series` values")
        if not self.applies_to:
            raise ValueError(f"index {self.name!r} applies to no columns")
        return self


class Counter(_Base):
    """A column that ticks each period — seasoning up, remaining term down."""

    column: str
    step: float | None = None
    expr: str | None = None
    clip_min: float | None = None
    clip_max: float | None = None
    dtype: DType | None = "int"

    @model_validator(mode="after")
    def _check(self) -> Counter:
        if (self.step is None) == (self.expr is None):
            raise ValueError(
                f"counter {self.column!r} needs exactly one of `step` (a fixed increment) "
                "or `expr` (a formula recomputed each period)"
            )
        return self


class Accrual(_Base):
    """A running total that grows while a condition holds and resets otherwise.

    Arrears are the canonical case: each period a borrower misses a payment,
    one more scheduled payment is added to the amount owed.
    """

    column: str
    add: str = Field(description="Column or number added per qualifying period.")
    when: Literal["not_performing", "in_states", "always"] = "not_performing"
    states: list[str] | None = Field(default=None, description="Used when `when` is 'in_states'.")
    reset_states: list[str] | None = Field(
        default=None, description="States that zero the counter. Defaults to the performing state."
    )
    performing_state: str | None = None

    @model_validator(mode="after")
    def _check(self) -> Accrual:
        if self.when == "in_states" and not self.states:
            raise ValueError(f"accrual {self.column!r} is 'in_states' but lists no states")
        return self


class Recovery(_Base):
    """What comes back after a write-off.

    A defaulted loan is not a total loss: the collateral is sold and part of the
    balance is recovered. Booked in the period the entity enters one of
    ``on_states``, as ``rate`` x the balance it carried on the way in.
    """

    rate: float = Field(ge=0.0, le=1.0, description="Share of the balance recovered.")
    balance: str = Field(description="Column holding the balance being recovered against.")
    target: str = Field(
        default="recovery_amount",
        description="Column the recovered amount is written to. Created if absent.",
    )
    on_states: list[str] = Field(
        default_factory=list,
        description="States that trigger the recovery. Defaults to the terminal states "
        "reachable from an absorbing one — the write-off states.",
    )


class Dynamics(_Base):
    amortisation: Amortisation | None = None
    indices: list[Index] = Field(default_factory=list)
    counters: list[Counter] = Field(default_factory=list)
    accruals: list[Accrual] = Field(default_factory=list)
    recovery: Recovery | None = None


# ---------------------------------------------------------------------------
# originations — an open pool
# ---------------------------------------------------------------------------


class Originations(_Base):
    """New entities joining the pool while it ages.

    Without this the pool is *closed*: every loan exists at the first cut-off and
    the pool only shrinks as loans redeem and write off. That is the right model
    for a static securitisation, and the wrong one for almost everything else — a
    lender keeps lending, a revolving deal keeps buying receivables, and a
    portfolio observed over two years contains loans written in both.

    Give either ``per_period`` (a fixed count) or ``rate`` (a share of the
    opening book, so the setting survives a change in scale). New entities are
    drawn from the same generators as the opening book, so they look like the
    portfolio they are joining, and enter at the cut-off they are created on.
    """

    per_period: int | None = Field(default=None, ge=0, description="New entities each period.")
    rate: float | None = Field(
        default=None,
        ge=0.0,
        description="New entities each period as a share of the opening book, e.g. 0.02 for 2%.",
    )
    start_period: int = Field(
        default=1,
        ge=1,
        description="First cut-off new entities appear at. Period 0 is the opening book itself.",
    )
    end_period: int | None = Field(
        default=None, ge=1, description="Last cut-off they appear at. Default: every period."
    )
    fresh: bool = Field(
        default=True,
        description=(
            "Treat new entities as newly originated rather than acquired. Sets them to the "
            "healthiest lifecycle state and zeroes every counter that ticks upward, because "
            "a counter that rises each period measures elapsed time and no time has elapsed. "
            "Counters that tick downward keep their sampled value — a new entity's remaining "
            "term is drawn from the book's own distribution."
        ),
    )
    reset: dict[str, Any] = Field(
        default_factory=dict,
        description="column -> literal value forced on a new entity, applied after `fresh`.",
    )
    reset_expr: dict[str, str] = Field(
        default_factory=dict,
        description=(
            "column -> expression, evaluated for each arriving cohort with the joining "
            "period available as `period`, `period_year`, `period_month` and `period_day`. "
            "This is how a loan written in June is given June's origination date: reset the "
            "date a derivation reads, and the columns computed from it follow. Resetting a "
            "derived column directly does not work — the derivation recomputes it."
        ),
    )

    @model_validator(mode="after")
    def _check(self) -> Originations:
        if (self.per_period is None) == (self.rate is None):
            raise ValueError(
                "originations needs exactly one of `per_period` (a fixed count) or `rate` "
                "(a share of the opening book)"
            )
        if self.end_period is not None and self.end_period < self.start_period:
            raise ValueError(
                f"originations end_period {self.end_period} is before start_period "
                f"{self.start_period}, so no entity would ever be created"
            )
        return self

    def count_for(self, period: int, opening_size: int) -> int:
        """How many entities join at ``period``. Zero outside the window."""
        if period < self.start_period:
            return 0
        if self.end_period is not None and period > self.end_period:
            return 0
        if self.per_period is not None:
            return int(self.per_period)
        assert self.rate is not None
        return round(opening_size * self.rate)


# ---------------------------------------------------------------------------
# scenarios
# ---------------------------------------------------------------------------


class Scenario(_Base):
    """A named stress overlay — the "age it based on the requirements" knob.

    A scenario does not restate the model; it *shifts* it. Multipliers apply to
    the base calibration so a single spec supports base / adverse / severe runs.
    """

    name: str
    description: str | None = None
    default_multiplier: float = Field(
        default=1.0,
        gt=0.0,
        description="Scales every transition probability that worsens an entity's state. "
        "2.0 doubles the chance of falling behind.",
    )
    prepayment_multiplier: float = Field(default=1.0, ge=0.0)
    recovery_multiplier: float = Field(
        default=1.0,
        ge=0.0,
        description="Scales `dynamics.recovery.rate`. A downturn that raises defaults usually "
        "lowers what comes back from them too, and without this a stress scenario recovers as "
        "much per write-off as the base case. The product is capped at 1.0 — a recovery cannot "
        "exceed the balance it recovers against.",
    )
    index_shift: dict[str, float] = Field(
        default_factory=dict,
        description="index name -> additive change to its annualised rate, e.g. {hpi: -0.10}.",
    )
    rate_shift: float = Field(
        default=0.0, description="Additive shift, in percentage points, to interest-rate columns."
    )
    rate_columns: list[str] = Field(default_factory=list)
    segment_stress: dict[str, dict[str, float]] = Field(
        default_factory=dict,
        description="Column -> {value -> multiplier}, stressing only the entities in that "
        "segment. `{industry: {Retail: 3.0, Energy: 2.5}}` triples the chance of a retail "
        "borrower slipping while leaving everyone else on the base calibration.\n\n"
        "`default_multiplier` moves the whole book at once, which is not how a downturn "
        "arrives: 2008 was not every sector worsening by the same factor. A recession lands "
        "on the sectors exposed to it, and a portfolio's real risk is how much of it sits in "
        "those sectors — which a uniform multiplier cannot express, because it leaves the "
        "concentration figures untouched no matter how severe it gets.\n\n"
        "Multiplies with `default_multiplier` rather than replacing it, so a scenario can "
        "raise the whole book and lean harder on part of it.",
    )

    @model_validator(mode="after")
    def _check_segments(self) -> Scenario:
        for column, table in self.segment_stress.items():
            for value, multiplier in table.items():
                if multiplier <= 0:
                    raise ValueError(
                        f"scenario {self.name!r} stresses {column}={value!r} by "
                        f"{multiplier}, which is not a multiplier. Use a value above 1.0 to "
                        "worsen and below 1.0 to improve; 0 would mean the segment can never "
                        "change state again"
                    )
        return self


# ---------------------------------------------------------------------------
# generation method and randomness
# ---------------------------------------------------------------------------

GenerationMethod = Literal[
    "statistical",  # moment-matched normals: mean and spread kept, shape simplified
    "distribution",  # the fitted named distribution per column (the profiler's choice)
    "rule_based",  # bounds and domains only, no fitted shape
    "sampling",  # resample the observed values, shape and spikes intact
    "ctgan",  # deep tabular model trained on the sample, joint structure learned
    "hybrid",  # distribution first, deep polish second
]


class CorrelationTarget(_Base):
    """The rank correlation observed between numeric columns in the sample.

    Marginal-by-marginal sampling produces independent columns. This records what
    the real data did, so :mod:`sdd.generate.randomness` can reimpose it by
    reordering — which changes how columns move together without touching any
    column's own distribution.
    """

    columns: list[str]
    matrix: list[list[float]]

    @model_validator(mode="after")
    def _check(self) -> CorrelationTarget:
        n = len(self.columns)
        if len(self.matrix) != n or any(len(row) != n for row in self.matrix):
            raise ValueError(
                f"correlation target covers {n} column(s) so the matrix must be {n}x{n}"
            )
        if any(abs(v) > 1.0 + 1e-9 for row in self.matrix for v in row):
            raise ValueError("correlation entries must lie between -1 and 1")
        return self


class Generation(_Base):
    """How period-0 values are drawn, and how much randomness is layered on top.

    ``method`` selects the sampler; the four rates below are applied after
    sampling, in this order: correlation, outliers, noise, missing values. Each
    is a share, so 0 means "leave it alone".
    """

    method: GenerationMethod = "distribution"
    noise: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="Gaussian jitter added to numeric columns, as a share of each column's "
        "own standard deviation.",
    )
    correlation: float = Field(
        default=1.0,
        ge=0.0,
        le=1.0,
        description="How much of `correlation_target` to reimpose. 0 leaves columns "
        "independent, 1 matches the sample.",
    )
    outliers: float = Field(
        default=0.0,
        ge=0.0,
        le=0.2,
        description="Share of rows pushed into the tail of a numeric column.",
    )
    outlier_sigma: float = Field(
        default=4.0, gt=0.0, description="How far into the tail an outlier is pushed."
    )
    missing: float = Field(
        default=0.0,
        ge=0.0,
        le=0.9,
        description="Share of values blanked in optional columns. Identifiers, dates, "
        "the state column and anything a state pins a value to are never blanked.",
    )
    correlation_target: CorrelationTarget | None = None
    polish_model: Literal["ctgan", "tvae"] = "ctgan"
    polish_epochs: int = Field(default=300, ge=1)

    @property
    def needs_sample(self) -> bool:
        """True when the method cannot run without the original tape."""
        return self.method in ("ctgan", "hybrid")


# ---------------------------------------------------------------------------
# emit / validation
# ---------------------------------------------------------------------------


class Emit(_Base):
    filename: str = Field(
        default="{name}_{yyyymm}.csv",
        description="Per-period filename template. Placeholders: {name} {yyyy} {mm} {yyyymm} "
        "{yyyy_mm_dd} {period} (0-based index).",
    )
    column_order: list[str] | None = Field(
        default=None, description="Exact output column order. Defaults to declaration order."
    )
    formats: list[Literal["csv", "parquet"]] = Field(default_factory=lambda: ["csv"])
    cutoff_dir: str | None = Field(
        default=None,
        description="Subdirectory for the per-period files, e.g. 'cutoffs'. Unset, they sit "
        "beside the panel and the manifest, which is fine for a handful and unreadable for "
        "sixty. Named rather than always-on, because moving them would relocate the output of "
        "every spec already written against the flat layout.",
    )
    panel_filename: str = "panel.parquet"
    write_panel: bool = True
    float_format: str | None = None


class InvariantToggles(_Base):
    """Which spec-derived checks the validator runs. All default on."""

    static_columns_stable: bool = True
    ids_unique_per_period: bool = True
    closed_pool: bool = True
    terminal_states_absorb: bool = True
    domains_respected: bool = True
    counters_step_correctly: bool = True
    state_fields_applied: bool = True
    group_columns_stable: bool = True
    non_negative_balances: bool = True


class CustomInvariant(_Base):
    name: str
    description: str | None = None
    sql: str = Field(
        description="A SELECT over the relation `panel` returning violating rows. "
        "Zero rows means the check passed."
    )


PlausibilityStatistic = Literal["median", "mean", "p10", "p90", "share", "distinct"]


class PlausibilityBand(_Base):
    """A range a portfolio characteristic should land inside to look real.

    Invariants ask "is this internally consistent?" — every one of them can pass
    on a book whose facilities average four thousand euro. Plausibility asks the
    other question: "does this look like the asset class it claims to be?"

    Deliberately a **band, not a distance**. Scoring against a reference tape is
    the right tool when a reference tape exists; for a bundled public pack there
    is none, and inventing one would mean shipping vendor-derived parameters —
    which §21 explicitly forbids. A declared range says what the pack believes
    the market looks like, in numbers a reader can argue with.

    Bands are assumptions, and are labelled as such. `note` is not optional
    decoration: a range with no stated reason is a number nobody can challenge.
    """

    name: str = Field(description="What is being checked, e.g. 'facility_size'.")
    column: str
    statistic: PlausibilityStatistic = "median"
    between: tuple[float, float] = Field(
        description="Inclusive lower and upper bound the statistic must land inside."
    )
    where: str | None = Field(
        default=None,
        description="Expression selecting the numerator, for `share`. Ignored otherwise.",
    )
    at_first_cutoff: bool = Field(
        default=False,
        description="Measure on the opening book rather than the whole panel. Origination "
        "facts belong here: a facility's size is decided once, and pooling it across "
        "cut-offs weights it by how long each facility survived.",
    )
    note: str = Field(description="Why this range, and where it comes from.")

    @model_validator(mode="after")
    def _check(self) -> PlausibilityBand:
        low, high = self.between
        if low > high:
            raise ValueError(f"plausibility band {self.name!r} has a lower bound above its upper")
        if self.statistic == "share" and not self.where:
            raise ValueError(
                f"plausibility band {self.name!r} measures a share but names no `where` to "
                "share on, so there is no numerator"
            )
        if self.statistic == "share" and not (0.0 <= low <= high <= 1.0):
            raise ValueError(
                f"plausibility band {self.name!r} is a share, so its bounds belong in [0, 1]; "
                f"got {self.between}"
            )
        return self


class BenchmarkObservable(_Base):
    """One noisy reading of the hidden driver, and how it was produced."""

    column: str = Field(description="The observable column a model may use.")
    centres: str = Field(
        description="Derivation whose `when` rules give this observable's mean per latent "
        "value, in the latent's declared order."
    )
    noise: str = Field(
        description="Helper column holding this observable's measurement error. Its "
        "generator's `stddev` is the width the inversion uses."
    )


Direction = Literal["increasing_risk", "decreasing_risk"]

MetricName = Literal[
    "roc_auc",  # ranking: can it tell good from bad at all
    "pr_auc",  # ranking under rarity, which is where credit lives
    "ks",  # the largest separation between the two distributions
    "brier",  # probability quality, not just order
    "calibration_error",  # does a predicted 3% happen 3% of the time
]


class ExpectedBehaviour(_Base):
    """What a model that learned correctly should look like.

    Everything else in a spec says how to *make* the data. This says what should
    be **true of a model** that has learned from it — and it is the difference
    between a dataset and a test with a mark scheme.

    The point is that these are written down *before* anyone runs a model.
    Judged afterwards, "the model uses sensible drivers" is a conversation.
    Declared here, it is a check that passes or fails, and it cannot be
    renegotiated once someone has seen the score they got.

    None of it changes a single generated row. The data is identical with this
    block or without it; what changes is whether the result can be marked.
    """

    directionality: dict[str, Direction] = Field(
        default_factory=dict,
        description="Observable -> the direction risk should move in. A model that ranks "
        "high-DTI borrowers as safer has learned something backwards, and will keep scoring "
        "well on aggregate while doing it.",
    )
    irrelevant_features: list[str] = Field(
        default_factory=list,
        description="Columns carrying no signal by construction. A model whose output varies "
        "with one of these has found noise — visible here because the generator knows they "
        "are noise, and invisible on a real portfolio where nobody does.",
    )
    min_signal_captured: float | None = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="How much of the available signal a model must find to pass, as a share "
        "of the ceiling above chance. Absolute scores cannot be compared across datasets "
        "with different ceilings; this can.",
    )
    max_calibration_error: float | None = Field(
        default=None,
        gt=0.0,
        description="Largest acceptable gap between predicted and observed rates. Only "
        "meaningful for a model emitting probabilities rather than ranks.",
    )


class Evaluation(_Base):
    """Which measurements decide whether a model passed.

    Declaring the metric list turns a benchmark from a description into a
    contract: everyone is judged on the same figures, chosen before the results
    were seen rather than after.

    ROC-AUC alone is the usual mistake in credit. Defaults are rare, so a model
    can rank well and still be useless for pricing — `pr_auc` says whether it
    finds the rare bad ones and `calibration_error` says whether a predicted 3%
    actually happens 3% of the time. A lender needs both.
    """

    metrics: list[MetricName] = Field(
        default_factory=lambda: ["roc_auc"],
        description="Metrics computed for every model scored against this benchmark.",
    )
    primary: MetricName = Field(
        default="roc_auc",
        description="The one compared against the ceiling. Only ranking metrics have a "
        "ceiling, so this must be one of those.",
    )

    @model_validator(mode="after")
    def _check(self) -> Evaluation:
        if not self.metrics:
            raise ValueError("evaluation declares no metrics, so nothing would be measured")
        if self.primary not in self.metrics:
            raise ValueError(
                f"the primary metric {self.primary!r} is not in `metrics` {self.metrics}; "
                "the figure a model is judged on has to be one of the figures computed"
            )
        if self.primary not in ("roc_auc", "pr_auc", "ks"):
            raise ValueError(
                f"{self.primary!r} cannot be the primary metric: the ceiling is a ranking "
                "bound, and comparing a Brier score against it would be meaningless"
            )
        return self


class Benchmark(_Base):
    """How to compute the best score any model could achieve on this data.

    Every other section describes what to generate. This one describes how the
    data was generated *well enough to invert* — which is the difference between
    a synthetic dataset and a measuring instrument.

    The argument in one line: a model scoring 0.84 on real data tells you
    nothing, because you never learn whether 0.86 or 0.99 was available. Here
    both are computable, so 0.84 becomes "captured 97% of the obtainable
    signal" — a statement about the model rather than about the dataset.

    Two numbers come out, and they answer different questions:

    ``oracle``
        What a model that could *see* the hidden driver would score. Nothing can
        beat it. Useful as a sanity bound and as a measure of how much the
        observables leave on the table.
    ``ceiling``
        The best score obtainable from the observables alone — the honest
        target, got by inverting the emission model below with Bayes' rule.

    Requires the emission model to be *stated*: each observable is its latent
    group's centre plus independent noise of known width. That is a real
    restriction and it is what buys the exact answer. A generator whose
    structure cannot be written down cannot be inverted, and a ceiling that was
    estimated rather than derived would be one more number to argue about.
    """

    latent: str = Field(
        description="The hidden column driving the outcome. Must be role `helper`, so it "
        "never reaches the output a model is scored on."
    )
    observables: list[BenchmarkObservable] = Field(
        default_factory=list,
        description="The noisy readings a model is allowed to use. Their measurement errors "
        "must be independent of one another — that independence is what makes combining "
        "them worth more than reading the best one alone.",
    )
    label_states: list[str] = Field(
        default_factory=list,
        description="Lifecycle states that count as the bad outcome. An entity reaching any "
        "of them at any cut-off is labelled 1.",
    )
    evaluation: Evaluation = Field(
        default_factory=Evaluation,
        description="Which measurements decide whether a model passed.",
    )
    expected_behaviour: ExpectedBehaviour = Field(
        default_factory=ExpectedBehaviour,
        description="What a model that learned correctly should look like, declared before "
        "anyone runs one.",
    )

    @model_validator(mode="after")
    def _check(self) -> Benchmark:
        if not self.observables:
            raise ValueError(
                f"benchmark on {self.latent!r} declares no observables, so there is nothing "
                "for a model to learn from and no ceiling to compute"
            )
        if not self.label_states:
            raise ValueError(
                f"benchmark on {self.latent!r} names no `label_states`, so there is no "
                "outcome to score against"
            )
        seen = [o.column for o in self.observables]
        if len(set(seen)) != len(seen):
            raise ValueError("benchmark lists the same observable twice")
        return self


class Validation(_Base):
    checks: InvariantToggles = Field(default_factory=InvariantToggles)
    custom: list[CustomInvariant] = Field(default_factory=list)
    non_negative_columns: list[str] = Field(default_factory=list)
    plausibility: list[PlausibilityBand] = Field(
        default_factory=list,
        description="Ranges the generated portfolio should land inside to be recognisable as "
        "the asset class it claims to be. Reported alongside the invariants.",
    )


# ---------------------------------------------------------------------------
# the spec
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# secondary chains — a second thing migrating alongside the first
# ---------------------------------------------------------------------------


class ChainCoupling(_Base):
    """How a secondary chain and the primary lifecycle affect each other.

    Left uncoupled the two are independent, and independence produces nonsense:
    a facility rated BB sitting in Defaulted, or a D-rated one performing
    happily. Both directions matter and they are not equally certain.

    ``forced_by`` is the direction with no ambiguity. A defaulted facility *is*
    rated D — that is what the rating means — so the primary state overwrites
    the secondary outright.

    ``stress`` is the direction that carries the modelling judgement. A worse
    rating should make distress more likely, and by how much is a choice rather
    than a fact. It scales the primary's *worsening* transitions for the
    entities in that secondary state, using the same reallocation a stress
    scenario applies globally — so a rating-driven stress and a scenario cannot
    drift into meaning different things.
    """

    forced_by: dict[str, str] = Field(
        default_factory=dict,
        description="Primary state -> the secondary state it forces, e.g. {Defaulted: D}.",
    )
    stress: dict[str, float] = Field(
        default_factory=dict,
        description="Secondary state -> multiplier on the primary's worsening transitions for "
        "entities in it. 1.0 is no effect; 3.0 triples the chance of falling further behind.",
    )

    @model_validator(mode="after")
    def _check(self) -> ChainCoupling:
        for state, multiplier in self.stress.items():
            if multiplier < 0:
                raise ValueError(f"stress multiplier for {state!r} is negative")
        return self


class SecondaryChain(_Base):
    """A second state machine running alongside the lifecycle.

    The lifecycle owns the entity's life: it decides who leaves the pool and
    when. A secondary chain owns one column and nothing else — it migrates, it
    is written out each period, and it cannot terminate anything.

    Credit ratings are the case this exists for. A rating moves on its own, and
    normally moves *before* distress is visible: a company is downgraded from B
    to B- while paying every instalment on time, and that downgrade is the early
    warning the rating exists to give. Derived from the credit state instead, as
    this pack did until now, the rating can only ever agree with what is already
    obvious, and the CCC share becomes the distressed share under another name.
    """

    name: str = Field(description="What is migrating, e.g. 'rating'.")
    lifecycle: Lifecycle = Field(
        description="The chain itself: its column, its states, its matrix. Reuses the lifecycle "
        "model, so a chain is validated exactly as the primary one is."
    )
    coupling: ChainCoupling = Field(default_factory=ChainCoupling)

    @model_validator(mode="after")
    def _check(self) -> SecondaryChain:
        if self.lifecycle.terminal:
            raise ValueError(
                f"secondary chain {self.name!r} declares terminal states "
                f"{sorted(self.lifecycle.terminal)}. Only the lifecycle ends an entity's life; "
                "a chain that could would silently remove entities the lifecycle still holds"
            )
        known = set(self.lifecycle.states)
        unknown = sorted(set(self.coupling.forced_by.values()) - known)
        if unknown:
            raise ValueError(f"coupling forces {self.name!r} into unknown states: {unknown}")
        unknown = sorted(set(self.coupling.stress) - known)
        if unknown:
            raise ValueError(f"coupling stresses unknown {self.name!r} states: {unknown}")
        return self


# ---------------------------------------------------------------------------
# metrics — the portfolio, summarised each cut-off
# ---------------------------------------------------------------------------


MetricKind = Literal[
    "sum",  # add a column up
    "count",  # how many rows
    "distinct_count",  # how many different values
    "weighted_mean",  # average, weighted by another column
    "share_where",  # what fraction of a total meets a condition
    "max_group_share",  # the largest single group's share of a total
    "cumulative",  # a running total across cut-offs
    "effective_count",  # how many groups the book behaves as, given concentration
    "turnover",  # how much of the book left since the last cut-off
]


class Metric(_Base):
    """One figure, computed for every cut-off.

    The panel says what each loan did. A portfolio report says what the *book*
    did — its size, its average coupon, how much of it is in trouble, how
    concentrated it is. Those are the numbers an investor reads, and until now
    the engine reported only a count of entities per state.

    Deliberately seven kinds rather than an expression language. Every figure the
    CLO specification asks for is one of these, and a metric that has to be
    readable by whoever inherits the spec is better as a named shape than as
    arbitrary code.
    """

    name: str = Field(description="Column name in the metrics table.")
    kind: MetricKind
    column: str | None = Field(
        default=None, description="The column being measured. Not needed by `count`."
    )
    weight: str | None = Field(default=None, description="Weighting column, for `weighted_mean`.")
    group: str | None = Field(
        default=None,
        description="Grouping column, for `max_group_share` and `effective_count`.",
    )
    entity_column: str | None = Field(
        default=None,
        description="Identifier column, for `turnover`. Defaults to the entity's own id.",
    )
    where: str | None = Field(
        default=None,
        description="Expression restricting which rows count, e.g. \"ccc_flag == 'Y'\". For "
        "`share_where` it selects the numerator; elsewhere it filters the whole figure.",
    )
    decimals: int | None = Field(default=None, ge=0, description="Round the result.")
    description: str | None = None

    @model_validator(mode="after")
    def _check(self) -> Metric:
        if self.kind != "count" and not self.column:
            raise ValueError(f"metric {self.name!r} of kind {self.kind!r} needs a `column`")
        if self.kind == "weighted_mean" and not self.weight:
            raise ValueError(
                f"metric {self.name!r} is a weighted mean with no `weight`. An unweighted "
                "average of a portfolio treats a EUR 40m facility and a EUR 400k one alike"
            )
        if self.kind == "max_group_share" and not self.group:
            raise ValueError(f"metric {self.name!r} is a group share with no `group`")
        if self.kind == "effective_count" and not self.group:
            raise ValueError(
                f"metric {self.name!r} counts effective groups but names no `group` to count. "
                "Without one there is nothing to be concentrated in"
            )
        if self.kind == "share_where" and not self.where:
            raise ValueError(f"metric {self.name!r} is a share with no `where` to share on")
        return self


# ---------------------------------------------------------------------------
# results — what the pack wants drawn
# ---------------------------------------------------------------------------


ChartKind = Literal[
    "series",  # one metric over the cut-offs
    "stacked_series",  # shares of a categorical column over the cut-offs
    "category_bar",  # a column totalled by category, at the last cut-off
    "histogram",  # the spread of a numeric column
]


class ChartSpec(_Base):
    """One picture on the results screen.

    The four charts drawn until now were fixed in the browser and named for a
    mortgage: a delinquency curve and a loan-to-value distribution. A CLO run
    drew both, and neither means anything for a corporate loan — there is no LTV,
    and the ladder is watchlist and distress rather than days past due.

    A pack declares what it wants drawn. Nothing here knows what asset class it
    is looking at, which is the point: the alternative was a branch on the pack's
    name in the interface.
    """

    kind: ChartKind
    title: str
    metric: str | None = Field(
        default=None,
        description="A metric to plot over the cut-offs. Cheaper and more consistent than "
        "re-aggregating the panel: the number drawn is then the same number the report "
        "carries, rather than a second calculation of it.",
    )
    column: str | None = Field(default=None, description="Column being charted.")
    group: str | None = Field(default=None, description="Column to group by, for a bar chart.")
    states: list[str] | None = Field(
        default=None, description="Which values to stack, in order. Defaults to all of them."
    )
    unit: Literal["money", "percent", "count", "number"] = "number"
    description: str | None = Field(
        default=None, description="One line under the title, saying what is plotted."
    )
    explain: str | None = Field(
        default=None,
        description="A longer note behind an information icon, for a reader who does not "
        "already know the vocabulary. A chart labelled 'CCC share' means nothing to someone "
        "who has never met a credit rating, and the one-line description has no room to say so.",
    )

    @model_validator(mode="after")
    def _check(self) -> ChartSpec:
        if self.kind == "series" and not (self.metric or self.column):
            raise ValueError(f"chart {self.title!r} plots nothing: give a `metric` or a `column`")
        if self.kind == "stacked_series" and not self.column:
            raise ValueError(f"chart {self.title!r} stacks nothing: give a `column`")
        if self.kind == "category_bar" and not (self.group and self.column):
            raise ValueError(f"chart {self.title!r} needs both `group` and `column`")
        if self.kind == "histogram" and not self.column:
            raise ValueError(f"chart {self.title!r} has no `column` to spread")
        return self


class Results(_Base):
    """The results screen, as this pack wants it."""

    charts: list[ChartSpec] = Field(default_factory=list)


class DesignSpec(_Base):
    spec_version: int = SPEC_VERSION
    meta: Meta
    entity: Entity
    params: dict[str, Any] = Field(
        default_factory=dict,
        description="Free-form values usable in id_format and expressions, e.g. deal_year.",
    )
    constants: dict[str, Any] = Field(default_factory=dict)
    columns: list[Column] = Field(default_factory=list)
    derivations: list[Derivation] = Field(default_factory=list)
    buckets: dict[str, Bucket] = Field(default_factory=dict)
    lifecycle: Lifecycle | None = None
    secondary_chains: list[SecondaryChain] = Field(
        default_factory=list,
        description="State machines running alongside the lifecycle, each owning one column — a credit rating that migrates on its own, for instance.",
    )
    dynamics: Dynamics = Field(default_factory=Dynamics)
    generation: Generation = Field(default_factory=Generation)
    groups: list[Group] = Field(
        default_factory=list,
        description="Parent records several entities share, e.g. an obligor behind several facilities.",
    )
    originations: Originations | None = None
    scenarios: dict[str, Scenario] = Field(default_factory=dict)
    metrics: list[Metric] = Field(
        default_factory=list,
        description="Portfolio-level figures computed at every cut-off.",
    )
    results: Results = Field(
        default_factory=Results,
        description="Charts the results screen should draw for this pack.",
    )
    emit: Emit = Field(default_factory=Emit)
    validation: Validation = Field(default_factory=Validation)
    benchmark: Benchmark | None = Field(
        default=None,
        description="How to compute the best achievable score on this data. Present only on "
        "packs built as measuring instruments; see `sdd.benchmark`.",
    )

    # -- convenience lookups ------------------------------------------------

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def column(self, name: str) -> Column | None:
        for c in self.columns:
            if c.name == name:
                return c
        return None

    @property
    def helper_columns(self) -> list[str]:
        return [c.name for c in self.columns if c.role == "helper"]

    @property
    def static_columns(self) -> list[str]:
        return [c.name for c in self.columns if c.role in ("static", "constant")]

    def output_columns(self) -> list[str]:
        """The columns written to disk, in order.

        Group attributes are joined onto every member, so they are output columns
        like any other — a spec should not have to declare them twice.
        """
        if self.emit.column_order:
            return list(self.emit.column_order)
        own = [c.name for c in self.columns if c.role != "helper"]
        # The key as well as the attributes: without the identifier a reader
        # cannot tell which rows belong to the same parent, which is the whole
        # point of having one.
        grouped: list[str] = []
        for group in self.groups:
            grouped.append(group.key)
            grouped += [c.name for c in group.columns if c.role != "helper"]
        return own + [name for name in grouped if name not in own]

    @property
    def group_column_names(self) -> list[str]:
        return [c.name for g in self.groups for c in g.columns]

    @model_validator(mode="after")
    def _check(self) -> DesignSpec:
        if self.spec_version != SPEC_VERSION:
            raise ValueError(
                f"spec_version {self.spec_version} is not supported by this build "
                f"(expected {SPEC_VERSION})"
            )
        names = self.column_names
        dupes = sorted({n for n in names if names.count(n) > 1})
        if dupes:
            raise ValueError(f"duplicate column names: {dupes}")

        known = set(names) | set(self.constants) | {d.target for d in self.derivations}

        target = self.generation.correlation_target
        if target:
            unknown = [c for c in target.columns if c not in known]
            if unknown:
                raise ValueError(
                    f"generation.correlation_target names columns this spec does not "
                    f"declare: {unknown}"
                )

        if self.originations:
            if self.lifecycle is None:
                raise ValueError(
                    "originations needs a lifecycle: new entities join the pool while it ages, "
                    "and without states there is no ageing for them to join"
                )
            for field_name, mapping in (
                ("reset", self.originations.reset),
                ("reset_expr", self.originations.reset_expr),
            ):
                unknown = [c for c in mapping if c not in known]
                if unknown:
                    raise ValueError(f"originations.{field_name} names unknown columns: {unknown}")

        recovery = self.dynamics.recovery
        if recovery:
            if recovery.balance not in known:
                raise ValueError(
                    f"dynamics.recovery reads balance from {recovery.balance!r}, "
                    "which this spec does not declare"
                )
            if self.lifecycle is None:
                raise ValueError(
                    "dynamics.recovery needs a lifecycle: recovery is booked when an entity "
                    "reaches a write-off state, and without states there are none"
                )
            unknown = [s for s in recovery.on_states if s not in set(self.lifecycle.states)]
            if unknown:
                raise ValueError(f"dynamics.recovery names unknown states: {unknown}")
        return self
