"""Read a *structure* definition — the schema half of the two inputs.

Sample data says how a portfolio behaves; a structure says which fields must
exist and what shape they take. Regulatory templates are the usual source, and
they carry real type information worth using.

Three formats are understood:

**ESMA/ECB taxonomy JSON**
    As produced by the deeploans ETL pipelines: a list of fields with a code, a
    name, a description, and a ``format_hint`` such as ``{MONETARY}`` or
    ``{ALPHANUM-28}``. The hints are a closed vocabulary and map cleanly onto
    types — that is what makes this the richest input.

**A bare CSV header**
    Just column names. Enough to fix the output schema; types come from the
    sample.

**A data dictionary CSV**
    A table with name/type/description columns, in whatever casing.

The result is a list of :class:`FieldSpec` — a declared schema with no
distributions attached. :mod:`sdd.profile.build` merges it with a profile to
produce a runnable spec.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from sdd.spec.schema import DType

# The ESMA format-hint vocabulary, mapped to a type and a sensible default
# domain. Taken from the ESMA disclosure technical standards; the same hints
# appear across Annexes 2 to 5, so one table covers every asset class.
FORMAT_HINTS: dict[str, dict[str, Any]] = {
    "MONETARY": {"dtype": "float", "note": "monetary amount"},
    "PERCENTAGE": {"dtype": "float", "min": 0.0, "max": 100.0, "note": "percentage"},
    "DATEFORMAT": {"dtype": "date", "note": "ISO 8601 date"},
    "YEAR": {"dtype": "int", "note": "four-digit year"},
    "INTEGER": {"dtype": "int"},
    "ALPHANUM": {"dtype": "str"},
    "LEI": {"dtype": "str", "note": "20-character Legal Entity Identifier"},
    "NUTS": {"dtype": "category", "note": "NUTS statistical region code"},
    "COUNTRYCODE": {"dtype": "category", "note": "ISO 3166 country code"},
    "CURRENCYCODE": {"dtype": "category", "note": "ISO 4217 currency code"},
    "Y/N": {"dtype": "category", "domain": ["Y", "N"]},
    "LIST": {"dtype": "category", "note": "closed list; values come from the sample"},
    "WATCHLIST": {"dtype": "category", "note": "closed list; values come from the sample"},
}

_HINT_RE = re.compile(r"\{([A-Z/]+)(?:[-_](\d+))?\}")


@dataclass
class FieldSpec:
    """One declared field, before any distribution is attached."""

    name: str
    dtype: DType | None = None
    code: str | None = None
    label: str | None = None
    description: str | None = None
    section: str | None = None
    domain: list[Any] | None = None
    minimum: float | None = None
    maximum: float | None = None
    max_length: int | None = None
    note: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {k: v for k, v in self.__dict__.items() if v is not None}


@dataclass
class Template:
    name: str
    fields: list[FieldSpec] = field(default_factory=list)
    asset_class: str | None = None
    source: str | None = None

    @property
    def column_names(self) -> list[str]:
        return [f.name for f in self.fields]

    def field(self, name: str) -> FieldSpec | None:
        return next((f for f in self.fields if f.name == name), None)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "asset_class": self.asset_class,
            "source": self.source,
            "field_count": len(self.fields),
            "fields": [f.to_dict() for f in self.fields],
        }

    def summary(self) -> str:
        typed = sum(1 for f in self.fields if f.dtype)
        lines = [f"{self.name}: {len(self.fields)} field(s), {typed} with a declared type"]
        if self.asset_class:
            lines.append(f"  asset class  {self.asset_class}")
        by_type: dict[str, int] = {}
        for f in self.fields:
            by_type[f.dtype or "untyped"] = by_type.get(f.dtype or "untyped", 0) + 1
        lines.append("  types        " + ", ".join(f"{v} {k}" for k, v in sorted(by_type.items())))
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# format hints
# ---------------------------------------------------------------------------


def parse_format_hint(hint: str | None) -> dict[str, Any]:
    """Turn an ESMA format hint into type information.

    ``{ALPHANUM-28}`` -> a string of at most 28 characters.
    ``{PERCENTAGE}``  -> a float bounded to 0-100.
    ``{Y/N}``         -> a category with exactly two allowed values.
    """
    if not hint:
        return {}
    match = _HINT_RE.search(hint.strip())
    if not match:
        return {}
    key, size = match.group(1), match.group(2)
    spec = dict(FORMAT_HINTS.get(key, {}))
    if size and key == "ALPHANUM":
        spec["max_length"] = int(size)
    elif size and key == "INTEGER":
        spec["maximum"] = float(size)
    return spec


# ---------------------------------------------------------------------------
# loaders
# ---------------------------------------------------------------------------


TEMPLATE_SUFFIXES = (
    ".json",
    ".csv",
    ".tsv",
    ".txt",
    ".parquet",
    ".pq",
    ".xlsx",
    ".xlsm",
    ".xls",
)


def load_template(path: str | Path, *, name_field: str = "field_name") -> Template:
    """Load a structure definition, detecting the format from the file."""
    path = Path(path)
    suffix = path.suffix.lower()
    if suffix == ".json":
        return load_json_template(path, name_field=name_field)
    if suffix in (".csv", ".tsv", ".txt"):
        return load_csv_template(path)
    if suffix in (".parquet", ".pq"):
        return load_parquet_template(path)
    if suffix in (".xlsx", ".xlsm", ".xls"):
        return load_frame_template(pd.read_excel(path, nrows=200), name=path.stem, source=str(path))
    raise ValueError(
        f"cannot read a schema definition from {path.suffix!r}; expected "
        ".csv, .parquet, .xlsx or .json"
    )


def load_json_template(path: str | Path, *, name_field: str = "field_name") -> Template:
    """Read a JSON schema, whether it is a taxonomy or a handful of sample rows.

    A taxonomy describes fields (``field_name``, ``format_hint``); a data extract
    *is* rows. Both arrive as JSON from the same upload box, so the shape decides
    which reader runs rather than the user having to say.
    """
    path = Path(path)
    raw = json.loads(path.read_text(encoding="utf-8"))
    entries = raw.get("fields", raw) if isinstance(raw, dict) else raw

    if isinstance(entries, list) and entries and isinstance(entries[0], dict):
        keys = set(entries[0])
        if not ({"field_name", "field_code", "format_hint", "name"} & keys):
            # Row objects, not field definitions: the schema is their keys.
            return load_frame_template(pd.json_normalize(entries), name=path.stem, source=str(path))
    return load_taxonomy_json(path, name_field=name_field)


def load_parquet_template(path: str | Path) -> Template:
    """Read a parquet schema — column names with their declared physical types."""
    import pyarrow.parquet as pq

    path = Path(path)
    schema = pq.read_schema(path)
    return Template(
        name=path.stem,
        fields=[
            FieldSpec(name=str(name), dtype=_dtype_from_arrow(str(field_type)))
            for name, field_type in zip(schema.names, schema.types, strict=False)
        ],
        source=str(path),
    )


def load_frame_template(frame: pd.DataFrame, *, name: str, source: str | None = None) -> Template:
    """Read a schema from an in-memory table: a data dictionary, or a header."""
    dictionary = _as_data_dictionary(frame, name=name, source=source)
    if dictionary is not None:
        return dictionary
    return Template(
        name=name,
        fields=[
            FieldSpec(name=str(c).strip(), dtype=_dtype_from_pandas(frame[c]))
            for c in frame.columns
        ],
        source=source,
    )


def load_taxonomy_json(path: str | Path, *, name_field: str = "field_name") -> Template:
    """Read an ESMA/ECB taxonomy JSON.

    ``name_field`` picks which attribute becomes the column name. The deeploans
    taxonomies carry both a code (``CREL1``) and a human name (``Unique
    Identifier``); sample tapes are usually keyed by code, so that is offered as
    an alternative rather than assumed.
    """
    path = Path(path)
    raw = json.loads(path.read_text(encoding="utf-8"))
    entries = raw.get("fields", raw) if isinstance(raw, dict) else raw
    if not isinstance(entries, list):
        raise ValueError(f"{path} does not contain a list of fields")

    fields: list[FieldSpec] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        name = entry.get(name_field) or entry.get("field_code") or entry.get("name")
        if not name:
            continue
        hint = parse_format_hint(entry.get("format_hint"))
        fields.append(
            FieldSpec(
                name=str(name),
                dtype=hint.get("dtype"),
                code=entry.get("field_code"),
                label=entry.get("field_name"),
                description=_shorten(entry.get("content_to_report") or entry.get("description")),
                section=entry.get("section"),
                domain=hint.get("domain"),
                minimum=hint.get("min"),
                maximum=hint.get("max") or hint.get("maximum"),
                max_length=hint.get("max_length"),
                note=hint.get("note"),
            )
        )

    return Template(
        name=str(raw.get("template", path.stem)) if isinstance(raw, dict) else path.stem,
        fields=fields,
        asset_class=str(raw.get("asset_class")) if isinstance(raw, dict) else None,
        source=str(path),
    )


def load_csv_template(path: str | Path) -> Template:
    """Read either a bare header row or a data dictionary."""
    path = Path(path)
    sep = "\t" if path.suffix.lower() == ".tsv" else ","
    head = pd.read_csv(path, nrows=200, sep=sep)
    dictionary = _as_data_dictionary(head, name=path.stem, source=str(path))
    if dictionary is not None:
        return dictionary
    return Template(
        name=path.stem,
        fields=[
            FieldSpec(name=str(c).strip(), dtype=_dtype_from_pandas(head[c])) for c in head.columns
        ],
        source=str(path),
    )


def _as_data_dictionary(frame: pd.DataFrame, *, name: str, source: str | None) -> Template | None:
    """Read a table *describing* columns, or return None if it holds data instead.

    A data dictionary has a name column and something saying what each field is;
    a tape has neither. Getting this wrong in either direction is expensive — a
    dictionary read as data produces a two-column schema, and a tape read as a
    dictionary produces one field per row — so both markers are required.
    """
    lowered = {str(c).strip().lower(): c for c in frame.columns}
    name_key = next(
        (
            lowered[k]
            for k in ("name", "field", "field_name", "column", "column_name")
            if k in lowered
        ),
        None,
    )
    type_key = next(
        (
            lowered[k]
            for k in ("type", "dtype", "data_type", "format", "format_hint")
            if k in lowered
        ),
        None,
    )
    desc_key = next(
        (lowered[k] for k in ("description", "content_to_report", "comment") if k in lowered),
        None,
    )
    if not name_key or not (type_key or desc_key):
        return None

    fields = [
        FieldSpec(
            name=str(row[name_key]).strip(),
            dtype=(
                _normalise_dtype(str(row[type_key]))
                if type_key and pd.notna(row.get(type_key))
                else None
            ),
            description=(
                _shorten(str(row[desc_key])) if desc_key and pd.notna(row.get(desc_key)) else None
            ),
        )
        for _, row in frame.iterrows()
        if pd.notna(row.get(name_key))
    ]
    return Template(name=name, fields=fields, source=source) if fields else None


def _dtype_from_pandas(series: pd.Series) -> DType | None:
    if pd.api.types.is_bool_dtype(series):
        return "bool"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "date"
    if pd.api.types.is_integer_dtype(series):
        return "int"
    if pd.api.types.is_float_dtype(series):
        return "float"
    return None


def _dtype_from_arrow(arrow_type: str) -> DType | None:
    text = arrow_type.lower()
    if text.startswith("bool"):
        return "bool"
    if text.startswith(("timestamp", "date")):
        return "date"
    if text.startswith(("int", "uint")):
        return "int"
    if text.startswith(("float", "double", "decimal")):
        return "float"
    if text.startswith(("string", "large_string", "utf8")):
        return "str"
    return None


def template_from_columns(names: list[str], *, name: str = "columns") -> Template:
    """Build a template from a list of column names."""
    return Template(name=name, fields=[FieldSpec(name=str(n)) for n in names])


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

_DTYPE_WORDS: dict[str, DType] = {
    "int": "int",
    "integer": "int",
    "bigint": "int",
    "long": "int",
    "float": "float",
    "double": "float",
    "decimal": "float",
    "numeric": "float",
    "number": "float",
    "money": "float",
    "monetary": "float",
    "percentage": "float",
    "percent": "float",
    "str": "str",
    "string": "str",
    "text": "str",
    "varchar": "str",
    "char": "str",
    "bool": "bool",
    "boolean": "bool",
    "date": "date",
    "datetime": "date",
    "timestamp": "date",
    "category": "category",
    "categorical": "category",
    "enum": "category",
    "list": "category",
}


def _normalise_dtype(value: str) -> DType | None:
    cleaned = value.strip().lower()
    if cleaned in _DTYPE_WORDS:
        return _DTYPE_WORDS[cleaned]
    hint = parse_format_hint(value)
    if hint.get("dtype"):
        return hint["dtype"]
    for word, dtype in _DTYPE_WORDS.items():
        if word in cleaned:
            return dtype
    return None


def _shorten(text: str | None, limit: int = 200) -> str | None:
    """Regulatory field descriptions run to paragraphs; keep the first sentence."""
    if not text:
        return None
    flat = " ".join(str(text).split())
    if len(flat) <= limit:
        return flat
    cut = flat[:limit].rsplit(". ", 1)[0]
    return (cut if len(cut) > 40 else flat[:limit]).rstrip(" .") + "…"
