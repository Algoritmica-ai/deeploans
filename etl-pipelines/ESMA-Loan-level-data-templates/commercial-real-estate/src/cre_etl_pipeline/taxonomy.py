from __future__ import annotations

import json
import re
import xml.etree.ElementTree as ET
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

XLSX_NS = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
FIELD_CODE_PATTERN = re.compile(r"^CREL\d+$")


@dataclass
class TaxonomyField:
    section: str
    field_code: str
    field_name: str
    content_to_report: str
    format_hint: str


def _load_shared_strings(xlsx_path: Path) -> list[str]:
    with zipfile.ZipFile(xlsx_path) as archive:
        if "xl/sharedStrings.xml" not in archive.namelist():
            return []
        root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
        values: list[str] = []
        for si in root.findall("a:si", XLSX_NS):
            values.append("".join((t.text or "") for t in si.findall(".//a:t", XLSX_NS)))
        return values


def _cell_value(cell: ET.Element, shared_strings: list[str]) -> str:
    value_node = cell.find("a:v", XLSX_NS)
    if value_node is None or value_node.text is None:
        return ""
    value = value_node.text
    if cell.get("t") == "s":
        idx = int(value)
        if 0 <= idx < len(shared_strings):
            return shared_strings[idx]
    return value


def load_cre_taxonomy(xlsx_path: str | Path) -> list[TaxonomyField]:
    """Extract CRE taxonomy rows from the Annex 3 workbook using only stdlib."""
    xlsx_path = Path(xlsx_path)
    shared_strings = _load_shared_strings(xlsx_path)

    rows: list[list[str]] = []
    with zipfile.ZipFile(xlsx_path) as archive:
        sheet_xml = archive.read("xl/worksheets/sheet1.xml")

    root = ET.fromstring(sheet_xml)
    for row in root.findall(".//a:sheetData/a:row", XLSX_NS):
        cells = row.findall("a:c", XLSX_NS)
        values = [_cell_value(cell, shared_strings).strip() for cell in cells]
        if values:
            rows.append(values)

    taxonomy: list[TaxonomyField] = []
    for row in rows:
        if len(row) < 8:
            continue
        field_code = row[2]
        if not FIELD_CODE_PATTERN.match(field_code):
            continue
        taxonomy.append(
            TaxonomyField(
                section=row[1],
                field_code=field_code,
                field_name=row[3],
                content_to_report=row[4],
                format_hint=row[7],
            )
        )
    return taxonomy


def export_taxonomy_json(xlsx_path: str | Path, output_path: str | Path) -> dict[str, Any]:
    fields = load_cre_taxonomy(xlsx_path)
    payload = {
        "asset_class": "cre",
        "template": "ESMA Annex 3 commercial real estate",
        "field_count": len(fields),
        "fields": [field.__dict__ for field in fields],
    }
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return payload
