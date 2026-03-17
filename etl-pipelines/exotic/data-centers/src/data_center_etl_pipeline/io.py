import csv
import os
from typing import Dict, Iterable, List


def read_csv(path: str) -> List[Dict[str, str]]:
    with open(path, "r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: str, rows: Iterable[Dict[str, object]]) -> None:
    output_rows = list(rows)
    if not output_rows:
        raise ValueError(f"Refusing to write empty dataset to {path}")

    fieldnames = list(output_rows[0].keys())
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(output_rows)
