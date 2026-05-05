import json
import subprocess
from pathlib import Path


def test_e2e_sample_generates_artifacts_and_valid_report(tmp_path: Path):
    base_dir = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "output"

    cmd = [
        "python",
        str(base_dir / "src" / "run_e2e_sample.py"),
        "--input",
        str(base_dir / "sample_data" / "raw_facilities.csv"),
        "--output",
        str(output_dir),
    ]
    result = subprocess.run(cmd, cwd=base_dir, capture_output=True, text=True, check=True)

    normalized_json = output_dir / "normalized" / "facility_normalized.json"
    report_json = output_dir / "reports" / "validation_report.json"

    assert normalized_json.exists(), result.stdout
    assert report_json.exists(), result.stdout

    rows = json.loads(normalized_json.read_text(encoding="utf-8"))
    report = json.loads(report_json.read_text(encoding="utf-8"))

    assert isinstance(rows, list)
    assert len(rows) > 0
    assert report["is_valid"] is True
    assert report["row_count"] == len(rows)
    assert report["missing_required"] == []
    assert report["invalid_status_rows"] == []
