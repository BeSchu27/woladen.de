#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from analysis.output_io import write_csv_atomic

DEFAULT_ANALYSIS_OUTPUT_DIR = REPO_ROOT / "analysis" / "output"
DEFAULT_INPUT_PATH = DEFAULT_ANALYSIS_OUTPUT_DIR / "evse_status_changes.csv"
DEFAULT_OUTPUT_PATH = DEFAULT_ANALYSIS_OUTPUT_DIR / "provider_evse_change_counts.csv"

OUTPUT_FIELDS = ["provider_evse_id", "station_id", "count"]


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _dominant_station_id(counter: Counter[str]) -> str:
    non_empty_items = [(station_id, count) for station_id, count in counter.items() if station_id]
    if not non_empty_items:
        return ""
    non_empty_items.sort(key=lambda item: (-item[1], item[0]))
    return non_empty_items[0][0]


def run_provider_evse_change_counts(
    *,
    evse_status_changes_path: Path = DEFAULT_INPUT_PATH,
    output_path: Path = DEFAULT_OUTPUT_PATH,
) -> dict[str, Any]:
    rows = _read_csv_rows(evse_status_changes_path)
    counts: Counter[str] = Counter()
    station_counts: dict[str, Counter[str]] = defaultdict(Counter)

    for row in rows:
        provider_evse_id = str(row.get("provider_evse_id") or "").strip()
        if not provider_evse_id:
            continue
        station_id = str(row.get("station_id") or "").strip()
        counts[provider_evse_id] += 1
        if station_id:
            station_counts[provider_evse_id][station_id] += 1

    output_rows = [
        {
            "provider_evse_id": provider_evse_id,
            "station_id": _dominant_station_id(station_counts[provider_evse_id]),
            "count": count,
        }
        for provider_evse_id, count in counts.items()
    ]
    output_rows.sort(key=lambda row: (-int(row["count"]), str(row["provider_evse_id"])))
    write_csv_atomic(output_path, OUTPUT_FIELDS, output_rows)

    top_row = output_rows[0] if output_rows else {"provider_evse_id": "", "station_id": "", "count": 0}
    return {
        "input_path": str(evse_status_changes_path.resolve()),
        "output_path": str(output_path.resolve()),
        "row_count": len(output_rows),
        "top_provider_evse_id": top_row["provider_evse_id"],
        "top_station_id": top_row["station_id"],
        "top_count": int(top_row["count"]),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Aggregate evse_status_changes.csv by provider_evse_id and emit provider_evse_id, station_id, count",
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT_PATH,
        help="Path to evse_status_changes.csv",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help="Path for the generated aggregated CSV",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = run_provider_evse_change_counts(
        evse_status_changes_path=args.input,
        output_path=args.output,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
