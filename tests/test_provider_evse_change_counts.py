from __future__ import annotations

import csv
from pathlib import Path

from analysis.provider_evse_change_counts import run_provider_evse_change_counts


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def test_run_provider_evse_change_counts_aggregates_and_sorts(tmp_path: Path):
    input_path = tmp_path / "evse_status_changes.csv"
    output_path = tmp_path / "provider_evse_change_counts.csv"
    _write_csv(
        input_path,
        ["provider_uid", "provider_evse_id", "station_id"],
        [
            {"provider_uid": "provider-a", "provider_evse_id": "EVSE-1", "station_id": "station-a"},
            {"provider_uid": "provider-a", "provider_evse_id": "EVSE-1", "station_id": "station-a"},
            {"provider_uid": "provider-a", "provider_evse_id": "EVSE-1", "station_id": "station-z"},
            {"provider_uid": "provider-a", "provider_evse_id": "EVSE-1", "station_id": "station-a"},
            {"provider_uid": "provider-b", "provider_evse_id": "EVSE-2", "station_id": "station-b"},
            {"provider_uid": "provider-b", "provider_evse_id": "EVSE-2", "station_id": "station-b"},
            {"provider_uid": "provider-c", "provider_evse_id": "", "station_id": "station-c"},
        ],
    )

    result = run_provider_evse_change_counts(
        evse_status_changes_path=input_path,
        output_path=output_path,
    )

    assert result["row_count"] == 2
    assert result["top_provider_evse_id"] == "EVSE-1"
    assert result["top_station_id"] == "station-a"
    assert result["top_count"] == 4

    rows = _read_csv(output_path)
    assert rows == [
        {"provider_evse_id": "EVSE-1", "station_id": "station-a", "count": "4"},
        {"provider_evse_id": "EVSE-2", "station_id": "station-b", "count": "2"},
    ]
