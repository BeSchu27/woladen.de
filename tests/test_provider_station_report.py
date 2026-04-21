from __future__ import annotations

import csv
import json
import sqlite3
from pathlib import Path

from scripts.provider_station_report import (
    build_summary,
    load_bundle_stations,
    load_dynamic_provider_station_sets,
    load_provider_metadata,
    load_static_provider_station_sets,
    render_markdown,
)


def _write_bundle_csv(path: Path) -> None:
    rows = [
        {"station_id": "station-a", "operator": "Alpha Charge", "city": "Berlin", "address": "A-Str. 1"},
        {"station_id": "station-b", "operator": "Beta Charge", "city": "Hamburg", "address": "B-Str. 2"},
        {"station_id": "station-c", "operator": "Gamma Charge", "city": "Köln", "address": "C-Str. 3"},
        {"station_id": "station-d", "operator": "Delta Charge", "city": "München", "address": "D-Str. 4"},
        {"station_id": "station-e", "operator": "Epsilon Charge", "city": "Bonn", "address": "E-Str. 5"},
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _write_static_matches_csv(path: Path) -> None:
    rows = [
        {"provider_uid": "alpha", "station_id": "station-a", "station_in_bundle": "1"},
        {"provider_uid": "alpha", "station_id": "station-b", "station_in_bundle": "1"},
        {"provider_uid": "beta", "station_id": "station-b", "station_in_bundle": "1"},
        {"provider_uid": "beta", "station_id": "station-c", "station_in_bundle": "1"},
        {"provider_uid": "ignored", "station_id": "outside", "station_in_bundle": "0"},
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _write_provider_configs_json(path: Path) -> None:
    payload = {
        "providers": [
            {"uid": "alpha", "display_name": "Alpha", "publisher": "Alpha GmbH"},
            {"uid": "beta", "display_name": "Beta", "publisher": "Beta GmbH"},
            {"uid": "delta", "display_name": "Delta", "publisher": "Delta GmbH"},
        ]
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_dynamic_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            """
            CREATE TABLE evse_current_state (
                provider_uid TEXT NOT NULL,
                provider_evse_id TEXT NOT NULL,
                station_id TEXT NOT NULL,
                fetched_at TEXT NOT NULL
            )
            """
        )
        conn.executemany(
            "INSERT INTO evse_current_state (provider_uid, provider_evse_id, station_id, fetched_at) VALUES (?, ?, ?, ?)",
            [
                ("alpha", "alpha-1", "station-b", "2026-04-21T18:00:00+00:00"),
                ("alpha", "alpha-2", "station-b", "2026-04-21T18:00:00+00:00"),
                ("beta", "beta-1", "station-c", "2026-04-21T18:01:00+00:00"),
                ("delta", "delta-1", "station-d", "2026-04-21T18:02:00+00:00"),
                ("delta", "delta-2", "outside", "2026-04-21T18:03:00+00:00"),
            ],
        )
        conn.commit()
    finally:
        conn.close()


def test_build_summary_combines_static_and_dynamic_matches(tmp_path: Path):
    bundle_path = tmp_path / "chargers_fast.csv"
    static_path = tmp_path / "matches.csv"
    dynamic_db_path = tmp_path / "live_state.sqlite3"
    provider_configs_path = tmp_path / "providers.json"

    _write_bundle_csv(bundle_path)
    _write_static_matches_csv(static_path)
    _write_dynamic_db(dynamic_db_path)
    _write_provider_configs_json(provider_configs_path)

    bundle_stations = load_bundle_stations(bundle_path)
    provider_metadata = load_provider_metadata(provider_configs_path)
    static_by_provider = load_static_provider_station_sets(static_path, bundle_station_ids=set(bundle_stations))
    dynamic_by_provider, snapshot_at = load_dynamic_provider_station_sets(
        dynamic_db_path,
        bundle_station_ids=set(bundle_stations),
    )

    summary = build_summary(
        bundle_stations=bundle_stations,
        provider_metadata=provider_metadata,
        static_by_provider=static_by_provider,
        dynamic_by_provider=dynamic_by_provider,
        dynamic_snapshot_at=snapshot_at,
        bundle_path=bundle_path,
        static_matches_path=static_path,
        dynamic_db_path=dynamic_db_path,
    )

    assert summary["bundle_station_count"] == 5
    assert summary["stations_with_static_match_count"] == 3
    assert summary["stations_with_dynamic_match_count"] == 3
    assert summary["stations_with_known_match_count"] == 4
    assert summary["stations_without_known_match_count"] == 1
    assert summary["dynamic_snapshot_at"] == "2026-04-21T18:03:00+00:00"
    assert [row["provider_uid"] for row in summary["provider_rows"]] == ["alpha", "beta", "delta"]

    alpha_row = summary["provider_rows"][0]
    assert alpha_row["static_station_count"] == 2
    assert alpha_row["dynamic_station_count"] == 1
    assert alpha_row["known_station_count"] == 2
    assert alpha_row["static_only_station_count"] == 1
    assert alpha_row["dynamic_only_station_count"] == 0
    assert alpha_row["overlap_station_count"] == 1

    assert summary["unmatched_examples"] == [
        {
            "station_id": "station-e",
            "operator": "Epsilon Charge",
            "city": "Bonn",
            "address": "E-Str. 5",
        }
    ]


def test_render_markdown_includes_snapshot_and_unmatched_examples(tmp_path: Path):
    bundle_path = tmp_path / "chargers_fast.csv"
    static_path = tmp_path / "matches.csv"
    dynamic_db_path = tmp_path / "live_state.sqlite3"
    provider_configs_path = tmp_path / "providers.json"

    _write_bundle_csv(bundle_path)
    _write_static_matches_csv(static_path)
    _write_dynamic_db(dynamic_db_path)
    _write_provider_configs_json(provider_configs_path)

    summary = build_summary(
        bundle_stations=load_bundle_stations(bundle_path),
        provider_metadata=load_provider_metadata(provider_configs_path),
        static_by_provider=load_static_provider_station_sets(static_path, bundle_station_ids={"station-a", "station-b", "station-c", "station-d", "station-e"}),
        dynamic_by_provider=load_dynamic_provider_station_sets(dynamic_db_path, bundle_station_ids={"station-a", "station-b", "station-c", "station-d", "station-e"})[0],
        dynamic_snapshot_at="2026-04-21T18:03:00+00:00",
        bundle_path=bundle_path,
        static_matches_path=static_path,
        dynamic_db_path=dynamic_db_path,
    )

    markdown = render_markdown(summary)

    assert "# Woladen Provider Station Report" in markdown
    assert "- Woladen bundle stations: 5" in markdown
    assert "- Stations with any static match: 3 (60.00%)" in markdown
    assert "- Stations with any dynamic match: 3 (60.00%)" in markdown
    assert "- Stations with no known static or dynamic match: 1 (20.00%)" in markdown
    assert "| `alpha` | 2 | 1 | 2 | 1 | 0 | 1 |" in markdown
    assert "## Unmatched Examples" in markdown
    assert "- `station-e`: Epsilon Charge, Bonn, E-Str. 5" in markdown
