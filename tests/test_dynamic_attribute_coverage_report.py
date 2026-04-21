from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

from scripts.dynamic_attribute_coverage_report import build_summary, load_bundle_station_ids, render_markdown


def _write_bundle_csv(path: Path) -> None:
    rows = [
        {"station_id": "station-a"},
        {"station_id": "station-b"},
        {"station_id": "station-c"},
        {"station_id": "station-d"},
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["station_id"])
        writer.writeheader()
        writer.writerows(rows)


def _write_dynamic_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            """
            CREATE TABLE evse_current_state (
                provider_uid TEXT NOT NULL,
                station_id TEXT NOT NULL,
                availability_status TEXT NOT NULL,
                operational_status TEXT NOT NULL,
                price_display TEXT NOT NULL,
                price_currency TEXT NOT NULL,
                price_energy_eur_kwh_min TEXT NOT NULL,
                price_energy_eur_kwh_max TEXT NOT NULL,
                price_time_eur_min_min REAL,
                price_time_eur_min_max REAL,
                price_complex INTEGER NOT NULL,
                next_available_charging_slots TEXT NOT NULL,
                supplemental_facility_status TEXT NOT NULL,
                source_observed_at TEXT NOT NULL,
                fetched_at TEXT NOT NULL,
                ingested_at TEXT NOT NULL
            )
            """
        )
        conn.executemany(
            """
            INSERT INTO evse_current_state (
                provider_uid, station_id, availability_status, operational_status,
                price_display, price_currency, price_energy_eur_kwh_min, price_energy_eur_kwh_max,
                price_time_eur_min_min, price_time_eur_min_max, price_complex,
                next_available_charging_slots, supplemental_facility_status,
                source_observed_at, fetched_at, ingested_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "alpha",
                    "station-a",
                    "free",
                    "AVAILABLE",
                    "0,49 €/kWh",
                    "EUR",
                    "0.49",
                    "0.49",
                    None,
                    None,
                    0,
                    "[{\"at\":\"2026-04-21T18:00:00+00:00\"}]",
                    "[]",
                    "2026-04-21T18:00:00+00:00",
                    "2026-04-21T18:01:00+00:00",
                    "2026-04-21T18:01:05+00:00",
                ),
                (
                    "beta",
                    "station-a",
                    "occupied",
                    "CHARGING",
                    "",
                    "",
                    "",
                    "",
                    0.1,
                    0.2,
                    1,
                    "[]",
                    "[\"staffed\"]",
                    "2026-04-21T18:02:00+00:00",
                    "2026-04-21T18:03:00+00:00",
                    "2026-04-21T18:03:05+00:00",
                ),
                (
                    "alpha",
                    "station-b",
                    "unknown",
                    "",
                    "",
                    "",
                    "",
                    "",
                    None,
                    None,
                    0,
                    "[]",
                    "[]",
                    "",
                    "2026-04-21T18:04:00+00:00",
                    "2026-04-21T18:04:05+00:00",
                ),
                (
                    "gamma",
                    "outside",
                    "free",
                    "AVAILABLE",
                    "",
                    "",
                    "",
                    "",
                    None,
                    None,
                    0,
                    "[]",
                    "[]",
                    "2026-04-21T18:05:00+00:00",
                    "2026-04-21T18:05:00+00:00",
                    "2026-04-21T18:05:05+00:00",
                ),
            ],
        )
        conn.commit()
    finally:
        conn.close()


def test_build_summary_counts_bundle_attribute_coverage_and_overlap(tmp_path: Path):
    bundle_path = tmp_path / "chargers_fast.csv"
    db_path = tmp_path / "live_state.sqlite3"
    _write_bundle_csv(bundle_path)
    _write_dynamic_db(db_path)

    summary = build_summary(
        db_path=db_path,
        bundle_station_ids=load_bundle_station_ids(bundle_path),
    )

    assert summary["bundle_station_count"] == 4
    assert summary["dynamic_station_count"] == 2
    assert summary["stations_with_multiple_providers_count"] == 1
    assert summary["max_provider_overlap_count"] == 2
    assert summary["provider_overlap_distribution"] == {1: 1, 2: 1}

    by_attribute = {row["attribute_key"]: row for row in summary["attribute_rows"]}
    assert by_attribute["availability_status"]["station_count"] == 2
    assert by_attribute["operational_status"]["station_count"] == 1
    assert by_attribute["any_price"]["station_count"] == 1
    assert by_attribute["price_display"]["station_count"] == 1
    assert by_attribute["price_energy"]["station_count"] == 1
    assert by_attribute["price_time"]["station_count"] == 1
    assert by_attribute["complex_price"]["station_count"] == 1
    assert by_attribute["next_available_charging_slots"]["station_count"] == 1
    assert by_attribute["supplemental_facility_status"]["station_count"] == 1
    assert by_attribute["source_observed_at"]["station_count"] == 1

    assert [row["provider_uid"] for row in summary["provider_rows"]] == ["alpha", "beta"]
    assert summary["provider_rows"][0]["station_count"] == 2
    assert summary["provider_rows"][0]["overlap_station_count"] == 1


def test_render_markdown_includes_overlap_and_attribute_sections(tmp_path: Path):
    bundle_path = tmp_path / "chargers_fast.csv"
    db_path = tmp_path / "live_state.sqlite3"
    _write_bundle_csv(bundle_path)
    _write_dynamic_db(db_path)

    summary = build_summary(
        db_path=db_path,
        bundle_station_ids=load_bundle_station_ids(bundle_path),
    )
    markdown = render_markdown(summary)

    assert "# Dynamic Attribute Coverage Report" in markdown
    assert "- Stations with any dynamic data: 2 (50.00%)" in markdown
    assert "- Stations with multiple providers: 1 (25.00%)" in markdown
    assert "| Availability | 2 | 50.00% |" in markdown
    assert "| `alpha` | 2 | 1 | 2 | 1 | 1 | 1 | 0 | 1 | 0 |" in markdown
