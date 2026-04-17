from __future__ import annotations

import csv
import json
from datetime import date
from pathlib import Path

from scripts.static_mapping_daily_report import build_summary, load_coverage_payload, load_match_rows, render_markdown


def _write_coverage_json(path: Path) -> None:
    payload = {
        "generated_at": "2026-04-16T13:34:29+00:00",
        "totals": {
            "stations": 8,
            "charging_points": 16,
            "bundle_stations": 5,
            "bundle_charging_points": 10,
        },
        "machine_certificate_probe": {
            "configured": False,
            "status": "missing_certificate",
        },
        "providers": [
            {
                "provider_uid": "alpha",
                "display_name": "Alpha",
                "publisher": "Alpha GmbH",
                "static": {
                    "fetch_status": "ok",
                    "access_mode": "noauth",
                    "matched_stations": 2,
                    "matched_charging_points": 4,
                    "station_coverage_ratio": 0.25,
                    "charging_point_coverage_ratio": 0.25,
                    "bundle_matched_stations": 2,
                    "bundle_matched_charging_points": 4,
                    "bundle_station_coverage_ratio": 0.4,
                    "bundle_charging_point_coverage_ratio": 0.4,
                },
            },
            {
                "provider_uid": "beta",
                "display_name": "Beta",
                "publisher": "Beta AG",
                "static": {
                    "fetch_status": "ok",
                    "access_mode": "auth",
                    "matched_stations": 2,
                    "matched_charging_points": 6,
                    "station_coverage_ratio": 0.25,
                    "charging_point_coverage_ratio": 0.375,
                    "bundle_matched_stations": 2,
                    "bundle_matched_charging_points": 6,
                    "bundle_station_coverage_ratio": 0.4,
                    "bundle_charging_point_coverage_ratio": 0.6,
                },
            },
            {
                "provider_uid": "gamma",
                "display_name": "Gamma",
                "publisher": "Gamma AG",
                "static": {
                    "fetch_status": "noauth: 500 Server Error: auth required",
                    "access_mode": "auth",
                    "matched_stations": 0,
                    "matched_charging_points": 0,
                    "station_coverage_ratio": 0.0,
                    "charging_point_coverage_ratio": 0.0,
                    "bundle_matched_stations": 0,
                    "bundle_matched_charging_points": 0,
                    "bundle_station_coverage_ratio": 0.0,
                    "bundle_charging_point_coverage_ratio": 0.0,
                },
            },
            {
                "provider_uid": "delta",
                "display_name": "Delta",
                "publisher": "Delta AG",
                "static": {
                    "fetch_status": "no_static_feed",
                    "access_mode": "",
                    "matched_stations": 0,
                    "matched_charging_points": 0,
                    "station_coverage_ratio": 0.0,
                    "charging_point_coverage_ratio": 0.0,
                    "bundle_matched_stations": 0,
                    "bundle_matched_charging_points": 0,
                    "bundle_station_coverage_ratio": 0.0,
                    "bundle_charging_point_coverage_ratio": 0.0,
                },
            },
            {
                "provider_uid": "epsilon",
                "display_name": "Epsilon",
                "publisher": "Epsilon AG",
                "static": {
                    "fetch_status": "ok",
                    "access_mode": "noauth",
                    "matched_stations": 0,
                    "matched_charging_points": 0,
                    "station_coverage_ratio": 0.0,
                    "charging_point_coverage_ratio": 0.0,
                    "bundle_matched_stations": 0,
                    "bundle_matched_charging_points": 0,
                    "bundle_station_coverage_ratio": 0.0,
                    "bundle_charging_point_coverage_ratio": 0.0,
                },
            },
        ],
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_matches_csv(path: Path) -> None:
    rows = [
        {
            "provider_uid": "alpha",
            "station_id": "station-a",
            "station_charging_points_count": "2",
            "station_in_bundle": "1",
        },
        {
            "provider_uid": "alpha",
            "station_id": "station-b",
            "station_charging_points_count": "2",
            "station_in_bundle": "1",
        },
        {
            "provider_uid": "beta",
            "station_id": "station-b",
            "station_charging_points_count": "2",
            "station_in_bundle": "1",
        },
        {
            "provider_uid": "beta",
            "station_id": "station-c",
            "station_charging_points_count": "4",
            "station_in_bundle": "1",
        },
        {
            "provider_uid": "alpha",
            "station_id": "station-z",
            "station_charging_points_count": "2",
            "station_in_bundle": "0",
        },
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def test_build_summary_counts_unique_bundle_coverage_from_match_rows(tmp_path: Path):
    coverage_path = tmp_path / "coverage.json"
    matches_path = tmp_path / "matches.csv"
    _write_coverage_json(coverage_path)
    _write_matches_csv(matches_path)

    summary = build_summary(
        coverage_payload=load_coverage_payload(coverage_path),
        match_rows=load_match_rows(matches_path),
        coverage_path=coverage_path,
        matches_path=matches_path,
        report_date=date(2026, 4, 17),
    )

    assert summary["providers_total"] == 5
    assert summary["providers_with_static_feed"] == 4
    assert summary["providers_successful_fetch"] == 3
    assert summary["providers_with_matches"] == 2
    assert summary["full_registry_station_count"] == 8
    assert summary["bundle_station_count"] == 5
    assert summary["union_matched_station_count"] == 4
    assert summary["union_matched_station_coverage_ratio"] == 0.5
    assert summary["union_matched_charging_point_count"] == 10
    assert summary["union_matched_charging_point_coverage_ratio"] == 0.625
    assert summary["bundle_union_matched_station_count"] == 3
    assert summary["bundle_union_matched_station_coverage_ratio"] == 0.6
    assert summary["bundle_union_matched_charging_point_count"] == 8
    assert summary["bundle_union_matched_charging_point_coverage_ratio"] == 0.8
    assert [row["provider_uid"] for row in summary["top_providers"]] == ["beta", "alpha"]
    assert [row["provider_uid"] for row in summary["providers_with_zero_matches_after_success"]] == ["epsilon"]
    assert [row["provider_uid"] for row in summary["providers_with_fetch_issues"]] == ["gamma"]


def test_render_markdown_includes_snapshot_and_issue_sections(tmp_path: Path):
    coverage_path = tmp_path / "coverage.json"
    matches_path = tmp_path / "matches.csv"
    _write_coverage_json(coverage_path)
    _write_matches_csv(matches_path)

    summary = build_summary(
        coverage_payload=load_coverage_payload(coverage_path),
        match_rows=load_match_rows(matches_path),
        coverage_path=coverage_path,
        matches_path=matches_path,
        report_date=date(2026, 4, 17),
    )
    markdown = render_markdown(summary)

    assert "# Static Mapping Coverage Report for 2026-04-17" in markdown
    assert "- Full-registry stations covered by any static provider: 4 / 8 (50.00%)" in markdown
    assert "- Bundle stations covered by any static provider: 3 / 5 (60.00%)" in markdown
    assert "## Top Providers" in markdown
    assert "- `beta`: 2 stations, 6 charging points, full-registry station coverage 25.00%, bundle station coverage 40.00%" in markdown
    assert "## Successful Fetches With Zero Full-Registry Matches" in markdown
    assert "- `epsilon`: access `noauth`, fetch `ok`" in markdown
    assert "## Fetch Or Access Issues" in markdown
    assert "- `gamma`: access `auth`, fetch `noauth: 500 Server Error: auth required`" in markdown
