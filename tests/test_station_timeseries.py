from __future__ import annotations

import csv
from pathlib import Path

from analysis.station_timeseries import _parse_station_reference, run_station_timeseries
from backend.config import AppConfig


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def test_parse_station_reference_accepts_woladen_url():
    assert (
        _parse_station_reference("https://woladen.de/?station=cf43ec02e883007d")
        == "cf43ec02e883007d"
    )


def test_run_station_timeseries_builds_target_and_nearby_outputs(tmp_path):
    chargers_csv_path = tmp_path / "chargers.csv"
    provider_daily_summary_path = tmp_path / "provider_daily_summary.csv"
    station_daily_summary_path = tmp_path / "station_daily_summary.csv"
    evse_status_changes_path = tmp_path / "evse_status_changes.csv"
    output_dir = tmp_path / "station-output"

    _write_csv(
        chargers_csv_path,
        [
            "station_id",
            "operator",
            "address",
            "postcode",
            "city",
            "lat",
            "lon",
            "charging_points_count",
            "max_power_kw",
            "detail_source_uid",
            "datex_site_id",
            "datex_station_ids",
            "datex_charge_point_ids",
        ],
        [
            {
                "station_id": "station-a",
                "operator": "Target Operator",
                "address": "Target Street 1",
                "postcode": "10115",
                "city": "Berlin",
                "lat": "52.5000",
                "lon": "13.4000",
                "charging_points_count": "2",
                "max_power_kw": "22",
                "detail_source_uid": "",
                "datex_site_id": "",
                "datex_station_ids": "",
                "datex_charge_point_ids": "",
            },
            {
                "station_id": "station-b",
                "operator": "Nearby Operator",
                "address": "Nearby Street 1",
                "postcode": "10117",
                "city": "Berlin",
                "lat": "52.5005",
                "lon": "13.4050",
                "charging_points_count": "1",
                "max_power_kw": "50",
                "detail_source_uid": "",
                "datex_site_id": "",
                "datex_station_ids": "",
                "datex_charge_point_ids": "",
            },
            {
                "station_id": "station-c",
                "operator": "Excluded Operator",
                "address": "Far Street 1",
                "postcode": "10243",
                "city": "Berlin",
                "lat": "52.5010",
                "lon": "13.4060",
                "charging_points_count": "1",
                "max_power_kw": "11",
                "detail_source_uid": "",
                "datex_site_id": "",
                "datex_station_ids": "",
                "datex_charge_point_ids": "",
            },
        ],
    )
    _write_csv(
        provider_daily_summary_path,
        [
            "archive_date",
            "provider_uid",
            "display_name",
            "publisher",
            "enabled_live_tracking",
            "has_static_feed",
            "has_dynamic_feed",
            "dynamic_delta_delivery",
            "dynamic_retention_period_minutes",
            "static_matched_station_count",
            "messages_total",
            "parseable_messages_total",
            "extracted_observation_count_total",
            "extracted_mapped_observation_count_total",
            "extracted_unmapped_observation_count_total",
            "mapped_observation_ratio",
            "competitive_analysis_eligible",
            "competitive_analysis_tier",
            "competitive_analysis_reason",
            "evses_observed",
            "mapped_evses_observed",
            "unmapped_evses_observed",
            "mapped_stations_observed",
            "free_evses_end_of_day",
            "occupied_evses_end_of_day",
            "out_of_order_evses_end_of_day",
            "unknown_evses_end_of_day",
            "stations_all_evses_out_of_order",
            "dynamic_station_coverage_ratio",
            "latest_event_timestamp",
        ],
        [
            {
                "archive_date": "2026-04-15",
                "provider_uid": "provider-target",
                "display_name": "Target Provider",
                "publisher": "",
                "enabled_live_tracking": "1",
                "has_static_feed": "1",
                "has_dynamic_feed": "1",
                "dynamic_delta_delivery": "0",
                "dynamic_retention_period_minutes": "",
                "static_matched_station_count": "1",
                "messages_total": "10",
                "parseable_messages_total": "10",
                "extracted_observation_count_total": "4",
                "extracted_mapped_observation_count_total": "4",
                "extracted_unmapped_observation_count_total": "0",
                "mapped_observation_ratio": "1.0",
                "competitive_analysis_eligible": "1",
                "competitive_analysis_tier": "eligible",
                "competitive_analysis_reason": "ratio_ge_0_5",
                "evses_observed": "2",
                "mapped_evses_observed": "2",
                "unmapped_evses_observed": "0",
                "mapped_stations_observed": "1",
                "free_evses_end_of_day": "1",
                "occupied_evses_end_of_day": "1",
                "out_of_order_evses_end_of_day": "0",
                "unknown_evses_end_of_day": "0",
                "stations_all_evses_out_of_order": "0",
                "dynamic_station_coverage_ratio": "1.0",
                "latest_event_timestamp": "2026-04-15T10:00:00+00:00",
            },
            {
                "archive_date": "2026-04-15",
                "provider_uid": "provider-target-alt",
                "display_name": "Target Provider Alt",
                "publisher": "",
                "enabled_live_tracking": "1",
                "has_static_feed": "1",
                "has_dynamic_feed": "1",
                "dynamic_delta_delivery": "0",
                "dynamic_retention_period_minutes": "",
                "static_matched_station_count": "1",
                "messages_total": "6",
                "parseable_messages_total": "6",
                "extracted_observation_count_total": "2",
                "extracted_mapped_observation_count_total": "1",
                "extracted_unmapped_observation_count_total": "1",
                "mapped_observation_ratio": "0.25",
                "competitive_analysis_eligible": "0",
                "competitive_analysis_tier": "review",
                "competitive_analysis_reason": "ratio_ge_0_2",
                "evses_observed": "1",
                "mapped_evses_observed": "1",
                "unmapped_evses_observed": "0",
                "mapped_stations_observed": "1",
                "free_evses_end_of_day": "1",
                "occupied_evses_end_of_day": "0",
                "out_of_order_evses_end_of_day": "0",
                "unknown_evses_end_of_day": "0",
                "stations_all_evses_out_of_order": "0",
                "dynamic_station_coverage_ratio": "1.0",
                "latest_event_timestamp": "2026-04-15T09:45:00+00:00",
            },
            {
                "archive_date": "2026-04-15",
                "provider_uid": "provider-nearby",
                "display_name": "Nearby Provider",
                "publisher": "",
                "enabled_live_tracking": "1",
                "has_static_feed": "1",
                "has_dynamic_feed": "1",
                "dynamic_delta_delivery": "0",
                "dynamic_retention_period_minutes": "",
                "static_matched_station_count": "1",
                "messages_total": "8",
                "parseable_messages_total": "8",
                "extracted_observation_count_total": "2",
                "extracted_mapped_observation_count_total": "1",
                "extracted_unmapped_observation_count_total": "1",
                "mapped_observation_ratio": "0.3",
                "competitive_analysis_eligible": "0",
                "competitive_analysis_tier": "review",
                "competitive_analysis_reason": "ratio_ge_0_2",
                "evses_observed": "1",
                "mapped_evses_observed": "1",
                "unmapped_evses_observed": "0",
                "mapped_stations_observed": "1",
                "free_evses_end_of_day": "0",
                "occupied_evses_end_of_day": "0",
                "out_of_order_evses_end_of_day": "1",
                "unknown_evses_end_of_day": "0",
                "stations_all_evses_out_of_order": "1",
                "dynamic_station_coverage_ratio": "1.0",
                "latest_event_timestamp": "2026-04-15T09:00:00+00:00",
            },
            {
                "archive_date": "2026-04-15",
                "provider_uid": "provider-excluded",
                "display_name": "Excluded Provider",
                "publisher": "",
                "enabled_live_tracking": "1",
                "has_static_feed": "1",
                "has_dynamic_feed": "1",
                "dynamic_delta_delivery": "0",
                "dynamic_retention_period_minutes": "",
                "static_matched_station_count": "1",
                "messages_total": "6",
                "parseable_messages_total": "6",
                "extracted_observation_count_total": "2",
                "extracted_mapped_observation_count_total": "0",
                "extracted_unmapped_observation_count_total": "2",
                "mapped_observation_ratio": "0.0",
                "competitive_analysis_eligible": "0",
                "competitive_analysis_tier": "exclude",
                "competitive_analysis_reason": "ratio_lt_0_2",
                "evses_observed": "1",
                "mapped_evses_observed": "1",
                "unmapped_evses_observed": "0",
                "mapped_stations_observed": "1",
                "free_evses_end_of_day": "1",
                "occupied_evses_end_of_day": "0",
                "out_of_order_evses_end_of_day": "0",
                "unknown_evses_end_of_day": "0",
                "stations_all_evses_out_of_order": "0",
                "dynamic_station_coverage_ratio": "1.0",
                "latest_event_timestamp": "2026-04-15T08:00:00+00:00",
            },
        ],
    )
    _write_csv(
        station_daily_summary_path,
        [
            "archive_date",
            "provider_uid",
            "station_id",
            "station_operator",
            "station_city",
            "station_catalog_charging_points_count",
            "evses_observed",
            "free_evses",
            "occupied_evses",
            "out_of_order_evses",
            "unknown_evses",
            "station_availability_status",
            "station_any_out_of_order",
            "station_all_evses_out_of_order",
            "station_coverage_vs_catalog",
            "latest_event_timestamp",
        ],
        [
            {
                "archive_date": "2026-04-15",
                "provider_uid": "provider-target",
                "station_id": "station-a",
                "station_operator": "Target Operator",
                "station_city": "Berlin",
                "station_catalog_charging_points_count": "2",
                "evses_observed": "2",
                "free_evses": "1",
                "occupied_evses": "1",
                "out_of_order_evses": "0",
                "unknown_evses": "0",
                "station_availability_status": "free",
                "station_any_out_of_order": "0",
                "station_all_evses_out_of_order": "0",
                "station_coverage_vs_catalog": "1.0",
                "latest_event_timestamp": "2026-04-15T10:00:00+00:00",
            },
            {
                "archive_date": "2026-04-15",
                "provider_uid": "provider-target-alt",
                "station_id": "station-a",
                "station_operator": "Target Operator",
                "station_city": "Berlin",
                "station_catalog_charging_points_count": "2",
                "evses_observed": "1",
                "free_evses": "1",
                "occupied_evses": "0",
                "out_of_order_evses": "0",
                "unknown_evses": "0",
                "station_availability_status": "free",
                "station_any_out_of_order": "0",
                "station_all_evses_out_of_order": "0",
                "station_coverage_vs_catalog": "0.5",
                "latest_event_timestamp": "2026-04-15T09:45:00+00:00",
            },
            {
                "archive_date": "2026-04-15",
                "provider_uid": "provider-nearby",
                "station_id": "station-b",
                "station_operator": "Nearby Operator",
                "station_city": "Berlin",
                "station_catalog_charging_points_count": "1",
                "evses_observed": "1",
                "free_evses": "0",
                "occupied_evses": "0",
                "out_of_order_evses": "1",
                "unknown_evses": "0",
                "station_availability_status": "out_of_order",
                "station_any_out_of_order": "1",
                "station_all_evses_out_of_order": "1",
                "station_coverage_vs_catalog": "1.0",
                "latest_event_timestamp": "2026-04-15T09:00:00+00:00",
            },
            {
                "archive_date": "2026-04-15",
                "provider_uid": "provider-excluded",
                "station_id": "station-c",
                "station_operator": "Excluded Operator",
                "station_city": "Berlin",
                "station_catalog_charging_points_count": "1",
                "evses_observed": "1",
                "free_evses": "1",
                "occupied_evses": "0",
                "out_of_order_evses": "0",
                "unknown_evses": "0",
                "station_availability_status": "free",
                "station_any_out_of_order": "0",
                "station_all_evses_out_of_order": "0",
                "station_coverage_vs_catalog": "1.0",
                "latest_event_timestamp": "2026-04-15T08:00:00+00:00",
            },
        ],
    )
    _write_csv(
        evse_status_changes_path,
        [
            "provider_uid",
            "provider_evse_id",
            "station_id",
            "site_id",
            "station_ref",
            "archive_date",
            "change_rank",
            "message_kind",
            "message_timestamp",
            "source_observed_at",
            "status_started_at",
            "next_status_started_at",
            "duration_seconds",
            "is_open_interval",
            "availability_status",
            "operational_status",
            "payload_sha256",
        ],
        [
            {
                "provider_uid": "provider-target",
                "provider_evse_id": "EVSE-A1",
                "station_id": "station-a",
                "site_id": "SITE-A",
                "station_ref": "REF-A",
                "archive_date": "2026-04-15",
                "change_rank": "1",
                "message_kind": "http_response",
                "message_timestamp": "2026-04-15T08:00:00+00:00",
                "source_observed_at": "2026-04-15T08:00:00+00:00",
                "status_started_at": "2026-04-15T08:00:00+00:00",
                "next_status_started_at": "2026-04-15T10:00:00+00:00",
                "duration_seconds": "7200",
                "is_open_interval": "0",
                "availability_status": "free",
                "operational_status": "AVAILABLE",
                "payload_sha256": "sha-a1",
            },
            {
                "provider_uid": "provider-target",
                "provider_evse_id": "EVSE-A2",
                "station_id": "station-a",
                "site_id": "SITE-A",
                "station_ref": "REF-A",
                "archive_date": "2026-04-15",
                "change_rank": "1",
                "message_kind": "http_response",
                "message_timestamp": "2026-04-15T08:30:00+00:00",
                "source_observed_at": "2026-04-15T08:30:00+00:00",
                "status_started_at": "2026-04-15T08:30:00+00:00",
                "next_status_started_at": "2026-04-15T10:00:00+00:00",
                "duration_seconds": "5400",
                "is_open_interval": "0",
                "availability_status": "occupied",
                "operational_status": "CHARGING",
                "payload_sha256": "sha-a2",
            },
            {
                "provider_uid": "provider-nearby",
                "provider_evse_id": "EVSE-B1",
                "station_id": "station-b",
                "site_id": "SITE-B",
                "station_ref": "REF-B",
                "archive_date": "2026-04-15",
                "change_rank": "1",
                "message_kind": "http_response",
                "message_timestamp": "2026-04-15T09:00:00+00:00",
                "source_observed_at": "2026-04-15T09:00:00+00:00",
                "status_started_at": "2026-04-15T09:00:00+00:00",
                "next_status_started_at": "2026-04-15T10:00:00+00:00",
                "duration_seconds": "3600",
                "is_open_interval": "0",
                "availability_status": "out_of_order",
                "operational_status": "OUT_OF_ORDER",
                "payload_sha256": "sha-b1",
            },
            {
                "provider_uid": "provider-excluded",
                "provider_evse_id": "EVSE-C1",
                "station_id": "station-c",
                "site_id": "SITE-C",
                "station_ref": "REF-C",
                "archive_date": "2026-04-15",
                "change_rank": "1",
                "message_kind": "http_response",
                "message_timestamp": "2026-04-15T08:00:00+00:00",
                "source_observed_at": "2026-04-15T08:00:00+00:00",
                "status_started_at": "2026-04-15T08:00:00+00:00",
                "next_status_started_at": "2026-04-15T09:00:00+00:00",
                "duration_seconds": "3600",
                "is_open_interval": "0",
                "availability_status": "free",
                "operational_status": "AVAILABLE",
                "payload_sha256": "sha-c1",
            },
        ],
    )

    result = run_station_timeseries(
        station_reference="https://woladen.de/?station=station-a",
        output_dir=output_dir,
        config=AppConfig(chargers_csv_path=chargers_csv_path),
        provider_daily_summary_path=provider_daily_summary_path,
        station_daily_summary_path=station_daily_summary_path,
        evse_status_changes_path=evse_status_changes_path,
        radius_m=1000.0,
        max_nearby=5,
        provider_tiers=("eligible", "review"),
    )

    assert result["station_id"] == "station-a"
    assert result["chosen_provider_uid"] == "provider-target"
    assert result["chosen_provider_tier"] == "eligible"
    assert result["nearby_station_count"] == 1
    assert result["nearby_primary_station_count"] == 0
    assert result["nearby_review_station_count"] == 1
    assert result["station_candidate_row_count"] == 3
    assert result["ambiguous_station_count"] == 1
    target_dir = output_dir / "station-a"
    assert target_dir.exists()

    nearby_rows = _read_csv(target_dir / "nearby_stations.csv")
    assert [row["station_id"] for row in nearby_rows] == ["station-a", "station-b"]
    assert [row["chosen_provider_tier"] for row in nearby_rows] == ["eligible", "review"]
    assert [row["chosen_comparison_bucket"] for row in nearby_rows] == ["target", "review"]
    candidate_rows = _read_csv(target_dir / "nearby_station_candidates.csv")
    assert [
        (row["station_id"], row["provider_uid"], row["candidate_rank"], row["is_selected_candidate"])
        for row in candidate_rows
    ] == [
        ("station-a", "provider-target", "1", "1"),
        ("station-a", "provider-target-alt", "2", "0"),
        ("station-b", "provider-nearby", "1", "1"),
    ]

    target_evse_rows = _read_csv(target_dir / "target_evse_status_timeline.csv")
    assert [row["provider_evse_id"] for row in target_evse_rows] == ["EVSE-A1", "EVSE-A2"]

    target_station_rows = _read_csv(target_dir / "target_station_status_timeline.csv")
    assert [row["interval_started_at"] for row in target_station_rows] == [
        "2026-04-15T08:00:00+00:00",
        "2026-04-15T08:30:00+00:00",
    ]
    assert target_station_rows[0]["free_evses"] == "1"
    assert target_station_rows[0]["occupied_evses"] == "0"
    assert target_station_rows[1]["free_evses"] == "1"
    assert target_station_rows[1]["occupied_evses"] == "1"

    target_evse_summary_rows = _read_csv(target_dir / "target_evse_status_summary.csv")
    assert [row["provider_evse_id"] for row in target_evse_summary_rows] == ["EVSE-A1", "EVSE-A2"]
    assert target_evse_summary_rows[0]["free_seconds"] == "7200"
    assert target_evse_summary_rows[0]["occupied_seconds"] == "0"
    assert target_evse_summary_rows[1]["free_seconds"] == "0"
    assert target_evse_summary_rows[1]["occupied_seconds"] == "5400"

    nearby_station_rows = _read_csv(target_dir / "nearby_station_status_timeline.csv")
    assert sorted({row["station_id"] for row in nearby_station_rows}) == ["station-a", "station-b"]
    assert sorted({row["comparison_bucket"] for row in nearby_station_rows}) == ["review", "target"]
    nearby_station_summary_rows = _read_csv(target_dir / "nearby_station_status_summary.csv")
    assert [row["station_id"] for row in nearby_station_summary_rows] == ["station-a", "station-b"]
    assert [row["comparison_bucket"] for row in nearby_station_summary_rows] == ["target", "review"]
    assert nearby_station_summary_rows[0]["free_seconds"] == "7200"
    assert nearby_station_summary_rows[0]["occupied_seconds"] == "0"
    assert nearby_station_summary_rows[0]["any_free_seconds"] == "7200"
    assert nearby_station_summary_rows[0]["any_occupied_seconds"] == "5400"
    assert nearby_station_summary_rows[1]["out_of_order_seconds"] == "3600"
    assert nearby_station_summary_rows[1]["all_evses_out_of_order_seconds"] == "3600"
    summary_text = (target_dir / "summary.md").read_text(encoding="utf-8")
    assert "Station Time Series" in summary_text
    assert "Primary Nearby Comparison Set" in summary_text
    assert "Review Nearby Stations" in summary_text
    assert "Provider Candidate Audit" in summary_text
    assert "selected `provider-target` from `2` candidates" in summary_text
    assert "`station-b`" in summary_text
    assert "EVSE summary rows" in summary_text
