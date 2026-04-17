from __future__ import annotations

import csv
from pathlib import Path

from analysis.provider_mapping_gap_report import run_provider_mapping_gap_report


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def test_run_provider_mapping_gap_report_builds_ranked_backlog(tmp_path):
    provider_daily_summary_path = tmp_path / "provider_daily_summary.csv"
    evse_observations_path = tmp_path / "evse_observations.csv"
    output_dir = tmp_path / "reports"
    _write_csv(
        provider_daily_summary_path,
        [
            "archive_date",
            "provider_uid",
            "display_name",
            "messages_total",
            "parseable_messages_total",
            "static_matched_station_count",
            "extracted_observation_count_total",
            "extracted_unmapped_observation_count_total",
            "mapped_observation_ratio",
            "competitive_analysis_tier",
            "competitive_analysis_reason",
        ],
        [
            {
                "archive_date": "2026-04-16",
                "provider_uid": "provider-uuid",
                "display_name": "Provider UUID",
                "messages_total": "100",
                "parseable_messages_total": "100",
                "static_matched_station_count": "5",
                "extracted_observation_count_total": "100",
                "extracted_unmapped_observation_count_total": "90",
                "mapped_observation_ratio": "0.10",
                "competitive_analysis_tier": "exclude",
                "competitive_analysis_reason": "ratio_lt_0_2",
            },
            {
                "archive_date": "2026-04-16",
                "provider_uid": "provider-afir",
                "display_name": "Provider AFIR",
                "messages_total": "50",
                "parseable_messages_total": "50",
                "static_matched_station_count": "3",
                "extracted_observation_count_total": "50",
                "extracted_unmapped_observation_count_total": "20",
                "mapped_observation_ratio": "0.40",
                "competitive_analysis_tier": "review",
                "competitive_analysis_reason": "ratio_ge_0_2",
            },
            {
                "archive_date": "2026-04-16",
                "provider_uid": "provider-static",
                "display_name": "Provider Static",
                "messages_total": "10",
                "parseable_messages_total": "10",
                "static_matched_station_count": "0",
                "extracted_observation_count_total": "10",
                "extracted_unmapped_observation_count_total": "10",
                "mapped_observation_ratio": "0.0",
                "competitive_analysis_tier": "exclude",
                "competitive_analysis_reason": "no_static_matches",
            },
            {
                "archive_date": "2026-04-16",
                "provider_uid": "provider-parser",
                "display_name": "Provider Parser",
                "messages_total": "8",
                "parseable_messages_total": "0",
                "static_matched_station_count": "2",
                "extracted_observation_count_total": "0",
                "extracted_unmapped_observation_count_total": "0",
                "mapped_observation_ratio": "0.0",
                "competitive_analysis_tier": "exclude",
                "competitive_analysis_reason": "no_parseable_messages",
            },
            {
                "archive_date": "2026-04-16",
                "provider_uid": "provider-eligible",
                "display_name": "Provider Eligible",
                "messages_total": "25",
                "parseable_messages_total": "25",
                "static_matched_station_count": "5",
                "extracted_observation_count_total": "25",
                "extracted_unmapped_observation_count_total": "0",
                "mapped_observation_ratio": "1.0",
                "competitive_analysis_tier": "eligible",
                "competitive_analysis_reason": "ratio_ge_0_5",
            },
        ],
    )
    _write_csv(
        evse_observations_path,
        [
            "archive_date",
            "provider_uid",
            "mapped_station",
            "provider_evse_id",
            "site_id",
            "station_ref",
            "availability_status",
        ],
        [
            {
                "archive_date": "2026-04-16",
                "provider_uid": "provider-uuid",
                "mapped_station": "0",
                "provider_evse_id": "123e4567-e89b-12d3-a456-426614174000",
                "site_id": "site-uuid-1",
                "station_ref": "station-ref-1",
                "availability_status": "occupied",
            },
            {
                "archive_date": "2026-04-16",
                "provider_uid": "provider-uuid",
                "mapped_station": "0",
                "provider_evse_id": "abcdef1234567890abcdef1234567890",
                "site_id": "site-uuid-1",
                "station_ref": "station-ref-1",
                "availability_status": "free",
            },
            {
                "archive_date": "2026-04-16",
                "provider_uid": "provider-afir",
                "mapped_station": "0",
                "provider_evse_id": "DEMSUESWBEB42",
                "site_id": "site-afir-1",
                "station_ref": "station-ref-2",
                "availability_status": "out_of_order",
            },
            {
                "archive_date": "2026-04-16",
                "provider_uid": "provider-afir",
                "mapped_station": "0",
                "provider_evse_id": "DEMSUESWBEB43",
                "site_id": "site-afir-2",
                "station_ref": "station-ref-3",
                "availability_status": "out_of_order",
            },
            {
                "archive_date": "2026-04-16",
                "provider_uid": "provider-static",
                "mapped_station": "0",
                "provider_evse_id": "SITELESS-1",
                "site_id": "site-static-1",
                "station_ref": "station-ref-4",
                "availability_status": "unknown",
            },
            {
                "archive_date": "2026-04-16",
                "provider_uid": "provider-eligible",
                "mapped_station": "1",
                "provider_evse_id": "DEOK1",
                "site_id": "site-ok",
                "station_ref": "station-ok",
                "availability_status": "free",
            },
        ],
    )

    result = run_provider_mapping_gap_report(
        provider_daily_summary_path=provider_daily_summary_path,
        evse_observations_path=evse_observations_path,
        output_dir=output_dir,
    )

    assert result["archive_date"] == "2026-04-16"
    assert result["provider_count"] == 4
    assert result["priority_counts"] == {"high": 3, "medium": 1}
    report_rows = _read_csv(output_dir / "provider_mapping_gaps_2026-04-16.csv")
    assert [row["provider_uid"] for row in report_rows] == [
        "provider-static",
        "provider-parser",
        "provider-uuid",
        "provider-afir",
    ]
    assert report_rows[0]["remediation_category"] == "bootstrap_static_mapping"
    assert report_rows[1]["remediation_category"] == "parser_coverage"
    assert report_rows[2]["identifier_pattern_hint"] == "uuid_like"
    assert report_rows[2]["remediation_category"] == "expand_static_match_coverage"
    assert report_rows[3]["identifier_pattern_hint"] == "afir_like"
    assert report_rows[3]["remediation_category"] == "reconcile_afir_identifiers"
    report_text = (output_dir / "provider_mapping_gaps_2026-04-16.md").read_text(encoding="utf-8")
    assert "Provider Mapping Gap Report 2026-04-16" in report_text
    assert "`bootstrap_static_mapping`" in report_text
    assert "`provider-afir`" in report_text
