from __future__ import annotations

import csv
from pathlib import Path

from analysis.provider_quality_report import run_provider_quality_report


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def test_run_provider_quality_report_builds_sorted_csv_and_markdown(tmp_path):
    provider_daily_summary_path = tmp_path / "provider_daily_summary.csv"
    output_dir = tmp_path / "reports"
    output_dir.mkdir()
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
                "provider_uid": "provider-a",
                "display_name": "Provider A",
                "publisher": "",
                "enabled_live_tracking": "1",
                "has_static_feed": "1",
                "has_dynamic_feed": "1",
                "dynamic_delta_delivery": "0",
                "dynamic_retention_period_minutes": "",
                "static_matched_station_count": "10",
                "messages_total": "100",
                "parseable_messages_total": "100",
                "extracted_observation_count_total": "100",
                "extracted_mapped_observation_count_total": "95",
                "extracted_unmapped_observation_count_total": "5",
                "mapped_observation_ratio": "0.95",
                "competitive_analysis_eligible": "1",
                "competitive_analysis_tier": "eligible",
                "competitive_analysis_reason": "ratio_ge_0_5",
                "evses_observed": "20",
                "mapped_evses_observed": "18",
                "unmapped_evses_observed": "2",
                "mapped_stations_observed": "8",
                "free_evses_end_of_day": "10",
                "occupied_evses_end_of_day": "8",
                "out_of_order_evses_end_of_day": "2",
                "unknown_evses_end_of_day": "0",
                "stations_all_evses_out_of_order": "1",
                "dynamic_station_coverage_ratio": "0.8",
                "latest_event_timestamp": "2026-04-15T10:00:00+00:00",
            },
            {
                "archive_date": "2026-04-15",
                "provider_uid": "provider-b",
                "display_name": "Provider B",
                "publisher": "",
                "enabled_live_tracking": "1",
                "has_static_feed": "1",
                "has_dynamic_feed": "1",
                "dynamic_delta_delivery": "0",
                "dynamic_retention_period_minutes": "",
                "static_matched_station_count": "10",
                "messages_total": "50",
                "parseable_messages_total": "50",
                "extracted_observation_count_total": "100",
                "extracted_mapped_observation_count_total": "35",
                "extracted_unmapped_observation_count_total": "65",
                "mapped_observation_ratio": "0.35",
                "competitive_analysis_eligible": "0",
                "competitive_analysis_tier": "review",
                "competitive_analysis_reason": "ratio_ge_0_2",
                "evses_observed": "12",
                "mapped_evses_observed": "7",
                "unmapped_evses_observed": "5",
                "mapped_stations_observed": "6",
                "free_evses_end_of_day": "3",
                "occupied_evses_end_of_day": "6",
                "out_of_order_evses_end_of_day": "3",
                "unknown_evses_end_of_day": "0",
                "stations_all_evses_out_of_order": "2",
                "dynamic_station_coverage_ratio": "0.6",
                "latest_event_timestamp": "2026-04-15T09:00:00+00:00",
            },
            {
                "archive_date": "2026-04-15",
                "provider_uid": "provider-c",
                "display_name": "Provider C",
                "publisher": "",
                "enabled_live_tracking": "1",
                "has_static_feed": "1",
                "has_dynamic_feed": "1",
                "dynamic_delta_delivery": "0",
                "dynamic_retention_period_minutes": "",
                "static_matched_station_count": "0",
                "messages_total": "10",
                "parseable_messages_total": "10",
                "extracted_observation_count_total": "10",
                "extracted_mapped_observation_count_total": "0",
                "extracted_unmapped_observation_count_total": "10",
                "mapped_observation_ratio": "0.0",
                "competitive_analysis_eligible": "0",
                "competitive_analysis_tier": "exclude",
                "competitive_analysis_reason": "no_static_matches",
                "evses_observed": "0",
                "mapped_evses_observed": "0",
                "unmapped_evses_observed": "0",
                "mapped_stations_observed": "0",
                "free_evses_end_of_day": "0",
                "occupied_evses_end_of_day": "0",
                "out_of_order_evses_end_of_day": "0",
                "unknown_evses_end_of_day": "0",
                "stations_all_evses_out_of_order": "0",
                "dynamic_station_coverage_ratio": "0.0",
                "latest_event_timestamp": "2026-04-15T08:00:00+00:00",
            },
            {
                "archive_date": "2026-04-14",
                "provider_uid": "old-provider",
                "display_name": "Old Provider",
                "publisher": "",
                "enabled_live_tracking": "1",
                "has_static_feed": "1",
                "has_dynamic_feed": "1",
                "dynamic_delta_delivery": "0",
                "dynamic_retention_period_minutes": "",
                "static_matched_station_count": "1",
                "messages_total": "1",
                "parseable_messages_total": "1",
                "extracted_observation_count_total": "1",
                "extracted_mapped_observation_count_total": "1",
                "extracted_unmapped_observation_count_total": "0",
                "mapped_observation_ratio": "1.0",
                "competitive_analysis_eligible": "1",
                "competitive_analysis_tier": "eligible",
                "competitive_analysis_reason": "ratio_ge_0_5",
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
                "latest_event_timestamp": "2026-04-14T08:00:00+00:00",
            },
        ],
    )
    _write_csv(
        output_dir / "provider_mapping_gaps_2026-04-15.csv",
        [
            "archive_date",
            "provider_uid",
            "remediation_priority",
            "remediation_category",
            "remediation_note",
        ],
        [
            {
                "archive_date": "2026-04-15",
                "provider_uid": "provider-b",
                "remediation_priority": "medium",
                "remediation_category": "expand_static_match_coverage",
                "remediation_note": "Review and extend static matches.",
            },
            {
                "archive_date": "2026-04-15",
                "provider_uid": "provider-c",
                "remediation_priority": "high",
                "remediation_category": "bootstrap_static_mapping",
                "remediation_note": "Build static mapping first.",
            },
        ],
    )

    result = run_provider_quality_report(
        provider_daily_summary_path=provider_daily_summary_path,
        output_dir=output_dir,
    )

    assert result["archive_date"] == "2026-04-15"
    assert result["provider_count"] == 3
    assert result["tier_counts"] == {"eligible": 1, "review": 1, "exclude": 1}
    assert result["comparison_counts"] == {"primary": 1, "secondary": 1, "backlog": 1}
    report_rows = _read_csv(output_dir / "provider_quality_2026-04-15.csv")
    assert [row["provider_uid"] for row in report_rows] == ["provider-a", "provider-b", "provider-c"]
    assert report_rows[0]["comparison_set_bucket"] == "primary"
    assert report_rows[1]["comparison_set_bucket"] == "secondary"
    assert report_rows[2]["comparison_set_bucket"] == "backlog"
    assert report_rows[1]["remediation_category"] == "expand_static_match_coverage"
    assert report_rows[2]["remediation_category"] == "bootstrap_static_mapping"
    report_text = (output_dir / "provider_quality_2026-04-15.md").read_text(encoding="utf-8")
    assert "Provider Quality Report 2026-04-15" in report_text
    assert "## Primary Comparison Set" in report_text
    assert "## Secondary Comparison Candidates" in report_text
    assert "review_ratio_and_coverage_gate" in report_text
    assert "## Review Backlog" in report_text
    assert "## Remediation Backlog" in report_text
    assert "## Artifact Index" in report_text
    assert "`provider-a`" in report_text
    assert "`provider-b`" in report_text
    assert "`expand_static_match_coverage`" in report_text
    assert "`no_static_matches`" in report_text
