#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path
from typing import Any, Sequence

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

DEFAULT_ANALYSIS_OUTPUT_DIR = REPO_ROOT / "analysis" / "output"
DEFAULT_REPORT_OUTPUT_DIR = DEFAULT_ANALYSIS_OUTPUT_DIR / "reports"

PROVIDER_QUALITY_FIELDS = [
    "archive_date",
    "provider_uid",
    "display_name",
    "competitive_analysis_tier",
    "competitive_analysis_eligible",
    "competitive_analysis_reason",
    "comparison_set_bucket",
    "comparison_set_reason",
    "remediation_priority",
    "remediation_category",
    "remediation_note",
    "mapped_observation_ratio",
    "dynamic_station_coverage_ratio",
    "static_matched_station_count",
    "mapped_stations_observed",
    "messages_total",
    "parseable_messages_total",
    "out_of_order_evses_end_of_day",
    "stations_all_evses_out_of_order",
]

TIER_ORDER = {"eligible": 0, "review": 1, "exclude": 2}
SECONDARY_REVIEW_MIN_RATIO = 0.35
SECONDARY_REVIEW_MIN_COVERAGE = 0.5


def _parse_archive_date(value: str) -> date:
    return date.fromisoformat(value.strip())


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, fieldnames: Sequence[str], rows: Sequence[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def _float_value(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _int_value(value: Any) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _report_sort_key(row: dict[str, Any]) -> tuple[int, float, float, int, str]:
    return (
        TIER_ORDER.get(str(row.get("competitive_analysis_tier") or "exclude"), 9),
        -_float_value(row.get("mapped_observation_ratio")),
        -_float_value(row.get("dynamic_station_coverage_ratio")),
        -_int_value(row.get("messages_total")),
        str(row.get("provider_uid") or ""),
    )


def _select_target_date(rows: Sequence[dict[str, str]], target_date: date | None) -> str:
    if target_date is not None:
        return target_date.isoformat()
    dates = sorted({str(row.get("archive_date") or "") for row in rows if str(row.get("archive_date") or "").strip()})
    if not dates:
        raise ValueError("no_archive_dates_in_provider_daily_summary")
    return dates[-1]


def _default_mapping_gap_csv_path(output_dir: Path, target_date_text: str) -> Path:
    return output_dir / f"provider_mapping_gaps_{target_date_text}.csv"


def _load_mapping_gap_rows(path: Path) -> dict[str, dict[str, str]]:
    if not path.exists():
        return {}
    rows = _read_csv_rows(path)
    return {
        str(row.get("provider_uid") or ""): row
        for row in rows
        if str(row.get("provider_uid") or "").strip()
    }


def _artifact_index(
    *,
    provider_daily_summary_path: Path,
    output_dir: Path,
    target_date_text: str,
) -> list[tuple[str, Path]]:
    analysis_output_dir = provider_daily_summary_path.parent
    artifacts = [
        ("provider_daily_summary", provider_daily_summary_path),
        ("station_daily_summary", analysis_output_dir / "station_daily_summary.csv"),
        ("evse_status_changes", analysis_output_dir / "evse_status_changes.csv"),
        ("provider_quality_csv", output_dir / f"provider_quality_{target_date_text}.csv"),
        ("provider_quality_markdown", output_dir / f"provider_quality_{target_date_text}.md"),
        ("provider_mapping_gaps_csv", output_dir / f"provider_mapping_gaps_{target_date_text}.csv"),
        ("provider_mapping_gaps_markdown", output_dir / f"provider_mapping_gaps_{target_date_text}.md"),
    ]
    return [(name, path) for name, path in artifacts if path.exists()]


def _comparison_set(row: dict[str, Any]) -> tuple[str, str]:
    tier = str(row.get("competitive_analysis_tier") or "exclude")
    mapped_observation_ratio = _float_value(row.get("mapped_observation_ratio"))
    dynamic_station_coverage_ratio = _float_value(row.get("dynamic_station_coverage_ratio"))
    if tier == "eligible":
        return ("primary", "eligible_primary")
    if (
        tier == "review"
        and mapped_observation_ratio >= SECONDARY_REVIEW_MIN_RATIO
        and dynamic_station_coverage_ratio >= SECONDARY_REVIEW_MIN_COVERAGE
    ):
        return ("secondary", "review_ratio_and_coverage_gate")
    if tier == "review":
        return ("backlog", "review_below_secondary_gate")
    return ("backlog", "excluded_from_comparison")


def run_provider_quality_report(
    *,
    provider_daily_summary_path: Path = DEFAULT_ANALYSIS_OUTPUT_DIR / "provider_daily_summary.csv",
    output_dir: Path = DEFAULT_REPORT_OUTPUT_DIR,
    target_date: date | None = None,
    mapping_gap_csv_path: Path | None = None,
) -> dict[str, Any]:
    rows = _read_csv_rows(provider_daily_summary_path)
    target_date_text = _select_target_date(rows, target_date)
    if mapping_gap_csv_path is None:
        mapping_gap_csv_path = _default_mapping_gap_csv_path(output_dir, target_date_text)
    mapping_gap_rows = _load_mapping_gap_rows(mapping_gap_csv_path)
    filtered_rows = [row for row in rows if str(row.get("archive_date") or "") == target_date_text and _int_value(row.get("messages_total")) > 0]
    report_rows = [
        {
            field: row.get(field, "")
            for field in PROVIDER_QUALITY_FIELDS
        }
            | {
                "comparison_set_bucket": _comparison_set(row)[0],
                "comparison_set_reason": _comparison_set(row)[1],
                "remediation_priority": mapping_gap_rows.get(str(row.get("provider_uid") or ""), {}).get("remediation_priority", ""),
                "remediation_category": mapping_gap_rows.get(str(row.get("provider_uid") or ""), {}).get("remediation_category", ""),
                "remediation_note": mapping_gap_rows.get(str(row.get("provider_uid") or ""), {}).get("remediation_note", ""),
            }
        for row in filtered_rows
    ]
    report_rows.sort(key=_report_sort_key)

    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / f"provider_quality_{target_date_text}.csv"
    md_path = output_dir / f"provider_quality_{target_date_text}.md"
    _write_csv(csv_path, PROVIDER_QUALITY_FIELDS, report_rows)

    tier_counts = Counter(str(row.get("competitive_analysis_tier") or "exclude") for row in report_rows)
    comparison_counts = Counter(str(row.get("comparison_set_bucket") or "backlog") for row in report_rows)
    reason_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in report_rows:
        reason_groups[str(row.get("competitive_analysis_reason") or "")].append(row)
    artifacts = _artifact_index(
        provider_daily_summary_path=provider_daily_summary_path,
        output_dir=output_dir,
        target_date_text=target_date_text,
    )

    lines = [
        f"# Provider Quality Report {target_date_text}",
        "",
        f"- Providers with any messages: `{len(report_rows)}`",
        f"- Primary comparison providers: `{comparison_counts.get('primary', 0)}`",
        f"- Secondary comparison candidates: `{comparison_counts.get('secondary', 0)}`",
        f"- Backlog providers: `{comparison_counts.get('backlog', 0)}`",
        f"- Eligibility tiers: eligible `{tier_counts.get('eligible', 0)}`, review `{tier_counts.get('review', 0)}`, exclude `{tier_counts.get('exclude', 0)}`",
        "",
        "## Primary Comparison Set",
        "",
    ]
    eligible_rows = [row for row in report_rows if row["competitive_analysis_tier"] == "eligible"]
    if eligible_rows:
        for row in eligible_rows:
            lines.append(
                f"- `{row['provider_uid']}`: ratio `{row['mapped_observation_ratio']}`, "
                f"coverage `{row['dynamic_station_coverage_ratio']}`, "
                f"out_of_order_evses `{row['out_of_order_evses_end_of_day']}`"
            )
    else:
        lines.append("- none")

    lines.extend(
        [
            "",
            "## Secondary Comparison Candidates",
            "",
            f"- Rule: `review` tier with mapped observation ratio `>= {SECONDARY_REVIEW_MIN_RATIO:.2f}` and dynamic station coverage ratio `>= {SECONDARY_REVIEW_MIN_COVERAGE:.2f}`.",
        ]
    )
    secondary_rows = [row for row in report_rows if row["comparison_set_bucket"] == "secondary"]
    if secondary_rows:
        for row in secondary_rows:
            lines.append(
                f"- `{row['provider_uid']}`: ratio `{row['mapped_observation_ratio']}`, "
                f"coverage `{row['dynamic_station_coverage_ratio']}`, "
                f"reason `{row['comparison_set_reason']}`, next `{row.get('remediation_category') or 'manual_review'}`"
            )
    else:
        lines.append("- none")

    lines.extend(["", "## Review Backlog", ""])
    review_backlog_rows = [
        row for row in report_rows if row["competitive_analysis_tier"] == "review" and row["comparison_set_bucket"] != "secondary"
    ]
    if review_backlog_rows:
        for row in review_backlog_rows:
            lines.append(
                f"- `{row['provider_uid']}`: ratio `{row['mapped_observation_ratio']}`, coverage `{row['dynamic_station_coverage_ratio']}`, "
                f"reason `{row['comparison_set_reason']}`, next `{row.get('remediation_category') or 'manual_review'}`"
            )
    else:
        lines.append("- none")

    lines.extend(["", "## Remediation Backlog", ""])
    remediation_rows = [row for row in report_rows if row["comparison_set_bucket"] != "primary"]
    if remediation_rows:
        for row in remediation_rows:
            lines.append(
                f"- `{row['provider_uid']}`: priority `{row.get('remediation_priority') or 'n/a'}`, "
                f"category `{row.get('remediation_category') or 'n/a'}`, "
                f"ratio `{row['mapped_observation_ratio']}`, reason `{row['competitive_analysis_reason']}`"
            )
    else:
        lines.append("- none")

    lines.extend(["", "## Exclusion Reasons", ""])
    for reason in sorted(reason_groups):
        if reason == "ratio_ge_0_5" or reason == "ratio_ge_0_2":
            continue
        providers = ", ".join(f"`{row['provider_uid']}`" for row in sorted(reason_groups[reason], key=lambda row: str(row["provider_uid"])))
        lines.append(f"- `{reason}`: {providers}")

    lines.extend(["", "## Outage Watchlist", ""])
    outage_rows = sorted(
        eligible_rows,
        key=lambda row: (
            -_int_value(row.get("stations_all_evses_out_of_order")),
            -_int_value(row.get("out_of_order_evses_end_of_day")),
            str(row.get("provider_uid") or ""),
        ),
    )[:5]
    if outage_rows:
        for row in outage_rows:
            lines.append(
                f"- `{row['provider_uid']}`: stations_all_out `{row['stations_all_evses_out_of_order']}`, "
                f"out_of_order_evses `{row['out_of_order_evses_end_of_day']}`"
            )
    else:
        lines.append("- none")

    lines.extend(["", "## Artifact Index", ""])
    for artifact_name, artifact_path in artifacts:
        lines.append(f"- `{artifact_name}`: `{artifact_path.resolve()}`")

    lines.extend(
        [
            "",
            "## Full Ranking",
            "",
            "| Comparison | Tier | Provider | Ratio | Coverage | Static matched stations | Mapped stations | Messages | Parseable | Remediation | Out-of-order EVSEs | All-out-of-order stations |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: |",
        ]
    )
    for row in report_rows:
        lines.append(
            f"| `{row['comparison_set_bucket']}` | `{row['competitive_analysis_tier']}` | `{row['provider_uid']}` | {row['mapped_observation_ratio']} | "
            f"{row['dynamic_station_coverage_ratio']} | {row['static_matched_station_count']} | {row['mapped_stations_observed']} | "
            f"{row['messages_total']} | {row['parseable_messages_total']} | `{row.get('remediation_category') or ''}` | {row['out_of_order_evses_end_of_day']} | "
            f"{row['stations_all_evses_out_of_order']} |"
        )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    return {
        "archive_date": target_date_text,
        "provider_count": len(report_rows),
        "tier_counts": dict(tier_counts),
        "comparison_counts": dict(comparison_counts),
        "output_dir": str(output_dir.resolve()),
        "outputs": {
            "csv": str(csv_path.resolve()),
            "markdown": str(md_path.resolve()),
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create a provider-quality report from provider_daily_summary.csv")
    parser.add_argument(
        "--provider-daily-summary",
        type=Path,
        default=DEFAULT_ANALYSIS_OUTPUT_DIR / "provider_daily_summary.csv",
        help="Path to provider_daily_summary.csv",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_REPORT_OUTPUT_DIR,
        help="Directory for generated provider quality report files",
    )
    parser.add_argument(
        "--mapping-gap-csv",
        type=Path,
        default=None,
        help="Optional provider_mapping_gaps_<date>.csv path; defaults to the report directory",
    )
    parser.add_argument("--date", type=_parse_archive_date, default=None, help="Target YYYY-MM-DD; defaults to latest")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = run_provider_quality_report(
        provider_daily_summary_path=args.provider_daily_summary,
        output_dir=args.output_dir,
        target_date=args.date,
        mapping_gap_csv_path=args.mapping_gap_csv,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
