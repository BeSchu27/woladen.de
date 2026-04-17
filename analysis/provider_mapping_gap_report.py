#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Sequence

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

DEFAULT_ANALYSIS_OUTPUT_DIR = REPO_ROOT / "analysis" / "output"
DEFAULT_REPORT_OUTPUT_DIR = DEFAULT_ANALYSIS_OUTPUT_DIR / "reports"

PROVIDER_MAPPING_GAP_FIELDS = [
    "archive_date",
    "provider_uid",
    "display_name",
    "competitive_analysis_tier",
    "competitive_analysis_reason",
    "remediation_priority",
    "remediation_category",
    "remediation_note",
    "identifier_pattern_hint",
    "mapped_observation_ratio",
    "messages_total",
    "parseable_messages_total",
    "static_matched_station_count",
    "extracted_observation_count_total",
    "extracted_unmapped_observation_count_total",
    "unique_unmapped_evse_id_count",
    "unique_unmapped_site_id_count",
    "unique_unmapped_station_ref_count",
    "sample_unmapped_provider_evse_ids_json",
    "sample_unmapped_site_ids_json",
    "sample_unmapped_station_refs_json",
    "unmapped_status_counts_json",
]

PRIORITY_ORDER = {"high": 0, "medium": 1, "low": 2}
TIER_ORDER = {"exclude": 0, "review": 1, "eligible": 2}
UUID_RE = re.compile(
    r"(?i)^(?:[0-9a-f]{32}|[0-9a-f]{16,64}|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$"
)
AFIR_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{4,}$")
NUMERIC_RE = re.compile(r"^\d{4,}$")


@dataclass
class UnmappedAccumulator:
    evse_ids: Counter[str] = field(default_factory=Counter)
    site_ids: Counter[str] = field(default_factory=Counter)
    station_refs: Counter[str] = field(default_factory=Counter)
    statuses: Counter[str] = field(default_factory=Counter)


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


def _select_target_date(rows: Sequence[dict[str, str]], target_date: date | None) -> str:
    if target_date is not None:
        return target_date.isoformat()
    dates = sorted({str(row.get("archive_date") or "") for row in rows if str(row.get("archive_date") or "").strip()})
    if not dates:
        raise ValueError("no_archive_dates_in_provider_daily_summary")
    return dates[-1]


def _top_counter_json(counter: Counter[str], *, limit: int = 5) -> str:
    rows = [{"value": value, "count": count} for value, count in counter.most_common(limit)]
    return json.dumps(rows, ensure_ascii=False)


def _top_counter_markdown(counter: Counter[str], *, limit: int = 3) -> str:
    if not counter:
        return "n/a"
    return ", ".join(f"`{value}` x{count}" for value, count in counter.most_common(limit))


def _identifier_kind(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return "missing"
    if UUID_RE.fullmatch(text):
        return "uuid_like"
    if AFIR_RE.fullmatch(text):
        return "afir_like"
    if NUMERIC_RE.fullmatch(text):
        return "numeric_like"
    return "other"


def _identifier_pattern_hint(counter: Counter[str]) -> str:
    if not counter:
        return "missing"
    pattern_counts: Counter[str] = Counter()
    for value, count in counter.items():
        pattern_counts[_identifier_kind(value)] += count
    if not pattern_counts:
        return "missing"
    if len(pattern_counts) == 1:
        return pattern_counts.most_common(1)[0][0]
    top_pattern, top_count = pattern_counts.most_common(1)[0]
    if top_count * 2 <= sum(pattern_counts.values()):
        return "mixed"
    return top_pattern


def _remediation_category(provider_row: dict[str, str], *, identifier_pattern_hint: str) -> str:
    provider_uid = str(provider_row.get("provider_uid") or "")
    reason = str(provider_row.get("competitive_analysis_reason") or "")
    static_matched_station_count = _int_value(provider_row.get("static_matched_station_count"))
    if provider_uid.startswith("deprecated_"):
        return "merge_or_exclude_deprecated_feed"
    if reason == "no_parseable_messages":
        return "parser_coverage"
    if reason == "no_static_matches" or static_matched_station_count <= 0:
        return "bootstrap_static_mapping"
    if identifier_pattern_hint == "afir_like":
        return "reconcile_afir_identifiers"
    if identifier_pattern_hint == "uuid_like":
        return "expand_static_match_coverage"
    return "manual_mapping_review"


def _remediation_priority(provider_row: dict[str, str]) -> str:
    tier = str(provider_row.get("competitive_analysis_tier") or "exclude")
    reason = str(provider_row.get("competitive_analysis_reason") or "")
    if reason in {"no_static_matches", "no_parseable_messages"}:
        return "high"
    if tier == "exclude":
        return "high"
    if tier == "review":
        return "medium"
    return "low"


def _remediation_note(category: str) -> str:
    notes = {
        "bootstrap_static_mapping": "No usable static matches exist yet; keep the provider out of comparison until static mapping lands.",
        "expand_static_match_coverage": "Dynamic EVSE identifiers are present but mostly unmatched; expand static/site coverage for the observed identifier inventory.",
        "reconcile_afir_identifiers": "Dynamic payloads look AFIR-native; reconcile those IDs against the mixed static inventory before comparing coverage.",
        "merge_or_exclude_deprecated_feed": "This looks like a deprecated or duplicate feed; either merge it intentionally or keep it excluded.",
        "parser_coverage": "Messages are arriving but not producing EVSE facts; inspect parser compatibility before any mapping work.",
        "manual_mapping_review": "Identifier evidence is mixed; inspect raw samples and existing static matches before changing comparison rules.",
    }
    return notes.get(category, "Review the provider manually.")


def _report_sort_key(row: dict[str, Any]) -> tuple[int, int, float, int, str]:
    return (
        PRIORITY_ORDER.get(str(row.get("remediation_priority") or "low"), 9),
        TIER_ORDER.get(str(row.get("competitive_analysis_tier") or "exclude"), 9),
        _float_value(row.get("mapped_observation_ratio")),
        -_int_value(row.get("extracted_unmapped_observation_count_total")),
        str(row.get("provider_uid") or ""),
    )


def run_provider_mapping_gap_report(
    *,
    provider_daily_summary_path: Path = DEFAULT_ANALYSIS_OUTPUT_DIR / "provider_daily_summary.csv",
    evse_observations_path: Path = DEFAULT_ANALYSIS_OUTPUT_DIR / "evse_observations.csv",
    output_dir: Path = DEFAULT_REPORT_OUTPUT_DIR,
    target_date: date | None = None,
) -> dict[str, Any]:
    provider_rows = _read_csv_rows(provider_daily_summary_path)
    target_date_text = _select_target_date(provider_rows, target_date)

    report_provider_rows = []
    provider_by_uid: dict[str, dict[str, str]] = {}
    for row in provider_rows:
        if str(row.get("archive_date") or "") != target_date_text:
            continue
        if _int_value(row.get("messages_total")) <= 0:
            continue
        if (
            str(row.get("competitive_analysis_tier") or "exclude") == "eligible"
            and _int_value(row.get("extracted_unmapped_observation_count_total")) <= 0
        ):
            continue
        provider_uid = str(row.get("provider_uid") or "")
        if not provider_uid:
            continue
        provider_by_uid[provider_uid] = row
        report_provider_rows.append(row)

    accumulators = {provider_uid: UnmappedAccumulator() for provider_uid in provider_by_uid}
    if accumulators:
        with evse_observations_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                if str(row.get("archive_date") or "") != target_date_text:
                    continue
                provider_uid = str(row.get("provider_uid") or "")
                accumulator = accumulators.get(provider_uid)
                if accumulator is None:
                    continue
                if str(row.get("mapped_station") or "") in {"1", "true", "True"}:
                    continue
                provider_evse_id = str(row.get("provider_evse_id") or "").strip()
                site_id = str(row.get("site_id") or "").strip()
                station_ref = str(row.get("station_ref") or "").strip()
                availability_status = str(row.get("availability_status") or "").strip()
                if provider_evse_id:
                    accumulator.evse_ids[provider_evse_id] += 1
                if site_id:
                    accumulator.site_ids[site_id] += 1
                if station_ref:
                    accumulator.station_refs[station_ref] += 1
                if availability_status:
                    accumulator.statuses[availability_status] += 1

    report_rows: list[dict[str, Any]] = []
    for provider_row in report_provider_rows:
        provider_uid = str(provider_row.get("provider_uid") or "")
        accumulator = accumulators[provider_uid]
        identifier_pattern_hint = _identifier_pattern_hint(accumulator.evse_ids)
        remediation_category = _remediation_category(provider_row, identifier_pattern_hint=identifier_pattern_hint)
        report_rows.append(
            {
                "archive_date": target_date_text,
                "provider_uid": provider_uid,
                "display_name": str(provider_row.get("display_name") or ""),
                "competitive_analysis_tier": str(provider_row.get("competitive_analysis_tier") or "exclude"),
                "competitive_analysis_reason": str(provider_row.get("competitive_analysis_reason") or ""),
                "remediation_priority": _remediation_priority(provider_row),
                "remediation_category": remediation_category,
                "remediation_note": _remediation_note(remediation_category),
                "identifier_pattern_hint": identifier_pattern_hint,
                "mapped_observation_ratio": provider_row.get("mapped_observation_ratio", ""),
                "messages_total": provider_row.get("messages_total", ""),
                "parseable_messages_total": provider_row.get("parseable_messages_total", ""),
                "static_matched_station_count": provider_row.get("static_matched_station_count", ""),
                "extracted_observation_count_total": provider_row.get("extracted_observation_count_total", ""),
                "extracted_unmapped_observation_count_total": provider_row.get(
                    "extracted_unmapped_observation_count_total", ""
                ),
                "unique_unmapped_evse_id_count": len(accumulator.evse_ids),
                "unique_unmapped_site_id_count": len(accumulator.site_ids),
                "unique_unmapped_station_ref_count": len(accumulator.station_refs),
                "sample_unmapped_provider_evse_ids_json": _top_counter_json(accumulator.evse_ids),
                "sample_unmapped_site_ids_json": _top_counter_json(accumulator.site_ids),
                "sample_unmapped_station_refs_json": _top_counter_json(accumulator.station_refs),
                "unmapped_status_counts_json": _top_counter_json(accumulator.statuses),
            }
        )
    report_rows.sort(key=_report_sort_key)

    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / f"provider_mapping_gaps_{target_date_text}.csv"
    md_path = output_dir / f"provider_mapping_gaps_{target_date_text}.md"
    _write_csv(csv_path, PROVIDER_MAPPING_GAP_FIELDS, report_rows)

    priority_counts = Counter(str(row.get("remediation_priority") or "low") for row in report_rows)
    category_groups: dict[str, list[dict[str, Any]]] = {}
    for row in report_rows:
        category_groups.setdefault(str(row.get("remediation_category") or "manual_mapping_review"), []).append(row)

    lines = [
        f"# Provider Mapping Gap Report {target_date_text}",
        "",
        f"- Providers in remediation set: `{len(report_rows)}`",
        f"- High priority: `{priority_counts.get('high', 0)}`",
        f"- Medium priority: `{priority_counts.get('medium', 0)}`",
        f"- Low priority: `{priority_counts.get('low', 0)}`",
        f"- Provider daily summary: `{provider_daily_summary_path.resolve()}`",
        f"- EVSE observations: `{evse_observations_path.resolve()}`",
        "",
        "## Recommended Buckets",
        "",
    ]
    if report_rows:
        for category in sorted(category_groups):
            providers = ", ".join(
                f"`{row['provider_uid']}`" for row in sorted(category_groups[category], key=lambda row: str(row["provider_uid"]))
            )
            lines.append(f"- `{category}`: {providers}")
    else:
        lines.append("- none")

    lines.extend(["", "## Provider Backlog", ""])
    if report_rows:
        for row in report_rows:
            lines.extend(
                [
                    f"### `{row['provider_uid']}`",
                    "",
                    f"- Tier: `{row['competitive_analysis_tier']}`",
                    f"- Reason: `{row['competitive_analysis_reason']}`",
                    f"- Priority: `{row['remediation_priority']}`",
                    f"- Category: `{row['remediation_category']}`",
                    f"- Note: {row['remediation_note']}",
                    f"- Mapped observation ratio: `{row['mapped_observation_ratio']}`",
                    f"- Unmapped observations: `{row['extracted_unmapped_observation_count_total']}`",
                    f"- Identifier pattern: `{row['identifier_pattern_hint']}`",
                    f"- Sample EVSE IDs: {_top_counter_markdown(accumulators[row['provider_uid']].evse_ids)}",
                    f"- Sample site IDs: {_top_counter_markdown(accumulators[row['provider_uid']].site_ids)}",
                    f"- Sample station refs: {_top_counter_markdown(accumulators[row['provider_uid']].station_refs)}",
                    f"- Unmapped status mix: {_top_counter_markdown(accumulators[row['provider_uid']].statuses)}",
                    "",
                ]
            )
    else:
        lines.append("- none")

    lines.extend(
        [
            "## Full Ranking",
            "",
            "| Priority | Tier | Provider | Category | Pattern | Ratio | Unmapped observations | Unique EVSE IDs | Unique site IDs |",
            "| --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in report_rows:
        lines.append(
            f"| `{row['remediation_priority']}` | `{row['competitive_analysis_tier']}` | `{row['provider_uid']}` | "
            f"`{row['remediation_category']}` | `{row['identifier_pattern_hint']}` | {row['mapped_observation_ratio']} | "
            f"{row['extracted_unmapped_observation_count_total']} | {row['unique_unmapped_evse_id_count']} | "
            f"{row['unique_unmapped_site_id_count']} |"
        )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    return {
        "archive_date": target_date_text,
        "provider_count": len(report_rows),
        "priority_counts": dict(priority_counts),
        "output_dir": str(output_dir.resolve()),
        "outputs": {
            "csv": str(csv_path.resolve()),
            "markdown": str(md_path.resolve()),
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create a provider mapping-gap remediation report")
    parser.add_argument(
        "--provider-daily-summary",
        type=Path,
        default=DEFAULT_ANALYSIS_OUTPUT_DIR / "provider_daily_summary.csv",
        help="Path to provider_daily_summary.csv",
    )
    parser.add_argument(
        "--evse-observations",
        type=Path,
        default=DEFAULT_ANALYSIS_OUTPUT_DIR / "evse_observations.csv",
        help="Path to evse_observations.csv",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_REPORT_OUTPUT_DIR,
        help="Directory for generated provider mapping-gap report files",
    )
    parser.add_argument("--date", type=_parse_archive_date, default=None, help="Target YYYY-MM-DD; defaults to latest")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = run_provider_mapping_gap_report(
        provider_daily_summary_path=args.provider_daily_summary,
        evse_observations_path=args.evse_observations,
        output_dir=args.output_dir,
        target_date=args.date,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
