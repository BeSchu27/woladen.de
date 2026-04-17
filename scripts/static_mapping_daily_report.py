#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import date
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.config import AppConfig


def _parse_date(value: str) -> date:
    return date.fromisoformat(value.strip())


def _text(value: Any) -> str:
    return str(value or "").strip()


def _to_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _to_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(float(numerator) / float(denominator), 6)


def _to_bool(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "y"}


def _format_percent(value: float) -> str:
    return f"{value * 100:.2f}%"


def _truncate(value: str, *, limit: int = 140) -> str:
    if len(value) <= limit:
        return value
    return f"{value[: limit - 1].rstrip()}…"


def load_coverage_payload(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("expected_static_coverage_json_object")
    providers = payload.get("providers")
    if not isinstance(providers, list):
        raise ValueError("expected_static_coverage_provider_list")
    return payload


def load_match_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def build_summary(
    *,
    coverage_payload: dict[str, Any],
    match_rows: list[dict[str, str]],
    coverage_path: Path,
    matches_path: Path,
    report_date: date,
) -> dict[str, Any]:
    totals = coverage_payload.get("totals")
    totals = totals if isinstance(totals, dict) else {}
    machine_probe = coverage_payload.get("machine_certificate_probe")
    machine_probe = machine_probe if isinstance(machine_probe, dict) else {}

    provider_rows: list[dict[str, Any]] = []
    providers = coverage_payload.get("providers")
    providers = providers if isinstance(providers, list) else []

    providers_with_static_feed = 0
    providers_successful_fetch = 0
    providers_with_matches = 0

    for provider in providers:
        if not isinstance(provider, dict):
            continue
        static = provider.get("static")
        static = static if isinstance(static, dict) else {}
        fetch_status = _text(static.get("fetch_status"))
        matched_stations = _to_int(static.get("matched_stations"))
        matched_charging_points = _to_int(static.get("matched_charging_points"))
        bundle_matched_stations = (
            _to_int(static.get("bundle_matched_stations"))
            if "bundle_matched_stations" in static
            else matched_stations
        )
        bundle_matched_charging_points = (
            _to_int(static.get("bundle_matched_charging_points"))
            if "bundle_matched_charging_points" in static
            else matched_charging_points
        )
        bundle_station_coverage_ratio = (
            _to_float(static.get("bundle_station_coverage_ratio"))
            if "bundle_station_coverage_ratio" in static
            else _to_float(static.get("station_coverage_ratio"))
        )
        bundle_charging_point_coverage_ratio = (
            _to_float(static.get("bundle_charging_point_coverage_ratio"))
            if "bundle_charging_point_coverage_ratio" in static
            else _to_float(static.get("charging_point_coverage_ratio"))
        )
        row = {
            "provider_uid": _text(provider.get("provider_uid")),
            "display_name": _text(provider.get("display_name")),
            "publisher": _text(provider.get("publisher")),
            "fetch_status": fetch_status,
            "access_mode": _text(static.get("access_mode")),
            "matched_stations": matched_stations,
            "matched_charging_points": matched_charging_points,
            "station_coverage_ratio": _to_float(static.get("station_coverage_ratio")),
            "charging_point_coverage_ratio": _to_float(static.get("charging_point_coverage_ratio")),
            "bundle_matched_stations": bundle_matched_stations,
            "bundle_matched_charging_points": bundle_matched_charging_points,
            "bundle_station_coverage_ratio": bundle_station_coverage_ratio,
            "bundle_charging_point_coverage_ratio": bundle_charging_point_coverage_ratio,
        }
        provider_rows.append(row)

        if fetch_status != "no_static_feed":
            providers_with_static_feed += 1
        if fetch_status == "ok":
            providers_successful_fetch += 1
        if matched_stations > 0:
            providers_with_matches += 1

    provider_rows.sort(key=lambda row: row["provider_uid"])

    unique_station_points: dict[str, int] = {}
    for row in match_rows:
        station_id = _text(row.get("station_id"))
        if not station_id:
            continue
        charging_point_count = _to_int(row.get("station_charging_points_count"))
        current_count = unique_station_points.get(station_id, 0)
        if charging_point_count > current_count:
            unique_station_points[station_id] = charging_point_count

    full_registry_station_count = _to_int(totals.get("stations"))
    full_registry_charging_point_count = _to_int(totals.get("charging_points"))
    bundle_station_count = _to_int(totals.get("bundle_stations") or totals.get("stations"))
    bundle_charging_point_count = _to_int(totals.get("bundle_charging_points") or totals.get("charging_points"))
    union_matched_station_count = len(unique_station_points)
    union_matched_charging_point_count = sum(unique_station_points.values())
    unique_bundle_station_points: dict[str, int] = {}
    for row in match_rows:
        station_id = _text(row.get("station_id"))
        if not station_id or not _to_bool(row.get("station_in_bundle")):
            continue
        charging_point_count = _to_int(row.get("station_charging_points_count"))
        current_count = unique_bundle_station_points.get(station_id, 0)
        if charging_point_count > current_count:
            unique_bundle_station_points[station_id] = charging_point_count
    bundle_union_matched_station_count = len(unique_bundle_station_points)
    bundle_union_matched_charging_point_count = sum(unique_bundle_station_points.values())

    top_providers = [
        row
        for row in sorted(
            provider_rows,
            key=lambda row: (
                -row["matched_stations"],
                -row["matched_charging_points"],
                row["provider_uid"],
            ),
        )
        if row["matched_stations"] > 0
    ][:10]

    providers_with_zero_matches_after_success = [
        row
        for row in provider_rows
        if row["fetch_status"] == "ok" and row["matched_stations"] == 0
    ]
    providers_with_fetch_issues = [
        row
        for row in provider_rows
        if row["fetch_status"] not in {"ok", "no_static_feed"}
    ]

    return {
        "report_date": report_date.isoformat(),
        "coverage_path": str(coverage_path.resolve()),
        "matches_path": str(matches_path.resolve()),
        "source_generated_at": _text(coverage_payload.get("generated_at")) or None,
        "machine_certificate_configured": bool(machine_probe.get("configured")),
        "machine_certificate_status": _text(machine_probe.get("status")) or "unknown",
        "full_registry_station_count": full_registry_station_count,
        "full_registry_charging_point_count": full_registry_charging_point_count,
        "bundle_station_count": bundle_station_count,
        "bundle_charging_point_count": bundle_charging_point_count,
        "providers_total": len(provider_rows),
        "providers_with_static_feed": providers_with_static_feed,
        "providers_successful_fetch": providers_successful_fetch,
        "providers_with_matches": providers_with_matches,
        "union_matched_station_count": union_matched_station_count,
        "union_matched_station_coverage_ratio": _ratio(
            union_matched_station_count,
            full_registry_station_count,
        ),
        "union_matched_charging_point_count": union_matched_charging_point_count,
        "union_matched_charging_point_coverage_ratio": _ratio(
            union_matched_charging_point_count,
            full_registry_charging_point_count,
        ),
        "bundle_union_matched_station_count": bundle_union_matched_station_count,
        "bundle_union_matched_station_coverage_ratio": _ratio(
            bundle_union_matched_station_count,
            bundle_station_count,
        ),
        "bundle_union_matched_charging_point_count": bundle_union_matched_charging_point_count,
        "bundle_union_matched_charging_point_coverage_ratio": _ratio(
            bundle_union_matched_charging_point_count,
            bundle_charging_point_count,
        ),
        "top_providers": top_providers,
        "providers_with_zero_matches_after_success": providers_with_zero_matches_after_success,
        "providers_with_fetch_issues": providers_with_fetch_issues,
    }


def render_markdown(summary: dict[str, Any]) -> str:
    lines = [
        f"# Static Mapping Coverage Report for {summary['report_date']}",
        "",
        f"- Report date: {summary['report_date']}",
        f"- Static snapshot generated at: {summary['source_generated_at'] or 'n/a'}",
        f"- Coverage source: `{summary['coverage_path']}`",
        f"- Match source: `{summary['matches_path']}`",
        f"- Machine certificate probe: {summary['machine_certificate_status']}",
        "",
        "## Snapshot",
        "",
        f"- Providers scanned: {summary['providers_total']}",
        f"- Providers with a static feed: {summary['providers_with_static_feed']}",
        f"- Providers with successful static fetches: {summary['providers_successful_fetch']}",
        f"- Providers with matched stations: {summary['providers_with_matches']}",
        f"- Full-registry stations covered by any static provider: {summary['union_matched_station_count']} / "
        f"{summary['full_registry_station_count']} "
        f"({_format_percent(summary['union_matched_station_coverage_ratio'])})",
        f"- Full-registry charging points covered by any static provider: {summary['union_matched_charging_point_count']} / "
        f"{summary['full_registry_charging_point_count']} "
        f"({_format_percent(summary['union_matched_charging_point_coverage_ratio'])})",
        f"- Bundle stations covered by any static provider: {summary['bundle_union_matched_station_count']} / "
        f"{summary['bundle_station_count']} "
        f"({_format_percent(summary['bundle_union_matched_station_coverage_ratio'])})",
        f"- Bundle charging points covered by any static provider: {summary['bundle_union_matched_charging_point_count']} / "
        f"{summary['bundle_charging_point_count']} "
        f"({_format_percent(summary['bundle_union_matched_charging_point_coverage_ratio'])})",
    ]

    top_providers = summary.get("top_providers") or []
    if top_providers:
        lines.extend(["", "## Top Providers", ""])
        for provider in top_providers:
            lines.append(
                f"- `{provider['provider_uid']}`: {provider['matched_stations']} stations, "
                f"{provider['matched_charging_points']} charging points, "
                f"full-registry station coverage {_format_percent(provider['station_coverage_ratio'])}, "
                f"bundle station coverage {_format_percent(provider['bundle_station_coverage_ratio'])}, "
                f"bundle charging-point coverage {_format_percent(provider['bundle_charging_point_coverage_ratio'])}"
            )

    zero_match_rows = summary.get("providers_with_zero_matches_after_success") or []
    if zero_match_rows:
        lines.extend(["", "## Successful Fetches With Zero Full-Registry Matches", ""])
        for provider in zero_match_rows:
            lines.append(
                f"- `{provider['provider_uid']}`: access `{provider['access_mode'] or 'n/a'}`, "
                f"fetch `{provider['fetch_status']}`"
            )

    fetch_issue_rows = summary.get("providers_with_fetch_issues") or []
    if fetch_issue_rows:
        lines.extend(["", "## Fetch Or Access Issues", ""])
        for provider in fetch_issue_rows:
            lines.append(
                f"- `{provider['provider_uid']}`: access `{provider['access_mode'] or 'n/a'}`, "
                f"fetch `{_truncate(provider['fetch_status'])}`"
            )

    lines.append("")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a dated Markdown summary from static mapping coverage artifacts",
    )
    parser.add_argument(
        "--coverage-path",
        type=Path,
        default=REPO_ROOT / "data" / "mobilithek_afir_static_coverage.json",
        help="Path to mobilithek_afir_static_coverage.json",
    )
    parser.add_argument(
        "--matches-path",
        type=Path,
        default=AppConfig().site_match_path,
        help="Path to mobilithek_afir_static_matches.csv",
    )
    parser.add_argument(
        "--report-date",
        type=_parse_date,
        default=date.today(),
        help="Report date in YYYY-MM-DD used for the output filename",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=REPO_ROOT / "analysis" / "output",
        help="Directory for the dated Markdown report",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional explicit output Markdown path",
    )
    parser.add_argument("--json", action="store_true", help="Print the computed summary as JSON")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    coverage_payload = load_coverage_payload(args.coverage_path)
    match_rows = load_match_rows(args.matches_path)
    summary = build_summary(
        coverage_payload=coverage_payload,
        match_rows=match_rows,
        coverage_path=args.coverage_path,
        matches_path=args.matches_path,
        report_date=args.report_date,
    )
    output_path = args.output or (args.output_dir / f"static_mapping_coverage_{summary['report_date']}.md")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_markdown(summary), encoding="utf-8")

    summary = {
        **summary,
        "report_path": str(output_path.resolve()),
    }
    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return
    print(summary["report_path"])


if __name__ == "__main__":
    main()
