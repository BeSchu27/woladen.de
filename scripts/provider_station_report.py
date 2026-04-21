#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.config import AppConfig


def _text(value: Any) -> str:
    return str(value or "").strip()


def _to_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _to_bool(value: Any) -> bool:
    text = _text(value).lower()
    return text in {"1", "true", "yes", "y"}


def _ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(float(numerator) / float(denominator), 6)


def _format_percent(value: float) -> str:
    return f"{value * 100:.2f}%"


def load_bundle_stations(path: Path) -> dict[str, dict[str, str]]:
    stations: dict[str, dict[str, str]] = {}
    with path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            station_id = _text(row.get("station_id"))
            if station_id:
                stations[station_id] = row
    return stations


def load_provider_metadata(path: Path) -> dict[str, dict[str, str]]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    providers = payload.get("providers") if isinstance(payload, dict) else None
    if not isinstance(providers, list):
        return {}

    metadata: dict[str, dict[str, str]] = {}
    for provider in providers:
        if not isinstance(provider, dict):
            continue
        provider_uid = _text(provider.get("uid"))
        if not provider_uid:
            continue
        metadata[provider_uid] = {
            "display_name": _text(provider.get("display_name")) or provider_uid,
            "publisher": _text(provider.get("publisher")),
        }
    return metadata


def load_static_provider_station_sets(
    path: Path,
    *,
    bundle_station_ids: set[str],
) -> dict[str, set[str]]:
    static_by_provider: dict[str, set[str]] = {}
    if not path.exists():
        return static_by_provider

    with path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            provider_uid = _text(row.get("provider_uid"))
            station_id = _text(row.get("station_id"))
            if not provider_uid or not station_id or station_id not in bundle_station_ids:
                continue
            if not _to_bool(row.get("station_in_bundle")):
                continue
            static_by_provider.setdefault(provider_uid, set()).add(station_id)
    return static_by_provider


def load_dynamic_provider_station_sets(
    path: Path,
    *,
    bundle_station_ids: set[str],
) -> tuple[dict[str, set[str]], str | None]:
    dynamic_by_provider: dict[str, set[str]] = {}
    snapshot_at: str | None = None
    if not path.exists():
        return dynamic_by_provider, snapshot_at

    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        snapshot_row = conn.execute("SELECT MAX(fetched_at) AS snapshot_at FROM evse_current_state").fetchone()
        snapshot_at = _text(snapshot_row["snapshot_at"]) if snapshot_row else None
        rows = conn.execute(
            """
            SELECT provider_uid, station_id
            FROM evse_current_state
            WHERE station_id <> ''
            GROUP BY provider_uid, station_id
            """
        ).fetchall()
    finally:
        conn.close()

    for row in rows:
        provider_uid = _text(row["provider_uid"])
        station_id = _text(row["station_id"])
        if not provider_uid or not station_id or station_id not in bundle_station_ids:
            continue
        dynamic_by_provider.setdefault(provider_uid, set()).add(station_id)

    return dynamic_by_provider, snapshot_at


def build_summary(
    *,
    bundle_stations: dict[str, dict[str, str]],
    provider_metadata: dict[str, dict[str, str]],
    static_by_provider: dict[str, set[str]],
    dynamic_by_provider: dict[str, set[str]],
    dynamic_snapshot_at: str | None,
    bundle_path: Path,
    static_matches_path: Path,
    dynamic_db_path: Path,
) -> dict[str, Any]:
    bundle_station_ids = set(bundle_stations)
    union_static = set().union(*static_by_provider.values()) if static_by_provider else set()
    union_dynamic = set().union(*dynamic_by_provider.values()) if dynamic_by_provider else set()
    union_known = union_static | union_dynamic
    unmatched_station_ids = sorted(bundle_station_ids - union_known)

    provider_rows: list[dict[str, Any]] = []
    provider_uids = set(static_by_provider) | set(dynamic_by_provider)
    for provider_uid in provider_uids:
        static_station_ids = static_by_provider.get(provider_uid, set())
        dynamic_station_ids = dynamic_by_provider.get(provider_uid, set())
        overlap_station_ids = static_station_ids & dynamic_station_ids
        known_station_ids = static_station_ids | dynamic_station_ids
        metadata = provider_metadata.get(provider_uid, {})
        provider_rows.append(
            {
                "provider_uid": provider_uid,
                "display_name": _text(metadata.get("display_name")) or provider_uid,
                "publisher": _text(metadata.get("publisher")),
                "static_station_count": len(static_station_ids),
                "dynamic_station_count": len(dynamic_station_ids),
                "known_station_count": len(known_station_ids),
                "static_only_station_count": len(static_station_ids - overlap_station_ids),
                "dynamic_only_station_count": len(dynamic_station_ids - overlap_station_ids),
                "overlap_station_count": len(overlap_station_ids),
                "static_bundle_coverage_ratio": _ratio(len(static_station_ids), len(bundle_station_ids)),
                "dynamic_bundle_coverage_ratio": _ratio(len(dynamic_station_ids), len(bundle_station_ids)),
                "known_bundle_coverage_ratio": _ratio(len(known_station_ids), len(bundle_station_ids)),
            }
        )

    provider_rows.sort(
        key=lambda row: (
            -int(row["static_station_count"]),
            -int(row["known_station_count"]),
            -int(row["dynamic_station_count"]),
            str(row["provider_uid"]),
        )
    )

    unmatched_examples: list[dict[str, str]] = []
    for station_id in unmatched_station_ids[:15]:
        row = bundle_stations[station_id]
        unmatched_examples.append(
            {
                "station_id": station_id,
                "operator": _text(row.get("operator")),
                "city": _text(row.get("city")),
                "address": _text(row.get("address")),
            }
        )

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "bundle_path": str(bundle_path.resolve()),
        "static_matches_path": str(static_matches_path.resolve()),
        "dynamic_db_path": str(dynamic_db_path.resolve()),
        "dynamic_snapshot_at": dynamic_snapshot_at,
        "bundle_station_count": len(bundle_station_ids),
        "stations_with_static_match_count": len(union_static),
        "stations_with_static_match_ratio": _ratio(len(union_static), len(bundle_station_ids)),
        "stations_with_dynamic_match_count": len(union_dynamic),
        "stations_with_dynamic_match_ratio": _ratio(len(union_dynamic), len(bundle_station_ids)),
        "stations_with_known_match_count": len(union_known),
        "stations_with_known_match_ratio": _ratio(len(union_known), len(bundle_station_ids)),
        "stations_without_known_match_count": len(unmatched_station_ids),
        "stations_without_known_match_ratio": _ratio(len(unmatched_station_ids), len(bundle_station_ids)),
        "provider_rows": provider_rows,
        "unmatched_examples": unmatched_examples,
    }


def render_markdown(summary: dict[str, Any]) -> str:
    lines = [
        "# Woladen Provider Station Report",
        "",
        f"- Generated at: {summary['generated_at']}",
        f"- Bundle station catalog: `{summary['bundle_path']}`",
        f"- Static match source: `{summary['static_matches_path']}`",
        f"- Dynamic source DB: `{summary['dynamic_db_path']}`",
        f"- Dynamic snapshot timestamp: {summary['dynamic_snapshot_at'] or 'n/a'}",
        "",
        "## Snapshot",
        "",
        f"- Woladen bundle stations: {summary['bundle_station_count']}",
        f"- Stations with any static match: {summary['stations_with_static_match_count']} ({_format_percent(summary['stations_with_static_match_ratio'])})",
        f"- Stations with any dynamic match: {summary['stations_with_dynamic_match_count']} ({_format_percent(summary['stations_with_dynamic_match_ratio'])})",
        f"- Stations with any known static or dynamic match: {summary['stations_with_known_match_count']} ({_format_percent(summary['stations_with_known_match_ratio'])})",
        f"- Stations with no known static or dynamic match: {summary['stations_without_known_match_count']} ({_format_percent(summary['stations_without_known_match_ratio'])})",
        "",
        "## Providers",
        "",
        "| Provider | Static | Dynamic | Known | Static-only | Dynamic-only | Overlap |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]

    for row in summary["provider_rows"]:
        lines.append(
            f"| `{row['provider_uid']}` | {row['static_station_count']} | {row['dynamic_station_count']} | "
            f"{row['known_station_count']} | {row['static_only_station_count']} | "
            f"{row['dynamic_only_station_count']} | {row['overlap_station_count']} |"
        )

    if summary["unmatched_examples"]:
        lines.extend(
            [
                "",
                "## Unmatched Examples",
                "",
            ]
        )
        for row in summary["unmatched_examples"]:
            lines.append(
                f"- `{row['station_id']}`: {row['operator'] or 'n/a'}, {row['city'] or 'n/a'}, {row['address'] or 'n/a'}"
            )

    return "\n".join(lines) + "\n"


def write_provider_csv(path: Path, provider_rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "provider_uid",
        "display_name",
        "publisher",
        "static_station_count",
        "dynamic_station_count",
        "known_station_count",
        "static_only_station_count",
        "dynamic_only_station_count",
        "overlap_station_count",
        "static_bundle_coverage_ratio",
        "dynamic_bundle_coverage_ratio",
        "known_bundle_coverage_ratio",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(provider_rows)


def parse_args() -> argparse.Namespace:
    config = AppConfig()
    parser = argparse.ArgumentParser(
        description="Build a Woladen provider station coverage report from static matches and a live SQLite snapshot"
    )
    parser.add_argument(
        "--bundle-chargers-csv",
        type=Path,
        default=config.chargers_csv_path,
        help="Path to data/chargers_fast.csv",
    )
    parser.add_argument(
        "--static-matches-path",
        type=Path,
        default=config.site_match_path,
        help="Path to data/mobilithek_afir_static_matches.csv",
    )
    parser.add_argument(
        "--dynamic-db-path",
        type=Path,
        default=config.db_path,
        help="Path to a live_state.sqlite3 snapshot",
    )
    parser.add_argument(
        "--provider-configs-path",
        type=Path,
        default=config.provider_config_path,
        help="Path to data/mobilithek_afir_provider_configs.json",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        required=True,
        help="Where to write the provider CSV report",
    )
    parser.add_argument(
        "--output-md",
        type=Path,
        required=True,
        help="Where to write the Markdown report",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    bundle_stations = load_bundle_stations(args.bundle_chargers_csv)
    provider_metadata = load_provider_metadata(args.provider_configs_path)
    static_by_provider = load_static_provider_station_sets(
        args.static_matches_path,
        bundle_station_ids=set(bundle_stations),
    )
    dynamic_by_provider, dynamic_snapshot_at = load_dynamic_provider_station_sets(
        args.dynamic_db_path,
        bundle_station_ids=set(bundle_stations),
    )

    summary = build_summary(
        bundle_stations=bundle_stations,
        provider_metadata=provider_metadata,
        static_by_provider=static_by_provider,
        dynamic_by_provider=dynamic_by_provider,
        dynamic_snapshot_at=dynamic_snapshot_at,
        bundle_path=args.bundle_chargers_csv,
        static_matches_path=args.static_matches_path,
        dynamic_db_path=args.dynamic_db_path,
    )

    write_provider_csv(args.output_csv, summary["provider_rows"])
    args.output_md.parent.mkdir(parents=True, exist_ok=True)
    args.output_md.write_text(render_markdown(summary), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
