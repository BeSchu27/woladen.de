#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import sys

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.config import AppConfig


ATTRIBUTE_LABELS: tuple[tuple[str, str], ...] = (
    ("availability_status", "Availability"),
    ("operational_status", "Operational Status"),
    ("any_price", "Any Price"),
    ("price_display", "Displayed Price"),
    ("price_energy", "Energy Price"),
    ("price_time", "Time Price"),
    ("complex_price", "Complex Tariff"),
    ("next_available_charging_slots", "Next Available Slots"),
    ("supplemental_facility_status", "Supplemental Facility Status"),
    ("source_observed_at", "Source Observed Timestamp"),
)


def _text(value: Any) -> str:
    return str(value or "").strip()


def _ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(float(numerator) / float(denominator), 6)


def _format_percent(value: float) -> str:
    return f"{value * 100:.2f}%"


def _json_nonempty(value: Any) -> bool:
    text = _text(value)
    if not text:
        return False
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return True
    return parsed not in (None, "", [], {})


def _has_any_price(row: sqlite3.Row | dict[str, Any]) -> bool:
    return bool(
        _text(row["price_display"])
        or _text(row["price_currency"])
        or _text(row["price_energy_eur_kwh_min"])
        or _text(row["price_energy_eur_kwh_max"])
        or row["price_time_eur_min_min"] is not None
        or row["price_time_eur_min_max"] is not None
        or int(row["price_complex"] or 0) != 0
    )


def _row_has_attribute(row: sqlite3.Row | dict[str, Any], attribute_key: str) -> bool:
    if attribute_key == "availability_status":
        return bool(_text(row["availability_status"]))
    if attribute_key == "operational_status":
        return bool(_text(row["operational_status"]))
    if attribute_key == "any_price":
        return _has_any_price(row)
    if attribute_key == "price_display":
        return bool(_text(row["price_display"]))
    if attribute_key == "price_energy":
        return bool(_text(row["price_energy_eur_kwh_min"]) or _text(row["price_energy_eur_kwh_max"]))
    if attribute_key == "price_time":
        return row["price_time_eur_min_min"] is not None or row["price_time_eur_min_max"] is not None
    if attribute_key == "complex_price":
        return int(row["price_complex"] or 0) != 0
    if attribute_key == "next_available_charging_slots":
        return _json_nonempty(row["next_available_charging_slots"])
    if attribute_key == "supplemental_facility_status":
        return _json_nonempty(row["supplemental_facility_status"])
    if attribute_key == "source_observed_at":
        return bool(_text(row["source_observed_at"]))
    raise KeyError(attribute_key)


def load_bundle_station_ids(path: Path) -> set[str]:
    station_ids: set[str] = set()
    with path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            station_id = _text(row.get("station_id"))
            if station_id:
                station_ids.add(station_id)
    return station_ids


def build_summary(*, db_path: Path, bundle_station_ids: set[str]) -> dict[str, Any]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT
                provider_uid,
                station_id,
                availability_status,
                operational_status,
                price_display,
                price_currency,
                price_energy_eur_kwh_min,
                price_energy_eur_kwh_max,
                price_time_eur_min_min,
                price_time_eur_min_max,
                price_complex,
                next_available_charging_slots,
                supplemental_facility_status,
                source_observed_at,
                fetched_at,
                ingested_at
            FROM evse_current_state
            WHERE station_id <> ''
            ORDER BY provider_uid, station_id
            """
        ).fetchall()
    finally:
        conn.close()

    filtered_rows = [row for row in rows if _text(row["station_id"]) in bundle_station_ids]
    station_providers: dict[str, set[str]] = defaultdict(set)
    provider_station_ids: dict[str, set[str]] = defaultdict(set)
    provider_attribute_stations: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    attribute_station_ids: dict[str, set[str]] = defaultdict(set)
    snapshot_at = ""

    for row in filtered_rows:
        provider_uid = _text(row["provider_uid"])
        station_id = _text(row["station_id"])
        if not provider_uid or not station_id:
            continue
        provider_station_ids[provider_uid].add(station_id)
        station_providers[station_id].add(provider_uid)
        snapshot_at = max(snapshot_at, _text(row["fetched_at"]))
        for attribute_key, _ in ATTRIBUTE_LABELS:
            if _row_has_attribute(row, attribute_key):
                provider_attribute_stations[provider_uid][attribute_key].add(station_id)
                attribute_station_ids[attribute_key].add(station_id)

    overlap_counter = Counter(len(provider_uids) for provider_uids in station_providers.values())
    stations_with_overlap = {station_id for station_id, provider_uids in station_providers.items() if len(provider_uids) > 1}

    provider_rows: list[dict[str, Any]] = []
    for provider_uid in sorted(
        provider_station_ids,
        key=lambda item: (-len(provider_station_ids[item]), item),
    ):
        row: dict[str, Any] = {
            "provider_uid": provider_uid,
            "station_count": len(provider_station_ids[provider_uid]),
            "overlap_station_count": sum(1 for station_id in provider_station_ids[provider_uid] if station_id in stations_with_overlap),
        }
        for attribute_key, _ in ATTRIBUTE_LABELS:
            row[f"{attribute_key}_station_count"] = len(provider_attribute_stations[provider_uid].get(attribute_key, set()))
        provider_rows.append(row)

    attribute_rows = [
        {
            "attribute_key": attribute_key,
            "attribute_label": attribute_label,
            "station_count": len(attribute_station_ids.get(attribute_key, set())),
            "coverage_ratio": _ratio(len(attribute_station_ids.get(attribute_key, set())), len(bundle_station_ids)),
        }
        for attribute_key, attribute_label in ATTRIBUTE_LABELS
    ]

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "db_path": str(db_path.resolve()),
        "dynamic_snapshot_at": snapshot_at or None,
        "bundle_station_count": len(bundle_station_ids),
        "dynamic_station_count": len(station_providers),
        "dynamic_station_ratio": _ratio(len(station_providers), len(bundle_station_ids)),
        "stations_with_multiple_providers_count": len(stations_with_overlap),
        "stations_with_multiple_providers_ratio": _ratio(len(stations_with_overlap), len(bundle_station_ids)),
        "max_provider_overlap_count": max(overlap_counter) if overlap_counter else 0,
        "provider_overlap_distribution": dict(sorted(overlap_counter.items())),
        "attribute_rows": attribute_rows,
        "provider_rows": provider_rows,
    }


def render_markdown(summary: dict[str, Any]) -> str:
    lines = [
        "# Dynamic Attribute Coverage Report",
        "",
        f"- Generated at: {summary['generated_at']}",
        f"- Dynamic source DB: `{summary['db_path']}`",
        f"- Dynamic snapshot timestamp: {summary['dynamic_snapshot_at'] or 'n/a'}",
        "",
        "## Snapshot",
        "",
        f"- Woladen bundle stations: {summary['bundle_station_count']}",
        f"- Stations with any dynamic data: {summary['dynamic_station_count']} ({_format_percent(summary['dynamic_station_ratio'])})",
        f"- Stations with multiple providers: {summary['stations_with_multiple_providers_count']} ({_format_percent(summary['stations_with_multiple_providers_ratio'])})",
        f"- Maximum provider overlap on a single station: {summary['max_provider_overlap_count']}",
        f"- Provider overlap distribution: {summary['provider_overlap_distribution']}",
        "",
        "## Attribute Coverage",
        "",
        "| Attribute | Stations | Coverage |",
        "| --- | ---: | ---: |",
    ]

    for row in summary["attribute_rows"]:
        lines.append(f"| {row['attribute_label']} | {row['station_count']} | {_format_percent(row['coverage_ratio'])} |")

    lines.extend(
        [
            "",
            "## Providers",
            "",
            "| Provider | Stations | Overlap | Availability | Operational | Any Price | Energy Price | Time Price | Next Slots | Supplemental |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )

    for row in summary["provider_rows"]:
        lines.append(
            f"| `{row['provider_uid']}` | {row['station_count']} | {row['overlap_station_count']} | "
            f"{row['availability_status_station_count']} | {row['operational_status_station_count']} | "
            f"{row['any_price_station_count']} | {row['price_energy_station_count']} | "
            f"{row['price_time_station_count']} | {row['next_available_charging_slots_station_count']} | "
            f"{row['supplemental_facility_status_station_count']} |"
        )

    return "\n".join(lines) + "\n"


def write_provider_csv(path: Path, provider_rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(provider_rows[0].keys()) if provider_rows else ["provider_uid", "station_count"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(provider_rows)


def parse_args() -> argparse.Namespace:
    config = AppConfig()
    parser = argparse.ArgumentParser(description="Build a dynamic attribute coverage report from a live sqlite snapshot")
    parser.add_argument(
        "--db-path",
        type=Path,
        required=True,
        help="Path to the sqlite snapshot containing evse_current_state",
    )
    parser.add_argument(
        "--bundle-chargers-csv",
        type=Path,
        default=config.chargers_csv_path,
        help="Path to data/chargers_fast.csv",
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
        help="Where to write the markdown report",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    summary = build_summary(
        db_path=args.db_path,
        bundle_station_ids=load_bundle_station_ids(args.bundle_chargers_csv),
    )
    write_provider_csv(args.output_csv, summary["provider_rows"])
    args.output_md.parent.mkdir(parents=True, exist_ok=True)
    args.output_md.write_text(render_markdown(summary), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
