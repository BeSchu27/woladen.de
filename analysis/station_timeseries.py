#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence
from urllib.parse import parse_qs, urlparse

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.config import AppConfig
from backend.datex import parse_iso_datetime
from backend.loaders import load_station_records

DEFAULT_ANALYSIS_OUTPUT_DIR = REPO_ROOT / "analysis" / "output"
DEFAULT_STATION_OUTPUT_DIR = DEFAULT_ANALYSIS_OUTPUT_DIR / "stations"
DEFAULT_ALLOWED_PROVIDER_TIERS = ("eligible", "review")
TIER_RANK = {"exclude": 0, "review": 1, "eligible": 2}

TARGET_EVSE_TIMELINE_FIELDS = [
    "station_id",
    "provider_uid",
    "provider_tier",
    "provider_evse_id",
    "site_id",
    "station_ref",
    "archive_date",
    "status_started_at",
    "next_status_started_at",
    "duration_seconds",
    "is_open_interval",
    "availability_status",
    "operational_status",
    "message_kind",
    "message_timestamp",
    "source_observed_at",
    "payload_sha256",
]

TARGET_EVSE_SUMMARY_FIELDS = [
    "station_id",
    "provider_uid",
    "provider_tier",
    "provider_evse_id",
    "site_id",
    "station_ref",
    "interval_count",
    "open_interval_count",
    "first_status_started_at",
    "last_status_started_at",
    "latest_availability_status",
    "latest_operational_status",
    "total_duration_seconds",
    "free_seconds",
    "occupied_seconds",
    "out_of_order_seconds",
    "unknown_seconds",
]

STATION_TIMELINE_FIELDS = [
    "station_id",
    "is_target_station",
    "comparison_bucket",
    "distance_m",
    "provider_uid",
    "provider_tier",
    "provider_reason",
    "interval_started_at",
    "interval_ended_at",
    "duration_seconds",
    "evses_active",
    "free_evses",
    "occupied_evses",
    "out_of_order_evses",
    "unknown_evses",
    "station_availability_status",
    "station_any_out_of_order",
    "station_all_evses_out_of_order",
    "provider_evse_ids_json",
]

STATION_STATUS_SUMMARY_FIELDS = [
    "station_id",
    "is_target_station",
    "comparison_bucket",
    "distance_m",
    "provider_uid",
    "provider_tier",
    "provider_reason",
    "interval_count",
    "first_interval_started_at",
    "last_interval_started_at",
    "latest_station_availability_status",
    "total_duration_seconds",
    "free_seconds",
    "occupied_seconds",
    "out_of_order_seconds",
    "unknown_seconds",
    "any_free_seconds",
    "any_occupied_seconds",
    "any_out_of_order_seconds",
    "all_evses_out_of_order_seconds",
]

NEARBY_STATIONS_FIELDS = [
    "station_id",
    "is_target_station",
    "chosen_comparison_bucket",
    "distance_m",
    "operator",
    "address",
    "postcode",
    "city",
    "lat",
    "lon",
    "charging_points_count",
    "max_power_kw",
    "chosen_provider_uid",
    "chosen_provider_tier",
    "chosen_provider_reason",
    "chosen_provider_mapped_observation_ratio",
    "latest_archive_date",
    "latest_station_availability_status",
    "latest_station_any_out_of_order",
    "latest_station_all_evses_out_of_order",
    "latest_free_evses",
    "latest_occupied_evses",
    "latest_out_of_order_evses",
    "latest_unknown_evses",
    "latest_event_timestamp",
]

STATION_CANDIDATE_FIELDS = [
    "station_id",
    "is_target_station",
    "distance_m",
    "candidate_count_for_station",
    "candidate_rank",
    "is_selected_candidate",
    "selected_provider_uid",
    "provider_uid",
    "provider_tier",
    "provider_reason",
    "provider_mapped_observation_ratio",
    "parseable_messages_total",
    "messages_total",
    "latest_archive_date",
    "latest_event_timestamp",
    "latest_station_availability_status",
    "latest_station_any_out_of_order",
    "latest_station_all_evses_out_of_order",
]


@dataclass(frozen=True)
class StationSelection:
    station_id: str
    is_target_station: bool
    distance_m: float
    operator: str
    address: str
    postcode: str
    city: str
    lat: float
    lon: float
    charging_points_count: int
    max_power_kw: float
    provider_uid: str
    provider_tier: str
    provider_reason: str
    provider_mapped_observation_ratio: float
    latest_archive_date: str
    latest_station_row: dict[str, Any]


def _parse_archive_date(value: str) -> date:
    return date.fromisoformat(value.strip())


def _parse_station_reference(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError("station_reference_required")
    if "://" not in text:
        return text
    parsed = urlparse(text)
    query_station = (parse_qs(parsed.query).get("station") or [""])[0].strip()
    if query_station:
        return query_station
    path_value = parsed.path.strip("/")
    if path_value:
        return path_value
    raise ValueError(f"station_not_found_in_reference:{text}")


def _parse_provider_tiers(value: str) -> tuple[str, ...]:
    tiers = tuple(part.strip() for part in str(value or "").split(",") if part.strip())
    if not tiers:
        raise ValueError("provider_tiers_required")
    unknown = [tier for tier in tiers if tier not in TIER_RANK]
    if unknown:
        raise ValueError(f"unknown_provider_tiers:{','.join(sorted(set(unknown)))}")
    return tiers


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


def _json_text(value: Any) -> str:
    if value in (None, "", [], {}):
        return ""
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _sort_timestamp(value: str) -> datetime:
    parsed = parse_iso_datetime(value)
    if parsed is not None:
        return parsed.astimezone(timezone.utc)
    return datetime.min.replace(tzinfo=timezone.utc)


def _safe_slug(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in str(value or "").strip()) or "station"


def _haversine_meters(lat_a: float, lon_a: float, lat_b: float, lon_b: float) -> float:
    lat1, lon1, lat2, lon2 = map(math.radians, [lat_a, lon_a, lat_b, lon_b])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    haversine = math.sin(dlat / 2.0) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2.0) ** 2
    return 2.0 * 6371000.0 * math.asin(math.sqrt(haversine))


def _station_status_from_counts(counts: dict[str, int]) -> str:
    if counts["free"] > 0:
        return "free"
    if counts["occupied"] > 0:
        return "occupied"
    if counts["out_of_order"] > 0:
        return "out_of_order"
    return "unknown"


def _comparison_bucket(*, is_target_station: bool, provider_tier: str) -> str:
    if is_target_station:
        return "target"
    if provider_tier == "review":
        return "review"
    return "primary"


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


def _provider_row_sort_key(row: dict[str, Any]) -> tuple[int, float, int, int, datetime]:
    return (
        TIER_RANK.get(str(row.get("competitive_analysis_tier") or "exclude"), 0),
        _float_value(row.get("mapped_observation_ratio")),
        _int_value(row.get("parseable_messages_total")),
        _int_value(row.get("messages_total")),
        _sort_timestamp(str(row.get("latest_event_timestamp") or row.get("archive_date") or "")),
    )


def _candidate_sort_key(candidate: dict[str, Any]) -> tuple[int, float, int, datetime]:
    provider_rows = candidate["provider_rows"]
    best_provider_row = max(provider_rows, key=_provider_row_sort_key)
    return (
        TIER_RANK.get(str(best_provider_row.get("competitive_analysis_tier") or "exclude"), 0),
        _float_value(best_provider_row.get("mapped_observation_ratio")),
        sum(_int_value(row.get("messages_total")) for row in provider_rows),
        max(
            (_sort_timestamp(str(row.get("latest_event_timestamp") or row.get("archive_date") or "")) for row in provider_rows),
            default=datetime.min.replace(tzinfo=timezone.utc),
        ),
    )


def _select_best_station_candidates(
    *,
    target_station_id: str,
    radius_m: float,
    max_nearby: int,
    allowed_provider_tiers: set[str],
    station_catalog: dict[str, Any],
    provider_daily_rows: Sequence[dict[str, str]],
    station_daily_rows: Sequence[dict[str, str]],
) -> tuple[list[StationSelection], list[dict[str, Any]]]:
    target_station = station_catalog.get(target_station_id)
    if target_station is None:
        raise ValueError(f"unknown_station_id:{target_station_id}")

    provider_by_key = {
        (str(row.get("archive_date") or ""), str(row.get("provider_uid") or "")): row for row in provider_daily_rows
    }
    candidates: dict[tuple[str, str], dict[str, Any]] = {}

    for station_row in station_daily_rows:
        station_id = str(station_row.get("station_id") or "").strip()
        provider_uid = str(station_row.get("provider_uid") or "").strip()
        if not station_id or not provider_uid:
            continue
        station_meta = station_catalog.get(station_id)
        if station_meta is None:
            continue
        provider_row = provider_by_key.get((str(station_row.get("archive_date") or ""), provider_uid))
        if provider_row is None:
            continue

        distance_m = _haversine_meters(target_station.lat, target_station.lon, station_meta.lat, station_meta.lon)
        is_target_station = station_id == target_station_id
        provider_tier = str(provider_row.get("competitive_analysis_tier") or "exclude")
        if not is_target_station:
            if distance_m > radius_m:
                continue
            if provider_tier not in allowed_provider_tiers:
                continue

        candidate = candidates.setdefault(
            (station_id, provider_uid),
            {
                "station_meta": station_meta,
                "distance_m": distance_m,
                "provider_rows": [],
                "station_rows": [],
            },
        )
        candidate["provider_rows"].append(provider_row)
        candidate["station_rows"].append(station_row)

    target_candidates = [item for (station_id, _), item in candidates.items() if station_id == target_station_id]
    if not target_candidates:
        raise ValueError(f"no_station_history_for_station:{target_station_id}")

    selections: list[StationSelection] = []
    chosen_by_station: dict[str, dict[str, Any]] = {}
    for (station_id, _provider_uid), candidate in candidates.items():
        current = chosen_by_station.get(station_id)
        if current is None or _candidate_sort_key(candidate) > _candidate_sort_key(current):
            chosen_by_station[station_id] = candidate

    chosen_station_ids = [target_station_id]
    nearby_candidates = [
        candidate for station_id, candidate in chosen_by_station.items() if station_id != target_station_id
    ]
    nearby_candidates.sort(key=lambda item: (item["distance_m"], -_candidate_sort_key(item)[0], item["station_meta"].station_id))
    for candidate in nearby_candidates[:max_nearby]:
        chosen_station_ids.append(candidate["station_meta"].station_id)

    for station_id in chosen_station_ids:
        candidate = chosen_by_station[station_id]
        best_provider_row = max(candidate["provider_rows"], key=_provider_row_sort_key)
        latest_station_row = max(
            candidate["station_rows"],
            key=lambda row: (
                _sort_timestamp(str(row.get("latest_event_timestamp") or row.get("archive_date") or "")),
                str(row.get("archive_date") or ""),
            ),
        )
        station_meta = candidate["station_meta"]
        selections.append(
            StationSelection(
                station_id=station_meta.station_id,
                is_target_station=station_meta.station_id == target_station_id,
                distance_m=round(float(candidate["distance_m"]), 3),
                operator=station_meta.operator,
                address=station_meta.address,
                postcode=station_meta.postcode,
                city=station_meta.city,
                lat=station_meta.lat,
                lon=station_meta.lon,
                charging_points_count=station_meta.charging_points_count,
                max_power_kw=station_meta.max_power_kw,
                provider_uid=str(best_provider_row.get("provider_uid") or ""),
                provider_tier=str(best_provider_row.get("competitive_analysis_tier") or "exclude"),
                provider_reason=str(best_provider_row.get("competitive_analysis_reason") or ""),
                provider_mapped_observation_ratio=_float_value(best_provider_row.get("mapped_observation_ratio")),
                latest_archive_date=str(latest_station_row.get("archive_date") or ""),
                latest_station_row=dict(latest_station_row),
            )
        )

    selections.sort(key=lambda item: (0 if item.is_target_station else 1, item.distance_m, item.station_id))
    selected_provider_by_station = {selection.station_id: selection.provider_uid for selection in selections}
    candidate_rows: list[dict[str, Any]] = []
    for station_id in chosen_station_ids:
        station_candidates = [
            candidate for (candidate_station_id, _provider_uid), candidate in candidates.items() if candidate_station_id == station_id
        ]
        station_candidates.sort(key=_candidate_sort_key, reverse=True)
        selected_provider_uid = selected_provider_by_station.get(station_id, "")
        for index, candidate in enumerate(station_candidates, start=1):
            best_provider_row = max(candidate["provider_rows"], key=_provider_row_sort_key)
            latest_station_row = max(
                candidate["station_rows"],
                key=lambda row: (
                    _sort_timestamp(str(row.get("latest_event_timestamp") or row.get("archive_date") or "")),
                    str(row.get("archive_date") or ""),
                ),
            )
            candidate_provider_uid = str(best_provider_row.get("provider_uid") or "")
            station_meta = candidate["station_meta"]
            candidate_rows.append(
                {
                    "station_id": station_id,
                    "is_target_station": 1 if station_id == target_station_id else 0,
                    "distance_m": round(float(candidate["distance_m"]), 3),
                    "candidate_count_for_station": len(station_candidates),
                    "candidate_rank": index,
                    "is_selected_candidate": 1 if candidate_provider_uid == selected_provider_uid else 0,
                    "selected_provider_uid": selected_provider_uid,
                    "provider_uid": candidate_provider_uid,
                    "provider_tier": str(best_provider_row.get("competitive_analysis_tier") or "exclude"),
                    "provider_reason": str(best_provider_row.get("competitive_analysis_reason") or ""),
                    "provider_mapped_observation_ratio": _float_value(best_provider_row.get("mapped_observation_ratio")),
                    "parseable_messages_total": _int_value(best_provider_row.get("parseable_messages_total")),
                    "messages_total": _int_value(best_provider_row.get("messages_total")),
                    "latest_archive_date": str(latest_station_row.get("archive_date") or ""),
                    "latest_event_timestamp": str(latest_station_row.get("latest_event_timestamp") or ""),
                    "latest_station_availability_status": str(latest_station_row.get("station_availability_status") or ""),
                    "latest_station_any_out_of_order": _int_value(latest_station_row.get("station_any_out_of_order")),
                    "latest_station_all_evses_out_of_order": _int_value(
                        latest_station_row.get("station_all_evses_out_of_order")
                    ),
                }
            )

    candidate_rows.sort(
        key=lambda row: (
            0 if _int_value(row.get("is_target_station")) else 1,
            _float_value(row.get("distance_m")),
            str(row.get("station_id") or ""),
            _int_value(row.get("candidate_rank")),
            str(row.get("provider_uid") or ""),
        )
    )
    return selections, candidate_rows


def _iter_filtered_status_changes(
    path: Path,
    *,
    selected_pairs: set[tuple[str, str]],
) -> Iterable[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            key = (str(row.get("station_id") or ""), str(row.get("provider_uid") or ""))
            if key in selected_pairs:
                yield row


def _build_station_interval_rows(
    *,
    selection: StationSelection,
    status_rows: Sequence[dict[str, str]],
) -> list[dict[str, Any]]:
    boundaries = sorted(
        {
            parse_iso_datetime(str(row.get("status_started_at") or ""))
            for row in status_rows
            if parse_iso_datetime(str(row.get("status_started_at") or "")) is not None
        }
        | {
            parse_iso_datetime(str(row.get("next_status_started_at") or ""))
            for row in status_rows
            if parse_iso_datetime(str(row.get("next_status_started_at") or "")) is not None
        }
    )
    intervals: list[dict[str, Any]] = []
    for index in range(len(boundaries) - 1):
        start_at = boundaries[index]
        end_at = boundaries[index + 1]
        active_rows = []
        for row in status_rows:
            row_start = parse_iso_datetime(str(row.get("status_started_at") or ""))
            row_end = parse_iso_datetime(str(row.get("next_status_started_at") or ""))
            if row_start is None or row_end is None:
                continue
            if row_start <= start_at < row_end:
                active_rows.append(row)
        if not active_rows or end_at <= start_at:
            continue

        counts = {
            "free": sum(1 for row in active_rows if row.get("availability_status") == "free"),
            "occupied": sum(1 for row in active_rows if row.get("availability_status") == "occupied"),
            "out_of_order": sum(1 for row in active_rows if row.get("availability_status") == "out_of_order"),
            "unknown": sum(1 for row in active_rows if row.get("availability_status") == "unknown"),
        }
        provider_evse_ids = sorted({str(row.get("provider_evse_id") or "") for row in active_rows if row.get("provider_evse_id")})
        intervals.append(
            {
                "station_id": selection.station_id,
                "is_target_station": 1 if selection.is_target_station else 0,
                "comparison_bucket": _comparison_bucket(
                    is_target_station=selection.is_target_station,
                    provider_tier=selection.provider_tier,
                ),
                "distance_m": selection.distance_m,
                "provider_uid": selection.provider_uid,
                "provider_tier": selection.provider_tier,
                "provider_reason": selection.provider_reason,
                "interval_started_at": start_at.replace(microsecond=0).isoformat(),
                "interval_ended_at": end_at.replace(microsecond=0).isoformat(),
                "duration_seconds": int((end_at - start_at).total_seconds()),
                "evses_active": len(active_rows),
                "free_evses": counts["free"],
                "occupied_evses": counts["occupied"],
                "out_of_order_evses": counts["out_of_order"],
                "unknown_evses": counts["unknown"],
                "station_availability_status": _station_status_from_counts(counts),
                "station_any_out_of_order": 1 if counts["out_of_order"] > 0 else 0,
                "station_all_evses_out_of_order": 1 if active_rows and counts["out_of_order"] == len(active_rows) else 0,
                "provider_evse_ids_json": _json_text(provider_evse_ids),
            }
        )
    intervals.sort(key=lambda row: (_sort_timestamp(str(row.get("interval_started_at") or "")), str(row.get("station_id") or "")))
    return intervals


def _write_summary_markdown(
    path: Path,
    *,
    station_reference: str,
    target_selection: StationSelection,
    nearby_rows: Sequence[dict[str, Any]],
    target_evse_rows: Sequence[dict[str, Any]],
    target_evse_summary_rows: Sequence[dict[str, Any]],
    target_station_timeline_rows: Sequence[dict[str, Any]],
    nearby_station_summary_rows: Sequence[dict[str, Any]],
    nearby_station_timeline_rows: Sequence[dict[str, Any]],
    station_candidate_rows: Sequence[dict[str, Any]],
    allowed_provider_tiers: Sequence[str],
    radius_m: float,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    primary_rows = [
        row for row in nearby_rows if str(row.get("chosen_comparison_bucket") or "") in {"target", "primary"}
    ]
    review_rows = [row for row in nearby_rows if str(row.get("chosen_comparison_bucket") or "") == "review"]
    ambiguous_candidate_rows = [
        row for row in station_candidate_rows if _int_value(row.get("candidate_count_for_station")) > 1
    ]
    ambiguous_station_ids = sorted({str(row.get("station_id") or "") for row in ambiguous_candidate_rows if str(row.get("station_id") or "")})
    lines = [
        f"# Station Time Series: `{target_selection.station_id}`",
        "",
        f"- Input reference: `{station_reference}`",
        f"- Chosen provider: `{target_selection.provider_uid}`",
        f"- Provider tier: `{target_selection.provider_tier}`",
        f"- Provider reason: `{target_selection.provider_reason}`",
        f"- Provider mapped observation ratio: `{target_selection.provider_mapped_observation_ratio:.6f}`",
        f"- Neighborhood filter: provider tiers `{', '.join(allowed_provider_tiers)}` within `{int(radius_m)}` meters",
        f"- Target station: `{target_selection.operator}`, `{target_selection.address}`",
        "",
        "## Target Station",
        "",
        f"- Charging points in bundle: `{target_selection.charging_points_count}`",
        f"- Max power in bundle: `{target_selection.max_power_kw}` kW",
        f"- EVSE interval rows: `{len(target_evse_rows)}`",
        f"- EVSE summary rows: `{len(target_evse_summary_rows)}`",
        f"- Station interval rows: `{len(target_station_timeline_rows)}`",
        f"- Nearby station summary rows: `{len(nearby_station_summary_rows)}`",
        f"- Candidate provider rows: `{len(station_candidate_rows)}`",
        f"- Stations with multiple provider candidates: `{len(ambiguous_station_ids)}`",
        f"- Primary nearby comparison stations: `{max(0, len(primary_rows) - 1)}`",
        f"- Review nearby stations: `{len(review_rows)}`",
        "",
        "## Primary Nearby Comparison Set",
        "",
        "| Distance m | Station ID | Provider | Tier | Latest status | EVSEs observed | City |",
        "| ---: | --- | --- | --- | --- | ---: | --- |",
    ]
    for row in primary_rows:
        lines.append(
            f"| {row['distance_m']} | `{row['station_id']}` | `{row['chosen_provider_uid']}` | "
            f"`{row['chosen_provider_tier']}` | `{row['latest_station_availability_status']}` | "
            f"{row['latest_free_evses']}/{row['latest_occupied_evses']}/{row['latest_out_of_order_evses']}/{row['latest_unknown_evses']} | "
            f"{row['city']} |"
        )
    lines.extend(["", "## Review Nearby Stations", ""])
    if review_rows:
        lines.extend(
            [
                "| Distance m | Station ID | Provider | Tier | Latest status | EVSEs observed | City |",
                "| ---: | --- | --- | --- | --- | ---: | --- |",
            ]
        )
        for row in review_rows:
            lines.append(
                f"| {row['distance_m']} | `{row['station_id']}` | `{row['chosen_provider_uid']}` | "
                f"`{row['chosen_provider_tier']}` | `{row['latest_station_availability_status']}` | "
                f"{row['latest_free_evses']}/{row['latest_occupied_evses']}/{row['latest_out_of_order_evses']}/{row['latest_unknown_evses']} | "
                f"{row['city']} |"
            )
    else:
        lines.append("- none")
    lines.extend(["", "## Provider Candidate Audit", ""])
    if ambiguous_station_ids:
        candidate_rows_by_station: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in ambiguous_candidate_rows:
            candidate_rows_by_station[str(row.get("station_id") or "")].append(row)
        for station_id in ambiguous_station_ids:
            rows = sorted(
                candidate_rows_by_station[station_id],
                key=lambda row: (_int_value(row.get("candidate_rank")), str(row.get("provider_uid") or "")),
            )
            selected = next((row for row in rows if _int_value(row.get("is_selected_candidate"))), rows[0])
            alternatives = [
                f"`{row['provider_uid']}` ({row['provider_tier']}, ratio {row['provider_mapped_observation_ratio']})"
                for row in rows
                if not _int_value(row.get("is_selected_candidate"))
            ]
            lines.append(
                f"- `{station_id}`: selected `{selected['provider_uid']}` from `{selected['candidate_count_for_station']}` candidates"
                + (f"; alternatives {', '.join(alternatives)}" if alternatives else "")
            )
    else:
        lines.append("- none")
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- `target_evse_status_timeline.csv` contains raw EVSE status intervals for the chosen provider at the target station.",
            "- `target_evse_status_summary.csv` aggregates those EVSE intervals into duration totals per charger.",
            "- `target_station_status_timeline.csv` aggregates those EVSE intervals into one station-level interval series.",
            "- `nearby_station_status_timeline.csv` does the same for the target station plus the nearby filtered stations.",
            "- `nearby_station_status_summary.csv` aggregates the station-level intervals into duration totals per station.",
            "- With the current archive inventory, this report only covers the dates already present in `analysis/output/`.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _build_target_evse_summary_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get("provider_evse_id") or "")].append(row)

    summary_rows: list[dict[str, Any]] = []
    for provider_evse_id in sorted(grouped):
        evse_rows = sorted(grouped[provider_evse_id], key=lambda row: _sort_timestamp(str(row.get("status_started_at") or "")))
        latest_row = max(evse_rows, key=lambda row: _sort_timestamp(str(row.get("status_started_at") or "")))
        counts = {
            "free": sum(_int_value(row.get("duration_seconds")) for row in evse_rows if row.get("availability_status") == "free"),
            "occupied": sum(_int_value(row.get("duration_seconds")) for row in evse_rows if row.get("availability_status") == "occupied"),
            "out_of_order": sum(
                _int_value(row.get("duration_seconds")) for row in evse_rows if row.get("availability_status") == "out_of_order"
            ),
            "unknown": sum(_int_value(row.get("duration_seconds")) for row in evse_rows if row.get("availability_status") == "unknown"),
        }
        summary_rows.append(
            {
                "station_id": latest_row.get("station_id") or "",
                "provider_uid": latest_row.get("provider_uid") or "",
                "provider_tier": latest_row.get("provider_tier") or "",
                "provider_evse_id": provider_evse_id,
                "site_id": latest_row.get("site_id") or "",
                "station_ref": latest_row.get("station_ref") or "",
                "interval_count": len(evse_rows),
                "open_interval_count": sum(_int_value(row.get("is_open_interval")) for row in evse_rows),
                "first_status_started_at": evse_rows[0].get("status_started_at") or "",
                "last_status_started_at": latest_row.get("status_started_at") or "",
                "latest_availability_status": latest_row.get("availability_status") or "",
                "latest_operational_status": latest_row.get("operational_status") or "",
                "total_duration_seconds": sum(_int_value(row.get("duration_seconds")) for row in evse_rows),
                "free_seconds": counts["free"],
                "occupied_seconds": counts["occupied"],
                "out_of_order_seconds": counts["out_of_order"],
                "unknown_seconds": counts["unknown"],
            }
        )
    return summary_rows


def _build_station_status_summary_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get("station_id") or "")].append(row)

    summary_rows: list[dict[str, Any]] = []
    for station_id in sorted(grouped):
        station_rows = sorted(grouped[station_id], key=lambda row: _sort_timestamp(str(row.get("interval_started_at") or "")))
        latest_row = max(station_rows, key=lambda row: _sort_timestamp(str(row.get("interval_started_at") or "")))
        counts = {
            "free": sum(
                _int_value(row.get("duration_seconds")) for row in station_rows if row.get("station_availability_status") == "free"
            ),
            "occupied": sum(
                _int_value(row.get("duration_seconds"))
                for row in station_rows
                if row.get("station_availability_status") == "occupied"
            ),
            "out_of_order": sum(
                _int_value(row.get("duration_seconds"))
                for row in station_rows
                if row.get("station_availability_status") == "out_of_order"
            ),
            "unknown": sum(
                _int_value(row.get("duration_seconds")) for row in station_rows if row.get("station_availability_status") == "unknown"
            ),
        }
        summary_rows.append(
            {
                "station_id": station_id,
                "is_target_station": _int_value(latest_row.get("is_target_station")),
                "comparison_bucket": latest_row.get("comparison_bucket") or "",
                "distance_m": _float_value(latest_row.get("distance_m")),
                "provider_uid": latest_row.get("provider_uid") or "",
                "provider_tier": latest_row.get("provider_tier") or "",
                "provider_reason": latest_row.get("provider_reason") or "",
                "interval_count": len(station_rows),
                "first_interval_started_at": station_rows[0].get("interval_started_at") or "",
                "last_interval_started_at": latest_row.get("interval_started_at") or "",
                "latest_station_availability_status": latest_row.get("station_availability_status") or "",
                "total_duration_seconds": sum(_int_value(row.get("duration_seconds")) for row in station_rows),
                "free_seconds": counts["free"],
                "occupied_seconds": counts["occupied"],
                "out_of_order_seconds": counts["out_of_order"],
                "unknown_seconds": counts["unknown"],
                "any_free_seconds": sum(
                    _int_value(row.get("duration_seconds")) for row in station_rows if _int_value(row.get("free_evses")) > 0
                ),
                "any_occupied_seconds": sum(
                    _int_value(row.get("duration_seconds"))
                    for row in station_rows
                    if _int_value(row.get("occupied_evses")) > 0
                ),
                "any_out_of_order_seconds": sum(
                    _int_value(row.get("duration_seconds"))
                    for row in station_rows
                    if _int_value(row.get("station_any_out_of_order"))
                ),
                "all_evses_out_of_order_seconds": sum(
                    _int_value(row.get("duration_seconds"))
                    for row in station_rows
                    if _int_value(row.get("station_all_evses_out_of_order"))
                ),
            }
        )
    summary_rows.sort(key=lambda row: (0 if _int_value(row["is_target_station"]) else 1, _float_value(row["distance_m"]), row["station_id"]))
    return summary_rows


def run_station_timeseries(
    *,
    station_reference: str,
    output_dir: Path,
    config: AppConfig | None = None,
    provider_daily_summary_path: Path = DEFAULT_ANALYSIS_OUTPUT_DIR / "provider_daily_summary.csv",
    station_daily_summary_path: Path = DEFAULT_ANALYSIS_OUTPUT_DIR / "station_daily_summary.csv",
    evse_status_changes_path: Path = DEFAULT_ANALYSIS_OUTPUT_DIR / "evse_status_changes.csv",
    radius_m: float = 10000.0,
    max_nearby: int = 6,
    provider_tiers: Sequence[str] = DEFAULT_ALLOWED_PROVIDER_TIERS,
    start_date: date | None = None,
    end_date: date | None = None,
) -> dict[str, Any]:
    effective_config = config or AppConfig()
    station_id = _parse_station_reference(station_reference)
    station_catalog = {row.station_id: row for row in load_station_records(effective_config.chargers_csv_path)}

    provider_daily_rows = [
        row
        for row in _read_csv_rows(provider_daily_summary_path)
        if (start_date is None or date.fromisoformat(row["archive_date"]) >= start_date)
        and (end_date is None or date.fromisoformat(row["archive_date"]) <= end_date)
    ]
    station_daily_rows = [
        row
        for row in _read_csv_rows(station_daily_summary_path)
        if (start_date is None or date.fromisoformat(row["archive_date"]) >= start_date)
        and (end_date is None or date.fromisoformat(row["archive_date"]) <= end_date)
    ]
    selections, station_candidate_rows = _select_best_station_candidates(
        target_station_id=station_id,
        radius_m=radius_m,
        max_nearby=max_nearby,
        allowed_provider_tiers=set(provider_tiers),
        station_catalog=station_catalog,
        provider_daily_rows=provider_daily_rows,
        station_daily_rows=station_daily_rows,
    )
    target_selection = next(selection for selection in selections if selection.is_target_station)
    selected_pairs = {(selection.station_id, selection.provider_uid) for selection in selections}

    status_rows_by_pair: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    for row in _iter_filtered_status_changes(evse_status_changes_path, selected_pairs=selected_pairs):
        key = (str(row.get("station_id") or ""), str(row.get("provider_uid") or ""))
        status_rows_by_pair[key].append(row)

    nearby_catalog_rows: list[dict[str, Any]] = []
    nearby_station_timeline_rows: list[dict[str, Any]] = []
    for selection in selections:
        latest_station_row = selection.latest_station_row
        nearby_catalog_rows.append(
            {
                "station_id": selection.station_id,
                "is_target_station": 1 if selection.is_target_station else 0,
                "chosen_comparison_bucket": _comparison_bucket(
                    is_target_station=selection.is_target_station,
                    provider_tier=selection.provider_tier,
                ),
                "distance_m": selection.distance_m,
                "operator": selection.operator,
                "address": selection.address,
                "postcode": selection.postcode,
                "city": selection.city,
                "lat": selection.lat,
                "lon": selection.lon,
                "charging_points_count": selection.charging_points_count,
                "max_power_kw": selection.max_power_kw,
                "chosen_provider_uid": selection.provider_uid,
                "chosen_provider_tier": selection.provider_tier,
                "chosen_provider_reason": selection.provider_reason,
                "chosen_provider_mapped_observation_ratio": selection.provider_mapped_observation_ratio,
                "latest_archive_date": selection.latest_archive_date,
                "latest_station_availability_status": latest_station_row.get("station_availability_status") or "",
                "latest_station_any_out_of_order": _int_value(latest_station_row.get("station_any_out_of_order")),
                "latest_station_all_evses_out_of_order": _int_value(latest_station_row.get("station_all_evses_out_of_order")),
                "latest_free_evses": _int_value(latest_station_row.get("free_evses")),
                "latest_occupied_evses": _int_value(latest_station_row.get("occupied_evses")),
                "latest_out_of_order_evses": _int_value(latest_station_row.get("out_of_order_evses")),
                "latest_unknown_evses": _int_value(latest_station_row.get("unknown_evses")),
                "latest_event_timestamp": latest_station_row.get("latest_event_timestamp") or "",
            }
        )
        nearby_station_timeline_rows.extend(
            _build_station_interval_rows(
                selection=selection,
                status_rows=status_rows_by_pair.get((selection.station_id, selection.provider_uid), []),
            )
        )

    target_evse_rows = sorted(
        [
            {
                "station_id": row["station_id"],
                "provider_uid": row["provider_uid"],
                "provider_tier": target_selection.provider_tier,
                "provider_evse_id": row["provider_evse_id"],
                "site_id": row["site_id"],
                "station_ref": row["station_ref"],
                "archive_date": row["archive_date"],
                "status_started_at": row["status_started_at"],
                "next_status_started_at": row["next_status_started_at"],
                "duration_seconds": _int_value(row.get("duration_seconds")),
                "is_open_interval": _int_value(row.get("is_open_interval")),
                "availability_status": row["availability_status"],
                "operational_status": row["operational_status"],
                "message_kind": row["message_kind"],
                "message_timestamp": row["message_timestamp"],
                "source_observed_at": row["source_observed_at"],
                "payload_sha256": row["payload_sha256"],
            }
            for row in status_rows_by_pair.get((target_selection.station_id, target_selection.provider_uid), [])
        ],
        key=lambda row: (
            _sort_timestamp(str(row.get("status_started_at") or "")),
            str(row.get("provider_evse_id") or ""),
        ),
    )
    target_station_timeline_rows = [
        row for row in nearby_station_timeline_rows if row["station_id"] == target_selection.station_id
    ]
    nearby_catalog_rows.sort(key=lambda row: (0 if _int_value(row["is_target_station"]) else 1, _float_value(row["distance_m"]), row["station_id"]))
    nearby_station_timeline_rows.sort(
        key=lambda row: (_sort_timestamp(str(row.get("interval_started_at") or "")), _float_value(row.get("distance_m")), row["station_id"])
    )

    station_output_dir = output_dir / _safe_slug(target_selection.station_id)
    target_evse_path = station_output_dir / "target_evse_status_timeline.csv"
    target_evse_summary_path = station_output_dir / "target_evse_status_summary.csv"
    target_station_path = station_output_dir / "target_station_status_timeline.csv"
    nearby_station_path = station_output_dir / "nearby_station_status_timeline.csv"
    nearby_station_summary_path = station_output_dir / "nearby_station_status_summary.csv"
    nearby_catalog_path = station_output_dir / "nearby_stations.csv"
    candidate_catalog_path = station_output_dir / "nearby_station_candidates.csv"
    summary_path = station_output_dir / "summary.md"

    target_evse_summary_rows = _build_target_evse_summary_rows(target_evse_rows)
    nearby_station_summary_rows = _build_station_status_summary_rows(nearby_station_timeline_rows)

    _write_csv(target_evse_path, TARGET_EVSE_TIMELINE_FIELDS, target_evse_rows)
    _write_csv(target_evse_summary_path, TARGET_EVSE_SUMMARY_FIELDS, target_evse_summary_rows)
    _write_csv(target_station_path, STATION_TIMELINE_FIELDS, target_station_timeline_rows)
    _write_csv(nearby_station_path, STATION_TIMELINE_FIELDS, nearby_station_timeline_rows)
    _write_csv(nearby_station_summary_path, STATION_STATUS_SUMMARY_FIELDS, nearby_station_summary_rows)
    _write_csv(nearby_catalog_path, NEARBY_STATIONS_FIELDS, nearby_catalog_rows)
    _write_csv(candidate_catalog_path, STATION_CANDIDATE_FIELDS, station_candidate_rows)
    _write_summary_markdown(
        summary_path,
        station_reference=station_reference,
        target_selection=target_selection,
        nearby_rows=nearby_catalog_rows,
        target_evse_rows=target_evse_rows,
        target_evse_summary_rows=target_evse_summary_rows,
        target_station_timeline_rows=target_station_timeline_rows,
        nearby_station_summary_rows=nearby_station_summary_rows,
        nearby_station_timeline_rows=nearby_station_timeline_rows,
        station_candidate_rows=station_candidate_rows,
        allowed_provider_tiers=provider_tiers,
        radius_m=radius_m,
    )

    return {
        "station_id": target_selection.station_id,
        "chosen_provider_uid": target_selection.provider_uid,
        "chosen_provider_tier": target_selection.provider_tier,
        "nearby_station_count": max(0, len(nearby_catalog_rows) - 1),
        "nearby_primary_station_count": sum(
            1
            for row in nearby_catalog_rows
            if not _int_value(row.get("is_target_station"))
            and str(row.get("chosen_comparison_bucket") or "") == "primary"
        ),
        "nearby_review_station_count": sum(
            1
            for row in nearby_catalog_rows
            if not _int_value(row.get("is_target_station"))
            and str(row.get("chosen_comparison_bucket") or "") == "review"
        ),
        "station_candidate_row_count": len(station_candidate_rows),
        "ambiguous_station_count": len(
            {str(row.get("station_id") or "") for row in station_candidate_rows if _int_value(row.get("candidate_count_for_station")) > 1}
        ),
        "target_evse_interval_count": len(target_evse_rows),
        "target_station_interval_count": len(target_station_timeline_rows),
        "nearby_station_interval_count": len(nearby_station_timeline_rows),
        "output_dir": str(station_output_dir.resolve()),
        "outputs": {
            "summary": str(summary_path.resolve()),
            "nearby_stations": str(nearby_catalog_path.resolve()),
            "nearby_station_candidates": str(candidate_catalog_path.resolve()),
            "target_evse_status_timeline": str(target_evse_path.resolve()),
            "target_evse_status_summary": str(target_evse_summary_path.resolve()),
            "target_station_status_timeline": str(target_station_path.resolve()),
            "nearby_station_status_timeline": str(nearby_station_path.resolve()),
            "nearby_station_status_summary": str(nearby_station_summary_path.resolve()),
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create EVSE and nearby-station time series from archive analysis CSVs")
    parser.add_argument("--station", required=True, help="Internal station id or woladen station URL")
    parser.add_argument(
        "--analysis-output-dir",
        type=Path,
        default=DEFAULT_ANALYSIS_OUTPUT_DIR,
        help="Directory containing provider_daily_summary.csv, station_daily_summary.csv, and evse_status_changes.csv",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_STATION_OUTPUT_DIR,
        help="Directory for generated station time-series files",
    )
    parser.add_argument("--radius-meters", type=float, default=10000.0, help="Nearby-station search radius in meters")
    parser.add_argument("--max-nearby", type=int, default=6, help="Maximum number of nearby stations to include")
    parser.add_argument(
        "--provider-tiers",
        default="eligible,review",
        help="Comma-separated provider tiers to include for nearby stations",
    )
    parser.add_argument("--start-date", type=_parse_archive_date, default=None, help="Inclusive YYYY-MM-DD filter")
    parser.add_argument("--end-date", type=_parse_archive_date, default=None, help="Inclusive YYYY-MM-DD filter")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    provider_tiers = _parse_provider_tiers(args.provider_tiers)
    analysis_output_dir = args.analysis_output_dir
    result = run_station_timeseries(
        station_reference=args.station,
        output_dir=args.output_dir,
        provider_daily_summary_path=analysis_output_dir / "provider_daily_summary.csv",
        station_daily_summary_path=analysis_output_dir / "station_daily_summary.csv",
        evse_status_changes_path=analysis_output_dir / "evse_status_changes.csv",
        radius_m=args.radius_meters,
        max_nearby=args.max_nearby,
        provider_tiers=provider_tiers,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
