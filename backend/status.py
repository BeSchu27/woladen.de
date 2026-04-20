from __future__ import annotations

import json
import re
from collections.abc import Iterable
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any

from .config import AppConfig
from .receipt_queue import ReceiptQueue
from .store import LiveStore

LATEST_ATTRIBUTE_FIELDS = (
    "availability_status",
    "operational_status",
    "price_display",
    "price_currency",
    "price_energy_eur_kwh_min",
    "price_energy_eur_kwh_max",
    "price_time_eur_min_min",
    "price_time_eur_min_max",
    "next_available_charging_slots",
    "supplemental_facility_status",
)

_FEATURE_RE = re.compile(r'"type"\s*:\s*"Feature"')
_STATION_ID_RE = re.compile(r'"station_id"\s*:\s*"((?:\\.|[^"\\])*)"')
_GEOJSON_SCAN_CHUNK_SIZE = 1024 * 1024
_GEOJSON_SCAN_CARRY_SIZE = 512


def _scan_bundle_geojson(path: Path) -> dict[str, Any]:
    feature_count = 0
    station_ids: set[str] = set()
    carry = ""

    with path.open("r", encoding="utf-8") as handle:
        while True:
            chunk = handle.read(_GEOJSON_SCAN_CHUNK_SIZE)
            if not chunk:
                break

            text = carry + chunk
            if len(text) <= _GEOJSON_SCAN_CARRY_SIZE:
                carry = text
                continue

            scan_text = text[:-_GEOJSON_SCAN_CARRY_SIZE]
            feature_count += sum(1 for _ in _FEATURE_RE.finditer(scan_text))
            for match in _STATION_ID_RE.finditer(scan_text):
                station_id = str(json.loads(f'"{match.group(1)}"') or "").strip()
                if station_id:
                    station_ids.add(station_id)
            carry = text[-_GEOJSON_SCAN_CARRY_SIZE:]

    if carry:
        feature_count += sum(1 for _ in _FEATURE_RE.finditer(carry))
        for match in _STATION_ID_RE.finditer(carry):
            station_id = str(json.loads(f'"{match.group(1)}"') or "").strip()
            if station_id:
                station_ids.add(station_id)

    return {
        "feature_count": feature_count,
        "station_ids": frozenset(station_ids),
        "unique_station_count": len(station_ids),
        "duplicate_station_id_count": feature_count - len(station_ids),
    }


@lru_cache(maxsize=4)
def _cached_bundle_station_summary(path_text: str, mtime_ns: int, size_bytes: int) -> dict[str, Any]:
    del mtime_ns, size_bytes
    return _scan_bundle_geojson(Path(path_text))


def load_bundle_station_summary(path: Path) -> dict[str, Any]:
    stat = path.stat()
    cached = _cached_bundle_station_summary(str(path), stat.st_mtime_ns, stat.st_size)
    return {
        "feature_count": int(cached["feature_count"]),
        "station_ids": set(cached["station_ids"]),
        "unique_station_count": int(cached["unique_station_count"]),
        "duplicate_station_id_count": int(cached["duplicate_station_id_count"]),
    }


def _parse_iso(value: str | None) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _max_timestamp(values: Iterable[str]) -> str | None:
    best_text = ""
    best_dt: datetime | None = None
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        parsed = _parse_iso(text)
        if parsed is None:
            if not best_text:
                best_text = text
            continue
        if best_dt is None or parsed > best_dt:
            best_dt = parsed
            best_text = text
    return best_text or None


def _pick_newer_timestamp(
    current_timestamp: str | None,
    current_station_id: str | None,
    candidate_timestamp: str | None,
    candidate_station_id: str | None,
) -> tuple[str | None, str | None]:
    current_text = str(current_timestamp or "").strip()
    candidate_text = str(candidate_timestamp or "").strip()
    candidate_station = str(candidate_station_id or "").strip() or None
    current_station = str(current_station_id or "").strip() or None

    if not candidate_text:
        return current_text or None, current_station
    if not current_text:
        return candidate_text, candidate_station

    current_dt = _parse_iso(current_text)
    candidate_dt = _parse_iso(candidate_text)
    if current_dt is not None and candidate_dt is not None:
        if candidate_dt > current_dt:
            return candidate_text, candidate_station
        if candidate_dt < current_dt:
            return current_text, current_station
        if candidate_station and (not current_station or candidate_station < current_station):
            return candidate_text, candidate_station
        return current_text, current_station

    if candidate_text > current_text:
        return candidate_text, candidate_station
    if candidate_text < current_text:
        return current_text, current_station
    if candidate_station and (not current_station or candidate_station < current_station):
        return candidate_text, candidate_station
    return current_text, current_station


def _fetch_distinct_nonempty_values(conn, table_name: str, column_name: str = "station_id") -> set[str]:
    rows = conn.execute(
        f"SELECT DISTINCT {column_name} FROM {table_name} WHERE COALESCE({column_name}, '') <> ''"
    ).fetchall()
    return {str(row[column_name]) for row in rows if str(row[column_name]).strip()}


def _ensure_provider_aggregate(provider_aggregates: dict[str, dict[str, Any]], provider_uid: str) -> dict[str, Any]:
    return provider_aggregates.setdefault(
        provider_uid,
        {
            "full_station_ids": set(),
            "bundle_station_ids": set(),
            "station_ids_not_in_full_registry": set(),
            "observation_rows": 0,
            "last_received_update_at_values": [],
            "last_source_update_at_values": [],
            "latest_updated_station_id": None,
            "latest_updated_station_timestamp": None,
            "latest_attribute_updates": {},
        },
    )


def _has_attribute_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, set, dict)):
        return bool(value)
    return True


def _decode_live_json_field(value: Any) -> Any:
    if isinstance(value, list):
        return value
    text = str(value or "").strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return value
    if parsed in (None, "", [], {}):
        return []
    return parsed


def _normalize_descriptive_price_value(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    try:
        number = float(text)
    except (TypeError, ValueError):
        return text
    return f"{number:.6f}".rstrip("0").rstrip(".")


def _update_latest_attribute(
    aggregate: dict[str, Any],
    *,
    attribute_name: str,
    value: Any,
    station_id: str,
    fetched_at: str,
    source_observed_at: str,
) -> None:
    if not _has_attribute_value(value):
        return

    current = aggregate["latest_attribute_updates"].get(attribute_name)
    next_timestamp, next_station_id = _pick_newer_timestamp(
        current.get("fetched_at") if current else None,
        current.get("station_id") if current else None,
        fetched_at,
        station_id,
    )
    if current and next_timestamp == current.get("fetched_at") and next_station_id == current.get("station_id"):
        return

    aggregate["latest_attribute_updates"][attribute_name] = {
        "station_id": next_station_id,
        "fetched_at": next_timestamp,
        "source_observed_at": source_observed_at or None,
        "value": value,
    }


def build_bundle_live_status_report(
    *,
    store: LiveStore,
    geojson_path: Path,
    receipt_queue_stats: dict[str, Any] | None = None,
) -> dict[str, Any]:
    bundle_summary = load_bundle_station_summary(geojson_path)
    bundle_station_ids = bundle_summary["station_ids"]
    recent_updates_by_provider = store.list_recent_provider_updates(limit_per_provider=10)

    provider_aggregates: dict[str, dict[str, Any]] = {}
    observed_full_station_ids: set[str] = set()
    observed_bundle_station_ids: set[str] = set()
    observed_station_ids_not_in_full_registry: set[str] = set()
    observed_station_ids_not_in_bundle: set[str] = set()
    latest_received_update_at: str | None = None
    latest_updated_station_id: str | None = None

    with store.connection() as conn:
        full_station_ids = _fetch_distinct_nonempty_values(conn, "stations")
        current_full_station_ids = _fetch_distinct_nonempty_values(conn, "station_current_state") & full_station_ids
        current_bundle_station_ids = current_full_station_ids & bundle_station_ids
        current_state_station_ids_not_in_full_registry = (
            _fetch_distinct_nonempty_values(conn, "station_current_state") - full_station_ids
        )
        current_state_station_ids_not_in_bundle = (
            _fetch_distinct_nonempty_values(conn, "station_current_state") - bundle_station_ids
        )

        current_rows = conn.execute(
            """
            SELECT
                provider_uid,
                provider_evse_id,
                station_id,
                fetched_at,
                source_observed_at,
                availability_status,
                operational_status,
                price_display,
                price_currency,
                price_energy_eur_kwh_min,
                price_energy_eur_kwh_max,
                price_time_eur_min_min,
                price_time_eur_min_max,
                next_available_charging_slots,
                supplemental_facility_status
            FROM evse_current_state
            WHERE station_id <> ''
            ORDER BY provider_uid, fetched_at DESC, station_id, provider_evse_id
            """
        ).fetchall()

        providers = [dict(row) for row in conn.execute("SELECT * FROM providers ORDER BY provider_uid").fetchall()]

    for row in current_rows:
        provider_uid = str(row["provider_uid"])
        station_id = str(row["station_id"])
        aggregate = _ensure_provider_aggregate(provider_aggregates, provider_uid)
        aggregate["observation_rows"] += 1
        if station_id not in full_station_ids:
            observed_station_ids_not_in_full_registry.add(station_id)
            aggregate["station_ids_not_in_full_registry"].add(station_id)
            continue

        observed_full_station_ids.add(station_id)
        aggregate["full_station_ids"].add(station_id)
        if station_id in bundle_station_ids:
            observed_bundle_station_ids.add(station_id)
            aggregate["bundle_station_ids"].add(station_id)
        else:
            observed_station_ids_not_in_bundle.add(station_id)

        last_received_update_at_value = str(row["fetched_at"] or "").strip()
        if last_received_update_at_value:
            aggregate["last_received_update_at_values"].append(last_received_update_at_value)
            (
                aggregate["latest_updated_station_timestamp"],
                aggregate["latest_updated_station_id"],
            ) = _pick_newer_timestamp(
                aggregate.get("latest_updated_station_timestamp"),
                aggregate.get("latest_updated_station_id"),
                last_received_update_at_value,
                station_id,
            )
            latest_received_update_at, latest_updated_station_id = _pick_newer_timestamp(
                latest_received_update_at,
                latest_updated_station_id,
                last_received_update_at_value,
                station_id,
            )
        source_observed_at = str(row["source_observed_at"] or "").strip()
        if source_observed_at:
            aggregate["last_source_update_at_values"].append(source_observed_at)
        fetched_at = last_received_update_at_value
        for attribute_name in LATEST_ATTRIBUTE_FIELDS:
            value = row[attribute_name]
            if attribute_name in {"next_available_charging_slots", "supplemental_facility_status"}:
                value = _decode_live_json_field(value)
            elif attribute_name in {"price_energy_eur_kwh_min", "price_energy_eur_kwh_max"}:
                value = _normalize_descriptive_price_value(value)
            _update_latest_attribute(
                aggregate,
                attribute_name=attribute_name,
                value=value,
                station_id=station_id,
                fetched_at=fetched_at,
                source_observed_at=source_observed_at,
            )

    full_station_count = int(len(full_station_ids))
    bundle_station_count = int(bundle_summary["unique_station_count"])
    observed_full_station_ids_outside_bundle = observed_full_station_ids - bundle_station_ids
    current_full_station_ids_outside_bundle = current_full_station_ids - bundle_station_ids
    provider_station_count_sum = int(sum(len(item["full_station_ids"]) for item in provider_aggregates.values()))
    provider_bundle_station_count_sum = int(sum(len(item["bundle_station_ids"]) for item in provider_aggregates.values()))

    providers_by_uid = {str(provider["provider_uid"]): provider for provider in providers}
    provider_items: list[dict[str, Any]] = []
    for provider_uid in sorted(set(providers_by_uid) | set(provider_aggregates)):
        provider = providers_by_uid.get(provider_uid, {})
        aggregate = provider_aggregates.get(provider_uid, {})
        full_provider_station_ids = aggregate.get("full_station_ids", set())
        bundle_provider_station_ids = aggregate.get("bundle_station_ids", set())
        last_received_update_at = _max_timestamp(aggregate.get("last_received_update_at_values", []))
        last_source_update_at = _max_timestamp(aggregate.get("last_source_update_at_values", []))
        provider_items.append(
            {
                "provider_uid": provider_uid,
                "display_name": str(provider.get("display_name") or ""),
                "publisher": str(provider.get("publisher") or ""),
                "enabled": bool(provider.get("enabled")) if provider else False,
                "fetch_kind": str(provider.get("fetch_kind") or ""),
                "delta_delivery": bool(provider.get("delta_delivery")) if provider else False,
                "delivery_mode": str(provider.get("delivery_mode") or "poll_only"),
                "push_fallback_after_seconds": int(provider.get("push_fallback_after_seconds") or 0) or None,
                "stations_with_any_live_observation": len(full_provider_station_ids),
                "stations_with_any_live_observation_in_bundle": len(bundle_provider_station_ids),
                "observation_rows": int(aggregate.get("observation_rows", 0) or 0),
                "coverage_ratio": (len(full_provider_station_ids) / full_station_count) if full_station_count else 0.0,
                "bundle_coverage_ratio": (
                    len(bundle_provider_station_ids) / bundle_station_count
                ) if bundle_station_count else 0.0,
                "station_ids_outside_bundle": len(full_provider_station_ids - bundle_station_ids),
                "station_ids_not_in_full_registry": len(aggregate.get("station_ids_not_in_full_registry", set())),
                "last_received_update_at": last_received_update_at,
                "last_source_update_at": last_source_update_at,
                "latest_updated_station_id": aggregate.get("latest_updated_station_id"),
                "latest_attribute_updates": aggregate.get("latest_attribute_updates", {}),
                "last_polled_at": str(provider.get("last_polled_at") or "") or None,
                "last_result": str(provider.get("last_result") or "") or None,
                "last_push_received_at": str(provider.get("last_push_received_at") or "") or None,
                "last_push_result": str(provider.get("last_push_result") or "") or None,
                "recent_updates": recent_updates_by_provider.get(provider_uid, []),
            }
        )

    provider_items.sort(
        key=lambda item: (
            -int(item["stations_with_any_live_observation"]),
            -int(item["observation_rows"]),
            str(item["provider_uid"]),
        )
    )

    return {
        "db_path": str(store.config.db_path.resolve()),
        "geojson_path": str(geojson_path.resolve()),
        "station_count": full_station_count,
        "full_registry_station_count": full_station_count,
        "bundle_feature_count": int(bundle_summary["feature_count"]),
        "bundle_station_count": bundle_station_count,
        "bundle_duplicate_station_id_count": int(bundle_summary["duplicate_station_id_count"]),
        "stations_with_any_live_observation": len(observed_full_station_ids),
        "stations_with_current_live_state": len(current_full_station_ids),
        "coverage_ratio": (len(observed_full_station_ids) / full_station_count) if full_station_count else 0.0,
        "bundle_stations_with_any_live_observation": len(observed_bundle_station_ids),
        "bundle_stations_with_current_live_state": len(current_bundle_station_ids),
        "bundle_coverage_ratio": (
            len(observed_bundle_station_ids) / bundle_station_count
        ) if bundle_station_count else 0.0,
        "last_received_update_at": latest_received_update_at,
        "latest_updated_station_id": latest_updated_station_id,
        "last_source_update_at": _max_timestamp(
            item["last_source_update_at"]
            for item in provider_items
            if item.get("last_source_update_at")
        ),
        "providers_with_any_live_observation": sum(
            1 for item in provider_items if int(item["stations_with_any_live_observation"]) > 0
        ),
        "providers_with_any_live_observation_in_bundle": sum(
            1 for item in provider_items if int(item["stations_with_any_live_observation_in_bundle"]) > 0
        ),
        "observed_station_ids_not_in_full_registry": len(observed_station_ids_not_in_full_registry),
        "current_state_station_ids_not_in_full_registry": len(current_state_station_ids_not_in_full_registry),
        "observed_station_ids_not_in_bundle": len(observed_station_ids_not_in_bundle),
        "current_state_station_ids_not_in_bundle": len(current_state_station_ids_not_in_bundle),
        "stations_with_any_live_observation_outside_bundle": len(observed_full_station_ids_outside_bundle),
        "stations_with_current_live_state_outside_bundle": len(current_full_station_ids_outside_bundle),
        "provider_station_count_sum": provider_station_count_sum,
        "provider_station_overlap_excess": provider_station_count_sum - len(observed_full_station_ids),
        "provider_bundle_station_count_sum": provider_bundle_station_count_sum,
        "provider_bundle_station_overlap_excess": provider_bundle_station_count_sum - len(observed_bundle_station_ids),
        "receipt_queue": receipt_queue_stats or {},
        "providers": provider_items,
    }


def build_status_report(config: AppConfig | None = None, store: LiveStore | None = None) -> dict[str, Any]:
    effective_config = config or AppConfig()
    effective_store = store or LiveStore(effective_config)
    effective_store.initialize()
    return build_bundle_live_status_report(
        store=effective_store,
        geojson_path=effective_config.chargers_geojson_path,
        receipt_queue_stats=ReceiptQueue(effective_config).stats(),
    )
