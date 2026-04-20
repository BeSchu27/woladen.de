#!/usr/bin/env python3
"""Patch one provider's static details onto the existing fast-charger bundle."""

from __future__ import annotations

import argparse
import copy
import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd
import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.build_data import (
    AfirStaticPublication,
    FAST_CSV_PATH,
    FAST_GEOJSON_PATH,
    MOBILITHEK_AFIR_PROVIDER_CONFIG_PATH,
    OPERATORS_JSON_PATH,
    STATIC_DETAIL_FIELDS,
    SUMMARY_JSON_PATH,
    apply_static_publication_payload,
    build_operator_list,
    build_static_station_indexes,
    dataframe_to_geojson,
    detail_nonempty_score,
    dumps_minified_json,
    dumps_pretty_json,
    fetch_mobilithek_access_token,
    fetch_mobilithek_static_payload_with_probe,
    finalize_bundle_geojson,
    load_json_object,
    load_static_subscription_ids,
    normalize_optional_text,
    probe_mobilithek_file_access,
    resolve_content_access_url,
    sanitize_json_value,
    should_attempt_static_payload_fetch,
    static_source_display_name,
    utc_now,
)

SITE_DATA_DIR = REPO_ROOT / "site" / "data"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Patch one provider's static details onto data/chargers_fast.csv "
            "without rerunning occupancy or amenity enrichment."
        )
    )
    parser.add_argument("provider_uid", help="Provider uid from mobilithek_afir_provider_configs.json")
    parser.add_argument("--fast-csv", type=Path, default=FAST_CSV_PATH)
    parser.add_argument("--summary-json", type=Path, default=SUMMARY_JSON_PATH)
    parser.add_argument("--geojson", type=Path, default=FAST_GEOJSON_PATH)
    parser.add_argument("--operators-json", type=Path, default=OPERATORS_JSON_PATH)
    parser.add_argument(
        "--sync-site-data",
        action="store_true",
        help="Also update site/data/{chargers_fast.geojson,operators.json,summary.json}",
    )
    return parser.parse_args()


def load_provider_static_publication(provider_uid: str) -> tuple[AfirStaticPublication, str]:
    payload = load_json_object(MOBILITHEK_AFIR_PROVIDER_CONFIG_PATH)
    for provider in payload.get("providers") or []:
        if not isinstance(provider, dict):
            continue
        if str(provider.get("uid") or "").strip() != provider_uid:
            continue
        static_feed = ((provider.get("feeds") or {}).get("static")) or {}
        publication_id = str(static_feed.get("publication_id") or "").strip()
        if not publication_id:
            raise RuntimeError(f"{provider_uid}: missing_static_publication_id")
        publication = AfirStaticPublication(
            uid=f"mobilithek_{provider_uid}_static",
            publication_id=publication_id,
            title=normalize_optional_text(static_feed.get("title")),
            publisher=normalize_optional_text(provider.get("publisher") or provider.get("display_name") or provider_uid),
            access_mode=str(static_feed.get("access_mode") or "").strip() or "auth",
            data_model=normalize_optional_text(static_feed.get("data_model")),
            access_url=resolve_content_access_url(static_feed.get("content_data") or {}),
        )
        return publication, publication_id
    raise RuntimeError(f"{provider_uid}: provider_not_found")


def station_detail_score(row: pd.Series) -> int:
    return detail_nonempty_score({field: row.get(field) for field in STATIC_DETAIL_FIELDS})


def build_patch_stats(publication: AfirStaticPublication) -> dict[str, Any]:
    return {
        "offers_discovered": 1,
        "static_offers_considered": 1,
        "sources_used": 0,
        "matched_sites": 0,
        "matched_stations": 0,
        "stations_with_price": 0,
        "stations_with_opening_hours": 0,
        "stations_with_helpdesk": 0,
        "errors": [],
        "sources": [],
        "publication_uid": publication.uid,
    }


def apply_provider_patch(
    df: pd.DataFrame,
    *,
    publication: AfirStaticPublication,
    static_subscription_id: str,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    patched = df.copy()
    row_lookup = {
        str(row.get("station_id") or ""): row_index
        for row_index, row in patched.reset_index(drop=True).iterrows()
        if str(row.get("station_id") or "")
    }
    detail_scores = {
        station_id: station_detail_score(patched.iloc[row_index])
        for station_id, row_index in row_lookup.items()
    }
    station_grid, station_by_id, evse_to_station_ids = build_static_station_indexes(patched)
    stats = build_patch_stats(publication)

    session = requests.Session()
    try:
        access_token = fetch_mobilithek_access_token(session)
    except Exception as exc:  # pragma: no cover - network/runtime branch
        access_token = None
        stats["errors"].append(f"mobilithek_auth_failed: {exc}")

    access_probe = probe_mobilithek_file_access(
        session,
        access_token=access_token,
        publication_id=publication.publication_id,
    )
    if not should_attempt_static_payload_fetch(
        access_probe,
        subscription_id=static_subscription_id,
        fallback_url=publication.access_url,
    ):
        stats["sources"].append(
            {
                "uid": publication.uid,
                "publication_id": publication.publication_id,
                "title": publication.title,
                "publisher": publication.publisher,
                "access_mode": publication.access_mode,
                "status": "not_accessible",
                "matched_sites": 0,
                "matched_stations": 0,
            }
        )
        return patched, stats

    payload, access_mode_used, fetch_error = fetch_mobilithek_static_payload_with_probe(
        session,
        publication_id=publication.publication_id,
        preferred_access_mode=publication.access_mode,
        access_token=access_token,
        subscription_id=static_subscription_id,
        fallback_url=publication.access_url,
    )
    if payload is None:
        stats["errors"].append(f"{publication.publication_id}: {fetch_error}")
        stats["sources"].append(
            {
                "uid": publication.uid,
                "publication_id": publication.publication_id,
                "title": publication.title,
                "publisher": publication.publisher,
                "access_mode": access_mode_used,
                "status": "fetch_failed",
                "error": fetch_error,
                "matched_sites": 0,
                "matched_stations": 0,
            }
        )
        return patched, stats

    apply_static_publication_payload(
        patched,
        publication=publication,
        payload=payload,
        access_mode_used=access_mode_used,
        row_lookup=row_lookup,
        detail_scores=detail_scores,
        station_grid=station_grid,
        station_by_id=station_by_id,
        evse_to_station_ids=evse_to_station_ids,
        stats=stats,
    )
    return patched, stats


def update_summary(
    summary: dict[str, Any],
    *,
    patched_df: pd.DataFrame,
    provider_uid: str,
    publication_id: str,
    provider_stats: dict[str, Any],
) -> dict[str, Any]:
    updated = copy.deepcopy(summary)
    finished_at = utc_now().replace(microsecond=0).isoformat()
    updated.setdefault("run", {})
    updated["run"]["started_at"] = finished_at
    updated["run"]["finished_at"] = finished_at

    records = updated.setdefault("records", {})
    records["fast_chargers_total"] = int(len(patched_df))
    records["stations_with_static_details"] = int(
        patched_df["detail_source_uid"].fillna("").astype(str).str.strip().ne("").sum()
    )
    records["stations_with_price"] = int(
        patched_df["price_display"].fillna("").astype(str).str.strip().ne("").sum()
    )
    records["stations_with_opening_hours"] = int(
        patched_df["opening_hours_display"].fillna("").astype(str).str.strip().ne("").sum()
    )
    records["stations_with_helpdesk"] = int(
        patched_df["helpdesk_phone"].fillna("").astype(str).str.strip().ne("").sum()
    )
    if "stations_with_amenities" not in records:
        records["stations_with_amenities"] = int((patched_df["amenities_total"] > 0).sum())

    lookup = updated.setdefault("static_detail_lookup", {})
    lookup["stations_with_price"] = records["stations_with_price"]
    lookup["stations_with_opening_hours"] = records["stations_with_opening_hours"]
    lookup["stations_with_helpdesk"] = records["stations_with_helpdesk"]

    old_sources = list(lookup.get("sources") or [])
    old_source = next(
        (
            item
            for item in old_sources
            if isinstance(item, dict) and str(item.get("uid") or "").strip() == f"mobilithek_{provider_uid}_static"
        ),
        None,
    )
    new_source = next((item for item in provider_stats.get("sources") or [] if isinstance(item, dict)), None)
    if new_source is None:
        return updated

    replaced_sources: list[dict[str, Any]] = []
    replaced = False
    for item in old_sources:
        if not isinstance(item, dict):
            continue
        if str(item.get("uid") or "").strip() == new_source["uid"]:
            replaced_sources.append(new_source)
            replaced = True
        else:
            replaced_sources.append(item)
    if not replaced:
        replaced_sources.append(new_source)
    lookup["sources"] = replaced_sources

    old_match_sites = int((old_source or {}).get("matched_sites") or 0)
    old_match_stations = int((old_source or {}).get("matched_stations") or 0)
    new_match_sites = int(new_source.get("matched_sites") or 0)
    new_match_stations = int(new_source.get("matched_stations") or 0)
    old_used = 1 if old_match_stations > 0 else 0
    new_used = 1 if new_match_stations > 0 else 0

    lookup["sources_used"] = max(0, int(lookup.get("sources_used") or 0) - old_used + new_used)
    lookup["matched_sites"] = max(0, int(lookup.get("matched_sites") or 0) - old_match_sites + new_match_sites)
    lookup["matched_stations"] = max(
        0,
        int(lookup.get("matched_stations") or 0) - old_match_stations + new_match_stations,
    )

    errors = [
        item
        for item in (lookup.get("errors") or [])
        if publication_error_key(provider_uid, publication_id) not in str(item)
    ]
    errors.extend(provider_stats.get("errors") or [])
    lookup["errors"] = errors
    return updated


def publication_error_key(provider_uid: str, publication_id: str) -> str:
    return publication_id or provider_uid or ""


def write_site_data_file(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(sanitize_json_value(payload), ensure_ascii=False, separators=(",", ":"), allow_nan=False),
        encoding="utf-8",
    )


def main() -> None:
    args = parse_args()
    provider_uid = str(args.provider_uid or "").strip()
    if not provider_uid:
        raise SystemExit("provider_uid is required")

    publication, _ = load_provider_static_publication(provider_uid)
    static_subscription_ids = load_static_subscription_ids()
    static_subscription_id = static_subscription_ids.get(provider_uid, "")

    fast_df = pd.read_csv(args.fast_csv)
    summary = json.loads(args.summary_json.read_text(encoding="utf-8"))

    patched_df, provider_stats = apply_provider_patch(
        fast_df,
        publication=publication,
        static_subscription_id=static_subscription_id,
    )

    dataframe_changed = not fast_df.equals(patched_df)
    updated_summary = update_summary(
        summary,
        patched_df=patched_df,
        provider_uid=provider_uid,
        publication_id=publication.publication_id,
        provider_stats=provider_stats,
    )
    summary_changed = summary != updated_summary

    if dataframe_changed:
        args.fast_csv.write_text(patched_df.to_csv(index=False), encoding="utf-8")
        source_meta = updated_summary.get("source") or {}
        geojson = finalize_bundle_geojson(dataframe_to_geojson(patched_df, source_meta))
        args.geojson.write_text(dumps_minified_json(geojson), encoding="utf-8")

    if summary_changed:
        args.summary_json.write_text(dumps_pretty_json(updated_summary), encoding="utf-8")

    operators_payload = None
    if dataframe_changed and args.operators_json.exists():
        operators_payload = build_operator_list(
            patched_df,
            min_stations=int((updated_summary.get("operators") or {}).get("min_stations") or 100),
        )
        args.operators_json.write_text(dumps_minified_json(operators_payload), encoding="utf-8")

    if args.sync_site_data:
        if dataframe_changed:
            geojson_payload = json.loads(args.geojson.read_text(encoding="utf-8"))
            write_site_data_file(SITE_DATA_DIR / args.geojson.name, geojson_payload)
        if summary_changed:
            write_site_data_file(SITE_DATA_DIR / args.summary_json.name, updated_summary)
        if operators_payload is not None:
            write_site_data_file(SITE_DATA_DIR / args.operators_json.name, operators_payload)

    provider_source = next((item for item in provider_stats.get("sources") or [] if isinstance(item, dict)), {})
    result = {
        "provider_uid": provider_uid,
        "static_subscription_id": static_subscription_id,
        "publication_id": publication.publication_id,
        "publication_title": publication.title,
        "source_name": static_source_display_name(publication),
        "dataframe_changed": dataframe_changed,
        "summary_changed": summary_changed,
        "records": {
            "stations_with_static_details": int(
                patched_df["detail_source_uid"].fillna("").astype(str).str.strip().ne("").sum()
            ),
            "stations_with_price": int(
                patched_df["price_display"].fillna("").astype(str).str.strip().ne("").sum()
            ),
            "stations_with_opening_hours": int(
                patched_df["opening_hours_display"].fillna("").astype(str).str.strip().ne("").sum()
            ),
            "stations_with_helpdesk": int(
                patched_df["helpdesk_phone"].fillna("").astype(str).str.strip().ne("").sum()
            ),
        },
        "provider_source": provider_source,
        "errors": provider_stats.get("errors") or [],
    }
    print(dumps_pretty_json(result))


if __name__ == "__main__":
    main()
