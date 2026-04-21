from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pandas as pd


def _load_configs_module():
    module_path = Path(__file__).resolve().parents[1] / "scripts" / "build_mobilithek_afir_configs.py"
    spec = importlib.util.spec_from_file_location("build_mobilithek_afir_configs_module", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


build_configs = _load_configs_module()


def test_load_dynamic_subscription_ids_reads_registry(tmp_path: Path):
    registry_path = tmp_path / "mobilithek_subscriptions.json"
    registry_path.write_text(
        json.dumps(
            {
                "edri": {
                    "subscription_id": "980986189821227008",
                    "static_subscription_id": "980986204027498496",
                },
                "invalid": [],
                "blank": {"subscription_id": ""},
            }
        ),
        encoding="utf-8",
    )

    assert build_configs.load_dynamic_subscription_ids(registry_path) == {
        "edri": "980986189821227008"
    }


def test_fetch_static_payload_with_probe_passes_subscription_id(monkeypatch):
    captured: dict[str, object] = {}

    def fake_fetch(session, *, publication_id, preferred_access_mode, access_token, subscription_id=""):
        captured.update(
            {
                "session": session,
                "publication_id": publication_id,
                "preferred_access_mode": preferred_access_mode,
                "access_token": access_token,
                "subscription_id": subscription_id,
            }
        )
        return {"source": "mtls"}, "mtls_subscription", None

    monkeypatch.setattr(build_configs, "fetch_datex_payload_with_probe", fake_fetch)

    session = object()
    payload, access_mode, error = build_configs.fetch_static_payload_with_probe(
        session,
        publication_id="972837891969273856",
        preferred_access_mode="auth",
        access_token="token",
        subscription_id="980986204027498496",
    )

    assert payload == {"source": "mtls"}
    assert access_mode == "mtls_subscription"
    assert error is None
    assert captured == {
        "session": session,
        "publication_id": "972837891969273856",
        "preferred_access_mode": "auth",
        "access_token": "token",
        "subscription_id": "980986204027498496",
    }


def test_summarize_static_coverage_reports_full_registry_and_bundle_counters():
    chargers_df = pd.DataFrame(
        [
            {"station_id": "station-a", "charging_points_count": 2},
            {"station_id": "station-b", "charging_points_count": 4},
            {"station_id": "station-c", "charging_points_count": 2},
            {"station_id": "station-z", "charging_points_count": 2},
        ]
    )
    bundle_df = pd.DataFrame(
        [
            {"station_id": "station-a", "charging_points_count": 2},
            {"station_id": "station-b", "charging_points_count": 4},
            {"station_id": "station-c", "charging_points_count": 2},
        ]
    )

    summary = build_configs.summarize_static_coverage(
        chargers_df,
        bundle_df,
        matches={"site-a": "station-a", "site-b": "station-z"},
        total_sites=2,
        fetch_status="ok",
        access_mode="noauth",
        site_operator_samples=["Example Operator"],
    )

    assert summary["matched_stations"] == 2
    assert summary["matched_charging_points"] == 4
    assert summary["station_coverage_ratio"] == 0.5
    assert summary["charging_point_coverage_ratio"] == 0.4
    assert summary["bundle_matched_stations"] == 1
    assert summary["bundle_matched_charging_points"] == 2
    assert summary["bundle_station_coverage_ratio"] == 0.333333
    assert summary["bundle_charging_point_coverage_ratio"] == 0.25


def test_score_site_to_station_rejects_close_candidate_with_postcode_conflict_only():
    site = build_configs.StaticSiteRecord(
        site_id="site-enio-rosenheim",
        station_ids=("KathreinECSim02", "KathreinECSim03", "KathreinECSim01"),
        evse_ids=(),
        lat=47.85713,
        lon=12.11810,
        postcode="83022",
        city="Rosenheim",
        address="Wittelsbacherstraße",
        total_evses=6,
        operator_name="",
    )
    station_row = pd.Series(
        {
            "station_id": "station-1",
            "lat": 47.857127,
            "lon": 12.118105,
            "postcode": "83026",
            "city": "Rosenheim",
            "address": "Äußere Münchenerstraße 70a 83026 Rosenheim",
            "operator": "Erich Vinzenz KFZ-Werkstatt",
            "charging_points_count": 1,
            "in_bundle": False,
        }
    )

    accepted, _, _, details = build_configs.score_site_to_station(
        site,
        station_row,
        publisher="ENIO GmbH",
    )
