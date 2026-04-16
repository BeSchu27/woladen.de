from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


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
