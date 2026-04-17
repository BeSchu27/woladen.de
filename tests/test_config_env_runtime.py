from __future__ import annotations

import os
from pathlib import Path

import backend.config as config_module
from backend.config import AppConfig, load_env_file


def test_app_config_reads_loaded_env_file_at_instantiation_time(tmp_path, monkeypatch):
    env_file = tmp_path / "woladen-live.env"
    env_file.write_text(
        "\n".join(
            [
                "WOLADEN_LIVE_RAW_PAYLOAD_DIR=/var/lib/woladen/live_raw",
                "WOLADEN_LIVE_ARCHIVE_DIR=/var/lib/woladen/live_archives",
                "WOLADEN_LIVE_HF_ARCHIVE_REPO_ID=loffenauer/AFIR",
                "WOLADEN_LIVE_HF_ARCHIVE_TOKEN_FILE=/etc/woladen/huggingface.token",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    monkeypatch.delenv("WOLADEN_LIVE_RAW_PAYLOAD_DIR", raising=False)
    monkeypatch.delenv("WOLADEN_LIVE_ARCHIVE_DIR", raising=False)
    monkeypatch.delenv("WOLADEN_LIVE_HF_ARCHIVE_REPO_ID", raising=False)
    monkeypatch.delenv("WOLADEN_LIVE_HF_ARCHIVE_TOKEN_FILE", raising=False)

    try:
        load_env_file(
            env_file,
            allowed_keys={
                "WOLADEN_LIVE_RAW_PAYLOAD_DIR",
                "WOLADEN_LIVE_ARCHIVE_DIR",
                "WOLADEN_LIVE_HF_ARCHIVE_REPO_ID",
                "WOLADEN_LIVE_HF_ARCHIVE_TOKEN_FILE",
            },
        )

        config = AppConfig()

        assert config.raw_payload_dir == Path("/var/lib/woladen/live_raw")
        assert config.archive_dir == Path("/var/lib/woladen/live_archives")
        assert config.hf_archive_repo_id == "loffenauer/AFIR"
        assert config.hf_archive_token_file == Path("/etc/woladen/huggingface.token")
    finally:
        for key in (
            "WOLADEN_LIVE_RAW_PAYLOAD_DIR",
            "WOLADEN_LIVE_ARCHIVE_DIR",
            "WOLADEN_LIVE_HF_ARCHIVE_REPO_ID",
            "WOLADEN_LIVE_HF_ARCHIVE_TOKEN_FILE",
        ):
            os.environ.pop(key, None)


def test_app_config_prefers_full_registry_catalog_when_default_artifact_exists(tmp_path, monkeypatch):
    default_fast = tmp_path / "chargers_fast.csv"
    default_full = tmp_path / "chargers_full.csv"
    default_full.write_text("station_id\n", encoding="utf-8")

    monkeypatch.setattr(config_module, "DEFAULT_FAST_CHARGERS_CSV_PATH", default_fast)
    monkeypatch.setattr(config_module, "DEFAULT_FULL_CHARGERS_CSV_PATH", default_full)
    monkeypatch.delenv("WOLADEN_LIVE_CHARGERS_CSV_PATH", raising=False)
    monkeypatch.delenv("WOLADEN_LIVE_FULL_CHARGERS_CSV_PATH", raising=False)

    config = AppConfig()

    assert config.chargers_csv_path == default_fast
    assert config.full_chargers_csv_path == default_full


def test_app_config_reuses_custom_chargers_catalog_as_full_registry_default(tmp_path):
    custom_catalog = tmp_path / "custom.csv"

    config = AppConfig(chargers_csv_path=custom_catalog)

    assert config.chargers_csv_path == custom_catalog
    assert config.full_chargers_csv_path == custom_catalog
