from __future__ import annotations

import sqlite3
from pathlib import Path

from scripts.live_ingester import bootstrap_loop_if_missing, receive_next_provider_resiliently


class DummyService:
    def __init__(self, db_path: Path, *, sqlite_busy_timeout_ms: int = 5000):
        self.config = type("Config", (), {"db_path": db_path, "sqlite_busy_timeout_ms": sqlite_busy_timeout_ms})()
        self.bootstrap_calls = 0
        self.receive_next_provider_calls = 0
        self.receive_next_provider_result = None
        self.receive_next_provider_exc = None

    def bootstrap(self) -> None:
        self.bootstrap_calls += 1

    def receive_next_provider(self, *, bootstrap: bool = False):
        self.receive_next_provider_calls += 1
        if self.receive_next_provider_exc is not None:
            raise self.receive_next_provider_exc
        return self.receive_next_provider_result


def test_bootstrap_loop_if_missing_bootstraps_when_db_is_missing(tmp_path: Path):
    service = DummyService(tmp_path / "live.sqlite3")
    result = bootstrap_loop_if_missing(service)
    assert result is True
    assert service.bootstrap_calls == 1


def test_bootstrap_loop_if_missing_refreshes_when_db_exists(tmp_path: Path):
    db_path = tmp_path / "live.sqlite3"
    db_path.write_text("", encoding="utf-8")
    service = DummyService(db_path)
    result = bootstrap_loop_if_missing(service)
    assert result is False
    assert service.bootstrap_calls == 1


def test_receive_next_provider_resiliently_returns_retry_payload_for_sqlite_lock(tmp_path: Path):
    service = DummyService(tmp_path / "live.sqlite3", sqlite_busy_timeout_ms=5000)
    service.receive_next_provider_exc = sqlite3.OperationalError("database is locked")

    result, sleep_seconds = receive_next_provider_resiliently(service, minimum_sleep_seconds=2.0)

    assert result == {
        "result": "sqlite_locked",
        "error": "database is locked",
        "sleep_seconds": 5.0,
    }
    assert sleep_seconds == 5.0
    assert service.receive_next_provider_calls == 1
