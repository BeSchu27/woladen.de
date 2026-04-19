#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.config import AppConfig
from backend.service import IngestionService


def _is_retryable_sqlite_lock(exc: sqlite3.OperationalError) -> bool:
    text = str(exc).strip().lower()
    return "locked" in text or "busy" in text


def _sqlite_lock_sleep_seconds(service: IngestionService, minimum_sleep_seconds: float = 0.0) -> float:
    base_sleep_seconds = max(float(service.config.sqlite_busy_timeout_ms) / 1000.0, 1.0)
    if minimum_sleep_seconds > 0:
        return max(base_sleep_seconds, minimum_sleep_seconds)
    return base_sleep_seconds


def receive_next_provider_resiliently(
    service: IngestionService,
    *,
    bootstrap: bool = False,
    minimum_sleep_seconds: float = 0.0,
) -> tuple[dict | None, float | None]:
    try:
        return service.receive_next_provider(bootstrap=bootstrap), None
    except sqlite3.OperationalError as exc:
        if not _is_retryable_sqlite_lock(exc):
            raise
        sleep_seconds = _sqlite_lock_sleep_seconds(service, minimum_sleep_seconds)
        return {
            "result": "sqlite_locked",
            "error": str(exc),
            "sleep_seconds": sleep_seconds,
        }, sleep_seconds


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the woladen live ingester")
    parser.add_argument("--provider", default="", help="Only ingest a single provider UID")
    parser.add_argument("--max-providers", type=int, default=None, help="Only ingest the first N enabled providers")
    parser.add_argument("--bootstrap-only", action="store_true", help="Initialize the database and seed metadata")
    parser.add_argument("--loop", action="store_true", help="Continuously poll providers in round-robin order")
    parser.add_argument("--sleep-seconds", type=float, default=0.0, help="Sleep between loop iterations")
    return parser.parse_args()


def bootstrap_loop_if_missing(service: IngestionService) -> bool:
    db_missing = not service.config.db_path.exists()
    service.bootstrap()
    return db_missing


def main() -> None:
    args = parse_args()
    service = IngestionService(AppConfig())

    if args.bootstrap_only:
        service.bootstrap()
        print(json.dumps({"result": "bootstrapped"}))
        return

    if args.loop:
        bootstrap_loop_if_missing(service)
        while True:
            result, transient_sleep_seconds = receive_next_provider_resiliently(
                service,
                bootstrap=False,
                minimum_sleep_seconds=args.sleep_seconds,
            )
            if result is not None:
                print(json.dumps(result))
                if transient_sleep_seconds is not None:
                    time.sleep(transient_sleep_seconds)
                    continue
                if args.sleep_seconds > 0:
                    time.sleep(args.sleep_seconds)
                continue

            sleep_seconds = service.seconds_until_next_provider_due(bootstrap=False)
            if sleep_seconds is None:
                sleep_seconds = max(args.sleep_seconds, 1.0) if args.sleep_seconds > 0 else 1.0
                print(json.dumps({"result": "no_enabled_provider", "sleep_seconds": sleep_seconds}))
            else:
                sleep_seconds = min(sleep_seconds, float(service.config.poll_idle_sleep_max_seconds))
                if args.sleep_seconds > 0:
                    sleep_seconds = max(sleep_seconds, args.sleep_seconds)
                print(json.dumps({"result": "idle", "sleep_seconds": sleep_seconds}))
            time.sleep(sleep_seconds)
        return

    result = service.ingest_once(
        provider_uid=args.provider or None,
        max_providers=args.max_providers,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
