#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.config import AppConfig
from backend.service import IngestionService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the woladen live queue worker")
    parser.add_argument("--once", action="store_true", help="Process at most one queued receipt")
    parser.add_argument("--max-items", type=int, default=None, help="Process at most N queued receipts")
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=None,
        help="Sleep between queue polls when idle",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    service = IngestionService(AppConfig())
    service.bootstrap()

    if args.once:
        result = service.process_next_receipt(bootstrap=False)
        print(json.dumps(result or {"result": "idle"}))
        return

    processed = 0
    idle_sleep_seconds = (
        float(args.sleep_seconds)
        if args.sleep_seconds is not None
        else float(service.config.queue_idle_sleep_seconds)
    )

    while True:
        result = service.process_next_receipt(bootstrap=False)
        if result is None:
            print(json.dumps({"result": "idle", "sleep_seconds": idle_sleep_seconds}))
            time.sleep(max(idle_sleep_seconds, 0.1))
            continue
        print(json.dumps(result))
        processed += 1
        if args.max_items is not None and processed >= args.max_items:
            return


if __name__ == "__main__":
    main()
