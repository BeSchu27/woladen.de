#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.archive import DailyResponseArchiveDownloader
from backend.config import AppConfig, load_env_file

ARCHIVE_ENV_FILE_KEYS = frozenset(
    {
        "WOLADEN_LIVE_ARCHIVE_DIR",
        "WOLADEN_LIVE_ARCHIVE_TIMEZONE",
        "WOLADEN_LIVE_HF_ARCHIVE_REPO_ID",
        "WOLADEN_LIVE_HF_ARCHIVE_REPO_TYPE",
        "WOLADEN_LIVE_HF_ARCHIVE_PATH_PREFIX",
        "WOLADEN_LIVE_HF_ARCHIVE_TOKEN_FILE",
        "WOLADEN_LIVE_HF_ARCHIVE_TOKEN",
        "HF_TOKEN",
        "HUGGINGFACE_HUB_TOKEN",
        "HUGGINGFACE_TOKEN",
    }
)


def _parse_date(value: str) -> date:
    return date.fromisoformat(value.strip())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download one archived live provider response tgz from Hugging Face")
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--date", dest="target_date", type=_parse_date, default=None, help="Archive date in YYYY-MM-DD")
    mode_group.add_argument(
        "--latest-available",
        action="store_true",
        help="Discover the newest archive date on Hugging Face and download that tgz",
    )
    mode_group.add_argument(
        "--list-available",
        action="store_true",
        help="List remote archive tgz files visible in the configured Hugging Face dataset",
    )
    parser.add_argument("--env-file", type=Path, default=None, help="Optional runtime env file with archive settings")
    parser.add_argument("--force", action="store_true", help="Re-download even if the target tgz already exists locally")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.env_file is not None:
        load_env_file(args.env_file, allowed_keys=ARCHIVE_ENV_FILE_KEYS)
    downloader = DailyResponseArchiveDownloader(AppConfig())
    if args.list_available:
        archives = downloader.list_available_archives()
        result = {
            "result": "available_archives",
            "archive_count": len(archives),
            "archives": archives,
        }
    else:
        target_date = args.target_date
        if args.latest_available:
            target_date = downloader.latest_available_date()
            if target_date is None:
                result = {
                    "result": "no_remote_archives",
                    "archive_count": 0,
                    "archives": [],
                }
                print(json.dumps(result, ensure_ascii=False, indent=2))
                return
        result = downloader.download_date(target_date, force=args.force)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
