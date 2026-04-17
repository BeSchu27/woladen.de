from __future__ import annotations

import gzip
import hashlib
import io
import json
import re
import shutil
import tarfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

from .config import AppConfig
from .models import FetchResponse

ARCHIVE_NAME_RE = re.compile(r"live-provider-responses-(\d{4}-\d{2}-\d{2})\.tgz$")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_now_iso() -> str:
    return _utc_now().replace(microsecond=0).isoformat()


def _parse_iso_datetime(value: str | None) -> datetime | None:
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


def _safe_provider_uid(provider_uid: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9._-]+", "_", str(provider_uid or "").strip())
    return sanitized or "unknown-provider"


def _decode_body_text(payload_bytes: bytes) -> tuple[str, bool]:
    is_gzip = payload_bytes[:2] == b"\x1f\x8b"
    raw_bytes = payload_bytes
    if is_gzip:
        try:
            raw_bytes = gzip.decompress(payload_bytes)
        except OSError:
            raw_bytes = payload_bytes
            is_gzip = False
    return raw_bytes.decode("utf-8", errors="replace"), is_gzip


class ResponseLogWriter:
    def __init__(self, config: AppConfig):
        self.config = config
        self.root_dir = config.raw_payload_dir

    def write_http_response(self, *, provider_uid: str, fetched_at: str, response: FetchResponse) -> Path:
        payload_sha256 = hashlib.sha256(response.body).hexdigest()
        body_text, body_is_gzip = _decode_body_text(response.body)
        record = {
            "kind": "http_response",
            "provider_uid": provider_uid,
            "fetched_at": fetched_at,
            "logged_at": _utc_now_iso(),
            "archive_date": self._archive_date_text(fetched_at),
            "http_status": int(response.http_status),
            "content_type": response.content_type,
            "headers_text": response.headers_text,
            "payload_sha256": payload_sha256,
            "payload_byte_length": len(response.body),
            "payload_is_gzip": body_is_gzip,
            "body_text": body_text,
        }
        filename = f"{self._filename_stamp()}-{int(response.http_status):03d}-{payload_sha256[:12]}.json"
        target_path = self._target_path(provider_uid, record["archive_date"], filename)
        self._write_json(target_path, record)
        return target_path

    def write_fetch_failure(
        self,
        *,
        provider_uid: str,
        fetched_at: str,
        failure_kind: str,
        error_text: str,
    ) -> Path:
        record = {
            "kind": "fetch_failure",
            "provider_uid": provider_uid,
            "fetched_at": fetched_at,
            "logged_at": _utc_now_iso(),
            "archive_date": self._archive_date_text(fetched_at),
            "failure_kind": failure_kind,
            "error_text": error_text,
        }
        filename = f"{self._filename_stamp()}-{failure_kind}.json"
        target_path = self._target_path(provider_uid, record["archive_date"], filename)
        self._write_json(target_path, record)
        return target_path

    def write_push_request(
        self,
        *,
        provider_uid: str,
        received_at: str,
        payload_bytes: bytes,
        content_type: str,
        content_encoding: str,
        subscription_id: str = "",
        publication_id: str = "",
        request_path: str = "",
        request_query: str = "",
        request_headers: Mapping[str, Any] | None = None,
    ) -> Path:
        payload_sha256 = hashlib.sha256(payload_bytes).hexdigest()
        body_text, body_is_gzip = _decode_body_text(payload_bytes)
        record = {
            "kind": "push_request",
            "provider_uid": provider_uid,
            "received_at": received_at,
            "logged_at": _utc_now_iso(),
            "archive_date": self._archive_date_text(received_at),
            "subscription_id": subscription_id,
            "publication_id": publication_id,
            "request_path": request_path,
            "request_query": request_query,
            "content_type": content_type,
            "content_encoding": content_encoding,
            "request_headers": self._normalize_headers(request_headers),
            "payload_sha256": payload_sha256,
            "payload_byte_length": len(payload_bytes),
            "payload_is_gzip": body_is_gzip,
            "body_text": body_text,
        }
        filename = f"{self._filename_stamp()}-push-{payload_sha256[:12]}.json"
        target_path = self._target_path(provider_uid, record["archive_date"], filename)
        self._write_json(target_path, record)
        return target_path

    def _archive_date_text(self, fetched_at: str) -> str:
        fetched_dt = _parse_iso_datetime(fetched_at) or _utc_now()
        return fetched_dt.astimezone(self.config.archive_timezone()).date().isoformat()

    def _filename_stamp(self) -> str:
        return _utc_now().strftime("%Y%m%dT%H%M%S%fZ")

    def _target_path(self, provider_uid: str, archive_date: str, filename: str) -> Path:
        target_dir = self.root_dir / _safe_provider_uid(provider_uid) / archive_date
        target_dir.mkdir(parents=True, exist_ok=True)
        return target_dir / filename

    def _write_json(self, target_path: Path, payload: dict[str, Any]) -> None:
        temp_path = target_path.with_suffix(f"{target_path.suffix}.tmp")
        temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        temp_path.replace(target_path)

    def _normalize_headers(self, request_headers: Mapping[str, Any] | None) -> dict[str, str]:
        if not request_headers:
            return {}
        return {
            str(key): str(value)
            for key, value in sorted(request_headers.items(), key=lambda item: str(item[0]).lower())
        }


class DailyResponseArchiver:
    def __init__(self, config: AppConfig, *, hf_api: Any | None = None):
        self.config = config
        self.hf_api = hf_api

    def default_target_date(self) -> date:
        now_local = _utc_now().astimezone(self.config.archive_timezone())
        return now_local.date() - timedelta(days=1)

    def archive_date(
        self,
        target_date: date | None = None,
        *,
        upload: bool = True,
        delete_source_on_success: bool = True,
    ) -> dict[str, Any]:
        effective_date = target_date or self.default_target_date()
        source_files = self._source_files_for_date(effective_date)
        if not source_files:
            return {"result": "no_files", "target_date": effective_date.isoformat(), "file_count": 0}

        if upload and not self.config.has_hf_archive_upload_config():
            return {
                "result": "skipped_missing_upload_config",
                "target_date": effective_date.isoformat(),
                "file_count": len(source_files),
            }

        archive_path = self._create_archive(effective_date, source_files)
        remote_path = ""
        if upload:
            remote_path = self._upload_archive(archive_path, effective_date)

        if remote_path:
            archive_path.unlink(missing_ok=True)

        if delete_source_on_success and (not upload or remote_path):
            self._delete_source_files(source_files)

        return {
            "result": "uploaded" if remote_path else "archived_local_only",
            "target_date": effective_date.isoformat(),
            "file_count": len(source_files),
            "provider_count": len({path.parts[-3] for path in source_files if len(path.parts) >= 3}),
            "archive_path": str(archive_path),
            "remote_path": remote_path,
        }

    def _source_files_for_date(self, target_date: date) -> list[Path]:
        archive_date = target_date.isoformat()
        root_dir = self.config.raw_payload_dir
        if not root_dir.exists():
            return []
        files = sorted(root_dir.glob(f"*/{archive_date}/*.json"))
        return [path for path in files if path.is_file()]

    def _archive_name(self, target_date: date) -> str:
        return f"live-provider-responses-{target_date.isoformat()}.tgz"

    def archive_name(self, target_date: date) -> str:
        return self._archive_name(target_date)

    def _create_archive(self, target_date: date, source_files: list[Path]) -> Path:
        self.config.archive_dir.mkdir(parents=True, exist_ok=True)
        archive_path = self.config.archive_dir / self._archive_name(target_date)
        with tarfile.open(archive_path, mode="w:gz") as archive_handle:
            for source_file in source_files:
                archive_handle.add(source_file, arcname=str(source_file.relative_to(self.config.raw_payload_dir)))
            manifest = {
                "target_date": target_date.isoformat(),
                "created_at": _utc_now_iso(),
                "archive_timezone": self.config.archive_timezone_name,
                "file_count": len(source_files),
                "source_files": [str(path.relative_to(self.config.raw_payload_dir)) for path in source_files],
            }
            manifest_bytes = (json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")
            manifest_info = tarfile.TarInfo(name="manifest.json")
            manifest_info.size = len(manifest_bytes)
            archive_handle.addfile(manifest_info, io.BytesIO(manifest_bytes))
        return archive_path

    def _remote_path_for_archive(self, target_date: date, archive_name: str) -> str:
        parts = [
            self.config.hf_archive_path_prefix,
            f"{target_date.year:04d}",
            f"{target_date.month:02d}",
            archive_name,
        ]
        return "/".join(part for part in parts if part)

    def remote_path_for_date(self, target_date: date) -> str:
        return self._remote_path_for_archive(target_date, self._archive_name(target_date))

    def _upload_archive(self, archive_path: Path, target_date: date) -> str:
        token = self.config.hf_archive_token()
        if self.hf_api is None:
            from huggingface_hub import HfApi

            api: Any = HfApi(token=token)
        else:
            api = self.hf_api
        remote_path = self._remote_path_for_archive(target_date, archive_path.name)
        api.upload_file(
            path_or_fileobj=str(archive_path),
            path_in_repo=remote_path,
            repo_id=self.config.hf_archive_repo_id,
            repo_type=self.config.hf_archive_repo_type,
            commit_message=f"Add live provider response archive for {target_date.isoformat()}",
        )
        return remote_path

    def _delete_source_files(self, source_files: list[Path]) -> None:
        cleanup_dirs: set[Path] = set()
        for source_file in source_files:
            cleanup_dirs.add(source_file.parent)
            source_file.unlink(missing_ok=True)
        for cleanup_dir in sorted(cleanup_dirs, key=lambda path: len(path.parts), reverse=True):
            if cleanup_dir.exists() and not any(cleanup_dir.iterdir()):
                cleanup_dir.rmdir()
            provider_dir = cleanup_dir.parent
            if provider_dir.exists() and provider_dir != self.config.raw_payload_dir and not any(provider_dir.iterdir()):
                provider_dir.rmdir()


class DailyResponseArchiveDownloader:
    def __init__(self, config: AppConfig, *, download_file: Any | None = None, hf_api: Any | None = None):
        self.config = config
        self.download_file = download_file
        self.hf_api = hf_api
        self.archiver = DailyResponseArchiver(config)

    def default_target_date(self) -> date:
        return self.archiver.default_target_date()

    def _client(self) -> Any:
        if self.hf_api is not None:
            return self.hf_api
        from huggingface_hub import HfApi

        return HfApi(token=(self.config.hf_archive_token() or None))

    def list_available_archives(self) -> list[dict[str, Any]]:
        if not self.config.hf_archive_repo_id:
            return []

        prefix = self.config.hf_archive_path_prefix.strip("/")
        prefix_root = f"{prefix}/" if prefix else ""
        rows: list[dict[str, Any]] = []
        for repo_path in self._client().list_repo_files(
            repo_id=self.config.hf_archive_repo_id,
            repo_type=self.config.hf_archive_repo_type,
        ):
            remote_path = str(repo_path).strip()
            if prefix_root and not remote_path.startswith(prefix_root):
                continue
            match = ARCHIVE_NAME_RE.fullmatch(Path(remote_path).name)
            if match is None:
                continue
            archive_date = date.fromisoformat(match.group(1))
            local_path = self.config.archive_dir / Path(remote_path).name
            rows.append(
                {
                    "target_date": archive_date.isoformat(),
                    "archive_name": local_path.name,
                    "remote_path": remote_path,
                    "local_path": str(local_path),
                    "is_downloaded": local_path.exists(),
                    "file_byte_length": int(local_path.stat().st_size) if local_path.exists() else 0,
                }
            )
        rows.sort(key=lambda row: (row["target_date"], row["remote_path"]))
        return rows

    def latest_available_date(self) -> date | None:
        archives = self.list_available_archives()
        if not archives:
            return None
        return date.fromisoformat(str(archives[-1]["target_date"]))

    def download_date(self, target_date: date | None = None, *, force: bool = False) -> dict[str, Any]:
        effective_date = target_date or self.default_target_date()
        if not self.config.hf_archive_repo_id:
            return {
                "result": "skipped_missing_repo_config",
                "target_date": effective_date.isoformat(),
            }

        archive_name = self.archiver.archive_name(effective_date)
        remote_path = self.archiver.remote_path_for_date(effective_date)
        target_path = self.config.archive_dir / archive_name
        self.config.archive_dir.mkdir(parents=True, exist_ok=True)

        if target_path.exists() and not force:
            return {
                "result": "already_present",
                "target_date": effective_date.isoformat(),
                "target_path": str(target_path),
                "remote_path": remote_path,
                "file_byte_length": int(target_path.stat().st_size),
            }

        downloader = self.download_file
        if downloader is None:
            from huggingface_hub import hf_hub_download

            downloader = hf_hub_download

        downloaded_path = Path(
            downloader(
                repo_id=self.config.hf_archive_repo_id,
                repo_type=self.config.hf_archive_repo_type,
                filename=remote_path,
                token=(self.config.hf_archive_token() or None),
                force_download=force,
            )
        )
        shutil.copyfile(downloaded_path, target_path)
        return {
            "result": "downloaded",
            "target_date": effective_date.isoformat(),
            "target_path": str(target_path),
            "remote_path": remote_path,
            "file_byte_length": int(target_path.stat().st_size),
        }
