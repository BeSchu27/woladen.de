from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import AppConfig


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return _utc_now().replace(microsecond=0).isoformat()


@dataclass(frozen=True)
class ReceiptTask:
    task_id: str
    task_kind: str
    provider_uid: str
    run_id: int
    receipt_log_path: str
    receipt_at: str
    content_type: str = ""
    http_status: int = 0
    subscription_id: str = ""
    publication_id: str = ""
    enqueued_at: str = ""
    claim_path: str = ""

    @classmethod
    def from_dict(cls, payload: dict[str, Any], *, claim_path: Path | None = None) -> "ReceiptTask":
        return cls(
            task_id=str(payload.get("task_id") or "").strip(),
            task_kind=str(payload.get("task_kind") or "").strip(),
            provider_uid=str(payload.get("provider_uid") or "").strip(),
            run_id=int(payload.get("run_id") or 0),
            receipt_log_path=str(payload.get("receipt_log_path") or "").strip(),
            receipt_at=str(payload.get("receipt_at") or "").strip(),
            content_type=str(payload.get("content_type") or "").strip(),
            http_status=int(payload.get("http_status") or 0),
            subscription_id=str(payload.get("subscription_id") or "").strip(),
            publication_id=str(payload.get("publication_id") or "").strip(),
            enqueued_at=str(payload.get("enqueued_at") or "").strip(),
            claim_path=str(claim_path) if claim_path is not None else str(payload.get("claim_path") or "").strip(),
        )

    def with_claim_path(self, claim_path: Path) -> "ReceiptTask":
        payload = asdict(self)
        payload["claim_path"] = str(claim_path)
        return ReceiptTask.from_dict(payload, claim_path=claim_path)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        if not payload["claim_path"]:
            payload.pop("claim_path", None)
        return payload


class ReceiptQueue:
    def __init__(self, config: AppConfig):
        self.config = config
        self.root_dir = config.queue_dir
        self.pending_dir = self.root_dir / "pending"
        self.processing_dir = self.root_dir / "processing"
        self.done_dir = self.root_dir / "done"
        self.failed_dir = self.root_dir / "failed"

    def initialize(self) -> None:
        for path in (self.pending_dir, self.processing_dir, self.done_dir, self.failed_dir):
            path.mkdir(parents=True, exist_ok=True)

    def enqueue(self, task: ReceiptTask) -> Path:
        self.initialize()
        target_path = self.pending_dir / f"{task.task_id}.json"
        self._write_json(target_path, task.to_dict())
        return target_path

    def build_task(
        self,
        *,
        task_kind: str,
        provider_uid: str,
        run_id: int,
        receipt_log_path: Path,
        receipt_at: str,
        content_type: str = "",
        http_status: int = 0,
        subscription_id: str = "",
        publication_id: str = "",
    ) -> ReceiptTask:
        stamp = _utc_now().strftime("%Y%m%dT%H%M%S%fZ")
        return ReceiptTask(
            task_id=f"{stamp}-{uuid.uuid4().hex[:12]}",
            task_kind=task_kind,
            provider_uid=provider_uid,
            run_id=run_id,
            receipt_log_path=str(receipt_log_path),
            receipt_at=receipt_at,
            content_type=content_type,
            http_status=http_status,
            subscription_id=subscription_id,
            publication_id=publication_id,
            enqueued_at=utc_now_iso(),
        )

    def claim_next(self) -> ReceiptTask | None:
        self.initialize()
        for pending_path in sorted(self.pending_dir.glob("*.json")):
            claim_path = self.processing_dir / pending_path.name
            try:
                pending_path.replace(claim_path)
            except FileNotFoundError:
                continue
            except OSError:
                continue
            return self._read_task(claim_path)
        return None

    def mark_done(self, task: ReceiptTask) -> None:
        claim_path = self._claim_path(task)
        self.initialize()
        target_path = self.done_dir / claim_path.name
        claim_path.replace(target_path)

    def mark_failed(self, task: ReceiptTask, *, error_text: str = "") -> None:
        claim_path = self._claim_path(task)
        payload = task.to_dict()
        if error_text:
            payload["error_text"] = error_text
        self.initialize()
        target_path = self.failed_dir / claim_path.name
        self._write_json(target_path, payload)
        claim_path.unlink(missing_ok=True)

    def stats(self) -> dict[str, Any]:
        self.initialize()
        pending_paths = sorted(self.pending_dir.glob("*.json"))
        oldest_pending_age_seconds = None
        oldest_enqueued_at = None
        if pending_paths:
            oldest_task = self._read_task(pending_paths[0])
            oldest_enqueued_at = oldest_task.enqueued_at or oldest_task.receipt_at or None
            if oldest_enqueued_at:
                try:
                    oldest_dt = datetime.fromisoformat(oldest_enqueued_at.replace("Z", "+00:00"))
                except ValueError:
                    oldest_dt = None
                if oldest_dt is not None:
                    oldest_pending_age_seconds = max(0.0, (_utc_now() - oldest_dt.astimezone(timezone.utc)).total_seconds())
        return {
            "pending_count": len(pending_paths),
            "processing_count": len(list(self.processing_dir.glob("*.json"))),
            "failed_count": len(list(self.failed_dir.glob("*.json"))),
            "oldest_pending_enqueued_at": oldest_enqueued_at,
            "oldest_pending_age_seconds": oldest_pending_age_seconds,
        }

    def _read_task(self, path: Path) -> ReceiptTask:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError(f"invalid_receipt_task:{path}")
        return ReceiptTask.from_dict(payload, claim_path=path)

    def _claim_path(self, task: ReceiptTask) -> Path:
        claim_path = Path(task.claim_path)
        if not str(claim_path).strip():
            raise ValueError("missing_claim_path")
        return claim_path

    def _write_json(self, target_path: Path, payload: dict[str, Any]) -> None:
        temp_path = target_path.with_suffix(f"{target_path.suffix}.tmp")
        temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        temp_path.replace(target_path)
