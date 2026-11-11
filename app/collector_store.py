"""采集端持久化：冻结批次、保存发送状态、恢复未确认批次。

使用独立 SQLite 文件（Compose 中挂载卷），因此进程/容器重启后批次与
回执仍然保留。所有写操作使用 ``BEGIN IMMEDIATE``，多个标签页/线程并发
提交同一内容时依靠 ``batch_id`` 主键自然合并为一批。
"""

from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Iterator, Optional

from .contract import build_payload

STATUS_UNCONFIRMED = "unconfirmed"
STATUS_CONFIRMED = "confirmed"
STATUS_REJECTED = "rejected"

_STATUS_LABELS = {
    STATUS_UNCONFIRMED: "未确认",
    STATUS_CONFIRMED: "已确认入库",
    STATUS_REJECTED: "已被下游拒绝",
}


def utc_now() -> str:
    """当前 UTC 时间（秒级精度，ISO 8601）。"""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def status_label(status: str) -> str:
    return _STATUS_LABELS.get(status, status)


class CollectorStore:
    """采集端 SQLite 存储。每个方法使用独立连接，避免线程共享问题。"""

    def __init__(self, path: str):
        self.path = path
        self._initialized = False

    # -- 连接与建表 ---------------------------------------------------------

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        directory = os.path.dirname(os.path.abspath(self.path))
        os.makedirs(directory, exist_ok=True)
        conn = sqlite3.connect(self.path, timeout=15)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("PRAGMA busy_timeout=15000")
        try:
            yield conn
        finally:
            conn.close()

    def init(self) -> None:
        if self._initialized:
            return
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS batches (
                    batch_id      TEXT PRIMARY KEY,
                    content_hash  TEXT NOT NULL,
                    source_url    TEXT NOT NULL,
                    record_count  INTEGER NOT NULL,
                    status        TEXT NOT NULL,
                    payload_json  TEXT NOT NULL,
                    receipt_json  TEXT,
                    last_error    TEXT,
                    attempts      INTEGER NOT NULL DEFAULT 0,
                    created_at    TEXT NOT NULL,
                    updated_at    TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_batches_status
                    ON batches(status);
                """
            )
            conn.commit()
        self._initialized = True

    # -- 冻结批次 -----------------------------------------------------------

    def freeze(
        self,
        records: list[dict],
        source_url: str,
        header_paths: list[str],
        *,
        now: Optional[str] = None,
    ) -> dict:
        """把全部记录冻结为持久批次；同内容再次提交返回同一批次。

        已确认的批次不会被重置；未确认/被拒绝的批次保持原冻结载荷与
        批次号，确保重试沿用原内容。
        """
        self.init()
        now = now or utc_now()
        payload = build_payload(records, source_url, header_paths, now)
        payload_json = json.dumps(
            payload, ensure_ascii=False, sort_keys=True
        )
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO batches
                        (batch_id, content_hash, source_url, record_count,
                         status, payload_json, attempts, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?)
                    """,
                    (
                        payload["batch_id"],
                        payload["content_hash"],
                        source_url,
                        payload["record_count"],
                        STATUS_UNCONFIRMED,
                        payload_json,
                        now,
                        now,
                    ),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            row = conn.execute(
                "SELECT * FROM batches WHERE batch_id = ?",
                (payload["batch_id"],),
            ).fetchone()
        return self._row_to_dict(row)

    # -- 查询 ---------------------------------------------------------------

    @staticmethod
    def _row_to_dict(row: sqlite3.Row) -> Optional[dict]:
        if row is None:
            return None
        data = dict(row)
        data["payload"] = json.loads(data.pop("payload_json"))
        raw_receipt = data.pop("receipt_json")
        data["receipt"] = json.loads(raw_receipt) if raw_receipt else None
        return data

    def get(self, batch_id: str) -> Optional[dict]:
        self.init()
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
        return self._row_to_dict(row)

    def list_all(self, limit: int = 200) -> list[dict]:
        self.init()
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM batches ORDER BY created_at DESC, batch_id LIMIT ?",
                (limit,),
            ).fetchall()
        return [self._row_to_dict(r) for r in rows if r is not None]

    def count_unconfirmed(self) -> int:
        self.init()
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS n FROM batches WHERE status = ?",
                (STATUS_UNCONFIRMED,),
            ).fetchone()
        return int(row["n"])

    # -- 状态更新 -----------------------------------------------------------

    def begin_attempt(self, batch_id: str, now: Optional[str] = None) -> None:
        """发送前记录一次尝试（用于观察重试次数）。"""
        self.init()
        now = now or utc_now()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                UPDATE batches
                   SET attempts = attempts + 1, updated_at = ?
                 WHERE batch_id = ?
                """,
                (now, batch_id),
            )
            conn.commit()

    def mark_confirmed(
        self,
        batch_id: str,
        receipt: dict,
        now: Optional[str] = None,
    ) -> None:
        self.init()
        now = now or utc_now()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                UPDATE batches
                   SET status = ?, receipt_json = ?, last_error = NULL,
                       updated_at = ?
                 WHERE batch_id = ?
                """,
                (
                    STATUS_CONFIRMED,
                    json.dumps(receipt, ensure_ascii=False, sort_keys=True),
                    now,
                    batch_id,
                ),
            )
            conn.commit()

    def mark_unconfirmed(
        self,
        batch_id: str,
        error: str,
        now: Optional[str] = None,
    ) -> None:
        """传输失败/结果未知：保持未确认，保留原载荷等待重试。

        已确认的批次不会被降级（幂等重试返回原回执后再确认一次即可）。
        """
        self.init()
        now = now or utc_now()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                UPDATE batches
                   SET status = CASE WHEN status = ? THEN status ELSE ? END,
                       last_error = CASE WHEN status = ? THEN last_error ELSE ? END,
                       updated_at = ?
                 WHERE batch_id = ?
                """,
                (
                    STATUS_CONFIRMED,
                    STATUS_UNCONFIRMED,
                    STATUS_CONFIRMED,
                    error,
                    now,
                    batch_id,
                ),
            )
            conn.commit()

    def mark_rejected(
        self,
        batch_id: str,
        error: str,
        now: Optional[str] = None,
    ) -> None:
        """下游明确拒绝（如同批次号内容冲突），记录原因供页面展示。"""
        self.init()
        now = now or utc_now()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                UPDATE batches
                   SET status = ?, last_error = ?, updated_at = ?
                 WHERE batch_id = ?
                """,
                (STATUS_REJECTED, error, now, batch_id),
            )
            conn.commit()
