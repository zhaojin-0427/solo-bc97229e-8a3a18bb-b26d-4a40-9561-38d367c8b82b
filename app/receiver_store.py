"""接收端持久化：整批原子入库、幂等回执、内容冲突拒绝、演示开关。

同一批次号：
* 内容指纹一致 → 不再写入，返回首次入库时的原回执；
* 内容指纹不同 → 拒绝（409），已有批次保持不变。

一个批次的主记录与全部观测记录在同一 ``BEGIN IMMEDIATE`` 事务中写入，
任何一步失败都会整体回滚，不会出现“半批”数据。
"""

from __future__ import annotations

import json
import os
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Iterator, Optional


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class IngestConflict(Exception):
    """同批次号但内容不同。"""


class ReceiverStore:
    def __init__(self, path: str):
        self.path = path
        self._initialized = False

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        directory = os.path.dirname(os.path.abspath(self.path))
        os.makedirs(directory, exist_ok=True)
        conn = sqlite3.connect(self.path, timeout=15, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("PRAGMA busy_timeout=15000")
        conn.execute("PRAGMA foreign_keys=ON")
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
                    receipt_id    TEXT NOT NULL,
                    received_at   TEXT NOT NULL,
                    payload_json  TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS records (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id      TEXT NOT NULL
                                  REFERENCES batches(batch_id) ON DELETE CASCADE,
                    seq           INTEGER NOT NULL,
                    reservoir     TEXT NOT NULL,
                    header_path   TEXT NOT NULL,
                    value         TEXT NOT NULL,
                    source_row    INTEGER NOT NULL,
                    source_col    INTEGER NOT NULL,
                    source_cell   TEXT NOT NULL,
                    source_url    TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_records_batch
                    ON records(batch_id);
                CREATE TABLE IF NOT EXISTS receiver_state (
                    id                  INTEGER PRIMARY KEY CHECK (id = 1),
                    disconnect_next     INTEGER NOT NULL DEFAULT 0,
                    disconnect_last_at  TEXT
                );
                INSERT OR IGNORE INTO receiver_state (id, disconnect_next)
                    VALUES (1, 0);
                """
            )
            conn.commit()
        self._initialized = True

    # -- 回执与查询 ---------------------------------------------------------

    @staticmethod
    def _receipt_for_row(row: sqlite3.Row, *, idempotent: bool) -> dict:
        return {
            "receipt_id": row["receipt_id"],
            "batch_id": row["batch_id"],
            "content_hash": row["content_hash"],
            "record_count": row["record_count"],
            "received_at": row["received_at"],
            "idempotent": idempotent,
            "status": "accepted",
        }

    def get_receipt(self, batch_id: str) -> Optional[dict]:
        self.init()
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
        if row is None:
            return None
        return self._receipt_for_row(row, idempotent=True)

    def get_batch(self, batch_id: str) -> Optional[dict]:
        self.init()
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
            if row is None:
                return None
            defect = dict(row)
            defect["payload"] = json.loads(defect.pop("payload_json"))
            records = conn.execute(
                """
                SELECT seq, reservoir, header_path, value, source_row,
                       source_col, source_cell, source_url
                  FROM records WHERE batch_id = ? ORDER BY seq
                """,
                (batch_id,),
            ).fetchall()
        defect["records"] = [dict(r) for r in records]
        return defect

    def list_batches(self, limit: int = 500) -> list[dict]:
        self.init()
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT batch_id, content_hash, source_url, record_count,
                       receipt_id, received_at
                  FROM batches ORDER BY received_at DESC, batch_id LIMIT ?
                """,
                (limit,),
            ).fetchall()
            totals = conn.execute(
                """
                SELECT COUNT(*) AS batch_count,
                       COALESCE(SUM(record_count), 0) AS record_count
                  FROM batches
                """
            ).fetchone()
        return {
            "batches": [dict(r) for r in rows],
            "batch_count": int(totals["batch_count"]),
            "record_count": int(totals["record_count"]),
        }

    # -- 演示开关 -----------------------------------------------------------

    def demo_state(self) -> dict:
        self.init()
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM receiver_state WHERE id = 1"
            ).fetchone()
        return {
            "disconnect_next": bool(row["disconnect_next"]),
            "disconnect_last_at": row["disconnect_last_at"],
        }

    def set_disconnect_next(self, enabled: bool) -> dict:
        self.init()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "UPDATE receiver_state SET disconnect_next = ? WHERE id = 1",
                (1 if enabled else 0,),
            )
            conn.commit()
        return self.demo_state()

    # -- 原子入库 -----------------------------------------------------------

    def ingest(self, payload: dict) -> tuple[dict, bool]:
        """整批原子入库。

        :returns: ``(receipt, disconnect_now)``。``disconnect_now`` 表示本次
            首次入库触发了“入库后断开连接”演示开关（已一次性消费）。
        :raises IngestConflict: 同批次号但内容指纹不同。
        """
        self.init()
        batch_id = payload["batch_id"]
        content_hash = payload["content_hash"]
        records = payload["records"]
        payload_json = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        now = utc_now()

        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                existing = conn.execute(
                    "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
                ).fetchone()
                if existing is not None:
                    if existing["content_hash"] != content_hash:
                        conn.rollback()
                        raise IngestConflict(
                            "同一批次号已以不同内容入库，拒绝覆盖。"
                        )
                    # 完全相同：不重复写记录，原回执原样返回。
                    conn.commit()
                    return (
                        self._receipt_for_row(existing, idempotent=True),
                        False,
                    )

                state = conn.execute(
                    "SELECT disconnect_next FROM receiver_state WHERE id = 1"
                ).fetchone()
                disconnect_now = bool(state["disconnect_next"])

                receipt_id = "RCPT-" + uuid.uuid4().hex
                conn.execute(
                    """
                    INSERT INTO batches
                        (batch_id, content_hash, source_url, record_count,
                         receipt_id, received_at, payload_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        batch_id,
                        content_hash,
                        payload.get("source_url", ""),
                        len(records),
                        receipt_id,
                        now,
                        payload_json,
                    ),
                )
                conn.executemany(
                    """
                    INSERT INTO records
                        (batch_id, seq, reservoir, header_path, value,
                         source_row, source_col, source_cell, source_url)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            batch_id,
                            idx,
                            rec["reservoir"],
                            rec["header_path"],
                            rec["value"],
                            rec["source_row"],
                            rec["source_col"],
                            rec["source_cell"],
                            rec["source_url"],
                        )
                        for idx, rec in enumerate(records, start=1)
                    ],
                )
                if disconnect_now:
                    conn.execute(
                        """
                        UPDATE receiver_state
                           SET disconnect_next = 0, disconnect_last_at = ?
                         WHERE id = 1
                        """,
                        (now,),
                    )
                conn.commit()
            except IngestConflict:
                raise
            except Exception:
                conn.rollback()
                raise

            row = conn.execute(
                "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
        return (
            self._receipt_for_row(row, idempotent=False),
            disconnect_now,
        )
