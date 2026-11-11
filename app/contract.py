"""采集端与接收端共享的批次载荷契约。

批次内容指纹只覆盖**业务内容**（来源 URL、表头路径、全部观测记录），
不包含创建时间等易变字段。因此同一转换结果无论从哪个标签页、在什么
时间提交，都会得到相同的 ``batch_id`` / ``content_hash``，从源头保证
幂等；重试时原样复用冻结载荷即可。
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Iterable

# 观测记录中参与内容指纹的字段（与 CSV 列一一对应）。
RECORD_FIELDS = (
    "reservoir",
    "header_path",
    "value",
    "source_row",
    "source_col",
    "source_cell",
    "source_url",
)

BATCH_PREFIX = "B-"


class ContractError(ValueError):
    """批次载荷不符合双方约定。"""


def normalize_records(records: Iterable[dict]) -> list[dict]:
    """只保留约定字段，并统一转为稳定的 JSON 基本类型。"""
    normalized: list[dict] = []
    for rec in records:
        normalized.append(
            {
                "reservoir": str(rec.get("reservoir", "")),
                "header_path": str(rec.get("header_path", "")),
                "value": str(rec.get("value", "")),
                "source_row": int(rec.get("source_row", 0)),
                "source_col": int(rec.get("source_col", 0)),
                "source_cell": str(rec.get("source_cell", "")),
                "source_url": str(rec.get("source_url", "")),
            }
        )
    return normalized


def canonical_content(
    records: Iterable[dict],
    source_url: str,
    header_paths: Iterable[str],
) -> str:
    """生成确定性的业务内容 JSON（键排序、紧凑分隔符、UTF-8 保留中文）。"""
    content = {
        "source_url": str(source_url),
        "header_paths": [str(p) for p in header_paths],
        "records": normalize_records(records),
    }
    return json.dumps(
        content,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def compute_content_hash(
    records: Iterable[dict],
    source_url: str,
    header_paths: Iterable[str],
) -> str:
    canonical = canonical_content(records, source_url, header_paths)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def batch_id_for(content_hash: str) -> str:
    """批次号由内容指纹直接派生，天然去重。"""
    return BATCH_PREFIX + content_hash


def build_payload(
    records: Iterable[dict],
    source_url: str,
    header_paths: Iterable[str],
    created_at: str,
) -> dict[str, Any]:
    """构造冻结批次载荷；同一内容的 ``batch_id`` 与 ``content_hash`` 恒定。"""
    normalized = normalize_records(records)
    paths = [str(p) for p in header_paths]
    content_hash = compute_content_hash(normalized, source_url, paths)
    return {
        "batch_id": batch_id_for(content_hash),
        "content_hash": content_hash,
        "source_url": str(source_url),
        "header_paths": paths,
        "record_count": len(normalized),
        "records": normalized,
        "created_at": created_at,
    }


def validate_payload(payload: Any) -> dict[str, Any]:
    """校验接收到的批次载荷，并重新计算内容指纹。

    契约要求：

    * 顶层必须是对象，且包含非空字符串 ``batch_id`` / ``content_hash``；
    * ``records`` 必须是对象数组，``record_count`` 与实际条数一致；
    * ``source_url`` / ``header_paths`` 类型正确；
    * 声明的 ``content_hash`` 必须与按业务内容重算的结果一致，
      防止“批次号相同但内容被篡改”。

    :returns: 规范化后的载荷副本。
    :raises ContractError: 任一条件不满足。
    """
    if not isinstance(payload, dict):
        raise ContractError("批次载荷必须是 JSON 对象。")

    batch_id = payload.get("batch_id")
    if not isinstance(batch_id, str) or not batch_id.strip():
        raise ContractError("缺少有效的 batch_id。")
    declared_hash = payload.get("content_hash")
    if not isinstance(declared_hash, str) or not declared_hash.strip():
        raise ContractError("缺少有效的 content_hash。")

    records = payload.get("records")
    if not isinstance(records, list):
        raise ContractError("records 必须是数组。")
    for idx, rec in enumerate(records, start=1):
        if not isinstance(rec, dict):
            raise ContractError(f"第 {idx} 条记录不是 JSON 对象。")
        for key in ("reservoir", "header_path", "value",
                    "source_cell", "source_url"):
            value = rec.get(key, "")
            if not isinstance(value, str):
                raise ContractError(f"第 {idx} 条记录的 {key} 必须是字符串。")
        for key in ("source_row", "source_col"):
            value = rec.get(key, 0)
            if isinstance(value, bool) or not isinstance(value, int):
                raise ContractError(f"第 {idx} 条记录的 {key} 必须是整数。")

    source_url = payload.get("source_url", "")
    if not isinstance(source_url, str):
        raise ContractError("source_url 必须是字符串。")
    header_paths = payload.get("header_paths", [])
    if not isinstance(header_paths, list) or not all(
        isinstance(p, str) for p in header_paths
    ):
        raise ContractError("header_paths 必须是字符串数组。")

    declared_count = payload.get("record_count", len(records))
    if isinstance(declared_count, bool) or not isinstance(declared_count, int):
        raise ContractError("record_count 必须是整数。")
    if declared_count != len(records):
        raise ContractError(
            f"record_count={declared_count} 与实际记录数 {len(records)} 不一致。"
        )

    normalized = normalize_records(records)
    recomputed = compute_content_hash(normalized, source_url, header_paths)
    if recomputed != declared_hash:
        raise ContractError("content_hash 与收到的业务内容不一致。")

    normalized_payload = dict(payload)
    normalized_payload["records"] = normalized
    normalized_payload["header_paths"] = [str(p) for p in header_paths]
    normalized_payload["source_url"] = source_url
    normalized_payload["record_count"] = len(normalized)
    normalized_payload["content_hash"] = recomputed
    normalized_payload["batch_id"] = batch_id.strip()
    return normalized_payload
