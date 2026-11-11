"""采集端 → 接收端的 HTTP 发送逻辑。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import requests


@dataclass
class SendResult:
    outcome: str  # confirmed / unconfirmed / rejected
    receipt: Optional[dict[str, Any]] = None
    error: Optional[str] = None
    http_status: Optional[int] = None


def send_batch(
    receiver_url: str,
    payload: dict[str, Any],
    *,
    timeout: float = 10.0,
    session: Optional[requests.Session] = None,
) -> SendResult:
    """把冻结载荷发送到接收端。

    * 200：拿到回执，确认入库；
    * 400/409：下游明确拒绝，记录原因；
    * 超时/连接断开/5xx/无法解析回执：结果未知，保持未确认。

    无论哪种情况都不会修改 ``payload``，重试仍复用原批次号与内容。
    """
    url = receiver_url.rstrip("/") + "/api/batches"
    client = session or requests
    try:
        resp = client.post(url, json=payload, timeout=timeout)
    except requests.RequestException as exc:
        return SendResult(
            outcome="unconfirmed",
            error=f"发送中断或接收端不可达：{exc.__class__.__name__}: {exc}",
        )

    if resp.status_code == 200:
        try:
            receipt = resp.json()
        except ValueError:
            return SendResult(
                outcome="unconfirmed",
                error="接收端返回 200 但回执不是合法 JSON，无法确认入库。",
                http_status=200,
            )
        if not isinstance(receipt, dict) or not receipt.get("receipt_id"):
            return SendResult(
                outcome="unconfirmed",
                error="接收端 200 响应缺少 receipt_id，无法确认入库。",
                http_status=200,
            )
        return SendResult(
            outcome="confirmed", receipt=receipt, http_status=200
        )

    if resp.status_code in (400, 409):
        detail = _error_detail(resp)
        code = ""
        try:
            body = resp.json()
            if isinstance(body, dict):
                code = str(body.get("code") or "")
        except ValueError:
            body = None
        prefix = {
            "content_mismatch": "下游拒绝：同批次号但内容不同。",
            "invalid_payload": "下游拒绝：批次载荷不合法。",
        }.get(code, "下游拒绝该批次。")
        return SendResult(
            outcome="rejected",
            error=f"{prefix}{detail}",
            http_status=resp.status_code,
        )

    # 5xx 等：无法判断接收端是否已落库，保持未确认以便重试。
    return SendResult(
        outcome="unconfirmed",
        error=(
            f"接收端返回 HTTP {resp.status_code}，无法确认入库，可安全重试。"
            f"{_error_detail(resp)}"
        ),
        http_status=resp.status_code,
    )


def fetch_downstream_batches(
    receiver_url: str,
    *,
    timeout: float = 5.0,
    session: Optional[requests.Session] = None,
) -> dict[str, Any]:
    """查询下游已收批次与记录数；失败时抛 ``requests.RequestException``。"""
    url = receiver_url.rstrip("/") + "/api/batches"
    client = session or requests
    resp = client.get(url, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, dict):
        raise ValueError("下游批次列表格式不正确。")
    return data


def get_demo_state(
    receiver_url: str,
    *,
    timeout: float = 5.0,
    session: Optional[requests.Session] = None,
) -> dict[str, Any]:
    url = receiver_url.rstrip("/") + "/api/demo/state"
    client = session or requests
    resp = client.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def set_demo_disconnect(
    receiver_url: str,
    enabled: bool,
    *,
    timeout: float = 5.0,
    session: Optional[requests.Session] = None,
) -> dict[str, Any]:
    url = receiver_url.rstrip("/") + "/api/demo/disconnect-next"
    client = session or requests
    resp = client.post(url, json={"enabled": enabled}, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def _error_detail(resp: requests.Response) -> str:
    try:
        body = resp.json()
    except ValueError:
        text = (resp.text or "").strip()
        return f"（HTTP {resp.status_code}：{text[:200]}）" if text else ""
    if isinstance(body, dict):
        message = body.get("error") or body.get("message") or ""
        return f"（{message}）" if message else f"（HTTP {resp.status_code}）"
    return f"（HTTP {resp.status_code}）"
