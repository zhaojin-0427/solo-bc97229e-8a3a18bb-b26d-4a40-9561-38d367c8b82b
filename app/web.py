"""Flask 采集入口：抓取公示页 → 选择表格 → 预览 → 下载 CSV / 发送入库。"""

from __future__ import annotations

import csv
import io
import os
import time
import urllib.parse
from typing import Optional

import requests
from bs4 import BeautifulSoup
from flask import (
    Flask,
    Response,
    abort,
    current_app,
    redirect,
    render_template,
    request,
    send_from_directory,
    url_for,
)

from .collector_store import (
    STATUS_UNCONFIRMED,
    CollectorStore,
    status_label,
)
from .grid_parser import (
    TableParseError,
    lenient_matrix,
    parse_table,
)
from .sender import (
    SendResult,
    fetch_downstream_batches,
    get_demo_state,
    send_batch,
    set_demo_disconnect,
)

app = Flask(__name__)

_DEFAULT_DB = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "collector.db",
)
app.config.setdefault(
    "COLLECTOR_DB", os.environ.get("COLLECTOR_DB", _DEFAULT_DB)
)
app.config.setdefault(
    "RECEIVER_URL", os.environ.get("RECEIVER_URL", "http://127.0.0.1:9000")
)
app.config.setdefault(
    "RECEIVER_PUBLIC_URL",
    os.environ.get("RECEIVER_PUBLIC_URL", app.config["RECEIVER_URL"]),
)
app.config.setdefault(
    "RECEIVER_TIMEOUT", float(os.environ.get("RECEIVER_TIMEOUT", "10"))
)


def collector_store() -> CollectorStore:
    """按当前应用配置获取采集端存储（测试可替换配置指向临时库）。"""
    store = CollectorStore(current_app.config["COLLECTOR_DB"])
    store.init()
    return store


def receiver_url() -> str:
    return current_app.config["RECEIVER_URL"]


def receiver_public_url() -> str:
    return current_app.config["RECEIVER_PUBLIC_URL"]


def receiver_timeout() -> float:
    return float(current_app.config["RECEIVER_TIMEOUT"])


def _attempt_send(batch: dict) -> SendResult:
    """向接收端发送已冻结批次并持久化结果。

    使用 ``batch['payload']`` 原样发送：重试绝不复用新的转换结果，
    批次号与内容始终不变。
    """
    store = collector_store()
    batch_id = batch["batch_id"]
    store.begin_attempt(batch_id)
    result = send_batch(
        receiver_url(), batch["payload"], timeout=receiver_timeout()
    )
    if result.outcome == "confirmed":
        store.mark_confirmed(batch_id, result.receipt or {})
    elif result.outcome == "rejected":
        store.mark_rejected(batch_id, result.error or "下游拒绝。")
    else:
        store.mark_unconfirmed(batch_id, result.error or "发送结果未知。")
    return result

# 演示用进程内缓存（单 worker 足够）：key -> {"html", "url", "tables"}
_FETCH_CACHE: dict[str, dict] = {}
_FETCH_TTL = 30 * 60  # 30 分钟
_FETCH_MAX = 50

SAMPLE_URL = "/sample/notice.html"
SAMPLE_ALT_URL = "/sample/notice_alt.html"
SAMPLE_ERROR_URL = "/sample/notice_error.html"
USER_AGENT = "HydroTableCollector/1.0 (+reservoir bulletin archiver)"


def _cache_put(key: str, payload: dict) -> None:
    now = time.time()
    expired = [k for k, v in _FETCH_CACHE.items() if now - v["ts"] > _FETCH_TTL]
    for k in expired:
        _FETCH_CACHE.pop(k, None)
    while len(_FETCH_CACHE) >= _FETCH_MAX:
        _FETCH_CACHE.pop(next(iter(_FETCH_CACHE)))
    payload["ts"] = now
    _FETCH_CACHE[key] = payload


def _cache_get(key: str) -> Optional[dict]:
    item = _FETCH_CACHE.get(key)
    if item is None:
        return None
    if time.time() - item["ts"] > _FETCH_TTL:
        _FETCH_CACHE.pop(key, None)
        return None
    return item


def _is_local_sample(url: str) -> bool:
    return url.startswith("/sample/")


def _normalize_url(raw: str) -> str:
    url = (raw or "").strip()
    if not url:
        raise TableParseError("请输入公示页 URL。")
    if url.startswith(("http://", "https://", "/")):
        return url
    return "https://" + url


def _fetch_html(url: str) -> str:
    if _is_local_sample(url):
        # 本地示例：直接读静态文件，保证离线可演示
        path = url[len("/sample/"):]
        safe = path.replace("/", "_")
        full = app.static_folder + "/samples/" + safe
        try:
            with open(full, "r", encoding="utf-8") as fh:
                return fh.read()
        except OSError as exc:
            raise TableParseError(f"本地示例页不存在：{url}（{exc}）")

    try:
        resp = requests.get(
            url,
            headers={"User-Agent": USER_AGENT},
            timeout=15,
            allow_redirects=True,
        )
        resp.raise_for_status()
    except requests.RequestException as exc:
        raise TableParseError(f"抓取页面失败：{exc}")

    # requests 按响应头推断编码，推断失败时回退 utf-8
    if not resp.encoding or resp.encoding.lower() == "iso-8859-1":
        resp.encoding = resp.apparent_encoding or "utf-8"
    return resp.text


def _parse_tables(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    summaries = []
    for idx, table in enumerate(soup.find_all("table")):
        rows = table.find_all("tr")
        cell_count = len(table.find_all(["td", "th"]))
        caption = table.find("caption")
        caption_text = caption.get_text(strip=True) if caption else ""
        # 容错预览：默认按 2 行表头展开
        grid, width = lenient_matrix(table, header_rows=min(2, max(1, len(rows))))
        preview = []
        for line in grid[: 6]:
            preview.append([p.text if p else "" for p in line[: 8]])
        summaries.append(
            {
                "index": idx,
                "n_rows": len(rows),
                "n_cells": cell_count,
                "width": width,
                "caption": caption_text,
                "preview": preview,
            }
        )
    return summaries


# ---------------------------------------------------------------------------
# 路由
# ---------------------------------------------------------------------------

@app.get("/")
def index():
    return render_template(
        "index.html",
        sample_url=SAMPLE_URL,
        sample_alt_url=SAMPLE_ALT_URL,
        sample_error_url=SAMPLE_ERROR_URL,
    )


@app.post("/fetch")
def fetch():
    raw_url = request.form.get("url", "")
    try:
        url = _normalize_url(raw_url)
        html = _fetch_html(url)
    except TableParseError as exc:
        return render_template("error.html", message=str(exc), stage="抓取"), 400

    soup = BeautifulSoup(html, "html.parser")
    tables_html = soup.find_all("table")
    if not tables_html:
        return render_template(
            "error.html",
            message="该页面没有解析到任何 <table> 表格，请确认 URL 指向公示页。",
            stage="解析",
        ), 400

    summaries = _parse_tables(html)
    cache_key = f"f{int(time.time() * 1000)}-{len(_FETCH_CACHE)}"
    _cache_put(
        cache_key,
        {"html": html, "url": url, "tables": summaries},
    )
    return render_template(
        "select.html",
        cache_key=cache_key,
        url=url,
        tables=summaries,
    )


@app.post("/parse")
def parse():
    cache_key = request.form.get("cache_key", "")
    cached = _cache_get(cache_key)
    if cached is None:
        return render_template(
            "error.html",
            message="会话已过期或缺失，请重新输入 URL 抓取。",
            stage="选择",
        ), 400

    try:
        table_index = int(request.form.get("table_index", ""))
        header_rows = int(request.form.get("header_rows", ""))
    except (TypeError, ValueError):
        return render_template(
            "error.html", message="表格编号或表头行数无效。", stage="选择"
        ), 400

    soup = BeautifulSoup(cached["html"], "html.parser")
    tables = soup.find_all("table")
    if not 0 <= table_index < len(tables):
        abort(400, "表格编号超出范围。")

    try:
        parsed = parse_table(
            tables[table_index], header_rows, source_url=cached["url"]
        )
    except TableParseError as exc:
        return render_template(
            "error.html", message=str(exc), stage="转换", stopped=True
        ), 422

    result_key = f"r{int(time.time() * 1000)}-{len(_FETCH_CACHE)}"
    _cache_put(
        result_key,
        {
            "records": parsed.records,
            "url": cached["url"],
            "header_paths": parsed.header_paths,
            "width": parsed.width,
            "total_rows": parsed.total_rows,
            "header_rows": parsed.header_rows,
        },
    )
    return render_template(
        "preview.html",
        result_key=result_key,
        url=cached["url"],
        parsed=parsed,
        records=parsed.records,
    )


@app.get("/download")
def download():
    result_key = request.args.get("key", "")
    cached = _cache_get(result_key)
    if cached is None:
        return render_template(
            "error.html",
            message="下载会话已过期，请重新抓取并转换。",
            stage="下载",
        ), 400

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(
        [
            "水库",
            "表头路径",
            "原始文本",
            "来源单元格行",
            "来源单元格列",
            "来源单元格坐标",
            "来源URL",
        ]
    )
    for rec in cached["records"]:
        writer.writerow(
            [
                rec["reservoir"],
                rec["header_path"],
                rec["value"],
                rec["source_row"],
                rec["source_col"],
                rec["source_cell"],
                rec["source_url"],
            ]
        )

    # utf-8-sig 便于 Excel 直接打开中文
    payload = buf.getvalue().encode("utf-8-sig")
    fname = "reservoir_observations.csv"
    quoted = urllib.parse.quote(fname)
    return Response(
        payload,
        mimetype="text/csv; charset=utf-8",
        headers={
            "Content-Disposition":
                f"attachment; filename={fname}; filename*=UTF-8''{quoted}"
        },
    )


@app.get("/sample/<path:filename>")
def sample(filename: str):
    return send_from_directory(
        app.static_folder + "/samples", filename, mimetype="text/html"
    )


# ---------------------------------------------------------------------------
# 发送入库与批次状态
# ---------------------------------------------------------------------------

@app.post("/deliver")
def deliver():
    """把当前转换预览的全部记录冻结为持久批次并发送入库。

    批次号由内容指纹派生：多个标签页同时提交同一转换结果时，本地
    SQLite 主键与接收端幂等逻辑共同保证只会存在一批。
    """
    result_key = request.form.get("result_key", "")
    cached = _cache_get(result_key)
    if cached is None:
        return render_template(
            "error.html",
            message=(
                "转换会话已过期或缺失，无法冻结记录。请重新抓取并转换；"
                "已冻结的未确认批次可在「入库发送记录」中直接重试。"
            ),
            stage="发送",
        ), 400

    store = collector_store()
    batch = store.freeze(
        cached["records"], cached["url"], cached["header_paths"]
    )
    _attempt_send(batch)
    return redirect(url_for("delivery_detail", batch_id=batch["batch_id"]))


@app.get("/deliveries")
def deliveries():
    """采集端发送状态 + 下游已收批次/记录数 + 演示开关。"""
    store = collector_store()
    batches = store.list_all()
    counts = {
        "total": len(batches),
        "confirmed": sum(b["status"] == "confirmed" for b in batches),
        "unconfirmed": sum(b["status"] == "unconfirmed" for b in batches),
        "rejected": sum(b["status"] == "rejected" for b in batches),
    }

    downstream = None
    downstream_error = None
    try:
        downstream = fetch_downstream_batches(
            receiver_url(), timeout=receiver_timeout()
        )
    except (requests.RequestException, ValueError) as exc:
        downstream_error = f"接收服务不可达或响应异常：{exc}"

    demo = None
    demo_error = None
    try:
        demo = get_demo_state(receiver_url(), timeout=receiver_timeout())
    except (requests.RequestException, ValueError) as exc:
        demo_error = f"无法读取演示开关状态：{exc}"

    return render_template(
        "deliveries.html",
        batches=batches,
        counts=counts,
        status_label=status_label,
        receiver_url=receiver_url(),
        receiver_public_url=receiver_public_url(),
        downstream=downstream,
        downstream_error=downstream_error,
        demo=demo,
        demo_error=demo_error,
        demo_notice=request.args.get("demo"),
        demo_error_notice=request.args.get("demo_error"),
        retry_result=request.args.get("retry_result"),
    )


@app.post("/deliveries/retry-unconfirmed")
def retry_unconfirmed():
    """重启恢复后的批量重试：对所有未确认批次复用原冻结载荷重发。"""
    store = collector_store()
    pending = [
        b for b in store.list_all() if b["status"] == STATUS_UNCONFIRMED
    ]
    confirmed = 0
    for batch in pending:
        if _attempt_send(batch).outcome == "confirmed":
            confirmed += 1
    return redirect(
        url_for(
            "deliveries",
            retry_result=f"{confirmed}/{len(pending)}",
        )
    )


@app.get("/deliveries/<batch_id>")
def delivery_detail(batch_id: str):
    batch = collector_store().get(batch_id)
    if batch is None:
        return render_template(
            "error.html", message="批次不存在。", stage="批次查询"
        ), 404
    return render_template(
        "delivery_detail.html",
        batch=batch,
        status_label=status_label,
        receiver_url=receiver_url(),
        receiver_public_url=receiver_public_url(),
    )


@app.post("/deliveries/<batch_id>/retry")
def retry_delivery(batch_id: str):
    """重试发送：始终复用已冻结的原批次号与原内容。"""
    store = collector_store()
    batch = store.get(batch_id)
    if batch is None:
        return render_template(
            "error.html", message="批次不存在。", stage="重试"
        ), 404
    _attempt_send(batch)
    return redirect(url_for("delivery_detail", batch_id=batch_id))


@app.post("/demo/disconnect-next")
def demo_disconnect_next():
    enabled = request.form.get("enabled", "0") == "1"
    try:
        set_demo_disconnect(
            receiver_url(), enabled, timeout=receiver_timeout()
        )
    except (requests.RequestException, ValueError) as exc:
        return redirect(
            url_for(
                "deliveries",
                demo_error=f"设置演示开关失败：{exc}",
            )
        )
    return redirect(
        url_for(
            "deliveries",
            demo="armed" if enabled else "disarmed",
        )
    )


@app.errorhandler(404)
def not_found(_err):
    return render_template("error.html", message="页面不存在。", stage="路由"), 404


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=False)
