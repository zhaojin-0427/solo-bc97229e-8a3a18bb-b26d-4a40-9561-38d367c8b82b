"""本地 SQLite 接收服务：整批原子入库 + 幂等回执 + 演示开关。

对外接口
========
``POST /api/batches``
    请求体为采集端冻结的批次载荷（见 :mod:`app.contract`）。首次入库
    返回 200 与新回执；同批次号同内容返回 200 与原回执（``idempotent``
    为 true）；同批次号不同内容返回 409。
``GET  /api/batches``
    已收批次列表及批次/记录总数。
``GET  /api/batches/<batch_id>``
    单个批次详情（含全部记录）。
``GET  /api/demo/state`` / ``POST /api/demo/disconnect-next``
    一次性“入库后断开连接”演示开关（提交后立即消费）。

接收端默认使用 Werkzeug 多线程服务器，以便演示开关可以在提交事务后
直接关闭 TCP 连接，让采集端真实地收不到回执。数据文件位于独立卷，
容器重启后仍然保留。
"""

from __future__ import annotations

import os
import socket
import struct
from flask import Flask, jsonify, redirect, render_template, request, url_for

from .contract import ContractError, validate_payload
from .receiver_store import IngestConflict, ReceiverStore

DEFAULT_DB = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "receiver.db",
)

receiver_app = Flask(__name__)
receiver_app.config.setdefault(
    "RECEIVER_DB", os.environ.get("RECEIVER_DB", DEFAULT_DB)
)
receiver_app.config.setdefault(
    "RECEIVER_MAX_BODY", int(os.environ.get("RECEIVER_MAX_BODY", 16 * 1024 * 1024))
)
receiver_app.config.setdefault(
    "RECEIVER_PORT", int(os.environ.get("RECEIVER_PORT", 9000))
)


def get_store() -> ReceiverStore:
    store = ReceiverStore(receiver_app.config["RECEIVER_DB"])
    store.init()
    return store


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------

@receiver_app.get("/api/health")
def api_health():
    return jsonify({"status": "ok", "service": "hydro-ingest-receiver"})


@receiver_app.post("/api/batches")
def api_ingest():
    if request.content_length and request.content_length > receiver_app.config[
        "RECEIVER_MAX_BODY"
    ]:
        return jsonify(
            {"code": "invalid_payload", "error": "批次载荷过大。"}
        ), 413
    payload = request.get_json(silent=True)
    try:
        normalized = validate_payload(payload)
    except ContractError as exc:
        return jsonify({"code": "invalid_payload", "error": str(exc)}), 400

    store = get_store()
    try:
        receipt, disconnect_now = store.ingest(normalized)
    except IngestConflict as exc:
        return jsonify(
            {
                "code": "content_mismatch",
                "error": str(exc),
                "batch_id": normalized["batch_id"],
            }
        ), 409

    if disconnect_now:
        # 数据已经提交并落库，随后不发送回执，直接断开连接。
        closed = _close_client_socket()
        if closed:
            # 返回一个空响应；客户端通常已在读到响应前观察到连接断开。
            return "", 204

    return jsonify(receipt), 200


@receiver_app.get("/api/batches")
def api_batches():
    return jsonify(get_store().list_batches())


@receiver_app.get("/api/batches/<batch_id>")
def api_batch_detail(batch_id: str):
    batch = get_store().get_batch(batch_id)
    if batch is None:
        return jsonify({"code": "not_found", "error": "批次不存在。"}), 404
    return jsonify(batch)


@receiver_app.get("/api/demo/state")
def api_demo_state():
    return jsonify(get_store().demo_state())


@receiver_app.post("/api/demo/disconnect-next")
def api_demo_disconnect_next():
    body = request.get_json(silent=True) or {}
    enabled = body.get("enabled", True)
    if not isinstance(enabled, bool):
        return jsonify(
            {"code": "invalid_payload", "error": "enabled 必须是布尔值。"}
        ), 400
    return jsonify(get_store().set_disconnect_next(enabled))


# ---------------------------------------------------------------------------
# 网页入口（接收端自己的状态页，便于单独演示开关）
# ---------------------------------------------------------------------------

@receiver_app.get("/")
def index():
    store = get_store()
    listing = store.list_batches()
    state = store.demo_state()
    return render_template(
        "receiver.html",
        batches=listing["batches"],
        batch_count=listing["batch_count"],
        record_count=listing["record_count"],
        demo=state,
    )


@receiver_app.post("/demo/disconnect-next")
def demo_disconnect_next_form():
    enabled = request.form.get("enabled", "0") == "1"
    get_store().set_disconnect_next(enabled)
    return redirect(url_for("index"))


# ---------------------------------------------------------------------------
# “入库后断开连接”实现
# ---------------------------------------------------------------------------

def _close_client_socket() -> bool:
    """提交事务后强制关闭当前客户端连接（发送 TCP RST）。

    只有 Werkzeug 服务器会在 WSGI environ 中暴露 ``werkzeug.socket``。
    若运行在其他 WSGI 容器中，则退化为 204 空响应（仍不返回回执）。
    """
    sock = request.environ.get("werkzeug.socket")
    if sock is None:
        return False
    try:
        # SO_LINGER(on, 0) 让 close 直接发出 RST，而不是正常四次挥手。
        sock.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_LINGER,
            struct.pack("ii", 1, 0),
        )
    except OSError:
        pass
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    try:
        sock.close()
    except OSError:
        pass
    return True


def main() -> None:
    port = receiver_app.config["RECEIVER_PORT"]
    # 使用 Werkzeug 多线程服务器：WSGI environ 中有 werkzeug.socket，
    # 便于演示“提交事务后强制断开客户端连接”。
    from werkzeug.serving import WSGIRequestHandler, make_server

    class QuietRequestHandler(WSGIRequestHandler):
        def log(self, type, message, *args):  # noqa: A002
            pass

    server = make_server(
        "0.0.0.0",
        port,
        receiver_app,
        threaded=True,
        request_handler=QuietRequestHandler,
    )
    print(f"hydro ingest receiver listening on 0.0.0.0:{port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
