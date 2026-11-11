"""发送入库链路测试：冻结批次、幂等、断连演示、重启恢复。"""

import json
import os
import tempfile
import threading
import time
import unittest

import requests
from werkzeug.serving import WSGIRequestHandler, make_server

from app import contract
from app.collector_store import CollectorStore, STATUS_UNCONFIRMED
from app.receiver import receiver_app
from app.receiver_store import IngestConflict, ReceiverStore
from app.web import app


def sample_records(n=5):
    return [
        {
            "reservoir": "青山水库",
            "header_path": "水位（米） / 旬初",
            "value": str(100 + i),
            "source_row": 3 + i,
            "source_col": 2,
            "source_cell": f"R{3 + i}C2",
            "source_url": "/sample/notice.html",
        }
        for i in range(n)
    ]


class ContractTests(unittest.TestCase):
    def test_hash_is_stable_and_batch_id_derived_from_content(self):
        a = contract.build_payload(
            sample_records(), "/sample/notice.html", ["a", "b"], "2026-01-01"
        )
        b = contract.build_payload(
            sample_records(), "/sample/notice.html", ["a", "b"], "2026-02-02"
        )
        self.assertEqual(a["content_hash"], b["content_hash"])
        self.assertEqual(a["batch_id"], b["batch_id"])
        self.assertEqual(a["batch_id"], "B-" + a["content_hash"])

        changed = sample_records()
        changed[0]["value"] = "999"
        c = contract.build_payload(
            changed, "/sample/notice.html", ["a", "b"], "2026-01-01"
        )
        self.assertNotEqual(a["content_hash"], c["content_hash"])

    def test_validate_rejects_tampered_content_and_bad_count(self):
        payload = contract.build_payload(
            sample_records(), "/sample/notice.html", ["a"], "2026-01-01"
        )
        tampered = json.loads(json.dumps(payload))
        tampered["records"][0]["value"] = "篡改"
        with self.assertRaises(contract.ContractError):
            contract.validate_payload(tampered)

        bad_count = json.loads(json.dumps(payload))
        bad_count["record_count"] = 99
        with self.assertRaises(contract.ContractError):
            contract.validate_payload(bad_count)


class ReceiverStoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "receiver.db")

    def tearDown(self):
        self.tmp.cleanup()

    def test_idempotent_same_content_returns_original_receipt(self):
        store = ReceiverStore(self.db)
        payload = contract.build_payload(
            sample_records(3), "u", ["p"], "t"
        )
        receipt1, disconnect = store.ingest(contract.validate_payload(payload))
        receipt2, _ = store.ingest(contract.validate_payload(payload))
        self.assertFalse(disconnect)
        self.assertEqual(receipt1["receipt_id"], receipt2["receipt_id"])
        self.assertFalse(receipt1["idempotent"])
        self.assertTrue(receipt2["idempotent"])

        listing = store.list_batches()
        self.assertEqual(listing["batch_count"], 1)
        self.assertEqual(listing["record_count"], 3)
        detail = store.get_batch(payload["batch_id"])
        self.assertEqual(len(detail["records"]), 3)
        self.assertEqual(detail["records"][0]["reservoir"], "青山水库")

    def test_same_batch_id_different_content_is_rejected_and_original_kept(self):
        store = ReceiverStore(self.db)
        first = contract.build_payload(sample_records(3), "u", ["p"], "t")
        store.ingest(contract.validate_payload(first))

        changed = sample_records(3)
        changed[0]["value"] = "999"
        second = contract.build_payload(changed, "u", ["p"], "t")
        second["batch_id"] = first["batch_id"]
        with self.assertRaises(IngestConflict):
            store.ingest(contract.validate_payload(second))

        detail = store.get_batch(first["batch_id"])
        self.assertEqual(detail["records"][0]["value"], "100")
        self.assertEqual(detail["record_count"], 3)

    def test_disconnect_flag_is_one_time_and_persisted(self):
        store = ReceiverStore(self.db)
        store.set_disconnect_next(True)
        payload = contract.build_payload(sample_records(2), "u", ["p"], "t")
        receipt, disconnect = store.ingest(contract.validate_payload(payload))
        self.assertTrue(disconnect)
        self.assertFalse(store.demo_state()["disconnect_next"])
        # 重复入库不会再次断连
        _, disconnect_again = store.ingest(
            contract.validate_payload(payload)
        )
        self.assertFalse(disconnect_again)

    def test_data_survives_new_store_instance(self):
        store = ReceiverStore(self.db)
        payload = contract.build_payload(sample_records(3), "u", ["p"], "t")
        store.ingest(contract.validate_payload(payload))
        store.set_disconnect_next(True)

        reopened = ReceiverStore(self.db)
        self.assertIsNotNone(reopened.get_receipt(payload["batch_id"]))
        self.assertTrue(reopened.demo_state()["disconnect_next"])

    def test_ingest_rolls_back_whole_batch_on_record_failure(self):
        """批次主记录写成功、记录写入失败时，整批回滚，不留半批数据。"""
        import sqlite3

        store = ReceiverStore(self.db)
        store.init()
        conn = sqlite3.connect(self.db)
        conn.executescript(
            """
            DROP TABLE records;
            CREATE TABLE records (id INTEGER PRIMARY KEY);
            """
        )
        conn.commit()
        conn.close()

        payload = contract.build_payload(sample_records(3), "u", ["p"], "t")
        with self.assertRaises(sqlite3.OperationalError):
            store.ingest(contract.validate_payload(payload))

        self.assertEqual(store.list_batches()["batch_count"], 0)
        self.assertIsNone(store.get_batch(payload["batch_id"]))


class CollectorStoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "collector.db")

    def tearDown(self):
        self.tmp.cleanup()

    def test_reopen_restores_unconfirmed_batch_with_same_payload(self):
        store = CollectorStore(self.db)
        batch = store.freeze(sample_records(4), "u", ["p"])
        store.begin_attempt(batch["batch_id"])
        store.mark_unconfirmed(batch["batch_id"], "连接断开")

        reopened = CollectorStore(self.db)
        restored = reopened.get(batch["batch_id"])
        self.assertEqual(restored["status"], STATUS_UNCONFIRMED)
        self.assertEqual(restored["attempts"], 1)
        self.assertEqual(restored["last_error"], "连接断开")
        self.assertEqual(
            restored["payload"]["content_hash"], batch["content_hash"]
        )
        self.assertEqual(reopened.count_unconfirmed(), 1)


class QuietHandler(WSGIRequestHandler):
    def log(self, type, message, *args):  # noqa: A002
        pass


class DeliveryFlowTests(unittest.TestCase):
    """真实启动接收端 HTTP 服务，覆盖发送、断连与重试。"""

    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        receiver_app.config.update(
            RECEIVER_DB=os.path.join(cls.tmp.name, "receiver.db"),
            RECEIVER_PORT=0,
        )
        cls.server = make_server(
            "127.0.0.1", 0, receiver_app, threaded=True,
            request_handler=QuietHandler,
        )
        cls.port = cls.server.port
        cls.thread = threading.Thread(
            target=cls.server.serve_forever, daemon=True
        )
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.thread.join(timeout=5)
        cls.tmp.cleanup()

    def setUp(self):
        # 每个用例使用独立的采集端库，接收端库在用例间清理。
        self.case_tmp = tempfile.TemporaryDirectory()
        app.config.update(
            TESTING=True,
            COLLECTOR_DB=os.path.join(self.case_tmp.name, "collector.db"),
            RECEIVER_URL=f"http://127.0.0.1:{self.port}",
            RECEIVER_PUBLIC_URL=f"http://127.0.0.1:{self.port}",
            RECEIVER_TIMEOUT=5,
        )
        # 清空接收端（通过独立数据库文件重建并重新绑定配置）
        receiver_app.config["RECEIVER_DB"] = os.path.join(
            self.case_tmp.name, "receiver.db"
        )
        self.client = app.test_client()
        self.http = requests.Session()

    def tearDown(self):
        try:
            self.http.post(
                f"http://127.0.0.1:{self.port}/api/demo/disconnect-next",
                json={"enabled": False}, timeout=5,
            )
        except requests.RequestException:
            pass
        self.http.close()
        self.case_tmp.cleanup()

    def _convert_once(self):
        resp = self.client.post("/fetch", data={"url": "/sample/notice.html"})
        self.assertEqual(resp.status_code, 200)
        cache_key = resp.data.decode().split(
            'name="cache_key" value="'
        )[1].split('"')[0]
        resp = self.client.post(
            "/parse",
            data={"cache_key": cache_key, "table_index": 0, "header_rows": 2},
        )
        self.assertEqual(resp.status_code, 200)
        result_key = resp.data.decode().split("/download?key=")[1].split('"')[0]
        return result_key

    def _deliver(self, result_key):
        resp = self.client.post("/deliver", data={"result_key": result_key})
        self.assertEqual(resp.status_code, 302)
        return resp.headers["Location"].rsplit("/", 1)[-1]

    def test_two_tabs_same_result_create_one_batch_and_receiver_one_batch(self):
        key1 = self._convert_once()
        time.sleep(0.01)
        key2 = self._convert_once()
        self.assertNotEqual(key1, key2)

        batch1 = self._deliver(key1)
        batch2 = self._deliver(key2)
        self.assertEqual(batch1, batch2)

        store = CollectorStore(app.config["COLLECTOR_DB"])
        self.assertEqual(len(store.list_all()), 1)
        self.assertEqual(store.list_all()[0]["record_count"], 35)
        self.assertEqual(store.list_all()[0]["status"], "confirmed")

        listing = self.http.get(
            f"http://127.0.0.1:{self.port}/api/batches"
        ).json()
        self.assertEqual(listing["batch_count"], 1)
        self.assertEqual(listing["record_count"], 35)

        detail = self.client.get(f"/deliveries/{batch1}")
        self.assertIn("已确认入库".encode(), detail.data)
        self.assertIn("幂等返回".encode(), detail.data)

    def test_disconnect_then_retry_recovers_original_receipt(self):
        # 开启一次性“入库后断开连接”
        self.http.post(
            f"http://127.0.0.1:{self.port}/api/demo/disconnect-next",
            json={"enabled": True}, timeout=5,
        )
        key = self._convert_once()
        batch_id = self._deliver(key)

        store = CollectorStore(app.config["COLLECTOR_DB"])
        pending = store.get(batch_id)
        self.assertEqual(pending["status"], "unconfirmed")
        self.assertIn("发送中断", pending["last_error"])
        self.assertIsNone(pending["receipt"])

        # 下游其实已经整批入库，只是回执在断连中丢失
        listing = self.http.get(
            f"http://127.0.0.1:{self.port}/api/batches"
        ).json()
        self.assertEqual(listing["batch_count"], 1)
        self.assertEqual(listing["record_count"], 35)
        self.assertFalse(
            self.http.get(
                f"http://127.0.0.1:{self.port}/api/demo/state"
            ).json()["disconnect_next"]
        )

        # 重试沿用原批次号与内容 → 返回原回执
        resp = self.client.post(f"/deliveries/{batch_id}/retry")
        self.assertEqual(resp.status_code, 302)
        recovered = store.get(batch_id)
        self.assertEqual(recovered["status"], "confirmed")
        self.assertIsNotNone(recovered["receipt"])
        self.assertTrue(recovered["receipt"]["idempotent"])
        self.assertEqual(recovered["attempts"], 2)

        # 下游仍只有一批，未重复写入
        listing = self.http.get(
            f"http://127.0.0.1:{self.port}/api/batches"
        ).json()
        self.assertEqual(listing["batch_count"], 1)
        self.assertEqual(listing["record_count"], 35)

    def test_bulk_retry_recovers_all_unconfirmed_batches(self):
        self.http.post(
            f"http://127.0.0.1:{self.port}/api/demo/disconnect-next",
            json={"enabled": True}, timeout=5,
        )
        key = self._convert_once()
        batch_id = self._deliver(key)
        store = CollectorStore(app.config["COLLECTOR_DB"])
        self.assertEqual(store.get(batch_id)["status"], "unconfirmed")

        resp = self.client.post("/deliveries/retry-unconfirmed")
        self.assertEqual(resp.status_code, 302)
        self.assertTrue(store.get(batch_id)["status"] == "confirmed")
        listing = self.http.get(
            f"http://127.0.0.1:{self.port}/api/batches"
        ).json()
        self.assertEqual(listing["batch_count"], 1)
        self.assertEqual(listing["record_count"], 35)

    def test_deliveries_page_shows_failure_reason_and_downstream_counts(self):
        self.http.post(
            f"http://127.0.0.1:{self.port}/api/demo/disconnect-next",
            json={"enabled": True}, timeout=5,
        )
        key = self._convert_once()
        batch_id = self._deliver(key)
        page = self.client.get("/deliveries")
        self.assertEqual(page.status_code, 200)
        text = page.data.decode()
        self.assertIn("未确认", text)
        self.assertIn("发送中断", text)
        self.assertIn("下游已收批次", text)
        self.assertIn("35", text)
        self.assertIn(batch_id[:22], text)

    def test_receiver_returns_409_for_same_id_different_content(self):
        payload = contract.build_payload(
            sample_records(2), "/sample/notice.html", ["p"], "t"
        )
        r1 = self.http.post(
            f"http://127.0.0.1:{self.port}/api/batches", json=payload, timeout=5
        )
        self.assertEqual(r1.status_code, 200)

        changed = sample_records(2)
        changed[0]["value"] = "999"
        payload2 = contract.build_payload(
            changed, "/sample/notice.html", ["p"], "t"
        )
        payload2["batch_id"] = payload["batch_id"]
        r2 = self.http.post(
            f"http://127.0.0.1:{self.port}/api/batches", json=payload2, timeout=5
        )
        self.assertEqual(r2.status_code, 409)
        self.assertEqual(r2.json()["code"], "content_mismatch")

    def test_receiver_rejects_tampered_payload(self):
        payload = contract.build_payload(
            sample_records(2), "/sample/notice.html", ["p"], "t"
        )
        payload["records"][0]["value"] = "篡改"
        resp = self.http.post(
            f"http://127.0.0.1:{self.port}/api/batches",
            json=payload, timeout=5,
        )
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.json()["code"], "invalid_payload")
        self.assertEqual(
            self.http.get(f"http://127.0.0.1:{self.port}/api/batches").json()[
                "batch_count"
            ],
            0,
        )


if __name__ == "__main__":
    unittest.main()
