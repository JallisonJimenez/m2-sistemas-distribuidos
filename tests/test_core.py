"""Contrato HTTP e evidências, sem substituir os experimentos com etcd real."""
import base64
import csv
import json
import tempfile
import threading
import unittest
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

from lab.core import Client, Cluster, LabError, Recorder, compose_spec, wait_for


@contextmanager
def http_endpoint(responses):
    """Servidor local que registra as requisições realmente recebidas."""
    requests = []
    pending = iter(responses)

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_POST(self):
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            requests.append({"path": self.path, "body": json.loads(body),
                             "content_type": self.headers.get("Content-Type"),
                             "remote_port": self.client_address[1]})
            status, data = next(pending)
            raw = data if isinstance(data, bytes) else json.dumps(data).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    client = Client(f"http://127.0.0.1:{server.server_port}", timeout=1.0)
    try:
        yield client, requests
    finally:
        client.close()
        server.shutdown()
        server.server_close()
        worker.join(timeout=2)


class ClientHttpTests(unittest.TestCase):
    def test_unicode_is_encoded_as_utf8_base64_and_response_preserved(self):
        response = {"header": {"revision": "9007199254740993", "raft_term": "8"}}
        with http_endpoint([(200, response)]) as (client, requests):
            self.assertEqual(client.put("/config/ação", "produção ☕"), response)
            request = requests[0]
            self.assertEqual(request["path"], "/v3/kv/put")
            self.assertEqual(request["content_type"], "application/json")
            self.assertEqual(base64.b64decode(request["body"]["key"]).decode("utf-8"), "/config/ação")
            self.assertEqual(base64.b64decode(request["body"]["value"]).decode("utf-8"), "produção ☕")

    def test_read_consistency_is_explicit_and_connections_are_reused(self):
        with http_endpoint([(200, {}), (200, {})]) as (client, requests):
            client.get("/x")
            client.get("/x", serializable=True)
            self.assertEqual([r["body"]["serializable"] for r in requests], [False, True])
            self.assertEqual([r["path"] for r in requests], ["/v3/kv/range"] * 2)
            self.assertEqual(requests[0]["remote_port"], requests[1]["remote_port"])

    def test_http_error_is_not_retried_and_next_call_opens_new_connection(self):
        # etcd >=3.6 uses message/code rather than a top-level error member.
        error = {"code": 4, "message": "context deadline exceeded", "details": []}
        with http_endpoint([(504, error), (200, {"kvs": []})]) as (client, requests):
            with self.assertRaisesRegex(LabError, "context deadline exceeded"):
                client.get("/x")
            self.assertIsNone(client.connection)
            self.assertEqual(len(requests), 1, "Falhas não podem gerar repetição automática")
            self.assertEqual(client.get("/x"), {"kvs": []})
            self.assertEqual(len(requests), 2)
            self.assertNotEqual(requests[0]["remote_port"], requests[1]["remote_port"])

    def test_legacy_gateway_error_in_success_http_is_rejected(self):
        with http_endpoint([(200, {"error": "etcdserver: no leader"})]) as (client, requests):
            with self.assertRaisesRegex(LabError, "no leader"):
                client.get("/x")
            self.assertEqual(len(requests), 1)
            self.assertIsNone(client.connection)

    def test_invalid_json_becomes_lab_error_and_connection_is_closed(self):
        with http_endpoint([(200, b"invalid-json")]) as (client, requests):
            with self.assertRaises(LabError):
                client.status()
            self.assertEqual(requests[0]["path"], "/v3/maintenance/status")
            self.assertEqual(requests[0]["body"], {})
            self.assertIsNone(client.connection)

    def test_value_distinguishes_missing_key_from_empty_string(self):
        self.assertIsNone(Client.value({"header": {"revision": "1"}}))
        self.assertIsNone(Client.value({"kvs": []}))
        self.assertEqual(Client.value({"kvs": [{"key": "eA==", "value": ""}]}), "")
        value = base64.b64encode("configuração".encode("utf-8")).decode("ascii")
        self.assertEqual(Client.value({"kvs": [{"value": value}]}), "configuração")


class ComposeTopologyTests(unittest.TestCase):
    def test_three_and_five_members_have_separate_peer_network_and_finite_resources(self):
        for nodes in (3, 5):
            with self.subTest(nodes=nodes):
                spec = compose_spec(nodes)
                self.assertEqual(len(spec["services"]), nodes)
                self.assertTrue(spec["networks"]["raft"]["internal"])
                peers = ",".join(f"etcd{i}=http://172.29.0.{10+i}:2380" for i in range(1, nodes + 1))
                ports = set()
                for i in range(1, nodes + 1):
                    service = spec["services"][f"etcd{i}"]
                    self.assertIn(f"--initial-cluster={peers}", service["command"])
                    self.assertIn(f"--listen-peer-urls=http://172.29.0.{10+i}:2380", service["command"])
                    self.assertNotIn("--listen-peer-urls=http://0.0.0.0:2380", service["command"])
                    self.assertEqual(set(service["networks"]), {"raft", "clients"})
                    self.assertEqual(service["mem_limit"], "256m")
                    self.assertEqual(service["cpus"], 0.5)
                    port = service["ports"][0]
                    self.assertTrue(port.startswith("127.0.0.1:"))
                    self.assertTrue(port.endswith(":2379"))
                    ports.add(port)
                self.assertEqual(len(ports), nodes)

    def test_unsupported_cluster_sizes_are_rejected(self):
        for nodes in (0, 1, 2, 4, 6):
            with self.subTest(nodes=nodes), self.assertRaises(ValueError):
                compose_spec(nodes)

    def test_endpoints_do_not_silently_fail_over(self):
        cluster = Cluster(5)
        self.assertEqual(cluster.client(1).url, "http://127.0.0.1:12379")
        self.assertEqual(cluster.client(5).url, "http://127.0.0.1:16379")
        with self.assertRaises(ValueError):
            cluster.client(6)


class RecorderTests(unittest.TestCase):
    def test_failed_samples_are_preserved_but_excluded_from_latency_statistics(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as directory, patch("lab.core.ROOT", Path(directory)):
            recorder = Recorder("E9", {"nodes": 3})
            recorder.measure("read-l", 10, success=True)
            recorder.measure("read-l", 20, success=True)
            recorder.measure("read-l", 2000, success=False, error="deadline")
            recorder.measure("read-s", 5, success=True)
            recorder.measure("all-failed", 3000, success=False)
            output = recorder.finish()
            raw = json.loads((output / "measurements.json").read_text(encoding="utf-8"))
            self.assertEqual(len(raw), 5)
            summary = {row["metric"]: row for row in json.loads((output / "summary.json").read_text(encoding="utf-8"))}
            linear = summary["read-l"]
            self.assertEqual(linear["attempts"], 3)
            self.assertEqual(linear["successful_samples"], 2)
            self.assertEqual(linear["failed_samples"], 1)
            self.assertEqual(linear["mean_ms"], 15)
            self.assertAlmostEqual(linear["sample_stdev_ms"], 50 ** 0.5)
            self.assertIsNone(summary["read-s"]["sample_stdev_ms"])
            self.assertIsNone(summary["all-failed"]["mean_ms"])
            self.assertIsNone(summary["all-failed"]["sample_stdev_ms"])
            with (output / "measurements.csv").open(encoding="utf-8", newline="") as handle:
                self.assertEqual(len(list(csv.DictReader(handle))), 5)
            metadata = json.loads((output / "metadata.json").read_text(encoding="utf-8"))
            self.assertEqual(metadata["nodes"], 3)
            self.assertIn("finished_utc", metadata)

    def test_empty_run_has_no_synthetic_measurements(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as directory, patch("lab.core.ROOT", Path(directory)):
            output = Recorder("E1").finish()
            self.assertEqual(json.loads((output / "measurements.json").read_text(encoding="utf-8")), [])
            self.assertEqual(json.loads((output / "summary.json").read_text(encoding="utf-8")), [])


class WaitForTests(unittest.TestCase):
    def test_transient_operational_error_is_retried_but_programming_error_is_not(self):
        count = 0

        def eventually():
            nonlocal count
            count += 1
            if count == 1:
                raise LabError("sem líder")
            return "healthy"

        self.assertEqual(wait_for(eventually, timeout=1, interval=0.001), "healthy")
        with self.assertRaisesRegex(ValueError, "bug"):
            wait_for(lambda: (_ for _ in ()).throw(ValueError("bug")), timeout=1)


if __name__ == "__main__":
    unittest.main()
