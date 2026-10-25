"""Verifica critérios de evidência, denominadores e restauração após falhas.

Estes testes usam um duplo mínimo da API; não substituem os experimentos reais.
"""
import base64
import json
import unittest
from pathlib import Path
from unittest.mock import patch

from lab.core import LabError
from lab.experiments import (
    _attempt, _e9, _partition_samples, _snapshot_evidence,
    _staleness_summary, run,
)


def response(value, revision=1):
    result = {"header": {"revision": str(revision), "raft_term": "1"}}
    if value is not None:
        result["kvs"] = [{"value": base64.b64encode(value.encode()).decode(), "mod_revision": str(revision)}]
    return result


class MemoryRecorder:
    def __init__(self, experiment="test", metadata=None):
        self.path = Path("unused-test-evidence")
        self.metadata = metadata or {}
        self.rows = []
        self.files = {}
        self.finished = False

    def measure(self, metric, elapsed_ms, **fields):
        self.rows.append({"metric": metric, "elapsed_ms": elapsed_ms, **fields})

    def json(self, name, data):
        self.files[name] = data

    def text(self, name, value):
        self.files[name] = value

    def finish(self):
        self.finished = True
        return self.path


class FakeClient:
    def __init__(self, cluster, member):
        self.cluster = cluster
        self.member = member

    def put(self, key, value):
        if self.cluster.fail_write and self.cluster.isolated:
            raise LabError("invalid credentials")
        self.cluster.revision += 1
        for member in self.cluster.members:
            if member not in self.cluster.isolated:
                self.cluster.values[member][key] = value
        return response(None, self.cluster.revision)

    def get(self, key, serializable=False):
        if self.cluster.fail_delayed_read and self.cluster.delay_ms:
            raise LabError("unexpected malformed response")
        if self.member in self.cluster.isolated:
            if not serializable:
                if not self.cluster.broken_partition:
                    raise LabError("context deadline exceeded")
                return response(self.cluster.values[1].get(key), self.cluster.revision)
            if self.cluster.fail_serializable:
                raise LabError("timed out")
        return response(self.cluster.values[self.member].get(key), self.cluster.revision)

    def status(self):
        return {"header": {"member_id": str(self.member), "raft_term": "1"},
                "leader": "1", "raftTerm": "1", "raftIndex": str(self.cluster.revision),
                "raftAppliedIndex": str(self.cluster.revision)}

    def close(self):
        pass


class FakeCluster:
    nodes = 3
    members = [1, 2, 3]

    def __init__(self):
        self.values = {member: {} for member in self.members}
        self.revision = 0
        self.isolated = set()
        self.healed = []
        self.delays = []
        self.delay_ms = 0
        self.recoveries = 0
        self.fail_write = False
        self.fail_serializable = False
        self.fail_delayed_read = False
        self.broken_partition = False

    def client(self, member, timeout=2):
        return FakeClient(self, member)

    def leader(self):
        return 1

    def isolate(self, member):
        self.isolated.add(member)

    def heal(self, member):
        self.isolated.discard(member)
        self.healed.append(member)
        self.values[member].update(self.values[1])

    def delay(self, delay):
        self.delays.append(delay)
        self.delay_ms = delay

    def wait_healthy(self, timeout=45):
        return True

    def recover(self):
        self.recoveries += 1
        self.isolated.clear()
        self.delay_ms = 0

    def logs(self):
        return "fake logs only for unit tests"


class StalenessTests(unittest.TestCase):
    def test_errors_are_not_stale_reads(self):
        records = [
            {"mode": "serializable", "ok": True, "expected": "nova", "observed": "antiga"},
            {"mode": "serializable", "ok": True, "expected": "nova", "observed": "nova"},
            {"mode": "serializable", "ok": False, "error": "timed out"},
            {"mode": "linearizable", "ok": False, "error": "timed out"},
        ]
        summary = _staleness_summary(records)
        self.assertEqual(summary["serializable"]["attempts"], 3)
        self.assertEqual(summary["serializable"]["successful_reads"], 2)
        self.assertEqual(summary["serializable"]["stale_reads"], 1)
        self.assertEqual(summary["serializable"]["stale_rate_successful_reads"], 0.5)
        self.assertEqual(summary["serializable"]["stale_rate_all_attempts"], 1 / 3)
        self.assertIsNone(summary["linearizable"]["stale_rate_successful_reads"])
        self.assertEqual(summary["linearizable"]["stale_reads"], 0)

    def test_empty_denominator_is_none(self):
        for result in _staleness_summary([]).values():
            self.assertIsNone(result["stale_rate_successful_reads"])
            self.assertIsNone(result["stale_rate_all_attempts"])

    def test_partition_compares_each_read_to_preceding_confirmed_write(self):
        cluster = FakeCluster()
        recorder = MemoryRecorder()
        data = _partition_samples(cluster, recorder, isolated=2, writer=1, count=5, linear_reads=5)
        self.assertFalse(cluster.isolated)
        self.assertEqual(cluster.healed, [2])
        self.assertEqual(data["summary"]["serializable"]["stale_reads"], 5)
        self.assertEqual(data["summary"]["linearizable"]["errors"], 5)
        revisions = [r["confirmed_write_revision"] for r in data["samples"] if r["mode"] == "serializable"]
        self.assertEqual(len(set(revisions)), 5)
        self.assertEqual(revisions, sorted(revisions))

    def test_partition_is_healed_after_unexpected_write_error(self):
        cluster = FakeCluster()
        cluster.fail_write = True
        with self.assertRaisesRegex(LabError, "invalid credentials"):
            _partition_samples(cluster, MemoryRecorder(), 2, 1, 5)
        self.assertFalse(cluster.isolated)
        self.assertEqual(cluster.healed, [2])

    def test_zero_success_local_reads_does_not_produce_stale_rate(self):
        cluster = FakeCluster()
        cluster.fail_serializable = True
        with self.assertRaisesRegex(LabError, "Nenhuma leitura local"):
            _partition_samples(cluster, MemoryRecorder(), 2, 1, 5)
        self.assertFalse(cluster.isolated)

    def test_linearizable_success_rejects_incomplete_partition(self):
        cluster = FakeCluster()
        cluster.broken_partition = True
        with self.assertRaisesRegex(LabError, "partição não foi demonstrada"):
            _partition_samples(cluster, MemoryRecorder(), 2, 1, 5)
        self.assertFalse(cluster.isolated)


class EvidenceAndCleanupTests(unittest.TestCase):
    def test_unexpected_errors_are_not_accepted_as_consensus_failure(self):
        recorder = MemoryRecorder()

        def broken():
            raise LabError("HTTP 403: invalid credentials")

        with self.assertRaisesRegex(LabError, "invalid credentials"):
            _attempt(broken, recorder, "test")
        self.assertFalse(recorder.rows[0]["success"])

    def test_timeout_is_preserved_as_failed_sample(self):
        recorder = MemoryRecorder()

        def offline():
            raise LabError("context deadline exceeded")

        result = _attempt(offline, recorder, "test", repetition=1)
        self.assertFalse(result["ok"])
        self.assertEqual(recorder.rows[0]["repetition"], 1)

    def test_snapshot_configuration_and_local_restore_are_not_transfer_evidence(self):
        logs = '\n'.join(json.dumps(line) for line in [
            {"level": "info", "msg": "starting etcd server", "snapshot-count": 1000},
            {"level": "info", "msg": "triggering snapshot", "snapshot-index": 3000},
            {"level": "info", "msg": "restored last-stored snapshot", "snapshot-index": 1000},
            {"level": "info", "msg": "successfully sent snapshot", "snapshot-index": 4000},
        ])
        self.assertEqual(_snapshot_evidence(logs), [])

    def test_snapshot_receipt_must_exceed_pre_stop_index(self):
        entries = [
            {"level": "info", "msg": "received and saved database snapshot", "incoming-snapshot-index": 100},
            {"level": "info", "msg": "applied incoming Raft snapshot", "snapshot-index": 2000},
            {"level": "error", "msg": "applied incoming Raft snapshot", "snapshot-index": 3000},
        ]
        logs = '\n'.join("etcd2-1 | " + json.dumps(line) for line in entries)
        evidence = _snapshot_evidence(logs, minimum_index=500)
        self.assertEqual(len(evidence), 1)
        self.assertIn("2000", evidence[0])

    def test_e9_removes_netem_when_delayed_condition_fails(self):
        cluster = FakeCluster()
        cluster.fail_delayed_read = True
        with self.assertRaisesRegex(LabError, "unexpected malformed"):
            _e9(cluster, MemoryRecorder(), repetitions=5, reads=5, delay_ms=25)
        self.assertEqual(cluster.delays, [25, 0])
        self.assertEqual(cluster.delay_ms, 0)

    def test_e9_keeps_conditions_separate_and_explicit_concurrency(self):
        cluster = FakeCluster()
        recorder = MemoryRecorder()
        _e9(cluster, recorder, repetitions=5, reads=5, delay_ms=25)
        conditions = recorder.files["leituras.json"]["conditions"]
        self.assertEqual(len(conditions), 4)
        self.assertEqual({(x["mode"], x["delay_ms_one_way_per_peer"]) for x in conditions},
                         {("linearizable", 0), ("serializable", 0), ("linearizable", 25), ("serializable", 25)})
        self.assertTrue(all(x["successful_reads"] == 25 and len(x["batches"]) == 5 for x in conditions))
        self.assertEqual(recorder.files["stale.json"]["summary"]["serializable"]["stale_reads"], 5)
        metrics = {row["metric"] for row in recorder.rows if row["metric"].startswith("read_latency_")}
        self.assertEqual(len(metrics), 4)

    def test_runner_preserves_failed_evidence_and_recovers(self):
        cluster = FakeCluster()
        recorder = MemoryRecorder()

        def broken(*args):
            cluster.isolate(2)
            raise LabError("controlled failure")

        with patch("lab.experiments.Cluster", return_value=cluster), \
             patch("lab.experiments.Recorder", return_value=recorder), \
             patch.dict("lab.experiments.EXPERIMENTS", {"E4": broken}):
            with self.assertRaisesRegex(LabError, "controlled failure"):
                run("E4")
        self.assertFalse(cluster.isolated)
        self.assertEqual(cluster.recoveries, 2)
        self.assertEqual(recorder.metadata["status"], "failed_or_interrupted")
        self.assertTrue(recorder.finished)
        self.assertIn("failure.json", recorder.files)

    def test_e9_rejects_zero_delay_before_creating_cluster(self):
        with patch("lab.experiments.Cluster") as constructor:
            with self.assertRaisesRegex(ValueError, "condição com atraso"):
                run("E9", delay_ms=0)
            constructor.assert_not_called()


if __name__ == "__main__":
    unittest.main()
