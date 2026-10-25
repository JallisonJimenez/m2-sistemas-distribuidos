"""Infraestrutura, API HTTP do etcd e evidências. Apenas biblioteca padrão."""
from __future__ import annotations

import base64
import csv
import http.client
import json
import os
import platform
import shutil
import statistics
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]
ETCD_IMAGE = "quay.io/coreos/etcd:v3.7.3"
NETEM_IMAGE = "sd-etcd-netem:local"


class LabError(RuntimeError):
    """Falha operacional com mensagem destinada ao usuário."""


def wait_for(predicate, timeout=30, interval=0.1):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            result = predicate()
            if result:
                return result
        except (LabError, OSError, http.client.HTTPException) as exc:
            last_error = exc
        time.sleep(interval)
    raise LabError(f"Tempo limite de {timeout}s aguardando condição; último erro: {last_error}")


def b64(value: str) -> str:
    return base64.b64encode(value.encode("utf-8")).decode("ascii")


class Client:
    """Um endpoint, sem failover ou repetição automática de operações."""

    def __init__(self, url: str, timeout=2.0):
        parts = urlsplit(url)
        if parts.scheme != "http" or not parts.hostname or parts.path not in ("", "/"):
            raise ValueError("Este laboratório usa URL http://host:porta, sem caminho.")
        self.url = url.rstrip("/")
        self.timeout = timeout
        self.host = parts.hostname
        self.port = parts.port or 80
        self.connection = None

    def close(self):
        if self.connection:
            self.connection.close()
            self.connection = None

    def post(self, path: str, payload: dict) -> dict:
        if self.connection is None:
            self.connection = http.client.HTTPConnection(self.host, self.port, timeout=self.timeout)
        try:
            body = json.dumps(payload).encode("utf-8")
            self.connection.request("POST", path, body, {"Content-Type": "application/json"})
            response = self.connection.getresponse()
            raw = response.read()
            data = json.loads(raw.decode("utf-8"))
            if response.status >= 400 or "error" in data:
                raise LabError(f"{self.url}{path}: HTTP {response.status}: {data}")
            return data
        except (OSError, http.client.HTTPException, ValueError, LabError) as exc:
            self.close()
            raise LabError(f"Operação falhou em {self.url}{path}: {exc}") from exc

    def put(self, key: str, value: str) -> dict:
        return self.post("/v3/kv/put", {"key": b64(key), "value": b64(value)})

    def get(self, key: str, serializable=False) -> dict:
        return self.post("/v3/kv/range", {"key": b64(key), "serializable": bool(serializable)})

    def status(self) -> dict:
        return self.post("/v3/maintenance/status", {})

    @staticmethod
    def value(response: dict) -> str | None:
        kvs = response.get("kvs", [])
        return base64.b64decode(kvs[0].get("value", "")).decode("utf-8") if kvs else None


def compose_spec(nodes: int) -> dict:
    if nodes not in (3, 5):
        raise ValueError("Use 3 ou 5 membros.")
    peers = ",".join(f"etcd{i}=http://172.29.0.{10+i}:2380" for i in range(1, nodes + 1))
    services, volumes = {}, {}
    for i in range(1, nodes + 1):
        peer, client = f"172.29.0.{10+i}", f"172.29.1.{10+i}"
        services[f"etcd{i}"] = {
            "image": ETCD_IMAGE,
            "entrypoint": ["/usr/local/bin/etcd"],
            "command": [f"--name=etcd{i}", "--data-dir=/etcd-data",
                        f"--listen-peer-urls=http://{peer}:2380",
                        f"--initial-advertise-peer-urls=http://{peer}:2380",
                        "--listen-client-urls=http://0.0.0.0:2379",
                        f"--advertise-client-urls=http://{client}:2379",
                        f"--initial-cluster={peers}", "--initial-cluster-state=new",
                        f"--initial-cluster-token=sd-etcd-n{nodes}",
                        "--heartbeat-interval=100", "--election-timeout=1000",
                        "--snapshot-count=1000", "--snapshot-catchup-entries=100",
                        "--log-level=info"],
            "mem_limit": "256m", "cpus": 0.5, "stop_grace_period": "10s",
            "ports": [f"127.0.0.1:{12379+(i-1)*1000}:2379"],
            "volumes": [f"data{i}:/etcd-data"],
            "networks": {"raft": {"ipv4_address": peer}, "clients": {"ipv4_address": client}},
            "healthcheck": {"test": ["CMD", "/usr/local/bin/etcdctl",
                                      "--endpoints=http://127.0.0.1:2379",
                                      "--command-timeout=2s", "endpoint", "health"],
                            "interval": "5s", "timeout": "3s", "retries": 12},
        }
        volumes[f"data{i}"] = {}
    return {"services": services, "volumes": volumes,
            "networks": {"raft": {"internal": True, "ipam": {"config": [{"subnet": "172.29.0.0/24"}]}},
                         "clients": {"ipam": {"config": [{"subnet": "172.29.1.0/24"}]}}}}


class Cluster:
    def __init__(self, nodes=None, root=ROOT):
        self.root = Path(root)
        self.runtime = self.root / ".lab"
        self.state = self.runtime / "state.json"
        if nodes is None:
            if not self.state.exists():
                raise LabError("Laboratório não configurado. Execute: python -m lab up --nodes 3")
            nodes = json.loads(self.state.read_text(encoding="utf-8"))["nodes"]
        if nodes not in (3, 5):
            raise ValueError("Use 3 ou 5 membros.")
        self.nodes = nodes
        self.members = list(range(1, nodes + 1))
        self.project = f"sd-etcd-n{nodes}"
        self.config = self.runtime / f"compose-{nodes}.json"
        self.network = f"{self.project}_raft"

    def _run(self, args, timeout=60, check=True):
        try:
            result = subprocess.run(args, cwd=self.root, capture_output=True, text=True,
                                    encoding="utf-8", errors="replace", timeout=timeout,
                                    creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
        except FileNotFoundError as exc:
            raise LabError("Docker não encontrado. Instale Docker Desktop/Engine e inicie-o.") from exc
        except subprocess.TimeoutExpired as exc:
            raise LabError(f"Tempo limite ({timeout}s): {' '.join(args)}") from exc
        if check and result.returncode:
            raise LabError(f"Comando falhou ({result.returncode}): {' '.join(args)}\n{result.stdout}{result.stderr}")
        # Saídas estruturadas (JSON/IDs) não podem receber avisos do stderr.
        return result.stdout

    def command(self, args, timeout=60):
        return self._run(["docker", "compose", "-p", self.project, "-f", str(self.config), *args], timeout)

    def up(self):
        self._run(["docker", "info"], timeout=20)
        if self.state.exists():
            previous = json.loads(self.state.read_text(encoding="utf-8"))["nodes"]
            if previous != self.nodes:
                old = Cluster(previous, self.root)
                if old.config.exists() and old.command(["ps", "-q"]).strip():
                    raise LabError("Pare o cluster atual com python -m lab down antes de trocar 3/5 membros.")
        self.runtime.mkdir(exist_ok=True)
        self.config.write_text(json.dumps(compose_spec(self.nodes), indent=2) + "\n", encoding="utf-8")
        self.state.write_text(json.dumps({"nodes": self.nodes, "image": ETCD_IMAGE}, indent=2), encoding="utf-8")
        self.command(["up", "-d"], timeout=300)
        self.wait_healthy(timeout=90)

    def url(self, member):
        self._validate(member)
        return f"http://127.0.0.1:{12379+(member-1)*1000}"

    def client(self, member, timeout=2.0):
        return Client(self.url(member), timeout)

    def _validate(self, member):
        if member not in self.members:
            raise ValueError(f"Membro inválido: {member}")

    def _ctl_args(self, member, args):
        self._validate(member)
        return ["exec", "-T", f"etcd{member}", "/usr/local/bin/etcdctl",
                "--endpoints=http://127.0.0.1:2379", "--dial-timeout=2s", "--command-timeout=5s", *args]

    def ctl(self, member, args, timeout=15):
        return self.command(self._ctl_args(member, args), timeout)

    def spawn_ctl(self, member, args):
        return subprocess.Popen(["docker", "compose", "-p", self.project, "-f", str(self.config),
                                 *self._ctl_args(member, args)], cwd=self.root, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True, encoding="utf-8", errors="replace",
                                creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)

    def container(self, member):
        self._validate(member)
        value = self.command(["ps", "-a", "-q", f"etcd{member}"]).strip()
        if not value:
            raise LabError(f"Contêiner etcd{member} não encontrado.")
        return value

    def stop(self, member):
        self._validate(member)
        self.command(["stop", f"etcd{member}"])

    def start(self, member):
        self._validate(member)
        self.command(["start", f"etcd{member}"])

    def isolate(self, member):
        self._run(["docker", "network", "disconnect", self.network, self.container(member)])

    def heal(self, member):
        container = self.container(member)
        networks = json.loads(self._run(["docker", "inspect", "--format", "{{json .NetworkSettings.Networks}}", container]))
        if self.network not in networks:
            self._run(["docker", "network", "connect", "--ip", f"172.29.0.{10+member}", self.network, container])

    def _netem(self, member, args):
        return self._run(["docker", "run", "--rm", "--network", f"container:{self.container(member)}",
                          "--cap-add=NET_ADMIN", "--memory=64m", NETEM_IMAGE, *args])

    def delay(self, ms):
        if ms < 0 or ms > 200:
            raise ValueError("Use atraso entre 0 e 200 ms por saída de cada interface Raft.")
        self._run(["docker", "build", "-t", NETEM_IMAGE, str(self.root / "netem")], timeout=300)
        for member in self.members:
            other = 1 if member != 1 else 2
            route = json.loads(self._netem(member, ["ip", "-j", "route", "get", f"172.29.0.{10+other}"]))
            dev = route[0]["dev"]
            if ms:
                self._netem(member, ["tc", "qdisc", "replace", "dev", dev, "root", "netem", "delay", f"{ms}ms"])
            else:
                # A inexistência de qdisc netem é idempotente; demais erros não são ocultados.
                output = self._netem(member, ["tc", "-j", "qdisc", "show", "dev", dev])
                if any(q.get("kind") == "netem" for q in json.loads(output)):
                    self._netem(member, ["tc", "qdisc", "del", "dev", dev, "root"])

    def leader(self):
        ids, statuses = {}, []
        for member in self.members:
            try:
                status = self.client(member).status()
                ids[str(status["header"]["member_id"])] = member
                statuses.append(status)
            except LabError:
                continue
        leaders = [str(s.get("leader", 0)) for s in statuses]
        for leader in leaders:
            if leader != "0" and leaders.count(leader) >= self.nodes // 2 + 1 and leader in ids:
                return ids[leader]
        raise LabError("Sem líder confirmado por uma maioria dos membros.")

    def wait_healthy(self, timeout=45):
        def healthy():
            statuses = [self.client(i).status() for i in self.members]
            leaders = {str(s.get("leader", 0)) for s in statuses}
            if len(leaders) != 1 or "0" in leaders:
                return False
            # Status local não prova disponibilidade de consenso; exigir leitura linearizável.
            for i in self.members:
                self.client(i).get("/sd/health")
            return statuses
        return wait_for(healthy, timeout)

    def logs(self):
        return self.command(["logs", "--no-color", "--timestamps"], timeout=30)

    def stats(self):
        containers = [self.container(i) for i in self.members]
        return self._run(["docker", "stats", "--no-stream", "--format", "{{json .}}", *containers], timeout=30)

    def recover(self):
        for i in self.members:
            # Restaurar o IP antes do bind: um membro parado e isolado não
            # consegue iniciar escutando em um endereço que ainda não existe.
            self.heal(i)
            self.start(i)
        # Não construir a imagem se nenhum atraso chegou a ser instalado.
        found = self._run(["docker", "images", "-q", NETEM_IMAGE]).strip()
        if found:
            self.delay(0)
        self.wait_healthy(timeout=90)

    def down(self):
        self.command(["down"], timeout=90)  # Sem -v: preserva os dados do laboratório.


class Recorder:
    def __init__(self, experiment, metadata=None):
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
        self.path = ROOT / "results" / f"{stamp}-{experiment.upper()}"
        self.path.mkdir(parents=True)
        self.rows = []
        self.metadata = {"experiment": experiment.upper(), "started_utc": stamp,
                         "python": platform.python_version(), "platform": platform.platform(),
                         "image": ETCD_IMAGE, "measurement_clock": "time.perf_counter/monotonic",
                         **(metadata or {})}
        self.json("metadata.json", self.metadata)

    def json(self, name, data):
        (self.path / name).write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    def text(self, name, value):
        (self.path / name).write_text(value, encoding="utf-8")

    def measure(self, metric, elapsed_ms, **fields):
        self.rows.append({"metric": metric, "elapsed_ms": float(elapsed_ms), **fields})

    def finish(self):
        self.json("measurements.json", self.rows)
        if self.rows:
            columns = list(dict.fromkeys(k for row in self.rows for k in row))
            with (self.path / "measurements.csv").open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=columns)
                writer.writeheader()
                writer.writerows(self.rows)
        summaries = []
        for metric in sorted({r["metric"] for r in self.rows}):
            rows = [r for r in self.rows if r["metric"] == metric]
            # Falhas permanecem nas amostras brutas e não são misturadas com latência de sucessos.
            values = [r["elapsed_ms"] for r in rows if r.get("success", True) and not r.get("error")]
            summaries.append({"metric": metric, "attempts": len(rows), "successful_samples": len(values),
                              "failed_samples": len(rows)-len(values),
                              "mean_ms": statistics.mean(values) if values else None,
                              "sample_stdev_ms": statistics.stdev(values) if len(values) > 1 else None})
        self.json("summary.json", summaries)
        self.metadata["finished_utc"] = datetime.now(timezone.utc).isoformat()
        self.json("metadata.json", self.metadata)
        return self.path


def doctor():
    result = {"python": platform.python_version(), "os": platform.platform(),
              "docker_cli": shutil.which("docker"), "docker_ready": False,
              "requirements": "Python >= 3.10 e Docker com Compose v2, contêineres Linux"}
    if result["docker_cli"]:
        try:
            cluster = Cluster(3)
            result["compose"] = cluster._run(["docker", "compose", "version"]).strip()
            result["docker_server"] = cluster._run(["docker", "info", "--format", "{{.OSType}}"], timeout=20).strip()
            result["docker_ready"] = result["docker_server"] == "linux"
        except LabError as exc:
            result["error"] = str(exc)
    return result
