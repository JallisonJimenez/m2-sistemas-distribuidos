"""Experimentos reais sobre o etcd; nunca gera medições sintéticas.

Cada execução conserva respostas da API e amostras brutas. Falhas operacionais
fora das condições previstas interrompem a execução e ficam registradas.
"""
from __future__ import annotations

import base64
import http.client
import json
import queue
import statistics
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

from .core import Cluster, Client, LabError, Recorder, wait_for


def _integer(data, *names):
    for name in names:
        if name in data:
            return int(data[name])
    return 0


def _revision(response):
    return _integer(response.get("header", {}), "revision")


def _term(status):
    return _integer(status, "raftTerm", "raft_term") or _integer(status.get("header", {}), "raft_term", "raftTerm")


def _index(status):
    return _integer(status, "raftIndex", "raft_index")


def _applied(status):
    return _integer(status, "raftAppliedIndex", "raft_applied_index")


def _member_id(status):
    return str(status.get("header", {}).get("member_id", status.get("header", {}).get("memberId", "0")))


def _statuses(cluster):
    return {str(i): cluster.client(i).status() for i in cluster.members}


def _new_key(experiment):
    return f"/sd-lab/{experiment.lower()}/{uuid.uuid4().hex}"


def _require_value(response, expected):
    actual = Client.value(response)
    if actual != expected:
        raise LabError(f"Valor inesperado: esperado={expected!r}, observado={actual!r}")
    return response


def _available_leader(cluster, members=None, minimum_term=0, different_from=None):
    """Confirma o líder pelos status dos membros selecionados, sem failover da API."""
    members = cluster.members if members is None else members
    statuses = {i: cluster.client(i, timeout=0.7).status() for i in members}
    ids = {_member_id(status): i for i, status in statuses.items()}
    leaders = {str(status.get("leader", "0")) for status in statuses.values()}
    if len(leaders) != 1 or "0" in leaders:
        return False
    leader_id = next(iter(leaders))
    elected = ids.get(leader_id)
    if elected is None or elected == different_from:
        return False
    if any(_term(status) < minimum_term for status in statuses.values()):
        return False
    return elected


def _expected_unavailability(exc):
    """Erros de programação/configuração não contam como prova de perda de quórum."""
    message = str(exc).lower()
    return any(fragment in message for fragment in (
        "deadline exceeded", "timed out", "timeout", "no leader", "not leader",
        "unavailable", "connection refused", "connection reset", "remote end closed",
        "failed to connect", "actively refused", "forcadamente", "recusou ativamente",
    ))


def _attempt(operation, recorder, metric, **fields):
    started = time.perf_counter()
    try:
        response = operation()
    except (LabError, OSError, http.client.HTTPException) as exc:
        elapsed = (time.perf_counter() - started) * 1000
        recorder.measure(metric, elapsed, success=False, error=str(exc), **fields)
        if not _expected_unavailability(exc):
            raise
        return {"ok": False, "error": str(exc), "elapsed_ms": elapsed}
    elapsed = (time.perf_counter() - started) * 1000
    recorder.measure(metric, elapsed, success=True, **fields)
    return {"ok": True, "response": response, "elapsed_ms": elapsed}


def _require_unavailable(result, operation):
    if result["ok"]:
        raise LabError(f"{operation} respondeu durante ausência de maioria; examine as evidências antes de concluir.")


def _e1(cluster, recorder, repetitions, reads, delay_ms):
    recorder.json("status.json", _statuses(cluster))
    recorder.text("member-list.json", cluster.ctl(1, ["member", "list", "-w", "json"]))
    recorder.text("endpoint-health.txt", "\n".join(
        f"etcd{i}: {cluster.ctl(i, ['endpoint', 'health']).strip()}" for i in cluster.members
    ))
    recorder.text("compose-ps.txt", cluster.command(["ps"]))
    recorder.text("resources.jsonl", cluster.stats())
    leader = cluster.leader()
    recorder.json("topologia.json", {
        "nodes": cluster.nodes, "majority": cluster.nodes // 2 + 1, "leader_observed": leader,
        "members": [{"name": f"etcd{i}", "host_endpoint": cluster.url(i),
                     "peer_endpoint": f"172.29.0.{10+i}:2380",
                     "client_endpoint": f"172.29.1.{10+i}:2379"} for i in cluster.members],
    })


def _e2(cluster, recorder, repetitions, reads, delay_ms):
    leader = cluster.leader()
    follower = next(i for i in cluster.members if i != leader)
    key = _new_key("E2")
    before = _statuses(cluster)
    response = cluster.client(follower).put(key, "producao")
    revision = _revision(response)
    if revision <= 0:
        raise LabError("Resposta de escrita sem revisão válida.")

    def caught_up():
        statuses = _statuses(cluster)
        if all(_index(statuses[str(i)]) > _index(before[str(i)]) and
               _applied(statuses[str(i)]) >= _index(before[str(i)]) + 1 for i in cluster.members):
            return statuses
        return False

    after = wait_for(caught_up)
    read = _require_value(cluster.client(leader).get(key), "producao")
    recorder.json("caminho.json", {"key": key, "entry_member": follower,
                                  "leader_before": leader, "before": before,
                                  "put_response": response, "read_on_other_member": read,
                                  "after": after,
                                  "scope": "Índices e resposta sustentam a análise; saltos Raft no diagrama são explicação do protocolo, não captura de pacotes."})
    recorder.text("sequence.mmd", f"""sequenceDiagram
    participant C as Cliente Python
    participant F as etcd{follower} (seguidor)
    participant L as etcd{leader} (líder observado)
    participant R as Demais réplicas
    C->>F: Put({key}, producao)
    F->>L: Proposta encaminhada
    L->>R: AppendEntries
    R-->>L: Confirmações da maioria
    L->>L: Commit e aplicação ordenada
    L-->>F: Resultado da proposta
    F-->>C: Resposta com revisão {revision}
""")


def _watch_once(url, key, start_revision, timeout=25):
    """Lê o primeiro evento do watch e fecha a conexão de streaming."""
    parts = urlsplit(url)
    connection = http.client.HTTPConnection(parts.hostname, parts.port, timeout=timeout)
    request = {"create_request": {"key": base64.b64encode(key.encode()).decode(),
                                   "start_revision": str(start_revision)}}
    try:
        connection.request("POST", "/v3/watch", json.dumps(request), {"Content-Type": "application/json"})
        response = connection.getresponse()
        if response.status != 200:
            raise LabError(f"Watch respondeu HTTP {response.status}: {response.read().decode(errors='replace')}")
        # O gateway envia um objeto JSON por linha, incluindo a confirmação de criação.
        while True:
            line = response.readline()
            if not line:
                raise LabError("Watch terminou sem evento.")
            data = json.loads(line)
            result = data.get("result", data)
            if result.get("canceled") or data.get("error"):
                raise LabError(f"Watch cancelado: {data}")
            if result.get("events"):
                return data
    finally:
        connection.close()


def _e3(cluster, recorder, repetitions, reads, delay_ms):
    writer, reader, observer = cluster.members[:3]
    key = _new_key("E3")
    initial = cluster.client(writer).put(key, "antes-do-watch")
    watched = queue.Queue()

    def observe():
        try:
            watched.put((True, _watch_once(cluster.url(observer), key, _revision(initial) + 1)))
        except Exception as exc:
            watched.put((False, exc))

    thread = threading.Thread(target=observe, daemon=True)
    thread.start()
    put_client, get_client = cluster.client(writer), cluster.client(reader)
    samples = []
    try:
        for repetition in range(1, repetitions + 1):
            value = f"versao-{repetition}"
            started = time.perf_counter()
            write = put_client.put(key, value)
            recorder.measure("put_latency", (time.perf_counter() - started) * 1000,
                             repetition=repetition, member=writer, success=True)
            ack_time = time.perf_counter()

            def visible():
                local = get_client.get(key, serializable=True)
                return local if Client.value(local) == value else False

            local = wait_for(visible, interval=0.001)
            recorder.measure("visibility_after_ack", (time.perf_counter() - ack_time) * 1000,
                             repetition=repetition, member=reader, success=True)
            started = time.perf_counter()
            read = _require_value(get_client.get(key), value)
            recorder.measure("get_latency", (time.perf_counter() - started) * 1000,
                             repetition=repetition, member=reader, success=True)
            samples.append({"repetition": repetition, "put": write,
                            "local_read_after_ack": local, "linearizable_read": read})
        try:
            ok, event = watched.get(timeout=30)
        except queue.Empty as exc:
            raise LabError("Não foi recebido evento de watch no terceiro membro.") from exc
        if not ok:
            raise LabError(f"Falha no watch: {event}")
        event_list = event.get("result", event).get("events", [])
        event_revision = _integer(event_list[0].get("kv", {}), "mod_revision", "modRevision")
        if event_revision < _revision(initial) + 1:
            raise LabError("Watch retornou revisão anterior ao início solicitado.")
        recorder.json("watch.json", {"observer": observer, "start_revision": _revision(initial) + 1,
                                      "response": event})
        recorder.json("replicacao.json", {"samples": samples,
            "measurement_scope": "visibility_after_ack mede o tempo após o ACK até observar a versão em leitura local de outra réplica; não é o tempo interno completo da replicação."})
    finally:
        put_client.close()
        get_client.close()
        thread.join(timeout=1)
    recorder.text("check-perf.txt", cluster.ctl(writer, ["check", "perf", "--load=s"], timeout=180))


def _staleness_summary(samples):
    """Denominadores explícitos: erro não é leitura desatualizada."""
    summary = {}
    for mode in ("serializable", "linearizable"):
        selected = [sample for sample in samples if sample["mode"] == mode]
        success = [sample for sample in selected if sample["ok"]]
        stale = sum(sample["observed"] != sample["expected"] for sample in success)
        summary[mode] = {"attempts": len(selected), "successful_reads": len(success),
                         "errors": len(selected) - len(success), "stale_reads": stale,
                         "stale_rate_successful_reads": stale / len(success) if success else None,
                         "stale_rate_all_attempts": stale / len(selected) if selected else None}
    return summary


def _partition_samples(cluster, recorder, isolated, writer, count, linear_reads=5):
    """Cada amostra vem depois de uma escrita confirmada pelo lado majoritário."""
    key = _new_key("partition")
    seed = "versao-anterior-a-particao"
    majority_client = cluster.client(writer)
    isolated_client = cluster.client(isolated, timeout=1.5)
    samples = []
    majority_client.put(key, seed)
    wait_for(lambda: Client.value(isolated_client.get(key, serializable=True)) == seed)
    cluster.isolate(isolated)
    try:
        # A leitura linearizável no escritor confirma que o lado majoritário segue disponível.
        wait_for(lambda: majority_client.get(key), timeout=30)
        previous_revision = 0
        for repetition in range(1, count + 1):
            expected = f"confirmada-{repetition}"
            write = majority_client.put(key, expected)
            revision = _revision(write)
            if revision <= previous_revision:
                raise LabError("A revisão das escritas confirmadas não avançou.")
            previous_revision = revision
            for mode in ("serializable", "linearizable"):
                if mode == "linearizable" and repetition > linear_reads:
                    continue
                result = _attempt(lambda mode=mode: isolated_client.get(key, serializable=mode == "serializable"),
                                  recorder, f"partition_{mode}_n{cluster.nodes}",
                                  repetition=repetition, member=isolated, mode=mode,
                                  confirmed_write_revision=revision)
                sample = {"mode": mode, "repetition": repetition, "expected": expected,
                          "confirmed_write": write, "confirmed_write_revision": revision,
                          "seed": seed, **result}
                if result["ok"]:
                    sample["observed"] = Client.value(result["response"])
                    sample["read_revision"] = _revision(result["response"])
                    if mode == "linearizable" and sample["observed"] != expected:
                        raise LabError("Leitura linearizável retornou versão anterior à escrita confirmada.")
                samples.append(sample)
                recorder.json("particao-parcial.json", samples)
        summary = _staleness_summary(samples)
        if not summary["serializable"]["successful_reads"]:
            raise LabError("Nenhuma leitura local respondeu; a taxa de desatualização não pode ser estimada.")
        if not summary["serializable"]["stale_reads"]:
            raise LabError("Nenhuma leitura desatualizada foi observada: verifique se o isolamento realmente ocorreu.")
        if summary["linearizable"]["successful_reads"]:
            raise LabError("O endpoint isolado respondeu leitura linearizável: a partição não foi demonstrada.")
        return {"isolated_member": isolated, "majority_writer": writer, "key": key,
                "comparison": "Última escrita confirmada antes da leitura, sem escritor concorrente.",
                "summary": summary, "samples": samples}
    finally:
        cluster.heal(isolated)
        majority_client.close()
        isolated_client.close()


def _e4(cluster, recorder, repetitions, reads, delay_ms):
    leader = cluster.leader()
    follower = next(i for i in cluster.members if i != leader)
    key = _new_key("E4")
    client = cluster.client(follower)
    cluster.client(leader).put(key, "normal")
    normal = []
    try:
        for repetition in range(1, repetitions + 1):
            for serializable in (False, True):
                mode = "serializable" if serializable else "linearizable"
                result = _attempt(lambda s=serializable: client.get(key, serializable=s), recorder,
                                  f"normal_{mode}", repetition=repetition, mode=mode)
                if not result["ok"]:
                    raise LabError(f"Leitura {mode} falhou no cluster saudável.")
                _require_value(result["response"], "normal")
                normal.append({"mode": mode, "repetition": repetition, **result})
        recorder.json("normal.json", normal)
    finally:
        client.close()
    recorder.json("particao.json", _partition_samples(cluster, recorder, follower, leader, repetitions, repetitions))


def _quorum_operations(cluster, recorder, active, key, stage, expected_value):
    member = active[0]
    client = cluster.client(member, timeout=1.5)
    results = {"active_members": active, "majority_required": cluster.nodes // 2 + 1, "endpoint": member}
    try:
        for mode in ("linearizable", "serializable"):
            results[mode] = _attempt(lambda m=mode: client.get(key, serializable=m == "serializable"),
                                     recorder, f"quorum_{stage}_{mode}", member=member)
        results["put"] = _attempt(lambda: client.put(key, expected_value), recorder,
                                  f"quorum_{stage}_put", member=member)
        return results
    finally:
        client.close()


def _e5(cluster, recorder, repetitions, reads, delay_ms):
    key = _new_key("E5")
    cluster.client(1).put(key, "antes-das-falhas")
    trials = []
    for repetition in range(1, repetitions + 1):
        cluster.wait_healthy()
        leader = cluster.leader()
        old_status = cluster.client(leader).status()
        before = {"leader": leader, "term": _term(old_status)}
        started = time.perf_counter()
        cluster.stop(leader)
        survivors = [i for i in cluster.members if i != leader]
        try:
            elected = wait_for(lambda: _available_leader(cluster, survivors, before["term"] + 1), timeout=45)
            elected_status = cluster.client(elected).status()
            recorder.measure("leader_failover", (time.perf_counter() - started) * 1000,
                             repetition=repetition, old_leader=leader, new_leader=elected,
                             success=True, scope="Inclui duração do docker compose stop e polling de status.")
            value = f"com-minoria-parada-{repetition}"
            minority_failure = _quorum_operations(cluster, recorder, survivors, key, "minority_failure", value)
            if any(not minority_failure[op]["ok"] for op in ("put", "linearizable", "serializable")):
                raise LabError("Cluster com maioria ativa não manteve todas as operações disponíveis.")
            for member in survivors:
                wait_for(lambda member=member: Client.value(cluster.client(member).get(key, serializable=True)) == value)
            # Em N=3: mais um membro parado. Em N=5: parar até restarem dois.
            keep = survivors[: cluster.nodes // 2]
            for member in survivors[cluster.nodes // 2 :]:
                cluster.stop(member)
            no_quorum = _quorum_operations(cluster, recorder, keep, key, "no_quorum", "nao-deve-confirmar")
            _require_unavailable(no_quorum["put"], "Escrita")
            _require_unavailable(no_quorum["linearizable"], "Leitura linearizável")
            if not no_quorum["serializable"]["ok"]:
                raise LabError("Leitura serializável local também falhou; examine o estado do membro sobrevivente.")
            _require_value(no_quorum["serializable"]["response"], value)
            trials.append({"repetition": repetition, "before": before,
                           "after_election": elected_status,
                           "minority_failure": minority_failure, "no_quorum": no_quorum})
            recorder.json("quorum.json", trials)
        finally:
            cluster.recover()


def _e6(cluster, recorder, repetitions, reads, delay_ms):
    trials = []
    for repetition in range(1, repetitions + 1):
        cluster.wait_healthy()
        old_leader = cluster.leader()
        old_term = _term(cluster.client(old_leader).status())
        key = _new_key("E6")
        seed = "antes-da-particao"
        cluster.client(old_leader).put(key, seed)
        for i in cluster.members:
            wait_for(lambda i=i: Client.value(cluster.client(i).get(key, serializable=True)) == seed)
        started = time.perf_counter()
        cluster.isolate(old_leader)
        majority = [i for i in cluster.members if i != old_leader]
        try:
            elected = wait_for(lambda: _available_leader(cluster, majority, old_term + 1), timeout=45)
            recorder.measure("partition_election", (time.perf_counter() - started) * 1000,
                             repetition=repetition, success=True, old_leader=old_leader, new_leader=elected)
            confirmed = cluster.client(elected).put(key, "depois-da-particao")
            majority_read = _require_value(cluster.client(elected).get(key), "depois-da-particao")
            minority = cluster.client(old_leader, timeout=1.5)
            try:
                local = _require_value(minority.get(key, serializable=True), seed)
                linear = _attempt(lambda: minority.get(key), recorder, "partition_old_leader_linearizable",
                                  repetition=repetition)
                write = _attempt(lambda: minority.put(key, "minoria-nao-confirmada"), recorder,
                                 "partition_old_leader_put", repetition=repetition)
                _require_unavailable(linear, "Leitura linearizável no antigo líder")
                _require_unavailable(write, "Escrita no antigo líder")
            finally:
                minority.close()
            trials.append({"repetition": repetition, "old_leader": old_leader, "old_term": old_term,
                           "new_leader": elected, "new_status": cluster.client(elected).status(),
                           "confirmed_majority_write": confirmed, "majority_read": majority_read,
                           "minority_serializable": local, "minority_linearizable": linear,
                           "minority_put": write})
            recorder.json("particao.json", trials)
        finally:
            cluster.heal(old_leader)
            cluster.wait_healthy()


def _catchup(cluster, follower, writer, key, expected, target_index):
    """Sem escritas concorrentes, exigir valor, termo e índice aplicado compatíveis."""
    read = cluster.client(follower).get(key, serializable=True)
    follower_status = cluster.client(follower).status()
    leader_status = cluster.client(writer).status()
    if (Client.value(read) == expected and _applied(follower_status) >= target_index and
            _term(follower_status) == _term(leader_status)):
        return {"follower_status": follower_status, "reference_status": leader_status, "read": read}
    return False


def _snapshot_evidence(logs, minimum_index=0):
    """Somente mensagens de recebimento/aplicação comprovam transferência recebida."""
    accepted = {"received and saved database snapshot", "applied incoming raft snapshot",
                "applied snapshot", "applying snapshot"}
    evidence = []
    for line in logs.splitlines():
        try:
            data = json.loads(line[line.index("{"):])
        except (ValueError, json.JSONDecodeError):
            continue
        index = _integer(data, "incoming-snapshot-index", "incoming-leader-snapshot-index", "snapshot-index")
        if (str(data.get("msg", "")).lower() in accepted and index > minimum_index and
                data.get("level") not in ("error", "fatal", "panic")):
            evidence.append(line)
    # Recuperação de um snapshot local no startup e criação/envio local não bastam.
    return evidence


def _e7(cluster, recorder, repetitions, reads, delay_ms):
    recovery = []
    for repetition in range(1, repetitions + 1):
        writer = cluster.leader()
        follower = next(i for i in cluster.members if i != writer)
        key = _new_key("E7")
        cluster.client(writer).put(key, "antes")
        wait_for(lambda: Client.value(cluster.client(follower).get(key, serializable=True)) == "antes")
        cluster.isolate(follower)
        try:
            expected = f"recuperada-{repetition}"
            write = cluster.client(writer).put(key, expected)
            target = _index(cluster.client(writer).status())
            started = time.perf_counter()
            cluster.heal(follower)
            caught = wait_for(lambda: _catchup(cluster, follower, writer, key, expected, target), timeout=60)
            recorder.measure("recovery_after_partition", (time.perf_counter() - started) * 1000,
                             repetition=repetition, success=True, member=follower)
            recovery.append({"repetition": repetition, "follower": follower, "write": write,
                             "target_index": target, "caught_up": caught})
            recorder.json("recuperacao.json", recovery)
        finally:
            cluster.heal(follower)
            cluster.wait_healthy()

    snapshot_trials = []
    for repetition in range(1, repetitions + 1):
        writer = cluster.leader()
        follower = next(i for i in cluster.members if i != writer)
        key = _new_key("E7-snapshot")
        cluster.client(writer).put(key, "antes-do-snapshot")
        wait_for(lambda: Client.value(cluster.client(follower).get(key, serializable=True)) == "antes-do-snapshot")
        before = cluster.client(follower).status()
        cluster.stop(follower)
        try:
            client = cluster.client(writer, timeout=5)
            try:
                # Mais de duas janelas de snapshot-count=1000 e de catchup=100.
                for number in range(2500):
                    client.put(key, f"snapshot-{repetition}-{number}")
            finally:
                client.close()
            expected = f"snapshot-{repetition}-2499"
            target = _index(cluster.client(writer).status())
            since = datetime.now(timezone.utc).isoformat()
            started = time.perf_counter()
            cluster.start(follower)
            caught = wait_for(lambda: _catchup(cluster, follower, writer, key, expected, target), timeout=90)
            recorder.measure("recovery_after_snapshot_gap", (time.perf_counter() - started) * 1000,
                             repetition=repetition, success=True, member=follower)
            logs = cluster.command(["logs", "--no-color", "--since", since, f"etcd{follower}"])
            evidence = _snapshot_evidence(logs, minimum_index=_index(before))
            recorder.text(f"snapshot-logs-{repetition}.txt", logs)
            trial = {"repetition": repetition, "follower": follower, "before": before,
                     "writes_while_stopped": 2500, "target_index": target, "caught_up": caught,
                     "snapshot_transfer_confirmed": bool(evidence), "snapshot_evidence_lines": evidence}
            snapshot_trials.append(trial)
            recorder.json("snapshots.json", snapshot_trials)
            if not evidence:
                raise LabError("O membro recuperou o estado, mas o log não comprovou recebimento/aplicação de snapshot. Consulte snapshots.json e os logs; não conclua que houve snapshot apenas pela recuperação.")
        finally:
            cluster.start(follower)
            cluster.wait_healthy(timeout=90)


def _e8(cluster, recorder, repetitions, reads, delay_ms):
    key = _new_key("E8")
    timeline = []
    last_revision = 0
    for repetition in range(1, repetitions + 1):
        cluster.wait_healthy()
        old_leader = cluster.leader()
        old_status = cluster.client(old_leader).status()
        first = cluster.client(old_leader).put(key, f"antes-{repetition}")
        if _revision(first) <= last_revision:
            raise LabError("Revisão global não avançou.")
        started = time.perf_counter()
        cluster.stop(old_leader)
        survivors = [i for i in cluster.members if i != old_leader]
        try:
            new_leader = wait_for(lambda: _available_leader(cluster, survivors, _term(old_status) + 1), timeout=45)
            recorder.measure("election_ordering", (time.perf_counter() - started) * 1000,
                             repetition=repetition, success=True)
            after = cluster.client(new_leader).put(key, f"depois-{repetition}")
            if _revision(after) <= _revision(first):
                raise LabError("A revisão da escrita posterior à eleição não avançou.")
            last_revision = _revision(after)
            timeline.append({"repetition": repetition, "old_leader": old_leader,
                             "before_status": old_status, "before_write": first,
                             "new_leader": new_leader, "after_status": cluster.client(new_leader).status(),
                             "after_write": after})
            recorder.json("termos-revisoes.json", timeline)
        finally:
            cluster.recover()
    recorder.json("manual-required.json", {
        "completed_automatically": "Termos, revisões e troca de líder Raft, com medições repetidas.",
        "still_required": ["Competição de dois clientes por etcdctl lock.",
                           "Competição e transferência de liderança de aplicação com etcdctl elect.",
                           "Cinco medições da transferência após morte do cliente detentor, conservando logs dos dois clientes."],
        "instructions": "docs/experimentos.md, seção E8. A eleição de aplicação não é a eleição do líder Raft.",
    })


def _e9(cluster, recorder, repetitions, reads, delay_ms):
    leader = cluster.leader()
    follower = next(i for i in cluster.members if i != leader)
    key = _new_key("E9")
    cluster.client(leader).put(key, "benchmark-estavel")
    wait_for(lambda: Client.value(cluster.client(follower).get(key, serializable=True)) == "benchmark-estavel")
    conditions = []
    # Atraso em todos os membros, só na saída da interface de pares.
    for delay in dict.fromkeys((0, delay_ms)):
        if delay:
            cluster.delay(delay)
        try:
            cluster.wait_healthy(timeout=90)
            # O endpoint do benchmark é sempre um seguidor no início da condição.
            condition_leader = cluster.leader()
            reader = next(i for i in cluster.members if i != condition_leader)
            clients = {mode: cluster.client(reader, timeout=5) for mode in ("linearizable", "serializable")}
            batches = {mode: [] for mode in clients}
            latencies = {mode: [] for mode in clients}
            try:
                # Aquecimento fora da amostra e ordem alternada para reduzir viés temporal.
                for mode, client in clients.items():
                    _require_value(client.get(key, serializable=mode == "serializable"), "benchmark-estavel")
                for repetition in range(1, repetitions + 1):
                    order = ("linearizable", "serializable") if repetition % 2 else ("serializable", "linearizable")
                    for mode in order:
                        label = f"n{cluster.nodes}_delay{delay}_{mode}"
                        client = clients[mode]
                        started_batch = time.perf_counter()
                        for number in range(1, reads + 1):
                            started = time.perf_counter()
                            try:
                                response = _require_value(client.get(key, serializable=mode == "serializable"), "benchmark-estavel")
                            except (LabError, OSError, http.client.HTTPException) as exc:
                                recorder.measure(f"read_latency_{label}", (time.perf_counter() - started) * 1000,
                                                 success=False, error=str(exc), repetition=repetition, sample=number,
                                                 nodes=cluster.nodes, member=reader, delay_ms=delay, mode=mode)
                                raise
                            elapsed = (time.perf_counter() - started) * 1000
                            latencies[mode].append(elapsed)
                            recorder.measure(f"read_latency_{label}", elapsed, success=True,
                                             repetition=repetition, sample=number, nodes=cluster.nodes,
                                             member=reader, delay_ms=delay, mode=mode,
                                             revision=_revision(response))
                        batch_ms = (time.perf_counter() - started_batch) * 1000
                        throughput = reads / (batch_ms / 1000)
                        recorder.measure(f"read_batch_duration_{label}", batch_ms, success=True,
                                         repetition=repetition, nodes=cluster.nodes, member=reader,
                                         delay_ms=delay, mode=mode, requests=reads, throughput_rps=throughput)
                        batches[mode].append({"repetition": repetition, "requests": reads,
                                              "elapsed_ms": batch_ms, "throughput_rps": throughput})
                for mode in clients:
                    values = latencies[mode]
                    rates = [batch["throughput_rps"] for batch in batches[mode]]
                    conditions.append({"nodes": cluster.nodes, "member": reader,
                                       "initial_leader": condition_leader, "delay_ms_one_way_per_peer": delay,
                                       "mode": mode, "repetitions": repetitions, "reads_per_repetition": reads,
                                       "successful_reads": len(values), "latency_mean_ms": statistics.mean(values),
                                       "latency_sample_stdev_ms": statistics.stdev(values) if len(values) > 1 else None,
                                       "throughput_mean_rps": statistics.mean(rates),
                                       "throughput_sample_stdev_rps": statistics.stdev(rates) if len(rates) > 1 else None,
                                       "batches": batches[mode]})
                recorder.json("leituras.json", {"conditions": conditions,
                    "method": "Cliente HTTP/JSON com conexão persistente, um endpoint e concorrência=1. Vazão sequencial observada inclui cliente, transporte e processamento; não é a capacidade máxima do etcd.",
                    "limitations": "Latências individuais compartilham uma máquina e não são amostras independentes de datacenters; comparar também a dispersão entre as repetições."})
            finally:
                for client in clients.values():
                    client.close()
        finally:
            if delay:
                cluster.delay(0)
            cluster.wait_healthy(timeout=90)
    leader = cluster.leader()
    follower = next(i for i in cluster.members if i != leader)
    recorder.json("stale.json", _partition_samples(cluster, recorder, follower, leader,
                                                  count=max(reads, repetitions), linear_reads=repetitions))
    recorder.json("comparison-required.json", {
        "this_run_nodes": cluster.nodes, "other_run_nodes": 5 if cluster.nodes == 3 else 3,
        "instructions": "Execute E9 nos clusters de 3 e 5 membros, com o mesmo computador, reads, repetitions e delay_ms. Conserve ambos os diretórios results. Troca de tamanho exige down do cluster anterior.",
        "complete_e9_requires_both_sizes": True,
    })


EXPERIMENTS = {f"E{i}": globals()[f"_e{i}"] for i in range(1, 10)}


def run(name: str, repetitions: int = 5, reads: int = 100, delay_ms: int = 25) -> Path:
    name = name.upper()
    if name not in EXPERIMENTS:
        raise ValueError("Escolha E1 a E9.")
    if repetitions < 5 or reads < 5:
        raise ValueError("Use pelo menos 5 repetições e 5 leituras por repetição.")
    if delay_ms < 0 or delay_ms > 200:
        raise ValueError("Atraso deve estar entre 0 e 200 ms.")
    if name == "E9" and delay_ms == 0:
        raise ValueError("E9 exige uma condição com atraso: use delay_ms entre 1 e 200.")
    cluster = Cluster()
    recorder = Recorder(name, {"nodes": cluster.nodes, "repetitions": repetitions,
                               "reads": reads, "delay_ms": delay_ms,
                               "evidence_origin": "Respostas e medições reais da execução; nenhum valor sintético."})
    try:
        cluster.recover()
        cluster.wait_healthy()
        recorder.json("status-before.json", _statuses(cluster))
        EXPERIMENTS[name](cluster, recorder, repetitions, reads, delay_ms)
        recorder.metadata["status"] = "manual_steps_required" if name == "E8" else "automated_part_completed"
    except BaseException as exc:
        recorder.metadata["status"] = "failed_or_interrupted"
        recorder.json("failure.json", {"type": type(exc).__name__, "error": str(exc),
                                       "evidence_directory": str(recorder.path)})
        raise
    finally:
        try:
            cluster.recover()
        except Exception as exc:
            recorder.metadata["cleanup_error"] = str(exc)
            recorder.metadata["status"] = "cleanup_failed"
            recorder.text("cleanup-error.txt", str(exc))
        try:
            recorder.text("cluster-logs.txt", cluster.logs())
        except Exception as exc:
            recorder.text("logs-error.txt", str(exc))
        recorder.finish()
    if recorder.metadata["status"] == "cleanup_failed":
        raise LabError(f"Recuperação automática falhou. Execute python -m lab recover. Evidências: {recorder.path}")
    return recorder.path
