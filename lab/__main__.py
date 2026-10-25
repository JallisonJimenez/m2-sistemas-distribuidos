"""Execute com python -m lab."""
import argparse
import json
import sys

from .core import Cluster, LabError, doctor


def main():
    parser = argparse.ArgumentParser(description="Laboratório etcd/Raft - Tema 5, ênfase A")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("doctor", help="Verifica Python e Docker")
    up = commands.add_parser("up", help="Inicia um cluster real")
    up.add_argument("--nodes", type=int, choices=[3, 5], default=3)
    commands.add_parser("status", help="Exibe membros, liderança, termo e índice")
    commands.add_parser("recover", help="Restaura os membros e remove as falhas de rede")
    commands.add_parser("down", help="Para o laboratório e preserva volumes")
    run = commands.add_parser("run", help="Executa e registra um experimento")
    run.add_argument("experiment", type=str.upper, choices=[f"E{i}" for i in range(1, 10)])
    run.add_argument("--repetitions", type=int, default=5)
    run.add_argument("--reads", type=int, default=100)
    run.add_argument("--delay-ms", type=int, default=25)
    args = parser.parse_args()
    try:
        if args.command == "doctor":
            result = doctor()
            print(json.dumps(result, indent=2, ensure_ascii=False))
            return 0 if result["docker_ready"] else 1
        if args.command == "up":
            cluster = Cluster(args.nodes)
            cluster.up()
            print(f"Cluster com {args.nodes} membros disponível.")
        elif args.command == "status":
            cluster = Cluster()
            print(cluster.ctl(1, ["member", "list", "-w", "table"]))
            for i in cluster.members:
                print(f"etcd{i}: {json.dumps(cluster.client(i).status(), ensure_ascii=False)}")
        elif args.command == "recover":
            Cluster().recover()
            print("Membros e comunicação restaurados.")
        elif args.command == "down":
            Cluster().down()
            print("Laboratório parado. Volumes preservados.")
        else:
            if args.repetitions < 5 or args.reads < 5 or not 1 <= args.delay_ms <= 200:
                raise LabError("Use pelo menos 5 repetições/leituras e atraso de 1 a 200 ms.")
            from .experiments import run
            result = run(args.experiment, repetitions=args.repetitions, reads=args.reads, delay_ms=args.delay_ms)
            print(f"Evidências gravadas em: {result}")
        return 0
    except (LabError, ValueError, OSError) as exc:
        print(f"Erro: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("Interrompido. Execute python -m lab recover para restaurar o ambiente.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    sys.exit(main())
