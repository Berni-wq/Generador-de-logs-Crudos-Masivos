"""Genera la muestra versionable en git (data/muestra, >= 50.000 eventos).

Sección 4.1 de la guía: la muestra SÍ se versiona en git (a diferencia de
data/raw, que está en .gitignore). Es un paso MANUAL de preparación del
repositorio, antes de hacer commit; no forma parte del flujo Docker/run_all.

Uso:
    python -m bench.generar_muestra

Reusa src.main (arquitectura shard, 1 worker) invocándolo como subproceso y
verifica el resultado con bench.validar.contar_eventos.
"""
import argparse
import subprocess
import sys
from pathlib import Path

from bench.validar import contar_eventos

MIN_EVENTOS = 50_000
RAIZ = Path(__file__).resolve().parent.parent
MUESTRA = RAIZ / "data" / "muestra"


def main():
    parser = argparse.ArgumentParser(
        description="Genera data/muestra (50.000 eventos de ejemplo versionables en git)"
    )
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--total-events", type=int, default=MIN_EVENTOS)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--arch", choices=["shard", "queue"], default="shard")
    args = parser.parse_args()

    cmd = [
        sys.executable, "-m", "src.main",
        "--workers", str(args.workers),
        "--total-events", str(args.total_events),
        "--seed", str(args.seed),
        "--arch", args.arch,
        "--output-dir", str(MUESTRA),
    ]
    r = subprocess.run(cmd, cwd=str(RAIZ))
    if r.returncode != 0:
        print(f"\n[ERROR] Falla al generar la muestra con src.main "
              f"(exit={r.returncode})", file=sys.stderr, flush=True)
        sys.exit(r.returncode)

    total = contar_eventos(str(MUESTRA))
    if total < MIN_EVENTOS:
        print(
            f"\n[ERROR] La muestra tiene {total:,} eventos, por debajo del "
            f"mínimo de {MIN_EVENTOS:,}.",
            file=sys.stderr, flush=True,
        )
        sys.exit(1)

    if not (MUESTRA / "manifiesto.json").is_file():
        print(f"\n[ERROR] Falta {MUESTRA / 'manifiesto.json'}", file=sys.stderr, flush=True)
        sys.exit(1)

    print(f"\nMuestra OK: {total:,} eventos en {MUESTRA} (versionada en git)", flush=True)


if __name__ == "__main__":
    main()
