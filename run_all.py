"""Orquestador one-shot: genera el dataset, corre el benchmark y produce el reporte.

Uso (dentro del contenedor, via ENTRYPOINT):
    python run_all.py --workers 4 --total-events 10000004 --seed 42
                      --arch shard --salida /app/salida

Flujo (en orden):
    1) Benchmark de escalabilidad contra un directorio TEMPORAL (data/bench_tmp).
       El benchmark limpia y regenera esa carpeta en cada corrida de medición,
       así que NUNCA toca data/raw (el dataset final).
    2) Generación del dataset final definitivo en data/raw, con los --workers
       y --total-events que realmente se van a entregar (>= 10.000.000 eventos).
    3) Validación del dataset final con bench/validar.py. Aborta con mensaje
       claro si la validación falla o si hay menos de 10.000.000 eventos.
    4) Reporte (PDF + PNG) a partir de bench/mediciones.csv.
    5) Copia a --salida: CSV/PNG/PDF + data/raw/manifiesto.json + un resumen
       (total de líneas y tamaño en bytes) como evidencia del dataset final.
       El dataset .jsonl en /app/data/raw queda persistido en el host via el
       volumen montado (se monta en ./dataset con `docker run -v`).

La salida se copia a --salida (por defecto /app/salida) para que el usuario
pueda montar un volumen y persistir los resultados.
"""
import argparse
import glob
import os
import shutil
import subprocess
import sys
from pathlib import Path

from bench.validar import contar_eventos

MIN_EVENTOS = 10_000_000
RAIZ = Path(__file__).resolve().parent
DATASET_DIR = "data/raw"
BENCH_DIR = "data/bench_tmp"


def correr(cmd, paso):
    print(f"\n=== [{paso}] {cmd[0]} ... ===", flush=True)
    r = subprocess.run([sys.executable, "-m", *cmd], cwd=str(RAIZ))
    if r.returncode != 0:
        print(f"\n[ERROR] Fallo en {paso}", file=sys.stderr, flush=True)
        sys.exit(r.returncode)
    return r


def limpiar_carpeta(carpeta):
    """Borra los archivos de `carpeta` y la recrea vacía."""
    if os.path.isdir(carpeta):
        for f in os.listdir(carpeta):
            fp = os.path.join(carpeta, f)
            if os.path.isfile(fp):
                os.remove(fp)
    os.makedirs(carpeta, exist_ok=True)


def resumen_dataset(carpeta):
    """Devuelve (total_lineas, total_bytes) del dataset .jsonl."""
    archivos = sorted(glob.glob(os.path.join(carpeta, "*.jsonl")))
    total_bytes = sum(os.path.getsize(r) for r in archivos)
    total_lineas = contar_eventos(carpeta)
    return total_lineas, total_bytes


def main():
    parser = argparse.ArgumentParser(description="One-shot TPC: generar + benchmark + reporte")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--total-events", type=int, default=10_000_004)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--arch", choices=["shard", "queue"], default="shard")
    parser.add_argument("--salida", type=str, default="/app/salida")
    args = parser.parse_args()

    if args.total_events < MIN_EVENTOS:
        print(
            f"\n[ERROR] --total-events={args.total_events:,} es menor al mínimo "
            f"entregable de {MIN_EVENTOS:,} eventos.",
            file=sys.stderr, flush=True,
        )
        sys.exit(1)

    salida = Path(args.salida)
    salida.mkdir(parents=True, exist_ok=True)

    # 1) Benchmark: usa SIEMPRE un directorio temporal (data/bench_tmp),
    #    nunca data/raw. El benchmark lo limpia en cada medición.
    correr([
        "bench.benchmark",
        "--total-events", str(args.total_events),
        "--repeticiones", "4",
        "--seed", str(args.seed),
        "--arch", args.arch,
        "--csv", "bench/mediciones.csv",
        "--data-dir", BENCH_DIR,
    ], "1/5 Benchmark de escalabilidad")

    # 2) Generar el dataset FINAL definitivo en data/raw, con los workers y
    #    total-events que realmente se entregan (>= 10.000.000 eventos).
    limpiar_carpeta(DATASET_DIR)
    correr([
        "src.main",
        "--workers", str(args.workers),
        "--total-events", str(args.total_events),
        "--seed", str(args.seed),
        "--arch", args.arch,
        "--output-dir", DATASET_DIR,
    ], "2/5 Generar dataset final")

    # 3) Validar el dataset final y verificar el conteo mínimo.
    correr(["bench.validar", DATASET_DIR], "3/5 Validar dataset final")
    total_lineas = contar_eventos(DATASET_DIR)
    if total_lineas < MIN_EVENTOS:
        print(
            f"\n[ERROR] El dataset final tiene {total_lineas:,} eventos, "
            f"por debajo del mínimo exigido de {MIN_EVENTOS:,}.",
            file=sys.stderr, flush=True,
        )
        sys.exit(1)
    print(f"Dataset final OK: {total_lineas:,} eventos >= {MIN_EVENTOS:,}", flush=True)

    # 4) Reporte (lee bench/mediciones.csv -> docs/reporte_benchmark.pdf + bench/speedup_vs_ideal.png)
    correr([
        "bench.reporte",
    ], "4/5 Generar reporte")

    # 5) Copiar resultados a la carpeta de salida: CSV/PNG/PDF + manifiesto
    #    + resumen (total de líneas y tamaño en bytes) del dataset final.
    for r in [
        "bench/mediciones.csv",
        "bench/speedup_vs_ideal.png",
        "docs/reporte_benchmark.pdf",
    ]:
        p = Path(r)
        if p.exists():
            destino = salida / p.name
            shutil.copy2(p, destino)
            print(f"  {r} -> {destino}", flush=True)
        else:
            print(f"  [aviso] no existe {r}", flush=True)

    manifiesto = Path(DATASET_DIR) / "manifiesto.json"
    if manifiesto.exists():
        destino = salida / "manifiesto.json"
        shutil.copy2(manifiesto, destino)
        print(f"  {manifiesto} -> {destino}", flush=True)
    else:
        print(f"  [aviso] no existe {manifiesto}", flush=True)

    total_lineas, total_bytes = resumen_dataset(DATASET_DIR)
    resumen = salida / "resumen_dataset.txt"
    resumen.write_text(
        f"Dataset final: {DATASET_DIR}\n"
        f"Total de eventos (lineas .jsonl): {total_lineas:,}\n"
        f"Tamaño total: {total_bytes:,} bytes ({total_bytes / (1024 ** 3):.2f} GB)\n"
        f"Tamaño medio por evento: {total_bytes / total_lineas:.1f} bytes\n"
        f"Conteo mínimo exigido: {MIN_EVENTOS:,}\n",
        encoding="utf-8",
    )
    print(f"  resumen_dataset.txt -> {resumen}", flush=True)

    print("\n=== LISTO ===", flush=True)
    print("Resultados en la carpeta montada /app/salida:", flush=True)
    for f in sorted(salida.iterdir()):
        print(f"  - {f.name} ({f.stat().st_size:,} bytes)", flush=True)

    print("\nEl dataset final .jsonl quedó en /app/data/raw, montado en el host "
          "como ./dataset (junto a salida/). No se copia a salida para no "
          "duplicar gigabytes; en salida/ está su evidencia: manifiesto.json + "
          "resumen_dataset.txt.", flush=True)


if __name__ == "__main__":
    main()