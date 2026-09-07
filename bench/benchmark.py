import argparse
import csv
import json
import os
import statistics
import subprocess
import sys


def correr_medicion(workers, total_events, seed, arch, output_dir, repeticion):
    """Ejecuta `src.main` (arquitectura shard o queue) y cronometra.

    La duración se toma del `manifiesto.json` que escribe `src.main`
    (`duracion_seg`), cuyo punto de inicio/fin es el mismo que usa
    `bench.costo_pickle`: antes de lanzar los procesos y después de que
    terminan de escribir. NO se cronometra el subproceso completo, porque
    eso incluiría el arranque/import de Python y la validación final que
    hace `src.main`, que no forman parte de la generación.
    """
    cmd = [
        sys.executable, "-m", "src.main",
        "--workers", str(workers),
        "--total-events", str(total_events),
        "--output-dir", output_dir,
        "--seed", str(seed),
        "--arch", arch,
    ]

    resultado = subprocess.run(cmd, capture_output=True, text=True)

    if resultado.returncode != 0:
        print(f"\nError en workers={workers}, rep={repeticion}:")
        print(resultado.stderr, file=sys.stderr)
        return None

    manifiesto_path = os.path.join(output_dir, "manifiesto.json")
    if not os.path.isfile(manifiesto_path):
        print(f"\nError en workers={workers}, rep={repeticion}: "
              f"falta {manifiesto_path}", file=sys.stderr)
        return None
    with open(manifiesto_path, encoding="utf-8") as f:
        manifiesto = json.load(f)

    duracion = manifiesto["duracion_seg"]
    eventos = manifiesto["total_eventos"]
    total_bytes = manifiesto["total_bytes"]

    return {
        "worker_count": workers,
        "repeticion": repeticion,
        "tiempo_s": round(duracion, 3),
        "eventos_por_s": round(eventos / duracion, 0),
        "mb_por_s": round((total_bytes / (1024 * 1024)) / duracion, 2),
        "bytes": total_bytes,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Benchmarking de escalabilidad del generador TPC"
    )
    parser.add_argument("--total-events", type=int, default=1_000_000,
                        help="Eventos finales por corrida (default: 1_000_000)")
    parser.add_argument("--repeticiones", type=int, default=4,
                        help="Repeticiones por configuración de workers (default: 4)")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--arch", choices=["shard", "queue"], default="shard",
                        help="Arquitectura a benchmarkear (default: shard)")
    parser.add_argument("--workers-list", type=str, default="1,2,4,8,16",
                        help="Lista de workers separada por coma (default: 1,2,4,8,16)")
    parser.add_argument("--csv", type=str, default="bench/mediciones.csv",
                        help="Archivo CSV de salida (default: bench/mediciones.csv)")
    parser.add_argument("--data-dir", type=str, default="data/bench_tmp",
                        help="Carpeta TEMPORAL de mediciones; se limpia en cada corrida "
                             "(default: data/bench_tmp). NUNCA data/raw: ahi se genera el "
                             "dataset final definitivo.")

    args = parser.parse_args()

    workers_list = [int(w.strip()) for w in args.workers_list.split(",")]
    cpu_count = os.cpu_count() or 1

    os.makedirs(os.path.dirname(args.csv) if os.path.dirname(args.csv) else ".", exist_ok=True)
    os.makedirs(args.data_dir, exist_ok=True)

    rows = []

    for workers in workers_list:
        if workers > cpu_count:
            print(f"  ⚠ workers={workers} > cpu_count={cpu_count} (over-subscription, resultado esperado)")

        for rep in range(1, args.repeticiones + 1):
            for f in os.listdir(args.data_dir):
                fp = os.path.join(args.data_dir, f)
                if os.path.isfile(fp):
                    os.remove(fp)

            resultado = correr_medicion(workers, args.total_events, args.seed,
                                        args.arch, args.data_dir, rep)
            if resultado is None:
                continue

            # La primera repetición sirve de warmup (fork, cache, etc.).
            tipo = "warmup" if rep == 1 else "medicion"
            resultado["tipo"] = tipo
            rows.append(resultado)

            print(f"  workers={workers:>2}  rep={rep}  {resultado['tiempo_s']:>7.3f}s  "
                  f"{resultado['eventos_por_s']:>10.0f} evt/s  "
                  f"{resultado['mb_por_s']:>6.2f} MB/s  [{tipo}]")

    for f in os.listdir(args.data_dir):
        fp = os.path.join(args.data_dir, f)
        if os.path.isfile(fp):
            os.remove(fp)

    campos = ["worker_count", "repeticion", "tiempo_s", "eventos_por_s",
              "mb_por_s", "bytes", "tipo"]
    with open(args.csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=campos)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nCSV guardado en {args.csv}")

    # Resumen (medianas, excluyendo warmup). String consistente: "medicion" (sin tilde).
    print("\n" + "=" * 70)
    print(f"{'Workers':>8} {'T mediana':>10} {'Speedup':>10} {'Eficiencia':>12} {'evt/s':>10} {'MB/s':>8}")
    print("-" * 70)

    t1_mediana = None
    for workers in workers_list:
        meds = [r["tiempo_s"] for r in rows
                if r["worker_count"] == workers and r["tipo"] == "medicion"]
        if not meds:
            continue
        t_med = statistics.median(meds)
        if t1_mediana is None:
            t1_mediana = t_med
        speedup = t1_mediana / t_med
        eficiencia = speedup / workers
        evt_s = statistics.median([
            r["eventos_por_s"] for r in rows
            if r["worker_count"] == workers and r["tipo"] == "medicion"
        ])
        mb_med = statistics.median([
            r["mb_por_s"] for r in rows
            if r["worker_count"] == workers and r["tipo"] == "medicion"
        ])
        print(f"{workers:>8} {t_med:>10.3f}s {speedup:>9.2f}x {eficiencia:>11.1%} "
              f"{evt_s:>10.0f} {mb_med:>7.2f}")

    print("=" * 70)


if __name__ == "__main__":
    main()
