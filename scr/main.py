import argparse
import glob
import json
import multiprocessing as mp
import os
import sys
import time
from pathlib import Path

from src.arquitectura_b import trabajador_shard
from src.arquitectura_a import lanzar_cola
from src.esquema import eventos_por_tick


def repartir_ticks(total_ticks, n_workers):
    """Reparte total_ticks entre n_workers con criterio compartido.

    Mismo reparto para shard y queue: `base_ticks` para todos + 1 tick extra
    a los primeros workers según el remanente. La suma de la lista es SIEMPRE
    exactamente `total_ticks` (ni pierde ni duplica ticks).
    """
    base_ticks, remanente = divmod(total_ticks, n_workers)
    ticks_por_worker = [
        base_ticks + (1 if i < remanente else 0)
        for i in range(n_workers)
    ]
    assert sum(ticks_por_worker) == total_ticks, (
        f"Reparto inválido: suma {sum(ticks_por_worker):,} != {total_ticks:,}"
    )
    return ticks_por_worker


def main():
    parser = argparse.ArgumentParser(
        description="Simulador de logs masivos - Terminal Puerto Coquimbo (TPC) Sitio 3"
    )
    parser.add_argument("--workers", type=int, default=4,
                        help="Número de procesos paralelos (default: 4). "
                             "Valores por sobre cpu_count solo generan una "
                             "advertencia, no un error (over-subscription "
                             "esperado en la curva de escalabilidad). "
                             "Se aborta únicamente si supera el máximo "
                             "razonable min(cpu_count*8, 128).")
    parser.add_argument("--total-events", type=int, default=1_000_000,
                        help="Total de eventos finales (.jsonl) a generar (default: 1_000_000)")
    parser.add_argument("--output-dir", type=str, default="data/raw",
                        help="Carpeta de salida para archivos .jsonl (default: data/raw)")
    parser.add_argument("--seed", type=int, default=42,
                        help="Semilla para reproducibilidad (default: 42)")
    parser.add_argument("--arch", choices=["shard", "queue"], default="shard",
                        help="Arquitectura: shard (archivos separados) o queue (escritor único) (default: shard)")

    args = parser.parse_args()

    cpu_count = os.cpu_count() or 1

    # Over-subscription intencional (e.g. 16 workers en 8 nucleos) NO es un
    # error: es parte de la curva de escalabilidad y hay que medirlo. Solo se
    # advierte. Se aborta unicamente con valores claramente absurdos (un
    # error de tipeo o algo que podria colgar la maquina).
    techo = min(cpu_count * 8, 128)
    if args.workers > techo:
        print(
            f"Error: --workers={args.workers} supera el máximo razonable "
            f"techo={techo} (cpu_count={cpu_count} * 8, tope 128). "
            f"Revisa el flag: probablemente es un error de tipeo "
            f"(e.g. --workers 200 en un equipo de 8 nucleos).",
            file=sys.stderr,
        )
        sys.exit(1)
    elif args.workers > cpu_count:
        print(
            f"Aviso: --workers={args.workers} > cpu_count={cpu_count} "
            f"(over-subscription, se espera rendimiento degradado)",
            file=sys.stderr,
        )

    os.makedirs(args.output_dir, exist_ok=True)

    ept = eventos_por_tick()
    # Redondeo hacia arriba (ceiling): total_ticks = ceil(total_events / ept),
    # para que el dataset generado sea SIEMPRE >= --total-events, nunca menos,
    # sin depender de que el caller pase un múltiplo exacto de ept.
    total_ticks = -(-args.total_events // ept)
    eventos_reales_ticks = total_ticks * ept
    if total_ticks < args.workers:
        print(
            f"Error: --total-events={args.total_events:,} es muy bajo. "
            f"Cada tick produce {ept} eventos, se necesitan al menos "
            f"{args.workers * ept:,} eventos para {args.workers} workers.",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"Generando dataset con {args.workers} workers (arquitectura {args.arch})...")
    print(f"  Eventos solicitados: {args.total_events:,} -> "
          f"eventos reales a generar: {eventos_reales_ticks:,} "
          f"(ajustado a múltiplo de {ept})")
    print(f"  Semilla: {args.seed}")
    print(f"  Salida: {args.output_dir}/")
    print(f"  Eventos por tick: {ept} -> {total_ticks:,} ticks totales")
    print()

    t_inicio = time.perf_counter()

    if args.arch == "shard":
        # Reparte los ticks entre workers; el remanente va como 1 tick extra a
        # los primeros workers para que se generen EXACTAMENTE total_ticks.
        ticks_por_worker = repartir_ticks(total_ticks, args.workers)
        base_ticks, remanente = divmod(total_ticks, args.workers)
        print(f"  {base_ticks:,} ticks base por worker ({base_ticks * ept:,} eventos)"
              f" + {remanente:,} workers con 1 tick extra")
        print()

        resultados = []
        with mp.Pool(processes=args.workers) as pool:
            tareas = []
            for i, ticks_worker in enumerate(ticks_por_worker):
                tareas.append(pool.apply_async(
                    trabajador_shard,
                    (i, ticks_worker, args.output_dir, args.seed),
                ))

            for tarea in tareas:
                resultados.append(tarea.get())

        total_eventos = sum(r["eventos"] for r in resultados)
        total_bytes = sum(r["bytes"] for r in resultados)

        manifiesto = {
            "total_eventos_solicitados": args.total_events,
            "total_eventos": total_eventos,
            "total_bytes": total_bytes,
            "arquitectura": "shard",
            "workers": args.workers,
            "seed": args.seed,
            "detalles": resultados,
        }

    else:
        ticks_por_worker = repartir_ticks(total_ticks, args.workers)
        resultado = lanzar_cola(
            ticks_por_worker, args.output_dir, args.seed
        )

        total_eventos = resultado["eventos"]
        total_bytes = resultado["bytes"]

        manifiesto = {
            "total_eventos_solicitados": args.total_events,
            "total_eventos": total_eventos,
            "total_bytes": total_bytes,
            "arquitectura": "queue",
            "workers": args.workers,
            "seed": args.seed,
            "detalles": [resultado],
        }

    if total_eventos != eventos_reales_ticks:
        print(
            f"  [Aviso] eventos generados ({total_eventos:,}) != "
            f"esperados ({eventos_reales_ticks:,}).",
            file=sys.stderr,
        )

    t_fin = time.perf_counter()
    duracion = t_fin - t_inicio

    manifiesto["duracion_seg"] = round(duracion, 3)

    manifiesto_path = Path(args.output_dir) / "manifiesto.json"
    with open(manifiesto_path, "w") as f:
        json.dump(manifiesto, f, indent=4)

    print(f"Manifiesto escrito en {manifiesto_path}")
    print(f"Duración: {duracion:.3f}s")
    print()

    total = 0
    archivos = sorted(glob.glob(os.path.join(args.output_dir, "*.jsonl")))

    if not archivos:
        print("No se encontraron archivos .jsonl")
    else:
        for ruta in archivos:
            with open(ruta, encoding="utf-8") as f:
                for i, linea in enumerate(f, 1):
                    try:
                        json.loads(linea)
                    except json.JSONDecodeError:
                        print(f"Error: línea inválida en {ruta}:{i}", file=sys.stderr)
                        sys.exit(1)
                    total += 1
        print(f"OK, {total:,} eventos válidos verificados.")


if __name__ == "__main__":
    main()
