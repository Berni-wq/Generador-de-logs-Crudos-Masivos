import argparse
import glob
import json
import multiprocessing as mp
import os
import sys
import time
from pathlib import Path

from .arquitectura_b import trabajador_shard
from .arquitectura_a import LOTE, lanzar_cola, lanzar_tcp
from .esquema import eventos_por_tick


def repartir_ticks(total_ticks, n_workers):
    """Reparte total_ticks entre n_workers con criterio compartido.

    Mismo reparto para shard y queue: `base_ticks` para todos + 1 tick extra
    a los primeros workers según el remanente. La suma de la lista es SIEMPRE
    exactamente `total_ticks` (ni pierde ni duplica ticks).

    El mismo algoritmo (base + remanente a los primeros) se reutiliza también
    para --arch tcp, pero ahí se le pasa directamente --total-events en lugar
    de una cantidad de ticks: como es un reparto genérico de un entero entre
    n_workers, sirve igual para repartir una cuota exacta de EVENTOS (ver
    main(), rama tcp).
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
                        help="Carpeta de salida local para shard/queue; no se usa en TCP (default: data/raw)")
    parser.add_argument("--seed", type=int, default=42,
                        help="Semilla para reproducibilidad (default: 42)")
    parser.add_argument("--arch", choices=["shard", "queue", "tcp"], default="shard",
                        help="Arquitectura: shard, queue o tcp "
                             "(default: shard). TCP usa TCP_HOST, TCP_PORT y BATCH_SIZE del entorno.")

    args = parser.parse_args()

    if args.workers < 1 or args.total_events < 1:
        parser.error("--workers y --total-events deben ser mayores que cero")

    if args.arch == "tcp":
        host = os.environ.get("TCP_HOST", "127.0.0.1").strip()
        try:
            port = int(os.environ.get("TCP_PORT", "9009"))
            batch_size = int(os.environ.get("BATCH_SIZE", str(LOTE)))
        except ValueError:
            parser.error("TCP_PORT y BATCH_SIZE deben ser números enteros")
        if not host:
            parser.error("TCP_HOST no puede estar vacío")
        if not 1 <= port <= 65535:
            parser.error("TCP_PORT debe estar entre 1 y 65535")
        if batch_size < 1:
            parser.error("BATCH_SIZE debe ser mayor que cero")

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

    if args.arch != "tcp":
        os.makedirs(args.output_dir, exist_ok=True)

    ept = eventos_por_tick()
    # Redondeo hacia arriba (ceiling): total_ticks = ceil(total_events / ept).
    #
    # Para shard/queue el dataset generado sigue siendo SIEMPRE >=
    # --total-events (se completa el último tick de cada worker), nunca
    # menos, sin depender de que el caller pase un múltiplo exacto de ept.
    # Este comportamiento NO cambia.
    #
    # Para tcp, en cambio, se debe enviar la cantidad EXACTA de
    # --total-events: se reparte esa cuota de EVENTOS (no de ticks) entre los
    # workers y cada uno recorta su último tick para no excederla (ver
    # worker_tcp/generar_lotes en arquitectura_a.py).
    total_ticks = -(-args.total_events // ept)

    if args.arch == "tcp":
        eventos_objetivo = args.total_events
        if args.total_events < args.workers:
            print(
                f"Error: --total-events={args.total_events:,} es muy bajo para "
                f"repartir entre {args.workers} workers (se necesita al menos "
                f"1 evento por worker).",
                file=sys.stderr,
            )
            sys.exit(1)
    else:
        eventos_objetivo = total_ticks * ept
        if total_ticks < args.workers:
            print(
                f"Error: --total-events={args.total_events:,} es muy bajo. "
                f"Cada tick produce {ept} eventos, se necesitan al menos "
                f"{args.workers * ept:,} eventos para {args.workers} workers.",
                file=sys.stderr,
            )
            sys.exit(1)

    print(f"Generando dataset con {args.workers} workers (arquitectura {args.arch})...")
    if args.arch == "tcp":
        print(f"  Eventos solicitados: {args.total_events:,} -> "
              f"eventos reales a enviar: {eventos_objetivo:,} "
              f"(cantidad exacta, sin redondear a múltiplo de {ept})")
    else:
        print(f"  Eventos solicitados: {args.total_events:,} -> "
              f"eventos reales a generar: {eventos_objetivo:,} "
              f"(ajustado a múltiplo de {ept})")
    print(f"  Semilla: {args.seed}")
    if args.arch == "tcp":
        print(f"  Destino TCP: {host}:{port} (lote: {batch_size:,} eventos)")
    else:
        print(f"  Salida: {args.output_dir}/")
    if args.arch == "tcp":
        print(f"  Eventos por tick: {ept} -> {total_ticks:,} ticks de referencia "
              f"(el último tick de cada worker se recorta para no exceder su cuota)")
    else:
        print(f"  Eventos por tick: {ept} -> {total_ticks:,} ticks totales")
    print()

    t_inicio = time.perf_counter()

    if args.arch == "tcp":
        # Cuota EXACTA de eventos por worker (no de ticks): se reutiliza el
        # mismo reparto base+remanente de repartir_ticks, ahora aplicado
        # sobre --total-events en lugar de sobre total_ticks.
        eventos_por_worker = repartir_ticks(args.total_events, args.workers)
        try:
            resultados = lanzar_tcp(eventos_por_worker, args.seed, host, port, batch_size)
        except (OSError, RuntimeError) as exc:
            print(f"Error: {exc}", file=sys.stderr)
            sys.exit(1)
        total_eventos = sum(r["eventos"] for r in resultados)
        total_bytes = sum(r["bytes"] for r in resultados)
        if total_eventos != eventos_objetivo:
            print(f"Error: enviados {total_eventos}, esperados {eventos_objetivo}",
                  file=sys.stderr)
            sys.exit(1)
        for resultado in resultados:
            print(f"  Worker {resultado['worker_id']}: {resultado['eventos']} eventos enviados")
        print(f"Total enviado por TCP: {total_eventos} eventos, {total_bytes} bytes")
        print(f"Duración: {time.perf_counter() - t_inicio:.3f}s")
        print("Verifica las líneas del archivo del receptor después de su flush/cierre. "
              "Este conteo corresponde al envío; el protocolo no tiene confirmación de persistencia.")
        return

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

    if total_eventos != eventos_objetivo:
        print(
            f"  [Aviso] eventos generados ({total_eventos:,}) != "
            f"esperados ({eventos_objetivo:,}).",
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
