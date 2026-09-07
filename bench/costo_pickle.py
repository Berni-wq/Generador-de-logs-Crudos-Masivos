import csv
import json
import multiprocessing as mp
import os
import subprocess
import sys
import time
from datetime import timedelta
from pathlib import Path

from src.esquema import crear_sensores, generar_evento, tiempo_inicio

TOTAL_TICKS = 71_429
WORKERS = 4
SEED = 42


def escritor_conteo(cola, n_workers, ruta_salida, contador):
    """Escritor único con fusión determinística de lotes (igual que src.arquitectura_a).

    Mantiene una cabeza por worker activo y emite la cabeza con menor clave
    (tick0, worker_id), de modo que el orden final no depende del scheduling.
    """
    f = open(ruta_salida, "w", encoding="utf-8", newline="\n", buffering=4 * 1024 * 1024)
    workers_terminados = 0
    activos = set(range(n_workers))
    pendientes = {}
    with contador.get_lock():
        contador.value = 0
    while workers_terminados < n_workers or pendientes:
        while any(w not in pendientes or not pendientes[w] for w in activos):
            lote = cola.get()
            if isinstance(lote, tuple) and lote[0] == "__FIN__":
                workers_terminados += 1
                activos.discard(lote[1])
                continue
            wid, tick0, lineas = lote
            pendientes.setdefault(wid, []).append((tick0, lineas))
        if not pendientes:
            continue
        wid = min(pendientes, key=lambda w: (pendientes[w][0][0], w))
        tick0, lineas = pendientes[wid].pop(0)
        if not pendientes[wid]:
            del pendientes[wid]
        f.writelines(lineas)
        with contador.get_lock():
            contador.value += len(lineas)
    f.close()


def worker_bench(wid, n_ticks, cola, semilla, batch_size):
    import random
    rng = random.Random(semilla + wid)
    sensores = crear_sensores(rng, wid)
    tiempo_actual = tiempo_inicio(semilla)
    buf = []
    tick0 = None
    for tick_abs in range(n_ticks):
        eventos = generar_evento(rng, wid, sensores, tiempo_actual)
        tiempo_actual += timedelta(seconds=15)
        for ev in eventos:
            if not buf:
                tick0 = tick_abs
            buf.append(json.dumps(ev, separators=(",", ":")) + "\n")
        if len(buf) >= batch_size:
            cola.put((wid, tick0, buf))
            buf = []
            tick0 = None
    if buf:
        cola.put((wid, tick0, buf))
    cola.put(("__FIN__", wid))


def correr_batch(batch_size):
    out_dir = f"bench/_tmp_pickle_{batch_size}"
    os.makedirs(out_dir, exist_ok=True)
    ruta = Path(out_dir) / "salida.pkl.jsonl"

    ticks_por_worker = TOTAL_TICKS // WORKERS
    cola = mp.Queue(maxsize=64)
    contador = mp.Value("i", 0)

    t0 = time.perf_counter()
    escritor = mp.Process(target=escritor_conteo, args=(cola, WORKERS, ruta, contador))
    escritor.start()

    workers = []
    for i in range(WORKERS):
        p = mp.Process(target=worker_bench, args=(i, ticks_por_worker, cola, SEED, batch_size))
        p.start()
        workers.append(p)

    for p in workers:
        p.join()
    escritor.join()
    duracion = time.perf_counter() - t0

    eventos = contador.value
    bytes_f = ruta.stat().st_size if ruta.exists() else 0

    try:
        os.remove(ruta)
        os.rmdir(out_dir)
    except OSError:
        pass

    return {
        "batch_size": batch_size,
        "duracion": round(duracion, 3),
        "eventos": eventos,
        "bytes": bytes_f,
        "evt_s": round(eventos / duracion, 0),
    }


def medir_memoria_externa():
    """Corre `bench.worker_mem` y captura la memoria del proceso escritor.

    `bench.worker_mem` ejecuta la arquitectura queue y reporta en stdout:
        PICO_MEMORIA_BYTES=<bytes | working set del proceso OS>
        PICO_TRACEMALLOC_CURRENT_BYTES=<bytes | objetos Python vivos al final>
        PICO_TRACEMALLOC_BYTES=<bytes | pico de objetos Python (tracemalloc)>
    """
    result = subprocess.run(
        [sys.executable, "-m", "bench.worker_mem"],
        capture_output=True, text=True, timeout=300,
        cwd=os.getcwd(),
    )
    valores = {}
    for linea in result.stdout.splitlines():
        if linea.startswith("PICO_") and "=" in linea:
            clave, _, valor = linea.partition("=")
            try:
                valores[clave] = int(valor)
            except ValueError:
                valores[clave] = 0
    return valores


def main():
    batch_sizes = [1, 100, 1_000, 10_000]
    total_eventos_aprox = TOTAL_TICKS * 7
    print(f"Benchmark pickle: {TOTAL_TICKS} ticks x {WORKERS} workers = ~{total_eventos_aprox:,} eventos")
    print(f"{'Batch':>10} {'Tiempo':>10} {'Eventos':>12} {'evt/s':>10}")
    print("-" * 50)

    resultados = []
    for bs in batch_sizes:
        r = correr_batch(bs)
        resultados.append(r)
        print(f"{r['batch_size']:>10} {r['duracion']:>9.3f}s {r['eventos']:>12,} {r['evt_s']:>10,.0f}")

    print(f"\n{'='*50}")
    print("Medicion de memoria (batch=10000, 4 workers)...")

    mem_vals = medir_memoria_externa()
    peak_ws = mem_vals.get("PICO_MEMORIA_BYTES", 0)
    current_trm = mem_vals.get("PICO_TRACEMALLOC_CURRENT_BYTES", 0)
    peak_trm = mem_vals.get("PICO_TRACEMALLOC_BYTES", 0)
    peak_ws_mb = peak_ws / (1024 * 1024)
    peak_trm_mb = peak_trm / (1024 * 1024)

    # Valores reales de la corrida de memoria (batch=10000), calculados en
    # tiempo real, no hardcodeados.
    corrida_mem = next(
        (r for r in resultados if r["batch_size"] == 10_000), None
    )

    mem = {
        "eventos": corrida_mem["eventos"] if corrida_mem else 0,
        "bytes": corrida_mem["bytes"] if corrida_mem else 0,
        "duracion_seg": corrida_mem["duracion"] if corrida_mem else 0,
        "peak_working_set_bytes": peak_ws,
        "peak_working_set_mb": round(peak_ws_mb, 2),
        "peak_tracemalloc_bytes": peak_trm,
        "peak_tracemalloc_mb": round(peak_trm_mb, 2),
        "current_tracemalloc_bytes": current_trm,
    }
    print(f"  Peak Working Set (proceso OS): {peak_ws:,} bytes ({peak_ws_mb:.2f} MB)")
    print(f"  Peak tracemalloc (objetos Python): {peak_trm:,} bytes ({peak_trm_mb:.2f} MB)")
    print(f"  tracemalloc actual al finalizar: {current_trm:,} bytes")

    with open("bench/pickle_benchmark.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["batch_size", "duracion", "eventos", "bytes", "evt_s"])
        w.writeheader()
        w.writerows(resultados)

    with open("bench/memoria_result.json", "w") as f:
        json.dump(mem, f, indent=2)

    print("\nCSV: bench/pickle_benchmark.csv")
    print("JSON: bench/memoria_result.json")


if __name__ == "__main__":
    main()
