"""Medición de memoria real del proceso escritor (arquitectura queue).

Módulo que `bench/costo_pickle.py` invoca vía:
    python -m bench.worker_mem

Corre la arquitectura queue (escritor único con cola) con los parámetros por
defecto. El proceso escritor mide su propio pico de memoria residente y lo
reporta al padre, que lo imprime en stdout como:

    PICO_MEMORIA_BYTES=<bytes>

En Windows se mide PeakWorkingSetSize con ctypes (no hay `resource`).
En Linux/macOS se usa resource.getrusage(...).ru_maxrss.
"""

import json
import multiprocessing as mp
import os
import sys
from datetime import timedelta
from pathlib import Path

from src.esquema import crear_sensores, generar_evento, tiempo_inicio

TOTAL_TICKS = 71_429
WORKERS = 4
SEED = 42


def _peak_memory_bytes():
    """Devuelve el pico de memoria residente del proceso actual, en bytes."""
    # Windows: PeakWorkingSetSize vía ctypes.
    try:
        import ctypes
        from ctypes import wintypes

        class PROCESS_MEMORY_COUNTERS(ctypes.Structure):
            _fields_ = [
                ("cb", wintypes.DWORD),
                ("PageFaultCount", wintypes.DWORD),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            ]

        psapi = ctypes.windll.psapi
        psapi.GetProcessMemoryInfo.argtypes = [
            wintypes.HANDLE,
            ctypes.POINTER(PROCESS_MEMORY_COUNTERS),
            wintypes.DWORD,
        ]
        psapi.GetProcessMemoryInfo.restype = wintypes.BOOL

        counters = PROCESS_MEMORY_COUNTERS()
        counters.cb = ctypes.sizeof(PROCESS_MEMORY_COUNTERS)
        handle = ctypes.windll.kernel32.GetCurrentProcess()
        if psapi.GetProcessMemoryInfo(handle, ctypes.byref(counters), counters.cb):
            return int(counters.PeakWorkingSetSize)
        return None
    except Exception:
        pass

    # Linux/macOS: resource.
    try:
        import resource
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
    except Exception:
        return None


def _escritor(cola, n_workers, ruta_salida, contador, resultado):
    """Proceso escritor: consume la cola y mide su propio pico de memoria.

    Usa la fusión determinística de lotes de src.arquitectura_a (emite el lote
    con menor clave (tick0, worker_id)), de modo que el orden final no depende
    del scheduling. Reporta la tupla (peak_working_set_os, current_tracemalloc,
    peak_tracemalloc): el primero es el pico de memoria del proceso completo a
    nivel de sistema operativo; los otros dos son el pico y el estado actual de
    la memoria asignada por objetos Python según tracemalloc (incluye el
    backlog de la cola).
    """
    import tracemalloc

    tracemalloc.start()
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
    current_trm, peak_trm = tracemalloc.get_traced_memory()
    f.close()
    tracemalloc.stop()
    peak_os = _peak_memory_bytes()
    resultado.put((peak_os, current_trm, peak_trm))


def _worker(wid, n_ticks, cola, semilla, batch_size):
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


def main():
    out_dir = Path("bench/_tmp_worker_mem")
    out_dir.mkdir(parents=True, exist_ok=True)
    ruta = out_dir / "salida_w_mem.jsonl"

    ticks_por_worker = TOTAL_TICKS // WORKERS
    cola = mp.Queue(maxsize=64)
    contador = mp.Value("i", 0)
    resultado = mp.Queue()

    escritor = mp.Process(
        target=_escritor,
        args=(cola, WORKERS, str(ruta), contador, resultado),
    )
    escritor.start()

    workers = []
    for i in range(WORKERS):
        p = mp.Process(target=_worker, args=(i, ticks_por_worker, cola, SEED, 10_000))
        p.start()
        workers.append(p)

    for p in workers:
        p.join()
    escritor.join()

    peak = resultado.get()
    try:
        ruta.unlink(missing_ok=True)
        out_dir.rmdir()
    except OSError:
        pass

    if isinstance(peak, tuple) and len(peak) == 3:
        peak_os, current_trm, peak_trm = peak
    else:
        peak_os, current_trm, peak_trm = peak or 0, 0, 0

    peak_os = peak_os or 0
    print(f"PICO_MEMORIA_BYTES={peak_os}")
    print(f"PICO_TRACEMALLOC_CURRENT_BYTES={current_trm}")
    print(f"PICO_TRACEMALLOC_BYTES={peak_trm}")


if __name__ == "__main__":
    main()
