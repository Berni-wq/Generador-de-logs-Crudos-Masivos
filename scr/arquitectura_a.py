import json
import multiprocessing as mp
import os
from datetime import timedelta
from pathlib import Path

from src.esquema import crear_sensores, generar_evento, tiempo_inicio


def escritor_colamanter(cola, n_workers, ruta_salida, contador):
    """Escritor único con fusión determinística de lotes.

    Cada lote llega como (worker_id, tick0, lineas). Para que el orden final del
    archivo dependa SOLO de (tick0, worker_id) y no del orden arbitrario de
    llegada a la cola, el escritor mantiene una cabeza disponible por cada
    worker activo y emite siempre la cabeza globalmente menor. El marcador de
    fin lleva el wid para saber a qué worker dejar de esperar.
    """
    f = open(ruta_salida, "w", encoding="utf-8", newline="\n", buffering=4 * 1024 * 1024)
    workers_terminados = 0
    activos = set(range(n_workers))
    pendientes = {}  # wid -> lista de (tick0, lineas)

    with contador.get_lock():
        contador.value = 0

    # Se continúa hasta recibir todos los FIN y drenar todos los lotes
    # pendientes (pueden llegar FIN antes de que un lote de otro worker se emita).
    while workers_terminados < n_workers or pendientes:
        # Garantizar una cabeza disponible para cada worker activo antes de emitir.
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


def worker_cola(wid, n_eventos, cola, semilla):
    import random

    rng = random.Random(semilla + wid)
    sensores = crear_sensores(rng, wid)
    tiempo_actual = tiempo_inicio(semilla)

    buf = []
    tick0 = None
    for tick_abs in range(n_eventos):
        eventos = generar_evento(rng, wid, sensores, tiempo_actual)
        tiempo_actual += timedelta(seconds=15)

        for ev in eventos:
            if not buf:
                tick0 = tick_abs
            buf.append(json.dumps(ev, separators=(",", ":")) + "\n")

        if len(buf) >= 10_000:
            cola.put((wid, tick0, buf))
            buf = []
            tick0 = None

    if buf:
        cola.put((wid, tick0, buf))

    cola.put(("__FIN__", wid))


def lanzar_cola(ticks_por_worker, carpeta, semilla):
    """Lanza la arquitectura queue con el reparto de ticks ya decidido.

    Recibe la lista `ticks_por_worker` (len == n_workers) que el llamador
    repartió con el MISMO criterio que la arquitectura shard (base_ticks +
    remanente como 1 tick extra a los primeros workers). Cada worker_cola
    recibe su propio n_eventos = ticks_por_worker[i], de modo que ninguna
    arquitectura pierde ni duplica ticks.
    """
    os.makedirs(carpeta, exist_ok=True)
    ruta_salida = Path(carpeta) / "salida_queue.jsonl"
    n_workers = len(ticks_por_worker)

    cola = mp.Queue(maxsize=64)
    contador = mp.Value("i", 0)

    escritor = mp.Process(
        target=escritor_colamanter,
        args=(cola, n_workers, ruta_salida, contador),
    )
    escritor.start()

    workers = []
    for i, n_eventos in enumerate(ticks_por_worker):
        p = mp.Process(
            target=worker_cola,
            args=(i, n_eventos, cola, semilla),
        )
        p.start()
        workers.append(p)

    for p in workers:
        p.join()

    escritor.join()

    return {
        "archivo": ruta_salida.name,
        "eventos": contador.value,
        "bytes": ruta_salida.stat().st_size if ruta_salida.exists() else 0,
    }
