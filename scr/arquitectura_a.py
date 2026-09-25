import json
import multiprocessing as mp
import os
import socket
from datetime import timedelta
from pathlib import Path

from .esquema import crear_sensores, generar_evento, tiempo_inicio, eventos_por_tick


LOTE = 10_000


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


def generar_lotes(wid, n_eventos, semilla, batch_size=LOTE, limite_eventos=None):
    """Comparte la generación y el batching originales entre cola y TCP.

    n_eventos conserva el significado original: cantidad de ticks a iterar
    como máximo (cota superior del bucle).
    Se termina cada tick antes de enviar, por lo que un lote puede superar
    ligeramente batch_size (cada tick produce siete eventos).

    limite_eventos (opcional, usado solo desde worker_tcp) acota el total de
    EVENTOS emitidos: al alcanzarlo se recorta el tick en curso (puede quedar
    con menos de siete eventos) y se detiene la generación, aunque n_eventos
    todavía permitiera más ticks. Con limite_eventos=None (comportamiento
    previo, el que sigue usando worker_cola/lanzar_cola) no cambia nada.
    """
    import random

    rng = random.Random(semilla + wid)
    sensores = crear_sensores(rng, wid)
    tiempo_actual = tiempo_inicio(semilla)

    buf = []
    tick0 = None
    emitidos = 0
    for tick_abs in range(n_eventos):
        if limite_eventos is not None and emitidos >= limite_eventos:
            break

        eventos = generar_evento(rng, wid, sensores, tiempo_actual)
        tiempo_actual += timedelta(seconds=15)

        if limite_eventos is not None and emitidos + len(eventos) > limite_eventos:
            eventos = eventos[: limite_eventos - emitidos]

        for ev in eventos:
            if not buf:
                tick0 = tick_abs
            buf.append(json.dumps(ev, separators=(",", ":")) + "\n")
        emitidos += len(eventos)

        if len(buf) >= batch_size:
            yield tick0, buf
            buf = []
            tick0 = None

    if buf:
        yield tick0, buf


def worker_cola(wid, n_eventos, cola, semilla):
    for tick0, buf in generar_lotes(wid, n_eventos, semilla):
        cola.put((wid, tick0, buf))

    cola.put(("__FIN__", wid))


def worker_tcp(wid, n_eventos, semilla, host, port, batch_size=LOTE):
    """Envía JSONL UTF-8 por una conexión persistente propia del worker.

    A diferencia de worker_cola (donde n_eventos es una cantidad de ticks),
    aquí n_eventos es la cuota EXACTA de eventos que debe enviar este worker:
    se generan los ticks completos que hagan falta
    (ceil(n_eventos / eventos_por_tick())) y generar_lotes recorta el último
    tick (vía limite_eventos) para no excederla. Así --arch tcp envía siempre
    exactamente --total-events en total, sin quedar atado a múltiplos de
    eventos por tick.
    """
    ept = eventos_por_tick()
    n_ticks = -(-n_eventos // ept)  # techo: ticks suficientes para cubrir la cuota
    enviados = 0
    total_bytes = 0
    try:
        with socket.create_connection((host, port), timeout=30) as conexion:
            for _, buf in generar_lotes(wid, n_ticks, semilla, batch_size, limite_eventos=n_eventos):
                datos = "".join(buf).encode("utf-8")
                # sendall maneja envíos parciales; los saltos de línea
                # delimitan los eventos, independientemente de los paquetes TCP.
                conexion.sendall(datos)
                enviados += len(buf)
                total_bytes += len(datos)
            # EOF permite que el receptor procese también su último lote parcial.
            conexion.shutdown(socket.SHUT_WR)
    except OSError as exc:
        # No reconectar/reintentar: un lote parcial podría quedar duplicado.
        raise RuntimeError(
            f"Worker {wid}: error TCP con {host}:{port} después de "
            f"{enviados} eventos enviados en lotes completos: {exc}"
        ) from exc

    return {"worker_id": wid, "eventos": enviados, "bytes": total_bytes}


def lanzar_tcp(eventos_por_worker, semilla, host, port, batch_size=LOTE):
    """Un proceso y una conexión por worker; propaga fallos al proceso principal.

    eventos_por_worker es la cuota EXACTA de eventos que debe enviar cada
    worker (no ticks): la suma de la lista es siempre --total-events. Cada
    worker (ver worker_tcp) genera los ticks completos que hagan falta y
    recorta el último para no excederla.
    """
    with mp.Pool(processes=len(eventos_por_worker)) as pool:
        return pool.starmap(worker_tcp, [
            (wid, cuota, semilla, host, port, batch_size)
            for wid, cuota in enumerate(eventos_por_worker)
        ], chunksize=1)


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
