import json
from datetime import timedelta
from pathlib import Path

from src.esquema import crear_sensores, generar_evento, tiempo_inicio


def trabajador_shard(wid, n_eventos, carpeta, semilla):
    import random

    rng = random.Random(semilla + wid)
    ruta = Path(carpeta) / f"part-{wid:03d}.jsonl"

    sensores = crear_sensores(rng, wid)
    tiempo_actual = tiempo_inicio(semilla)

    escritos = 0
    buf = []

    with open(ruta, "w", encoding="utf-8", newline="\n", buffering=4 * 1024 * 1024) as f:
        for _ in range(n_eventos):
            eventos = generar_evento(rng, wid, sensores, tiempo_actual)
            tiempo_actual += timedelta(seconds=15)

            for ev in eventos:
                buf.append(json.dumps(ev, separators=(",", ":")) + "\n")

            if len(buf) >= 10_000:
                f.writelines(buf)
                escritos += len(buf)
                buf = []

        if buf:
            f.writelines(buf)
            escritos += len(buf)

    return {
        "worker_id": wid,
        "archivo": ruta.name,
        "eventos": escritos,
        "bytes": ruta.stat().st_size,
    }
