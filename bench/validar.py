"""Validador estructural del dataset JSONL generado por el simulador."""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path


REQUIRED_FIELDS = {
    "timestamp",
    "sensor_id",
    "metric",
    "value",
    "unit",
    "worker_id",
}
METRIC_UNITS = {
    "temperature": "C",
    "humidity": "%",
    "vibration": "mm/s",
    "current": "A",
    "weight": "kg",
}
TIMESTAMP_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$"
)
SENSOR_ID_PATTERN = re.compile(r"^[A-Z]+_S3_[A-Z0-9]+$")


def _es_entero_positivo(valor: object) -> bool:
    return (
        isinstance(valor, int)
        and not isinstance(valor, bool)
        and valor > 0
    )


def _es_numero_finito(valor: object, *, positivo: bool = True) -> bool:
    if isinstance(valor, bool) or not isinstance(valor, (int, float)):
        return False
    numero = float(valor)
    if not math.isfinite(numero):
        return False
    return numero > 0 if positivo else numero >= 0


def entero_positivo(valor: str) -> int:
    try:
        numero = int(valor)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("debe ser un entero") from exc
    if numero <= 0:
        raise argparse.ArgumentTypeError("debe ser mayor que cero")
    return numero


def _validar_evento(evento: object) -> str | None:
    if not isinstance(evento, dict):
        return "el JSON debe ser un objeto"
    faltantes = REQUIRED_FIELDS.difference(evento)
    if faltantes:
        return "faltan campos: " + ", ".join(sorted(faltantes))

    timestamp = evento["timestamp"]
    if not isinstance(timestamp, str) or not TIMESTAMP_PATTERN.fullmatch(
        timestamp
    ):
        return "timestamp debe usar ISO-8601 UTC con milisegundos y sufijo Z"
    try:
        datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        return "timestamp contiene una fecha u hora invalida"

    sensor_id = evento["sensor_id"]
    if not isinstance(sensor_id, str) or not SENSOR_ID_PATTERN.fullmatch(
        sensor_id
    ):
        return "sensor_id debe seguir la convencion TIPO_S3_CORRELATIVO"

    metric = evento["metric"]
    if metric not in METRIC_UNITS:
        return f"metric no admitida: {metric!r}"
    if evento["unit"] != METRIC_UNITS[metric]:
        return (
            f"unidad {evento['unit']!r} incompatible con metric={metric!r}; "
            f"se esperaba {METRIC_UNITS[metric]!r}"
        )

    value = evento["value"]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return "value debe ser numerico"
    if not math.isfinite(float(value)):
        return "value debe ser finito"

    worker_id = evento["worker_id"]
    if isinstance(worker_id, bool) or not isinstance(worker_id, int):
        return "worker_id debe ser entero"
    if worker_id < 0:
        return "worker_id no puede ser negativo"
    return None


def _cargar_manifiesto(ruta: Path) -> tuple[dict | None, str | None]:
    if not ruta.exists():
        return None, None
    try:
        with ruta.open(encoding="utf-8") as archivo:
            manifiesto = json.load(archivo)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        return None, f"manifiesto invalido: {exc}"
    if not isinstance(manifiesto, dict):
        return None, "manifiesto debe ser un objeto JSON"
    return manifiesto, None


def validar(
    carpeta: str | Path,
    *,
    expected_events: int | None = None,
    expected_workers: int | None = None,
    require_manifest: bool = False,
    max_errors: int = 20,
) -> bool:
    carpeta = Path(carpeta)
    archivos = sorted(carpeta.glob("*.jsonl"))
    if not archivos:
        print(f"Error: no se encontraron archivos .jsonl en {carpeta}", file=sys.stderr)
        return False

    errores: list[str] = []
    total = 0
    total_bytes = 0
    worker_counts: Counter[int] = Counter()

    def registrar(mensaje: str) -> None:
        if len(errores) < max_errors:
            errores.append(mensaje)

    for ruta in archivos:
        try:
            total_bytes += ruta.stat().st_size
            with ruta.open(encoding="utf-8") as archivo:
                for numero_linea, linea in enumerate(archivo, 1):
                    total += 1
                    if not linea.strip():
                        registrar(f"{ruta}:{numero_linea}: linea vacia")
                        continue
                    try:
                        evento = json.loads(linea)
                    except json.JSONDecodeError as exc:
                        registrar(
                            f"{ruta}:{numero_linea}: JSON invalido "
                            f"(columna {exc.colno})"
                        )
                        continue
                    problema = _validar_evento(evento)
                    if problema:
                        registrar(f"{ruta}:{numero_linea}: {problema}")
                    elif isinstance(evento, dict):
                        worker_counts[evento["worker_id"]] += 1
        except (OSError, UnicodeError) as exc:
            registrar(f"{ruta}: no se pudo leer: {exc}")

    manifiesto_path = carpeta / "manifiesto.json"
    manifiesto, error_manifiesto = _cargar_manifiesto(manifiesto_path)
    if error_manifiesto:
        registrar(error_manifiesto)
    if require_manifest and manifiesto is None and error_manifiesto is None:
        registrar(f"falta el manifiesto requerido: {manifiesto_path}")

    if expected_events is not None and total != expected_events:
        registrar(
            f"conteo total={total:,}; se esperaban {expected_events:,} eventos"
        )
    if expected_workers is not None:
        ids_esperados = set(range(expected_workers))
        ids_reales = set(worker_counts)
        if ids_reales != ids_esperados:
            registrar(
                f"worker_id presentes={sorted(ids_reales)}; "
                f"se esperaban {sorted(ids_esperados)}"
            )

    if manifiesto is not None:
        requeridos = {
            "total_eventos",
            "total_eventos_solicitados",
            "total_bytes",
            "arquitectura",
            "workers",
            "seed",
            "batch_size",
            "sim_days",
            "duracion_seg",
            "detalles",
        }
        faltantes = requeridos.difference(manifiesto)
        if faltantes:
            registrar(
                "manifiesto: faltan campos: " + ", ".join(sorted(faltantes))
            )
        else:
            if not _es_entero_positivo(manifiesto["total_eventos"]):
                registrar("manifiesto.total_eventos debe ser entero positivo")
            elif manifiesto["total_eventos"] != total:
                registrar(
                    "manifiesto.total_eventos no coincide con las lineas JSONL: "
                    f"{manifiesto['total_eventos']!r} != {total}"
                )
            if not _es_entero_positivo(
                manifiesto["total_eventos_solicitados"]
            ):
                registrar(
                    "manifiesto.total_eventos_solicitados debe ser entero "
                    "positivo"
                )
            elif manifiesto["total_eventos_solicitados"] != total:
                registrar(
                    "el dataset no contiene exactamente el volumen solicitado: "
                    f"{manifiesto['total_eventos_solicitados']!r} != {total}"
                )
            if not _es_entero_positivo(manifiesto["total_bytes"]):
                registrar("manifiesto.total_bytes debe ser entero positivo")
            elif manifiesto["total_bytes"] != total_bytes:
                registrar(
                    "manifiesto.total_bytes no coincide con los archivos: "
                    f"{manifiesto['total_bytes']!r} != {total_bytes}"
                )
            workers_manifiesto = manifiesto["workers"]
            if not _es_entero_positivo(workers_manifiesto):
                registrar("manifiesto.workers debe ser un entero positivo")
            elif set(worker_counts) != set(range(workers_manifiesto)):
                registrar(
                    "los worker_id no coinciden con manifiesto.workers="
                    f"{workers_manifiesto}"
                )
            if manifiesto["arquitectura"] not in {"shard", "queue"}:
                registrar("manifiesto.arquitectura debe ser shard o queue")
            if not _es_entero_positivo(manifiesto["batch_size"]):
                registrar("manifiesto.batch_size debe ser entero positivo")
            if not _es_numero_finito(manifiesto["sim_days"]):
                registrar("manifiesto.sim_days debe ser finito y positivo")
            if not _es_numero_finito(
                manifiesto["duracion_seg"],
                positivo=False,
            ):
                registrar(
                    "manifiesto.duracion_seg debe ser finito y no negativo"
                )
            semilla = manifiesto["seed"]
            if isinstance(semilla, bool) or not isinstance(semilla, int):
                registrar("manifiesto.seed debe ser entero")

            detalles = manifiesto["detalles"]
            if not isinstance(detalles, list) or not detalles:
                registrar("manifiesto.detalles debe ser una lista no vacia")
            else:
                nombres: list[str] = []
                eventos_detalle = 0
                bytes_detalle = 0
                worker_ids_detalle: set[int] = set()
                detalles_validos = True
                for indice, detalle in enumerate(detalles):
                    prefijo = f"manifiesto.detalles[{indice}]"
                    if not isinstance(detalle, dict):
                        registrar(f"{prefijo} debe ser un objeto")
                        detalles_validos = False
                        continue
                    faltantes_detalle = {
                        "archivo",
                        "eventos",
                        "bytes",
                    }.difference(detalle)
                    if faltantes_detalle:
                        registrar(
                            f"{prefijo}: faltan campos: "
                            + ", ".join(sorted(faltantes_detalle))
                        )
                        detalles_validos = False
                        continue
                    nombre = detalle["archivo"]
                    if (
                        not isinstance(nombre, str)
                        or Path(nombre).name != nombre
                        or not nombre.endswith(".jsonl")
                    ):
                        registrar(f"{prefijo}.archivo no es un nombre JSONL")
                        detalles_validos = False
                    else:
                        nombres.append(nombre)
                    if not _es_entero_positivo(detalle["eventos"]):
                        registrar(f"{prefijo}.eventos debe ser entero positivo")
                        detalles_validos = False
                    else:
                        eventos_detalle += detalle["eventos"]
                    if not _es_entero_positivo(detalle["bytes"]):
                        registrar(f"{prefijo}.bytes debe ser entero positivo")
                        detalles_validos = False
                    else:
                        bytes_detalle += detalle["bytes"]
                    if "worker_id" in detalle:
                        worker_id = detalle["worker_id"]
                        if (
                            isinstance(worker_id, bool)
                            or not isinstance(worker_id, int)
                            or worker_id < 0
                        ):
                            registrar(
                                f"{prefijo}.worker_id debe ser entero no negativo"
                            )
                            detalles_validos = False
                        else:
                            worker_ids_detalle.add(worker_id)

                if detalles_validos:
                    nombres_reales = {ruta.name for ruta in archivos}
                    if set(nombres) != nombres_reales:
                        registrar(
                            "manifiesto.detalles no enumera exactamente los "
                            "archivos JSONL"
                        )
                    if eventos_detalle != total:
                        registrar(
                            "la suma de manifiesto.detalles.eventos no coincide "
                            "con el total"
                        )
                    if bytes_detalle != total_bytes:
                        registrar(
                            "la suma de manifiesto.detalles.bytes no coincide "
                            "con el total"
                        )
                    if (
                        manifiesto["arquitectura"] == "shard"
                        and _es_entero_positivo(workers_manifiesto)
                        and worker_ids_detalle
                        != set(range(workers_manifiesto))
                    ):
                        registrar(
                            "los worker_id de manifiesto.detalles no coinciden "
                            "con manifiesto.workers"
                        )

    if errores:
        for error in errores:
            print(f"Error: {error}", file=sys.stderr)
        if len(errores) == max_errors:
            print(
                f"Error: se alcanzo el limite de {max_errors} diagnosticos",
                file=sys.stderr,
            )
        print(
            f"Validacion fallida tras revisar {total:,} lineas en "
            f"{len(archivos)} archivos.",
            file=sys.stderr,
        )
        return False

    print(
        f"OK: {total:,} eventos con esquema valido en {len(archivos)} "
        f"archivo(s); {len(worker_counts)} worker(s)."
    )
    return True


def crear_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Valida sintaxis, esquema, conteos y manifiesto del JSONL"
    )
    parser.add_argument("carpeta", help="carpeta que contiene los .jsonl")
    parser.add_argument("--expected-events", type=entero_positivo)
    parser.add_argument("--expected-workers", type=entero_positivo)
    parser.add_argument("--require-manifest", action="store_true")
    parser.add_argument("--max-errors", type=entero_positivo, default=20)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = crear_parser()
    args = parser.parse_args(argv)
    carpeta = Path(args.carpeta).expanduser().resolve()
    if not carpeta.is_dir():
        parser.error(f"no es un directorio valido: {carpeta}")
    return 0 if validar(
        carpeta,
        expected_events=args.expected_events,
        expected_workers=args.expected_workers,
        require_manifest=args.require_manifest,
        max_errors=args.max_errors,
    ) else 1


if __name__ == "__main__":
    raise SystemExit(main())
