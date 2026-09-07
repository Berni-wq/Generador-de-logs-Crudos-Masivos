"""Benchmark reproducible de escalabilidad para el generador TPC."""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import statistics
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MIN_EVENTOS_GUIA = 10_000_000
DEFAULT_WORKERS = (1, 2, 4, 8, 16)
CSV_FIELDS = (
    "arquitectura",
    "total_eventos_solicitados",
    "total_eventos_reales",
    "seed",
    "batch_size",
    "sim_days",
    "worker_count",
    "repeticion",
    "tipo",
    "tiempo_s",
    "eventos_por_s",
    "mb_por_s",
    "bytes",
    "fecha_utc",
)


class BenchmarkError(RuntimeError):
    """Error que invalida una corrida completa de benchmark."""


def entero_positivo(valor: str) -> int:
    try:
        numero = int(valor)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("debe ser un entero") from exc
    if numero <= 0:
        raise argparse.ArgumentTypeError("debe ser mayor que cero")
    return numero


def flotante_positivo(valor: str) -> float:
    try:
        numero = float(valor)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("debe ser un numero") from exc
    if not math.isfinite(numero) or numero <= 0:
        raise argparse.ArgumentTypeError("debe ser mayor que cero")
    return numero


def parsear_workers(valor: str) -> tuple[int, ...]:
    partes = [parte.strip() for parte in valor.split(",")]
    if not partes or any(not parte for parte in partes):
        raise argparse.ArgumentTypeError(
            "--workers-list debe contener enteros separados por comas"
        )
    try:
        workers = tuple(int(parte) for parte in partes)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "--workers-list solo admite enteros"
        ) from exc
    if any(worker <= 0 for worker in workers):
        raise argparse.ArgumentTypeError("todos los workers deben ser positivos")
    if len(set(workers)) != len(workers):
        raise argparse.ArgumentTypeError("--workers-list no admite duplicados")
    if tuple(sorted(workers)) != workers:
        raise argparse.ArgumentTypeError(
            "--workers-list debe estar ordenada de menor a mayor"
        )
    if workers[0] != 1:
        raise argparse.ArgumentTypeError(
            "--workers-list debe comenzar en 1 para calcular el speedup"
        )
    return workers


def _resolver_ruta(ruta: str | Path) -> Path:
    path = Path(ruta).expanduser()
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path.resolve()


def _leer_manifiesto(
    ruta: Path,
    *,
    workers: int,
    total_events: int,
    seed: int,
    arch: str,
    batch_size: int,
    sim_days: float,
) -> dict:
    try:
        with ruta.open(encoding="utf-8") as archivo:
            manifiesto = json.load(archivo)
    except (OSError, json.JSONDecodeError) as exc:
        raise BenchmarkError(f"manifiesto invalido o ilegible: {ruta}") from exc

    requeridos = {
        "duracion_seg",
        "total_eventos",
        "total_eventos_solicitados",
        "total_bytes",
        "arquitectura",
        "workers",
        "seed",
        "batch_size",
        "sim_days",
    }
    faltantes = requeridos.difference(manifiesto)
    if faltantes:
        raise BenchmarkError(
            "el manifiesto no contiene: " + ", ".join(sorted(faltantes))
        )

    esperado = {
        "total_eventos_solicitados": total_events,
        "total_eventos": total_events,
        "arquitectura": arch,
        "workers": workers,
        "seed": seed,
        "batch_size": batch_size,
    }
    for campo, valor in esperado.items():
        if manifiesto[campo] != valor:
            raise BenchmarkError(
                f"manifiesto inconsistente: {campo}={manifiesto[campo]!r}; "
                f"se esperaba {valor!r}"
            )

    try:
        duracion = float(manifiesto["duracion_seg"])
        total_bytes = int(manifiesto["total_bytes"])
        sim_days_real = float(manifiesto["sim_days"])
    except (TypeError, ValueError) as exc:
        raise BenchmarkError(
            "el manifiesto contiene tipos numericos invalidos"
        ) from exc
    if duracion <= 0 or total_bytes <= 0:
        raise BenchmarkError("duracion_seg y total_bytes deben ser positivos")
    if abs(sim_days_real - sim_days) > 1e-9:
        raise BenchmarkError(
            f"manifiesto inconsistente: sim_days={sim_days_real}; "
            f"se esperaba {sim_days}"
        )
    return manifiesto


def correr_medicion(
    workers: int,
    total_events: int,
    seed: int,
    arch: str,
    output_dir: Path,
    repeticion: int,
    batch_size: int,
    sim_days: float,
    timeout: float,
    fecha_utc: str,
) -> dict:
    """Ejecuta una corrida y valida su manifiesto antes de medirla."""
    cmd = [
        sys.executable,
        "-m",
        "src.main",
        "--workers",
        str(workers),
        "--total-events",
        str(total_events),
        "--output-dir",
        str(output_dir),
        "--seed",
        str(seed),
        "--arch",
        arch,
        "--batch-size",
        str(batch_size),
        "--sim-days",
        str(sim_days),
        "--skip-content-validation",
    ]

    try:
        resultado = subprocess.run(
            cmd,
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise BenchmarkError(
            f"timeout ({timeout:g}s) con workers={workers}, rep={repeticion}"
        ) from exc

    if resultado.returncode != 0:
        detalle = (resultado.stderr or resultado.stdout).strip()
        if len(detalle) > 2_000:
            detalle = detalle[-2_000:]
        raise BenchmarkError(
            f"src.main fallo con workers={workers}, rep={repeticion}, "
            f"codigo={resultado.returncode}: {detalle or 'sin diagnostico'}"
        )

    manifiesto = _leer_manifiesto(
        output_dir / "manifiesto.json",
        workers=workers,
        total_events=total_events,
        seed=seed,
        arch=arch,
        batch_size=batch_size,
        sim_days=sim_days,
    )
    duracion = float(manifiesto["duracion_seg"])
    eventos = int(manifiesto["total_eventos"])
    total_bytes = int(manifiesto["total_bytes"])

    return {
        "arquitectura": arch,
        "total_eventos_solicitados": total_events,
        "total_eventos_reales": eventos,
        "seed": seed,
        "batch_size": batch_size,
        "sim_days": sim_days,
        "worker_count": workers,
        "repeticion": repeticion,
        "tipo": "warmup" if repeticion == 1 else "medicion",
        "tiempo_s": round(duracion, 6),
        "eventos_por_s": round(eventos / duracion, 3),
        "mb_por_s": round((total_bytes / (1024 * 1024)) / duracion, 3),
        "bytes": total_bytes,
        "fecha_utc": fecha_utc,
    }


def validar_completitud(
    rows: Iterable[dict], workers_list: tuple[int, ...], repeticiones: int
) -> list[dict]:
    rows = list(rows)
    esperado = len(workers_list) * repeticiones
    if len(rows) != esperado:
        raise BenchmarkError(
            f"CSV incompleto: {len(rows)} filas; se esperaban {esperado}"
        )

    for workers in workers_list:
        grupo = [row for row in rows if row["worker_count"] == workers]
        reps = sorted(row["repeticion"] for row in grupo)
        if reps != list(range(1, repeticiones + 1)):
            raise BenchmarkError(
                f"repeticiones incompletas para workers={workers}: {reps}"
            )
        warmups = [row for row in grupo if row["tipo"] == "warmup"]
        mediciones = [row for row in grupo if row["tipo"] == "medicion"]
        if len(warmups) != 1 or warmups[0]["repeticion"] != 1:
            raise BenchmarkError(f"warmup invalido para workers={workers}")
        if len(mediciones) != repeticiones - 1:
            raise BenchmarkError(
                f"numero de mediciones invalido para workers={workers}"
            )

    volumenes = {row["total_eventos_reales"] for row in rows}
    solicitados = {row["total_eventos_solicitados"] for row in rows}
    if len(volumenes) != 1 or volumenes != solicitados:
        raise BenchmarkError(
            "el volumen real no fue fijo o no coincide con el solicitado"
        )
    return rows


def escribir_csv_atomico(ruta: Path, rows: list[dict]) -> None:
    ruta.parent.mkdir(parents=True, exist_ok=True)
    temporal: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="",
            dir=ruta.parent,
            prefix=f".{ruta.name}.",
            suffix=".tmp",
            delete=False,
        ) as archivo:
            temporal = Path(archivo.name)
            writer = csv.DictWriter(archivo, fieldnames=CSV_FIELDS)
            writer.writeheader()
            writer.writerows(rows)
        os.replace(temporal, ruta)
    finally:
        if temporal is not None and temporal.exists():
            temporal.unlink()


def imprimir_resumen(rows: list[dict], workers_list: tuple[int, ...]) -> None:
    print("\n" + "=" * 78)
    print(
        f"{'Workers':>8} {'T mediana':>11} {'Speedup':>10} "
        f"{'Eficiencia':>12} {'evt/s':>12} {'MB/s':>9}"
    )
    print("-" * 78)
    t1_mediana = statistics.median(
        row["tiempo_s"]
        for row in rows
        if row["worker_count"] == 1 and row["tipo"] == "medicion"
    )
    for workers in workers_list:
        grupo = [
            row
            for row in rows
            if row["worker_count"] == workers and row["tipo"] == "medicion"
        ]
        t_mediana = statistics.median(row["tiempo_s"] for row in grupo)
        speedup = t1_mediana / t_mediana
        eficiencia = speedup / workers
        evt_s = statistics.median(row["eventos_por_s"] for row in grupo)
        mb_s = statistics.median(row["mb_por_s"] for row in grupo)
        print(
            f"{workers:>8} {t_mediana:>10.3f}s {speedup:>9.2f}x "
            f"{eficiencia:>11.1%} {evt_s:>12,.0f} {mb_s:>9.2f}"
        )
    print("=" * 78)


def crear_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Benchmark de escalabilidad del generador TPC"
    )
    parser.add_argument(
        "--total-events",
        type=entero_positivo,
        default=10_000_000,
        help="eventos exactos por corrida (default: 10000000)",
    )
    parser.add_argument(
        "--repeticiones",
        type=entero_positivo,
        default=4,
        help="corridas por worker: 1 warmup + al menos 3 mediciones (default: 4)",
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--arch", choices=("shard", "queue"), default="shard"
    )
    parser.add_argument(
        "--batch-size", type=entero_positivo, default=10_000
    )
    parser.add_argument(
        "--sim-days", type=flotante_positivo, default=9.0
    )
    parser.add_argument(
        "--workers-list",
        type=parsear_workers,
        default=DEFAULT_WORKERS,
        help="lista creciente que comienza en 1 (default: 1,2,4,8,16)",
    )
    parser.add_argument(
        "--csv", default="bench/mediciones.csv", help="CSV de salida"
    )
    parser.add_argument(
        "--data-dir",
        default="data/raw",
        help="raiz para temporales aislados; no se borran sus otros archivos",
    )
    parser.add_argument(
        "--timeout",
        type=flotante_positivo,
        default=3_600.0,
        help="timeout por corrida en segundos (default: 3600)",
    )
    parser.add_argument(
        "--allow-small",
        action="store_true",
        help="permite menos de 10M solo para pruebas de humo",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = crear_parser()
    args = parser.parse_args(argv)
    if args.repeticiones < 4:
        parser.error("--repeticiones debe ser >= 4 (1 warmup + 3 mediciones)")
    if args.total_events < MIN_EVENTOS_GUIA and not args.allow_small:
        parser.error(
            f"--total-events debe ser >= {MIN_EVENTOS_GUIA}; "
            "use --allow-small unicamente para pruebas de humo"
        )

    csv_path = _resolver_ruta(args.csv)
    data_dir = _resolver_ruta(args.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    cpu_count = os.cpu_count() or 1
    fecha_utc = datetime.now(timezone.utc).isoformat(timespec="seconds")
    rows: list[dict] = []

    try:
        for workers in args.workers_list:
            if workers > cpu_count:
                print(
                    f"Aviso: workers={workers} excede los {cpu_count} "
                    "procesadores logicos detectados."
                )
            for repeticion in range(1, args.repeticiones + 1):
                with tempfile.TemporaryDirectory(
                    prefix=f"tpc_w{workers}_r{repeticion}_", dir=data_dir
                ) as temporal:
                    resultado = correr_medicion(
                        workers=workers,
                        total_events=args.total_events,
                        seed=args.seed,
                        arch=args.arch,
                        output_dir=Path(temporal),
                        repeticion=repeticion,
                        batch_size=args.batch_size,
                        sim_days=args.sim_days,
                        timeout=args.timeout,
                        fecha_utc=fecha_utc,
                    )
                rows.append(resultado)
                print(
                    f"workers={workers:>2} rep={repeticion} "
                    f"{resultado['tiempo_s']:>9.3f}s "
                    f"{resultado['eventos_por_s']:>12,.0f} evt/s "
                    f"{resultado['mb_por_s']:>8.2f} MB/s "
                    f"[{resultado['tipo']}]"
                )

        validar_completitud(rows, args.workers_list, args.repeticiones)
        escribir_csv_atomico(csv_path, rows)
    except (BenchmarkError, OSError) as exc:
        print(f"Error: benchmark abortado: {exc}", file=sys.stderr)
        return 1

    print(f"\nCSV completo guardado en {csv_path}")
    imprimir_resumen(rows, args.workers_list)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

