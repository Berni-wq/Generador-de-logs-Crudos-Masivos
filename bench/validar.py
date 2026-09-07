import argparse
import glob
import json
import os
import sys


def contar_eventos(carpeta):
    """Cuenta las líneas (eventos) de los archivos .jsonl de una carpeta."""
    total = 0
    archivos = sorted(glob.glob(os.path.join(carpeta, "*.jsonl")))
    for ruta in archivos:
        with open(ruta, encoding="utf-8") as f:
            for _ in f:
                total += 1
    return total


def validar(carpeta):
    total = 0
    archivos = sorted(glob.glob(os.path.join(carpeta, "*.jsonl")))

    if not archivos:
        print(f"No se encontraron archivos .jsonl en {carpeta}")
        return False

    ok = True
    for ruta in archivos:
        with open(ruta, encoding="utf-8") as f:
            for i, linea in enumerate(f, 1):
                try:
                    json.loads(linea)
                except json.JSONDecodeError:
                    print(f"Error: línea inválida en {ruta}:{i}", file=sys.stderr)
                    ok = False
                total += 1

    if ok:
        print(f"OK, {total:,} eventos válidos verificados en {len(archivos)} archivos.")
    else:
        print(f"Se encontraron errores en {total:,} eventos revisados.", file=sys.stderr)

    return ok


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validador de archivos .jsonl")
    parser.add_argument("carpeta", help="Carpeta que contiene los archivos .jsonl")
    args = parser.parse_args()

    if not os.path.isdir(args.carpeta):
        print(f"Error: {args.carpeta} no es un directorio válido", file=sys.stderr)
        sys.exit(1)

    sys.exit(0 if validar(args.carpeta) else 1)
