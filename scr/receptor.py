import asyncio
import os
import signal
import sys
from pathlib import Path


BIND_HOST = os.environ.get("BIND_HOST", "0.0.0.0")
TCP_PORT = int(os.environ.get("TCP_PORT", "9009"))
DATA_DIR = os.environ.get("DATA_DIR", "./data")

# Cada cuantos eventos acumulados se imprime un mensaje de progreso.
PROGRESO_CADA = 100_000

""" Margen de espera durante el cierre ordenado, en segundos. Debe quedar por
 debajo del stop_grace_period del docker-compose.yml (30s) para no correr
 el riesgo de que Docker mande SIGKILL antes de que terminemos de esperar.
"""
ESPERA_CIERRE_SEG = 25

LIMITE_LECTURA = 8 * 1024 * 1024  # 8 MiB


class EstadoReceptor:

    def __init__(self, archivo):
        self.archivo = archivo
        self.total_eventos = 0
        self.total_bytes = 0
        self.conexiones_activas = 0
        self.cerrando = False
        # Se marca cuando el servidor deja de aceptar conexiones nuevas.
        self.server = None

    def escribir_lote(self, lineas):
        ##Escribe un lote ya completo de lineas al archivo compartido.

        if not lineas:
            return
        if self.archivo.closed:

            print(f"[receptor_tcp] AVISO: se descartaron {len(lineas):,} "
                  f"eventos porque el archivo ya estaba cerrado (timeout "
                  f"de cierre agotado con esta conexion todavia activa)",
                  flush=True)
            return
        self.archivo.writelines(lineas)
        self.total_eventos += len(lineas)
        self.total_bytes += sum(len(l.encode("utf-8")) for l in lineas)


async def manejar_conexion(reader, writer, estado: EstadoReceptor):
    """Handler de una conexion (un worker del simulador).
    Lee lineas JSON completas (delimitadas por '\\n'
    """
    peer = writer.get_extra_info("peername")
    estado.conexiones_activas += 1
    print(f"[receptor_tcp] conexion abierta: {peer} "
          f"(activas={estado.conexiones_activas})", flush=True)

    buf = []
    LOTE = 10_000  # mismo tamano de lote que LOTE en arquitectura_a.py

    try:
        while True:
            try:
                linea = await reader.readline()
            except (ConnectionResetError, asyncio.IncompleteReadError,
                     asyncio.LimitOverrunError):
                break

            if not linea:
                """EOF: el worker cerro su extremo del socket -> fin de esa
                     conexion. No hace falta un mensaje centinela explicito,
                    el cierre del socket ES la señal de termino.
                """
                break

            texto = linea.decode("utf-8")
            if not texto.strip():
                continue
            buf.append(texto if texto.endswith("\n") else texto + "\n")

            if len(buf) >= LOTE:
                estado.escribir_lote(buf)
                if estado.total_eventos % PROGRESO_CADA < LOTE:
                    print(f"[receptor_tcp] progreso: "
                          f"{estado.total_eventos:,} eventos, "
                          f"{estado.total_bytes / (1024*1024):.1f} MB",
                          flush=True)
                buf = []
    finally:
        if buf:
            estado.escribir_lote(buf)

        estado.conexiones_activas -= 1
        print(f"[receptor_tcp] conexion cerrada: {peer} "
              f"(activas={estado.conexiones_activas})", flush=True)

        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


def _preparar_archivo_salida():
    Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
    ruta = Path(DATA_DIR) / "eventos.jsonl"
    return open(ruta, "w", encoding="utf-8", newline="\n", buffering=4 * 1024 * 1024)


async def cierre_ordenado(estado: EstadoReceptor):
    """Deja de aceptar conexiones nuevas, espera a que las conexiones en
    curso terminen de volcar su buffer pendiente, y recien ahi hace
    flush+fsync+close del archivo. Se invoca desde el handler de SIGTERM.
    """
    if estado.cerrando:
        return
    estado.cerrando = True

    print("[receptor_tcp] SIGTERM recibido: dejando de aceptar conexiones "
          "nuevas y esperando conexiones activas...", flush=True)

    if estado.server is not None:
        estado.server.close()
        await estado.server.wait_closed()

    """Da margen para que los handlers en curso terminen de escribir su
    ultimo buffer parcial (el `finally` de manejar_conexion ya se encarga
    de volcarlo apenas el reader llega a EOF/error). ESPERA_CIERRE_SEG
    queda por debajo del stop_grace_period de Compose (30s) a proposito,
     para no arriesgarnos a que Docker mande SIGKILL antes de terminar.
     """
    espera = 0.0
    paso = 0.2
    while estado.conexiones_activas > 0 and espera < ESPERA_CIERRE_SEG:
        await asyncio.sleep(paso)
        espera += paso

    if estado.conexiones_activas > 0:

        print(f"[receptor_tcp] AVISO: se agoto la ventana de "
              f"{ESPERA_CIERRE_SEG}s de espera con "
              f"{estado.conexiones_activas} conexion(es) todavia activa(s). "
              f"Se cierra igual para respetar el stop_grace_period.",
              flush=True)

    print("SIGTERM recibido: cerrando sockets y haciendo flush del .jsonl",
          flush=True)
    estado.archivo.flush()
    os.fsync(estado.archivo.fileno())
    estado.archivo.close()

    print(f"[receptor_tcp] cierre limpio. Total: "
          f"{estado.total_eventos:,} eventos, "
          f"{estado.total_bytes / (1024*1024):.1f} MB", flush=True)


async def main():
    archivo = _preparar_archivo_salida()
    estado = EstadoReceptor(archivo)

    loop = asyncio.get_running_loop()

    async def _handler(reader, writer):
        await manejar_conexion(reader, writer, estado)

    server = await asyncio.start_server(
        _handler, host=BIND_HOST, port=TCP_PORT, limit=LIMITE_LECTURA
    )
    estado.server = server


    def _pedir_cierre():
        asyncio.ensure_future(cierre_ordenado(estado))

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _pedir_cierre)
        except NotImplementedError:
            # add_signal_handler no esta disponible en windows, pero en dokcer no problema
          
            print(f"[receptor_tcp] aviso: no se pudo instalar handler para "
                  f"{sig!r} en esta plataforma", flush=True)

    addr = ", ".join(str(s.getsockname()) for s in server.sockets)
    print(f"[receptor_tcp] escuchando en {addr} "
          f"(DATA_DIR={DATA_DIR})", flush=True)

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    sys.exit(0)
