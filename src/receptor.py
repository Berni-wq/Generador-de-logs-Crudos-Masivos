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
        # Conexiones TCP abiertas, para poder cerrarlas de forma activa al apagar.
        # En Python 3.12 asyncio.Server.wait_closed() espera a que terminen TODOS los
        # handlers, asi que una conexion colgada lo bloquearia para siempre y el
        # flush/fsync del archivo no llegaria a ejecutarse nunca.
        self.writers = set()
        # Tarea de cierre en curso, para poder esperarla antes de que asyncio.run la cancele.
        self.cierre_task = None

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
    estado.writers.add(writer)
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
        estado.writers.discard(writer)
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
          "nuevas y volcando el archivo...", flush=True)

    if estado.server is not None:
        estado.server.close()

    try:
        """Volcado INMEDIATO del archivo, antes de esperar nada.

        Es la unica garantia real de no perder eventos: si mas adelante algo falla o
        Docker manda SIGKILL, lo que ya estaba en el buffer de 4 MiB queda en disco.
        Va primero a proposito, porque en Python >= 3.12 wait_closed() se bloquea si
        queda alguna conexion viva y el codigo que viene despues no se alcanzaria.
        """
        estado.archivo.flush()
        os.fsync(estado.archivo.fileno())
        print(f"[receptor_tcp] flush+fsync completado: "
              f"{estado.total_eventos:,} eventos, "
              f"{estado.total_bytes / (1024*1024):.1f} MB", flush=True)

        """Cierre activo de las conexiones que sigan abiertas. Al cerrarlas, sus
        handlers reciben el fin de lectura, salen del bucle y ejecutan su finally,
        que vuelca el buffer parcial que tuvieran pendiente.
        """
        for conexion in list(estado.writers):
            try:
                conexion.close()
            except Exception:
                pass

        """Espera ACOTADA. ESPERA_CIERRE_SEG (25 s) queda por debajo del
        stop_grace_period de docker-compose.yml (30 s) para no arriesgarse a que
        Docker mande SIGKILL antes de terminar.
        """
        if estado.server is not None:
            try:
                await asyncio.wait_for(estado.server.wait_closed(),
                                       timeout=ESPERA_CIERRE_SEG)
            except asyncio.TimeoutError:
                print(f"[receptor_tcp] AVISO: wait_closed() no completo en "
                      f"{ESPERA_CIERRE_SEG}s; se cierra igual para respetar el "
                      f"stop_grace_period.", flush=True)

    finally:
        """Volcado final: recoge lo que hayan escrito los handlers al terminar y
        cierra el archivo. Va en finally para que el archivo quede cerrado y
        sincronizado aunque la espera haya fallado.
        """
        try:
            estado.archivo.flush()
            os.fsync(estado.archivo.fileno())
        finally:
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
        estado.cierre_task = asyncio.ensure_future(cierre_ordenado(estado))

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

    # serve_forever() vuelve en cuanto server.close() se ejecuta, pero el volcado final
    # del archivo ocurre en la tarea de cierre. Hay que esperarla aqui: si main() volviera
    # antes, asyncio.run() cancelaria esa tarea a mitad del flush y se perderian eventos.
    if estado.cierre_task is not None:
        try:
            await estado.cierre_task
        except Exception:
            pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    sys.exit(0)
