import csv
import json
import os
import platform
import shutil
import statistics
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages


CSV_PATH = Path("bench/mediciones.csv")
PICKLE_CSV = Path("bench/pickle_benchmark.csv")
MEM_JSON = Path("bench/memoria_result.json")
PDF_PATH = Path("docs/reporte_benchmark.pdf")
PNG_PATH = Path("bench/speedup_vs_ideal.png")


def cargar_datos(csv_path):
    rows = []
    with open(csv_path, newline="") as f:
        for row in csv.DictReader(f):
            row["worker_count"] = int(row["worker_count"])
            row["repeticion"] = int(row["repeticion"])
            row["tiempo_s"] = float(row["tiempo_s"])
            row["eventos_por_s"] = float(row["eventos_por_s"])
            row["mb_por_s"] = float(row["mb_por_s"])
            row["bytes"] = int(row["bytes"])
            rows.append(row)
    return rows


def calcular_resumen(rows):
    workers_set = sorted(set(r["worker_count"] for r in rows))
    t1_mediana = None
    resumen = []

    for w in workers_set:
        mediciones = [r["tiempo_s"] for r in rows
                      if r["worker_count"] == w and r["tipo"] == "medicion"]
        if not mediciones:
            continue
        t_med = statistics.median(mediciones)
        if t1_mediana is None:
            t1_mediana = t_med
        speedup = t1_mediana / t_med
        eficiencia = speedup / w
        evt_s = statistics.median([r["eventos_por_s"] for r in rows
                                   if r["worker_count"] == w and r["tipo"] == "medicion"])
        mb_s = statistics.median([r["mb_por_s"] for r in rows
                                  if r["worker_count"] == w and r["tipo"] == "medicion"])
        resumen.append({
            "workers": w,
            "t_mediana": round(t_med, 2),
            "speedup": round(speedup, 2),
            "eficiencia": round(eficiencia, 4),
            "evt_s": round(evt_s, 0),
            "mb_s": round(mb_s, 2),
        })
    return resumen


def detectar_hardware():
    """Detecta componentes del equipo.

    Prioriza las variables de entorno HOST_* que el host Windows pasa al
    contenedor Docker (ejecutar.bat). Si no existen (ejecucion nativa o de
    otro entorno), detecta en vivo con la stdlib.
    """
    info = {
        "cpu": (os.environ.get("HOST_CPU") or "").strip(),
        "nucleos_logicos": os.cpu_count() or 1,
        "ram_gb": None,
        "disco_total_gb": None,
        "so": os.environ.get("HOST_SO") or f"{platform.system()} {platform.release()}",
        "python": sys.version.split()[0],
    }

    if not info["cpu"]:
        info["cpu"] = (platform.processor() or platform.machine()).strip()
        try:
            if sys.platform == "win32":
                import winreg
                with winreg.OpenKey(
                    winreg.HKEY_LOCAL_MACHINE,
                    r"HARDWARE\DESCRIPTION\System\CentralProcessor\0",
                ) as clave:
                    nombre = winreg.QueryValueEx(clave, "ProcessorNameString")[0].strip()
                    if nombre:
                        info["cpu"] = nombre
        except Exception:
            pass

    if os.environ.get("HOST_RAM"):
        try:
            info["ram_gb"] = float(os.environ["HOST_RAM"])
        except ValueError:
            pass
    if info["ram_gb"] is None:
        try:
            import ctypes
            from ctypes import wintypes

            class MEMORYSTATUSEX(ctypes.Structure):
                _fields_ = [
                    ("dwLength", wintypes.DWORD),
                    ("dwMemoryLoad", wintypes.DWORD),
                    ("ullTotalPhys", ctypes.c_uint64),
                    ("ullAvailPhys", ctypes.c_uint64),
                    ("ullTotalPageFile", ctypes.c_uint64),
                    ("ullAvailPageFile", ctypes.c_uint64),
                    ("ullTotalVirtual", ctypes.c_uint64),
                    ("ullAvailVirtual", ctypes.c_uint64),
                    ("ullAvailExtendedVirtual", ctypes.c_uint64),
                ]

            status = MEMORYSTATUSEX()
            status.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
            ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status))
            info["ram_gb"] = round(status.ullTotalPhys / (1024 ** 3), 1)
        except Exception:
            try:
                with open("/proc/meminfo", encoding="utf-8") as f:
                    for linea in f:
                        if linea.startswith("MemTotal:"):
                            info["ram_gb"] = round(int(linea.split()[1]) / (1024 * 1024), 1)
                            break
            except Exception:
                pass

    if os.environ.get("HOST_DISCO"):
        try:
            info["disco_total_gb"] = float(os.environ["HOST_DISCO"])
        except ValueError:
            pass
    if info["disco_total_gb"] is None:
        try:
            total, usado, libre = shutil.disk_usage(str(Path.home()))
            info["disco_total_gb"] = round(total / (1024 ** 3), 1)
        except Exception:
            pass

    return info


def graficar_speedup(resumen, save_png=True):
    workers = [r["workers"] for r in resumen]
    speedup_real = [r["speedup"] for r in resumen]
    eficiencia = [r["eficiencia"] * 100 for r in resumen]
    ideal = workers.copy()

    fig, ax1 = plt.subplots(figsize=(8, 5))

    ax1.plot(workers, ideal, "k--", linewidth=1.5, label="Ideal (lineal)")
    ax1.plot(workers, speedup_real, "o-", color="#2563eb", linewidth=2,
             markersize=8, label="Speedup real S(n)")

    ax1.set_xlabel("Workers", fontsize=12)
    ax1.set_ylabel("Speedup S(n)", fontsize=12, color="#2563eb")
    ax1.tick_params(axis="y", labelcolor="#2563eb")
    ax1.set_xticks(workers)
    ax1.set_xscale("log", base=2)
    ax1.set_xticklabels(workers)

    ax2 = ax1.twinx()
    ax2.bar([w * 1.12 for w in workers], eficiencia, width=0.15,
            alpha=0.25, color="#f59e0b", label="Eficiencia E(n)")
    ax2.set_ylabel("Eficiencia E(n) %", fontsize=12, color="#f59e0b")
    ax2.tick_params(axis="y", labelcolor="#f59e0b")
    ax2.set_ylim(0, 110)

    for i, (w, s, e) in enumerate(zip(workers, speedup_real, eficiencia)):
        ax1.annotate(f"{s:.2f}x", (w, s), textcoords="offset points",
                     xytext=(0, 10), ha="center", fontsize=9, color="#2563eb")

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left",
               fontsize=10, framealpha=0.9)

    ax1.set_title("Speedup y Eficiencia vs Workers\n(Arquitectura shard, 1M eventos)",
                  fontsize=13, fontweight="bold")
    ax1.grid(True, alpha=0.3)

    fig.tight_layout()

    if save_png:
        fig.savefig(PNG_PATH, dpi=150, bbox_inches="tight")
        print(f"PNG guardado en {PNG_PATH}")

    return fig


def generar_pdf(resumen, rows):
    PDF_PATH.parent.mkdir(parents=True, exist_ok=True)

    with PdfPages(PDF_PATH) as pdf:
        hw = detectar_hardware()

        # Pagina 1: Portada
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.axis("off")
        ax.text(0.5, 0.7, "Simulador de Logs Masivos",
                ha="center", va="center", fontsize=22, fontweight="bold")
        ax.text(0.5, 0.55, "Terminal Puerto Coquimbo (TPC) - Sitio 3",
                ha="center", va="center", fontsize=14, color="#555")
        ax.text(0.5, 0.4, "Fase 1: Benchmark de Escalabilidad",
                ha="center", va="center", fontsize=13, color="#333")
        ax.text(0.5, 0.25, f"Arquitectura shard | ~10M eventos | {hw['cpu']}",
                ha="center", va="center", fontsize=11, color="#777")
        ax.text(0.5, 0.1, "Programacion Avanzada - Semestre 2, 2026",
                ha="center", va="center", fontsize=10, color="#999")
        pdf.savefig(fig, bbox_inches="tight")
        plt.close(fig)

        # Pagina 2: Grafico speedup vs ideal
        fig = graficar_speedup(resumen, save_png=False)
        pdf.savefig(fig, bbox_inches="tight")
        plt.close(fig)

        # Pagina 3: Tabla resumen
        fig, ax = plt.subplots(figsize=(8, 4))
        ax.axis("off")
        ax.set_title("Tabla Resumen - Benchmark de Escalabilidad",
                     fontsize=13, fontweight="bold", pad=20)

        headers = ["Workers", "T mediana (s)", "Speedup S(n)", "Eficiencia E(n)", "evt/s", "MB/s"]
        cell_text = []
        for r in resumen:
            cell_text.append([
                str(r["workers"]),
                f"{r['t_mediana']:.2f}",
                f"{r['speedup']:.2f}x",
                f"{r['eficiencia']:.1%}",
                f"{r['evt_s']:,.0f}",
                f"{r['mb_s']:.2f}",
            ])

        table = ax.table(cellText=cell_text, colLabels=headers,
                         cellLoc="center", loc="center")
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1.2, 1.6)

        for i in range(len(headers)):
            table[0, i].set_facecolor("#2563eb")
            table[0, i].set_text_props(color="white", fontweight="bold")

        for i in range(1, len(cell_text) + 1):
            color = "#f0f7ff" if i % 2 == 0 else "white"
            for j in range(len(headers)):
                table[i, j].set_facecolor(color)

        pdf.savefig(fig, bbox_inches="tight")
        plt.close(fig)

        # Pagina 4: Analisis
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.axis("off")
        ax.set_title("Analisis de Escalabilidad", fontsize=13,
                     fontweight="bold", pad=20)

        analisis = f"""
1 -> 2 workers: Buen salto (1.66x, 82.9% eficiencia).
   Cada worker corre en un nucleo fisico dedicado.

2 -> 4 workers: Sigue escalando (2.06x, 51.4% eficiencia).
   El overhead de fork + escritura comienza a pesar.

4 -> 8 workers: Curva se aplana (2.47x, 30.8% eficiencia).
   Se ocupan los {hw['nucleos_logicos']} hilos de {hw['cpu']}.

8 -> 16 workers: Casi plano (2.56x, 16.0% eficiencia).
   SMT no ayuda en CPU-bound: los dos hilos de un nucleo
   compiten por la misma ALU/FPU.

PUNTO DULCE: 4 workers
  -> Buena relacion rendimiento/costo.
  -> Mas alla de 4, cada worker extra aporta menos
     por overhead de multiprocessing.

Los speedups de referencia (1.66x, 2.06x, ...) corresponden al
equipo de desarrollo. Los valores de ESTE equipo estan en la tabla
y el grafico de Speedup vs Ideal de las paginas anteriores.

NOTA: --total-events indica eventos finales (.jsonl),
no ticks. Cada tick genera 7 eventos (1 por sensor).
El benchmark se corrio con --total-events 10000004
(volumen real del dataset final = ~10M eventos).
"""
        ax.text(0.05, 0.95, analisis, transform=ax.transAxes,
                fontsize=10, verticalalignment="top",
                fontfamily="monospace",
                bbox=dict(boxstyle="round", facecolor="#f8f9fa", alpha=0.8))

        pdf.savefig(fig, bbox_inches="tight")
        plt.close(fig)

        # Pagina 5: Entorno de pruebas
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.axis("off")
        ax.set_title("Entorno de Pruebas", fontsize=13,
                     fontweight="bold", pad=20)

        env_headers = ["Componente", "Detalle"]
        cpu_detalle = hw["cpu"]
        if hw.get("nucleos_logicos"):
            cpu_detalle += f" ({hw['nucleos_logicos']} hilos)"
        env_data = [
            ["CPU", cpu_detalle],
            ["RAM", f"{hw['ram_gb']} GB" if hw.get("ram_gb") else "No detectado"],
            ["Disco", f"{hw['disco_total_gb']} GB" if hw.get("disco_total_gb") else "No detectado"],
            ["SO", hw["so"]],
            ["Python", hw["python"]],
        ]

        table = ax.table(cellText=env_data, colLabels=env_headers,
                         cellLoc="center", loc="center")
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1.2, 1.6)

        for i in range(len(env_headers)):
            table[0, i].set_facecolor("#2563eb")
            table[0, i].set_text_props(color="white", fontweight="bold")

        for i in range(1, len(env_data) + 1):
            color = "#f0f7ff" if i % 2 == 0 else "white"
            for j in range(len(env_headers)):
                table[i, j].set_facecolor(color)

        pdf.savefig(fig, bbox_inches="tight")
        plt.close(fig)

        # Pagina 6: Costo de pickle
        pickle_rows = []
        if PICKLE_CSV.exists():
            with open(PICKLE_CSV) as f:
                for row in csv.DictReader(f):
                    pickle_rows.append({
                        "batch_size": int(row["batch_size"]),
                        "duracion": float(row["duracion"]),
                        "eventos": int(row["eventos"]),
                        "evt_s": float(row["evt_s"]),
                    })

        fig, ax = plt.subplots(figsize=(8, 5))
        ax.axis("off")
        ax.set_title("Costo de Pickle (Arquitectura queue)\n"
                     "500K eventos finales, 4 workers, 32-bit integer counter",
                     fontsize=12, fontweight="bold", pad=20)

        if pickle_rows:
            pk_headers = ["Lote (eventos)", "Tiempo (s)", "evt/s"]
            pk_data = []
            for r in pickle_rows:
                pk_data.append([
                    f"{r['batch_size']:,}",
                    f"{r['duracion']:.3f}",
                    f"{r['evt_s']:,.0f}",
                ])

            table = ax.table(cellText=pk_data, colLabels=pk_headers,
                             cellLoc="center", loc="center")
            table.auto_set_font_size(False)
            table.set_fontsize(11)
            table.scale(1.2, 1.6)

            for i in range(len(pk_headers)):
                table[0, i].set_facecolor("#2563eb")
                table[0, i].set_text_props(color="white", fontweight="bold")

            for i in range(1, len(pk_data) + 1):
                color = "#f0f7ff" if i % 2 == 0 else "white"
                for j in range(len(pk_headers)):
                    table[i, j].set_facecolor(color)

            note = (
                "Batch=1 es ~37% mas lento que los lotes de 100-10.000.\n"
                "Cada cola.put() serializa el lote con pickle.dumps() y lo\n"
                "pasa por un pipe del SO; con batch=1 eso implica ~500K put()s,\n"
                "pagando el costo fijo de serializacion por cada evento.\n"
                "Con lotes de 100-1.000, ese costo se amortiza entre miles\n"
                "de eventos, acelerando la ejecucion notablemente."
            )
            ax.text(0.05, 0.05, note, transform=ax.transAxes,
                    fontsize=9, verticalalignment="bottom",
                    fontfamily="monospace",
                    bbox=dict(boxstyle="round", facecolor="#fff7ed", alpha=0.8))

        pdf.savefig(fig, bbox_inches="tight")
        plt.close(fig)

        # Pagina 7: Gestion de memoria
        mem_data = {}
        if MEM_JSON.exists():
            with open(MEM_JSON) as f:
                mem_data = json.load(f)

        fig, ax = plt.subplots(figsize=(8, 5))
        ax.axis("off")
        ax.set_title("Gestion de Memoria", fontsize=13,
                     fontweight="bold", pad=20)

        mem_text = """Arquitectura queue: Queue(maxsize=64)

El maxsize=64 en la Queue aplica contrapresion: si los workers
producen mas rapido de lo que el escritor puede vaciar a disco,
cola.put() bloquea automaticamente en vez de acumular lotes en
RAM sin limite. Esto evita que la memoria crezca descontroladamente
bajo carga desbalanceada (workers rapidos + disco lento).

Sin maxsize, N workers x lotes de 10K eventos podrian acumular
gigabytes en la Queue antes de que el escritor los vacie.

Medicion de memoria (500K eventos, 4 workers, batch=10000):
"""

        if mem_data:
            mem_text += f"""
  Peak Working Set (escritor): {mem_data.get('peak_working_set_mb', 0):.2f} MB
  Datos escritos: {mem_data.get('bytes', 0) / (1024*1024):.2f} MB
  Eventos: {mem_data.get('eventos', 0):,}

El Working Set del proceso escritor se mantuvo en ~20 MB para
500K eventos (61 MB en disco), lo que confirma que el buffering
de 4 MB + flush por lotes mantiene el consumo de memoria bajo
incluso con cientos de miles de eventos en transito.
"""
        else:
            mem_text += "\n  [Datos no disponibles - ejecutar bench/costo_pickle.py]"

        ax.text(0.05, 0.95, mem_text, transform=ax.transAxes,
                fontsize=9.5, verticalalignment="top",
                fontfamily="monospace",
                bbox=dict(boxstyle="round", facecolor="#f0f7ff", alpha=0.8))

        pdf.savefig(fig, bbox_inches="tight")
        plt.close(fig)

    print(f"PDF guardado en {PDF_PATH}")


def main():
    print("Cargando mediciones...")
    rows = cargar_datos(CSV_PATH)
    resumen = calcular_resumen(rows)

    print("\nResumen calculado:")
    print(f"{'Workers':>8} {'T mediana':>10} {'Speedup':>10} {'Eficiencia':>12} {'evt/s':>10} {'MB/s':>8}")
    for r in resumen:
        print(f"{r['workers']:>8} {r['t_mediana']:>9.2f}s {r['speedup']:>9.2f}x {r['eficiencia']:>11.1%} {r['evt_s']:>10,.0f} {r['mb_s']:>7.2f}")

    print("\nGenerando grafico speedup vs ideal...")
    graficar_speedup(resumen, save_png=True)

    print("\nGenerando reporte PDF...")
    generar_pdf(resumen, rows)

    print("\nListo.")


if __name__ == "__main__":
    main()

