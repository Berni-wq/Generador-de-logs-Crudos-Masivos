# Generador-de-logs-Crudos-Masivos# Simulador de Logs Masivos - TPC Sitio 3

Generador de eventos JSON Lines (.jsonl) para el Terminal Puerto Coquimbo (TPC) Sitio 3 — un frente de atraque con fajas transportadoras, grúas móviles, báscula de acceso y contenedores refrigerados (reefers).

## Requisitos

- Python 3.10+
- Solo librería estándar (sin dependencias externas)
- matplotlib (solo para gráficos del reporte)

## Estructura


> **Benchmark vs dataset final:** `bench/benchmark.py` escribe sus mediciones en `data/bench_tmp/` y limpia esa carpeta antes de cada corrida para no contaminar los tiempos. `data/raw/` queda reservado para el **dataset final entregable**, que se genera una sola vez al final.

## Uso

### Generar eventos (Opción B - shard, por defecto)

```bash
python -m src.main --workers 4 --total-events 1000000
```

### Generar eventos (Opción A - queue)

```bash
python -m src.main --workers 4 --total-events 1000000 --arch queue
```

### Flags disponibles

| Flag | Default | Descripción |
|------|---------|-------------|
| `--workers` | 4 | Número de procesos paralelos |
| `--total-events` | 1.000.000 | Total de eventos finales (.jsonl) a generar |
| `--output-dir` | data/raw | Carpeta de salida |
| `--seed` | 42 | Semilla para reproducibilidad |
| `--arch` | shard | `shard` o `queue` |

### Validar datos generados

```bash
python bench/validar.py data/raw
```

### Regenerar la muestra versionable (`data/muestra`)

Sección 4.1 de la guía: la muestra (50.000 eventos de ejemplo, `data/muestra/`) **sí se versiona en git**, a diferencia de `data/raw/` (ignorada). Es un paso manual de preparación antes de hacer commit (no forma parte del flujo Docker):

```bash
python -m bench.generar_muestra
```

Invoca `src.main` (shard, 1 worker) y verifica con `bench.validar` que la muestra tenga ≥ 50.000 líneas. Con `--total-events 50000` y el redondeo hacia arriba a múltiplo de 7 genera 50.001 eventos, junto con su `manifiesto.json`.

### Benchmarking

Corre las mediciones y guarda el CSV. Escribe sus datos en `data/bench_tmp/` (directorio temporal que se limpia en cada medición), **nunca** en `data/raw`:

```bash
python bench/benchmark.py --total-events 10000000 --repeticiones 3
```

Genera el resumen en texto y el PDF con gráficos a partir del CSV:

```bash
python bench/reporte.py
```

> **Nota sobre `--total-events`:** El flag indica el número de **eventos finales** (.jsonl) **solicitados**, no ticks. Cada tick genera 7 eventos (uno por sensor) y el generador usa **redondeo hacia arriba** al múltiplo de 7 más cercano (`total_ticks = ceil(total_events / 7)`), de modo que el dataset generado es siempre **≥** lo pedido, nunca menos (e.g., `--total-events 10000000` genera 10.000.004 eventos). No hace falta pasar un múltiplo exacto a mano.

## Uso con Docker (one-shot: genera + benchmark + reporte)

La imagen corre **todo de un tirón** en este orden: benchmark de escalabilidad → generación del dataset final → validación → reporte. El benchmark usa una carpeta temporal (`data/bench_tmp`) y el dataset final se genera después en `data/raw` con el `--total-events` entregable (≥ 10.000.000 eventos), se valida con `bench/validar.py` y se aborta si falla o si tiene menos de 10 millones de eventos. Solo necesita Docker instalado, no Python ni matplotlib en el host.

### Construir la imagen

```bash
docker build -t tpc-generador .
```

### Preparar las carpetas de salida

Crea (o deja que Docker las cree) las carpetas `salida/` y `dataset/` junto al proyecto; la segunda es la que persistirá el dataset final:

```bash
mkdir -p salida dataset
```

### Ejecutar (todo en un comando)

```bash
docker run --rm -v ${PWD}/salida:/app/salida -v ${PWD}/dataset:/app/data/raw tpc-generador --total-events 10000004
```

El flag `--rm` borra el contenedor al terminar. Se montan **dos volúmenes**:

- `-v ${PWD}/salida:/app/salida` — resultados (CSV, gráfico, PDF, manifiesto y resumen) en `salida/`.
- `-v ${PWD}/dataset:/app/data/raw` — el dataset final .jsonl se escribe directo en `dataset/` del host, así sobrevive aunque el contenedor se elimine con `--rm`.

Se pasa `--total-events 10000004` para pedir ≥ 10.000.000 eventos (el generador redondea hacia arriba al múltiplo de 7 más cercano, así que ya no es obligatorio que el valor sea un múltiplo exacto; solo se usa un valor real del orden entregable).

**Resultados en `salida/`:**
- `mediciones.csv` — datos crudos del benchmark
- `speedup_vs_ideal.png` — gráfico de speedup
- `reporte_benchmark.pdf` — reporte completo
- `manifiesto.json` — manifiesto del dataset final (eventos, bytes, workers, seed)
- `resumen_dataset.txt` — total de líneas y tamaño en bytes del dataset final

**Dataset final en `dataset/`:** los .jsonl (varios GB) se escriben directo en esa carpeta del host. Si no montas el volumen `dataset`, Docker la crea temporalmente dentro del contenedor y se pierde al terminar el `--rm`. No se copia a `salida/` para no duplicar gigabytes; `manifiesto.json` y `resumen_dataset.txt` de `salida/` son la evidencia del dataset montado en `dataset/`.

### Sobrescribir parámetros

```bash
docker run --rm -v ${PWD}/salida:/app/salida -v ${PWD}/dataset:/app/data/raw tpc-generador --workers 4 --total-events 10000004 --seed 42 --arch shard
```

| Flag | Default | Descripción |
|------|---------|-------------|
| `--workers` | 4 | Procesos paralelos |
| `--total-events` | 10.000.004 | Eventos finales (mínimo entregable: 10.000.000) |
| `--arch` | shard | `shard` o `queue` |
| `--seed` | 42 | Semilla para reproducibilidad |
| `--salida` | /app/salida | Carpeta de salida (móntala como volumen) |

> Si `--total-events` se deja por debajo de 10.000.000, `run_all.py` aborta antes de empezar. `ejecutar.bat` ya pasa `--total-events 10000004` y monta `salida/` + `dataset/` automáticamente (los crea si no existen).

> El benchmark mide cómo escala según la CPU/RAM/disco del equipo anfitrión y puede tardar varios minutos.

## Sensores simulados

| Sensor | sensor_id | Métrica | Unidad |
|--------|-----------|---------|--------|
| Reefer | REEFER_S3_XX | temperature | C |
| Faja | FAJA_S3_CXX | vibration | mm/s |
| Motor de faja | MOTOR_S3_CXX | amperage | A |
| Báscula | BASCULA_S3_XX | weight | kg |
| Grúa móvil | GRUA_S3_STSXX | position | deg |
| Sensor ambiental | AMBIENTE_S3_XX | temperature/humidity | C/% |

## Entorno de pruebas

| Componente | Detalle |
|------------|---------|
| CPU | AMD Ryzen 7 4800H (8 nucleos fisicos, 16 hilos SMT) |
| RAM | 16 GB DDR4 |
| Disco | NVMe SSD - SAMSUNG MZVLQ512HALU-000H1 |
| SO | Windows 11 Home Single Language (10.0.26200) |
| Python | 3.14.4 |

## Análisis de escalabilidad (shard, 10M eventos)

| Workers | T mediana | Speedup S(n) | Eficiencia E(n) | evt/s | MB/s |
|--------:|----------:|-------------:|----------------:|------:|-----:|
| 1 | 144.52s | 1.00x | 100.0% | 69,198 | 8.52 |
| 2 | 87.16s | 1.66x | 82.9% | 114,770 | 14.12 |
| 4 | 70.24s | 2.06x | 51.4% | 142,418 | 17.51 |
| 8 | 58.61s | 2.47x | 30.8% | 170,624 | 20.98 |
| 16 | 56.35s | 2.56x | 16.0% | 178,600 | 22.03 |

**Interpretación:**

- **1→2 workers**: buen salto (1.66x, 82.9% eficiencia). Cada worker corre en un núcleo físico dedicado.
- **2→4 workers**: sigue escalando (2.06x), eficiencia 51.4%. El overhead de fork + escritura comienza a pesar.
- **4→8 workers**: curva se aplana (2.47x, 30.8% eficiencia). Se ocupan los 8 núcleos físicos del Ryzen 7 4800H.
- **8→16 workers**: casi plano (2.56x, 16.0% eficiencia). De 9 a 16 workers, los procesos comparten núcleos físicos vía SMT (hyper-threading). SMT aporta poco en carga CPU-bound porque no hay esperas de I/O que aprovechar — los dos hilos de un mismo núcleo compiten por la misma ALU/FPU. El resultado es que duplicar workers de 8 a 16 apenas reduce el tiempo un 4%.

**Punto dulce: 4 workers** — buena relación rendimiento/costó. Más allá de 4, cada worker extra aporta menos por overhead de multiprocessing.

## Notas

- Cada tick (15s simulados) genera 7 eventos: uno por cada sensor (Reefer, Faja, Motor, Báscula, Grúa, Ambiental-temp, Ambiental-humedad)
- Con la misma semilla y workers, las ejecuciones son reproducibles
- Los archivos .jsonl son válidos para Spark (sin compresión gzip)
