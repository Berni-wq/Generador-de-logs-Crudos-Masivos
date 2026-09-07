# Imagen de runtime del generador de logs masivos TPC Sitio 3.
# Un solo `docker run` hace TODO: genera el dataset, corre el benchmark
# de escalabilidad y produce el reporte (CSV + grafico + PDF) con los
# resultados del equipo donde se ejecute.
FROM python:3.14-slim

WORKDIR /app

# Copia el codigo fuente del generador y el benchmark.
COPY src/ ./src/
COPY bench/ ./bench/

# Instala matplotlib (unico requisito para los graficos/PDF del reporte).
# El generador y el benchmark usan unicamente la libreria estandar.
RUN pip install --no-cache-dir matplotlib

# Orquestador: corre generacion + benchmark + reporte en secuencia.
COPY run_all.py ./run_all.py

# Ruta de salida por defecto (sobrescribible con --output en docker run).
ENV SALIDA=/app/salida

ENTRYPOINT ["python", "run_all.py"]
