@echo off
setlocal enabledelayedexpansion

echo.
echo =============================================
echo  Simulador TPC Sitio 3 - Generador + Benchmark
echo  Detecta tu PC y corre todo via Docker
echo =============================================
echo.

:: --- Verificar que Docker este corriendo ---
where docker >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo [ERROR] Docker no esta instalado o no esta en el PATH.
    echo Instala Docker Desktop: https://docs.docker.com/desktop/install/windows-install/
    pause
    exit /b 1
)

docker info >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo [ERROR] El daemon de Docker no esta corriendo.
    echo Abre Docker Desktop y espera a que este listo, luego vuelve a ejecutar este archivo.
    pause
    exit /b 1
)

:: --- Detectar componentes reales de la PC ---
echo Detectando hardware de la PC...
for /f "delims=" %%i in ('powershell -NoProfile -ExecutionPolicy Bypass -Command "(Get-CimInstance Win32_Processor).Name.Trim()"') do set "HOST_CPU=%%i"
for /f "delims=" %%i in ('powershell -NoProfile -ExecutionPolicy Bypass -Command "[math]::Round((Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory / 1GB, 1)"') do set "HOST_RAM=%%i"
for /f "delims=" %%i in ('powershell -NoProfile -ExecutionPolicy Bypass -Command "[math]::Round(((Get-PSDrive C).Used + (Get-PSDrive C).Free) / 1GB, 1)"') do set "HOST_DISCO=%%i"
for /f "delims=" %%i in ('powershell -NoProfile -ExecutionPolicy Bypass -Command "(Get-CimInstance Win32_OperatingSystem).Caption.Trim()"') do set "HOST_SO=%%i"

echo   CPU  : !HOST_CPU!
echo   RAM  : !HOST_RAM! GB
echo   Disco: !HOST_DISCO! GB
echo   SO   : !HOST_SO!
echo.

:: --- Construir imagen Docker (solo primera vez; despues usa cache) ---
echo [1/3] Construyendo imagen Docker (la primera vez tarda ~1 minuto)...
docker build -t tpc-generador . >nul
if %ERRORLEVEL% neq 0 (
    echo [ERROR] Falla al construir la imagen Docker.
    pause
    exit /b 1
)
echo       Imagen lista.
echo.

:: --- Ejecutar todo: benchmark + dataset final + validacion + reporte ---
echo [2/3] Ejecutando benchmark + dataset final (10,000,004 eventos) + reporte...
echo       Esto tarda ~1-2 horas segun tu PC (10M de eventos, 5 configs x 4 reps).
echo       El benchmark usa una carpeta temporal (data/bench_tmp); el dataset
echo       final se escribe en ./dataset (montado desde /app/data/raw), se valida
echo       y su evidencia se copia a salida/.
echo.
docker run --rm -e "HOST_CPU=!HOST_CPU!" -e "HOST_RAM=!HOST_RAM!" -e "HOST_DISCO=!HOST_DISCO!" -e "HOST_SO=!HOST_SO!" -v "%CD%\salida:/app/salida" -v "%CD%\dataset:/app/data/raw" tpc-generador --total-events 10000004

if %ERRORLEVEL% neq 0 (
    echo.
    echo [ERROR] El contenedor Docker fallo. Revisa el mensaje de arriba.
    pause
    exit /b 1
)

:: --- Verificar que las carpetas de salida existan ---
if not exist "%CD%\salida" mkdir "%CD%\salida"
if not exist "%CD%\dataset" mkdir "%CD%\dataset"

:: --- Listo ---
echo.
echo =============================================
echo  [3/3] LISTO. Resultados en la carpeta: %CD%\salida
echo =============================================
echo.
echo   - mediciones.csv            (datos del benchmark)
echo   - speedup_vs_ideal.png      (grafico)
echo   - reporte_benchmark.pdf     (reporte con tus componentes reales)
echo   - manifiesto.json           (evidencia del dataset final)
echo   - resumen_dataset.txt       (lineas totales y bytes del dataset final)
echo.
echo  Dataset final (.jsonl) en: %CD%\dataset\  (montado desde /app/data/raw)
echo.
pause
