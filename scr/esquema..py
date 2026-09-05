from datetime import datetime, timedelta, timezone

from src.simulador import (
    Reefer, Faja, MotorFaja, Bascula, GruaMovil, SensorAmbiental,
)


EVENTOS_POR_TICK = 7

# Instante base de la simulación: no depende del reloj del sistema.
TIEMPO_EPOCA = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def tiempo_inicio(semilla):
    """Timestamp base determinístico de la simulación.

    Derivado de la semilla 
    """
    return TIEMPO_EPOCA + timedelta(hours=int(semilla))


def eventos_por_tick():
    return EVENTOS_POR_TICK


def crear_sensores(rng, worker_id):
    sensores = []
    sensores.append(Reefer(f"REEFER_S3_{worker_id:02d}", rng))
    sensores.append(Faja(f"FAJA_S3_C{worker_id:02d}", rng))
    sensores.append(MotorFaja(f"MOTOR_S3_C{worker_id:02d}", rng))
    sensores.append(Bascula(f"BASCULA_S3_{worker_id:02d}", rng))
    sensores.append(GruaMovil(f"GRUA_S3_STS{worker_id:02d}", rng))
    sensores.append(SensorAmbiental(f"AMBIENTE_S3_{worker_id:02d}", rng))
    return sensores


def generar_evento(rng, wid, sensores, tiempo_actual):
    eventos = []
    for sensor in sensores:
        if isinstance(sensor, Reefer):
            valor = sensor.tick(15)
            metric = "temperature"
            unit = "C"
        elif isinstance(sensor, Faja):
            if rng.random() < 0.3:
                sensor.set_carga(rng.uniform(500, 4000))
            valor = sensor.tick(15)
            metric = "vibration"
            unit = "mm/s"
        elif isinstance(sensor, MotorFaja):
            sensor.encendido = rng.random() < 0.7
            valor = sensor.tick(15)
            metric = "amperage"
            unit = "A"
        elif isinstance(sensor, Bascula):
            if rng.random() < 0.15:
                sensor.activo = True
                valor = sensor.medir(rng.uniform(5000, 35000))
            else:
                sensor.activo = False
                valor = sensor.tick(15)
            metric = "weight"
            unit = "kg"
        elif isinstance(sensor, GruaMovil):
            sensor.en_movimiento = rng.random() < 0.5
            valor = sensor.tick(15)
            metric = "position"
            unit = "deg"
        elif isinstance(sensor, SensorAmbiental):
            datos = sensor.tick(15)
            valor = datos["temp"]
            metric = "temperature"
            unit = "C"
        else:
            continue

        eventos.append({
            "timestamp": tiempo_actual.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "sensor_id": sensor.sid,
            "metric": metric,
            "value": valor,
            "unit": unit,
            "worker_id": wid,
        })

    ambiental = next((s for s in sensores if isinstance(s, SensorAmbiental)), None)
    if ambiental:
        eventos.append({
            "timestamp": tiempo_actual.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "sensor_id": ambiental.sid,
            "metric": "humidity",
            "value": round(ambiental.humedad, 2),
            "unit": "%",
            "worker_id": wid,
        })

    return eventos

