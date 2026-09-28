import random


class Reefer:
    def __init__(self, sid, rng, setpoint=-18.0):
        self.sid = sid
        self.rng = rng
        self.setpoint = setpoint
        self.temp = setpoint + rng.uniform(-0.4, 0.4)
        self.puerta_abierta = False

    def tick(self, dt_s):
        if self.puerta_abierta:
            self.temp += 0.9 * (dt_s / 60.0)
        else:
            self.temp += (self.setpoint - self.temp) * 0.08
        self.temp += self.rng.gauss(0.0, 0.05)
        return round(self.temp, 2)


class Faja:
    def __init__(self, sid, rng):
        self.sid = sid
        self.rng = rng
        self.vibracion = 0.0
        self.carga = 0.0

    def set_carga(self, carga_kg):
        self.carga = max(0.0, min(carga_kg, 5000.0))

    def tick(self, dt_s):
        if self.carga > 0:
            objetivo = 0.3 + (self.carga / 5000.0) * 2.7
        else:
            objetivo = 0.0
        self.vibracion += (objetivo - self.vibracion) * 0.1
        self.vibracion += self.rng.gauss(0.0, 0.03)
        self.vibracion = max(0.0, self.vibracion)
        return round(self.vibracion, 2)


class MotorFaja:
    def __init__(self, sid, rng):
        self.sid = sid
        self.rng = rng
        self.amperaje = 2.0
        self.encendido = False

    def tick(self, dt_s):
        if self.encendido:
            objetivo = 8.0 + self.rng.gauss(0.0, 0.3)
        else:
            objetivo = 0.2
        self.amperaje += (objetivo - self.amperaje) * 0.15
        self.amperaje += self.rng.gauss(0.0, 0.05)
        self.amperaje = max(0.0, self.amperaje)
        return round(self.amperaje, 2)


class Bascula:
    def __init__(self, sid, rng):
        self.sid = sid
        self.rng = rng
        self.peso = 0.0
        self.activo = False

    def medir(self, peso_kg):
        self.peso = peso_kg + self.rng.gauss(0.0, 0.5)
        self.peso = max(0.0, self.peso)
        return round(self.peso, 2)

    def tick(self, dt_s):
        if not self.activo:
            self.peso = 0.0
        return round(self.peso, 2)


class GruaMovil:
    def __init__(self, sid, rng):
        self.sid = sid
        self.rng = rng
        self.posicion = 0.0
        self.velocidad = 0.0
        self.en_movimiento = False

    def tick(self, dt_s):
        if self.en_movimiento:
            objetivo = 45.0 + self.rng.gauss(0.0, 2.0)
            self.velocidad += (objetivo - self.velocidad) * 0.15
        else:
            self.velocidad *= 0.95
        self.velocidad += self.rng.gauss(0.0, 0.1)
        self.velocidad = max(0.0, self.velocidad)
        self.posicion += self.velocidad * (dt_s / 60.0)
        self.posicion = self.posicion % 360.0
        return round(self.posicion, 2)


class SensorAmbiental:
    def __init__(self, sid, rng):
        self.sid = sid
        self.rng = rng
        self.temp = 18.0 + rng.uniform(-2.0, 2.0)
        self.humedad = 65.0 + rng.uniform(-5.0, 5.0)

    def tick(self, dt_s):
        self.temp += self.rng.gauss(0.0, 0.1)
        self.temp = max(5.0, min(45.0, self.temp))
        self.humedad += self.rng.gauss(0.0, 0.3)
        self.humedad = max(20.0, min(100.0, self.humedad))
        return {"temp": round(self.temp, 2), "humedad": round(self.humedad, 2)}
