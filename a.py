import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
import random, argparse, multiprocessing as mp
import os

class reefer: 
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
    

    def generar_evento(rng, wid, sensor, tiempo):
        valor_temp = sensor.tick(15)
        evento = {
            "timestamp": tiempo_actual.strftime("%Y-%m-%dT%H:%M:%S.%F")[:-3] + "Z",
            "sensor_id": sensor.sid,
            "metric": "temperature",
            "value": valor_temp,
            "unit": "ºC",
            "worker_id": wid
        }
        return evento
    
    def worker_shard(wid, n_eventos, carpeta, semilla):
        rng = random.Random(semilla + wid)
        ruta = Path(carpeta) / f"part-{wid:03d}.jsonl"
        sensor = reefer(f"REEFER_S3_(wid:02d)", rng)
        tiempo_actual = datetime.now(timezone.utc)
        escritos = 0 
        buf = []

        with open(ruta, "w", encoding = "utf-8", newline = "\n", buffering = 4*1024*1024) as f:
            for _ in rango(n_eventos):
                ev = generar_evento(rng, wid,sensor,tiempo_actual)
                tiempo_actual += timedelta(seconds=15)
                buf.apped(json.dump(ev, separators=(',',':'))+ "\n")
                
                if len(buf) == 10000:
                    f.writelines(buf)
                    escritos += len(buf)
                    buf = []

            if buf:
                f.writelines(buf)
                escritos += len(buf)

        return {
            "worker_id": id,
            "archivo": ruta.name,
            "eventos": escritos,
            "bytes": ruta.stat().st_size
        }
    
    if __name__ == "__main__":
        parser = argparse.ArgumentParser(description="simulador tpc sitio 3")
        parser.add_argument("--workers--", type=int, default=4, help="numero de precesos")
        parser.add_argument("--total-events", type=int, default=1000000, help="total de eventos")
        parser.add_argument("--output-dir", type=str, default="data/raw", help="directorio")
        parser.add_argument("--seed", type=int, default=70, help="semilla de aleatoriedad")

        args = parser.parse_args(args=["--workers", "4", "--total-events", "1000000",])
        os.makedirs(args.output_dir, exist_ok=True)
        eventos_por_worker = args.total_events // args.workers

        print(f"iniciando simulacion con { args.workers} workers")

        resultados = []
        with mp.Pool(processes=args.workers) as pool:
            tareas = [
                pool.apply_async(worker_shard, (i, eventos_por_worker, args.output_dir, args.seed))
                for i in range(args.workers) 
            ]
            for tarea in tareas:
                resultados.append(tarea.get())
        
        total_eventos = sum(r["eventos"] for r in resultados)
        total_bytes = sum(r["bytes"] for r in resultados)

        manifesto = {
            "total_eventos": total_eventos,
            "total_bytes": total_bytes,
            "arquitectura_worker":"shards",
            "worker": args.workers,
            "seed": args.seed,
            "detalles": resultados
        }

        with open(Path(args.output_dir) / "manifesto.json", "w") as f:
            json.dump(manifesto, f, indent=4)

        print ("simulacion completada")

