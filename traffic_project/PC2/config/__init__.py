"""
Config — Configuración centralizada del Servicio de Analítica.

Lee config.json al arrancar y expone todos los parámetros como atributos.
Ninguna clase tiene valores hardcodeados: todo viene de aquí.

Estructura esperada de config.json:
{
  "red": {
    "broker_url":         "tcp://192.168.1.10:5555",
    "semaforos_url":      "tcp://localhost:5556",
    "bd_replica_url":     "tcp://localhost:5557",
    "bd_principal_url":   "tcp://192.168.1.30:5558",
    "query_handler_url":  "tcp://*:5560",
    "pc3_health_url":     "tcp://192.168.1.30:5562"
  },
  "topicos": {
    "camara":  "camara",
    "espira":  "espira_inductiva",
    "gps":     "gps"
  },
  "health_check": {
    "intervalo_s": 5,
    "timeout_s":   2
  },
  "sensores": [
    { "sensor_id": "CAM-C5", "tipo": "camara", "interseccion": "INT_C5",
      "calle_id": "fila_C", "direccion": "fila" },
    ...
  ],
  "intersecciones": [
    { "interseccion_id": "INT_C5", "calle_fila": "fila_C", "calle_columna": "col_5" },
    ...
  ]
}
"""

import json
from pathlib import Path


class Config:

    def __init__(self, ruta: str = "config/config.json"):
        ruta_path = Path(ruta)
        if not ruta_path.exists():
            raise FileNotFoundError(f"No se encontró el archivo de configuración: {ruta}")

        with open(ruta_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        red = data["red"]
        self.broker_url = red["broker_url"]
        self.semaforos_url = red["semaforos_url"]
        self.bd_replica_url = red["bd_replica_url"]
        self.bd_principal_url = red["bd_principal_url"]
        self.query_handler_url = red["query_handler_url"]
        self.pc3_health_url = red["pc3_health_url"]

        topicos = data["topicos"]
        self.topico_camara = topicos["camara"]
        self.topico_espira = topicos["espira"]
        self.topico_gps = topicos["gps"]

        hc = data["health_check"]
        self.health_intervalo_s = int(hc["intervalo_s"])
        self.health_timeout_s = int(hc["timeout_s"])

        self.sensores = data["sensores"]
        self.intersecciones = data["intersecciones"]

    def __repr__(self) -> str:
        return (
            f"Config(broker={self.broker_url}, "
            f"sensores={len(self.sensores)}, "
            f"intersecciones={len(self.intersecciones)})"
        )
