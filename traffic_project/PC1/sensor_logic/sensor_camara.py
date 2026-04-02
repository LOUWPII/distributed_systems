from datetime import datetime, timezone

from distributed_systems.traffic_project.PC1.sensor_logic.sensor_base import SensorBase


class SensorCamara(SensorBase):
    # Parámetros configurables de la cámara
    Q_COLA_MAX = 20  # Máximo de vehículos en cola visibles
    VF = 50          # Velocidad de flujo libre (km/h)

    def generar_evento(self, nivel):
        """
        Calcula la cola y la velocidad promedio.
        Cola: crece con el nivel.
        Velocidad: caesegún Greenshields.
        """
        cola = int(nivel * self.Q_COLA_MAX)
        velocidad = round((1 - nivel) * self.VF, 1)

        return {
            "sensor_id": self.sensor_id,
            "tipo_sensor": "camara",
            "interseccion": self.interseccion,
            "volumen": cola,  # Número de vehículos en espera
            "velocidad_promedio": velocidad,
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S") + "Z"
        }