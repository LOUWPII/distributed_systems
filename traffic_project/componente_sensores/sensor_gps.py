from datetime import datetime

from distributed_systems.traffic_project.componente_sensores.sensor_base import SensorBase


class SensorGPS(SensorBase):
    VF = 50  # Velocidad de flujo libre (km/h) [3]

    def generar_evento(self, nivel):
        """
        Calcula la velocidad y determina el nivel categórico de congestión.
        """
        velocidad = round((1 - nivel) * self.VF, 1)
        congestion = self._calcular_congestion(velocidad)

        return {
            "sensor_id": self.sensor_id,
            "nivel_congestion": congestion,
            "velocidad_promedio": velocidad,
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }

    def _calcular_congestion(self, velocidad):
        """Clasificación según los umbrales del enunciado"""
        if velocidad < 10:
            return 'ALTA'    # Nivel de tráfico severo (> 0.8)
        if velocidad <= 40:
            return 'NORMAL'  # Operación normal (0.2 a 0.8)
        return 'BAJA'        # Tráfico fluido (< 0.2)