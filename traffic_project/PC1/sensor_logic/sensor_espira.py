from datetime import datetime, timedelta, timezone

from sensor_logic.sensor_base import SensorBase

class SensorEspira(SensorBase):
    # Capacidad máxima de la intersección en 30 segundos
    Q_MAX = 40

    def generar_evento(self, nivel):
        """
        Convierte el nivel de densidad en flujo vehicular (Q)
        usando la fórmula: Q = nivel * (1 - nivel) * Q_MAX
        """
        # Cálculo físico: el flujo es máximo cuando el nivel es 0.5
        vehiculos = int(nivel * (1 - nivel) * self.Q_MAX)

        # Tiempos requeridos por el enunciado para el reporte de 30s
        ahora = datetime.now(timezone.utc)
        inicio = ahora - timedelta(seconds=30)

        # Estructura del JSON
        return {
            "sensor_id": self.sensor_id,
            "tipo_sensor": "espira_inductiva",
            "interseccion": self.interseccion,
            "calle_id": self.config.get('calle_id', ''), 
            "vehiculos_contados": vehiculos,
            "intervalo_segundos": 30,
            "timestamp_inicio": inicio.strftime("%Y-%m-%dT%H:%M:%S") + "Z",
            "timestamp_fin": ahora.strftime("%Y-%m-%dT%H:%M:%S") + "Z"
        }