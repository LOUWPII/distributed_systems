from datetime import datetime, timedelta

from distributed_systems.traffic_project.componente_sensores.sensor_base import SensorBase


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
        ahora = datetime.utcnow()
        inicio = ahora - timedelta(seconds=30)

        # Estructura exacta del JSON solicitada
        return {
            "sensor_id": self.sensor_id,
            "tipo_sensor": "espira_inductiva",
            "interseccion": self.interseccion,
            "vehiculos_contados": vehiculos,
            "intervalo_segundos": 30,
            "timestamp_inicio": inicio.isoformat() + "Z",
            "timestamp_fin": ahora.isoformat() + "Z"
        }