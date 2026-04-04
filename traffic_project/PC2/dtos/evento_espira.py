from enums import TipoSensor
from .evento_sensor import EventoSensor

class EventoEspira(EventoSensor):
    """
    Evento generado por sensores tipo Espira Inductiva.
    Mide: cuántos vehículos cruzaron sobre la espira en un intervalo de tiempo.
    Tópico ZMQ: "espira"
    """

    def __init__(self, sensor_id, interseccion_id, calle_id, timestamp, vehiculos_contados, intervalo_s, timestamp_fin=None):
        # Llamamos al padre con el timestamp de INICIO
        super().__init__(sensor_id, interseccion_id, calle_id, timestamp) 
        self.vehiculos_contados = vehiculos_contados
        self.intervalo_s = intervalo_s
        self.timestamp_fin = timestamp_fin # Guardamos el fin por separado

    @classmethod
    def from_json(cls, data: dict) -> "EventoEspira":
        return cls(
            sensor_id = data["sensor_id"],
            interseccion_id = data["interseccion"],
            calle_id = data.get("calle_id") or data.get("calle", ""),
            timestamp = data["timestamp_inicio"], # <--- ESTO ES VITAL PARA EL PADRE
            vehiculos_contados = int(data["vehiculos_contados"]),
            intervalo_s = int(data["intervalo_segundos"]),
            timestamp_fin = data.get("timestamp_fin") # <--- EXTRA
        )

    def to_registro(self) -> dict:
        return {
            **self._campos_base(), # Aquí ya irá el timestamp de inicio
            "tipo_sensor": "espira_inductiva",
            "vehiculos_contados": self.vehiculos_contados,
            "intervalo_s": self.intervalo_s,
            "timestamp_fin": self.timestamp_fin # Añadimos el fin al log de la DB
        }

    # Validar que los datos del evento sean correctos
    def validar(self) -> bool:
        return (
            0 <= self.vehiculos_contados <= 200
            and self.intervalo_s > 0
        )