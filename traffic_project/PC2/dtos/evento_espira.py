from enums import TipoSensor
from .evento_sensor import EventoSensor

class EventoEspira(EventoSensor):
    """
    Evento generado por sensores tipo Espira Inductiva.
    Mide: cuántos vehículos cruzaron sobre la espira en un intervalo de tiempo.
    Tópico ZMQ: "espira"
    """

    def __init__(
        self,
        sensor_id: str,
        interseccion_id: str,
        calle_id: str,
        timestamp: str,
        vehiculos_contados: int,
        intervalo_s: int,
    ):
        super().__init__(sensor_id, interseccion_id, calle_id, timestamp)
        self.vehiculos_contados = vehiculos_contados
        self.intervalo_s = intervalo_s

    def validar(self) -> bool:
        return (
            0 <= self.vehiculos_contados <= 200
            and self.intervalo_s > 0
        )

    def to_registro(self) -> dict:
        return {
            **self._campos_base(),
            "tipo_sensor": TipoSensor.ESPIRA.value,
            "vehiculos_contados": self.vehiculos_contados,
            "intervalo_s": self.intervalo_s,
        }

    @classmethod
    def from_json(cls, data: dict) -> "EventoEspira":
        return cls(
            sensor_id = data["sensor_id"],
            interseccion_id = data["interseccion"],
            calle_id = data.get("calle_id", ""),
            timestamp = data["timestamp_inicio"],
            vehiculos_contados = int(data["vehiculos_contados"]),
            intervalo_s = int(data["intervalo_segundos"]),
        )