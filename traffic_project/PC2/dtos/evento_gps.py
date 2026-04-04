from enums import TipoSensor
from .evento_sensor import EventoSensor

class EventoGPS(EventoSensor):
    """
    Evento generado por sensores tipo GPS.
    Mide: velocidad promedio y nivel de congestión clasificado.
    Tópico ZMQ: "gps"
    Clasificación: ALTA (<10 km/h), NORMAL (10-40), BAJA (>40)
    """

    def __init__(
        self,
        sensor_id: str,
        interseccion_id: str,
        calle_id: str,
        timestamp: str,
        nivel_congestion: str,
        velocidad_promedio: float,
    ):
        super().__init__(sensor_id, interseccion_id, calle_id, timestamp)
        self.nivel_congestion   = nivel_congestion
        self.velocidad_promedio = velocidad_promedio

    # Crear un EventoGPS a partir de un JSON
    @classmethod
    def from_json(cls, data: dict) -> "EventoGPS":
        return cls(
            sensor_id = data["sensor_id"],
            interseccion_id = data.get("interseccion", ""),
            calle_id = data.get("calle_id", ""),
            timestamp = data["timestamp"],
            nivel_congestion = data["nivel_congestion"],
            velocidad_promedio = float(data["velocidad_promedio"]),
        )

    # Validar que los datos del evento sean correctos
    def validar(self) -> bool:
        return (
            self.nivel_congestion in ("ALTA", "NORMAL", "BAJA")
            and 0.0 <= self.velocidad_promedio <= 50.0
        )

    # Serializar el estado actual para persistirlo en la BD
    def to_registro(self) -> dict:
        return {
            **self._campos_base(),
            "tipo_sensor": TipoSensor.GPS.value,
            "nivel_congestion": self.nivel_congestion,
            "velocidad_promedio": self.velocidad_promedio,
        }