from enums import TipoSensor
from .evento_sensor import EventoSensor

class EventoCamara(EventoSensor):
    """
    Evento generado por sensores tipo Cámara.
    Mide: longitud de cola (número de vehículos esperando) y velocidad promedio.
    Tópico ZMQ: "camara"
    """

    def __init__(
        self,
        sensor_id: str,
        interseccion_id: str,
        calle_id: str,
        timestamp: str,
        volumen: int,
        velocidad_promedio: float,
    ):
        super().__init__(sensor_id, interseccion_id, calle_id, timestamp)
        self.volumen = volumen
        self.velocidad_promedio = velocidad_promedio

    
    # Crear un EventoCamara a partir de un JSON
    @classmethod
    def from_json(cls, data: dict) -> "EventoCamara":
        return cls(
            sensor_id = data["sensor_id"],
            interseccion_id = data["interseccion"],
            calle_id = data.get("calle_id") or data.get("calle", ""),
            timestamp = data["timestamp"],
            volumen = int(data["volumen"]),
            velocidad_promedio = float(data["velocidad_promedio"]),
        )

    # Validar que los datos del evento sean correctos
    def validar(self) -> bool:
        return (
            0 <= self.volumen <= 100
            and 0.0 <= self.velocidad_promedio <= 50.0
        )

    # Serializar el estado actual para persistirlo en la BD
    def to_registro(self) -> dict:
        return {
            **self._campos_base(),
            "tipo_sensor": TipoSensor.CAMARA.value,
            "volumen": self.volumen,
            "velocidad_promedio": self.velocidad_promedio,
        }