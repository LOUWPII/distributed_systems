from abc import ABC, abstractmethod
from datetime import datetime

class EventoSensor(ABC):

    def __init__(
        self,
        sensor_id: str,
        interseccion_id: str,
        calle_id: str,
        timestamp: str,
    ):
        self.sensor_id = sensor_id
        self.interseccion_id = interseccion_id
        self.calle_id = calle_id
        self.timestamp = datetime.fromisoformat(
            timestamp.replace("Z", "+00:00")
        )

    @abstractmethod
    # Convierte el evento a un dict listo para persistir en la BD.
    def to_registro(self) -> dict:
        pass

    @classmethod
    @abstractmethod
    # Construye la instancia desde un dict deserializado de JSON.
    def from_json(cls, data: dict) -> "EventoSensor":
        pass

    @abstractmethod
    # Verifica que los valores del evento están dentro de rangos válidos.
    def validar(self) -> bool:
        pass

    # Campos comunes para todos los to_registro().
    def _campos_base(self) -> dict:
        return {
            "sensor_id": self.sensor_id,
            "interseccion_id": self.interseccion_id,
            "calle_id": self.calle_id,
            "timestamp": self.timestamp.isoformat(),
        }

    # Representación en texto del evento.
    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"sensor={self.sensor_id}, "
            f"calle={self.calle_id}, "
            f"ts={self.timestamp.strftime('%H:%M:%S')})"
        )