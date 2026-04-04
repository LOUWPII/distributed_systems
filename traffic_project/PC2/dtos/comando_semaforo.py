import json
from dataclasses import dataclass, field
from datetime import datetime

from enums import EstadoSemaforo

@dataclass
class ComandoSemaforo:
    """
    DTO de salida: orden que el GestorSalida envía al Control de Semáforos (PC2).
    Se serializa a JSON para viajar por el socket PUSH.
    """
    semaforo_id: str
    interseccion_id: str
    calle_id: str
    nuevo_estado: EstadoSemaforo
    duracion_s: int
    motivo: str
    timestamp: datetime = field(default_factory=datetime.now)

    def to_json(self) -> str:
        return json.dumps({
            "semaforo_id": self.semaforo_id,
            "interseccion_id": self.interseccion_id,
            "calle_id": self.calle_id,
            "nuevo_estado": self.nuevo_estado.value,
            "duracion_s": self.duracion_s,
            "motivo": self.motivo,
            "timestamp": self.timestamp.isoformat(),
        })

    # Crear un ComandoSemaforo a partir de un JSON
    @classmethod
    def from_json(cls, raw: str) -> "ComandoSemaforo":
        data = json.loads(raw)
        return cls(
            semaforo_id = data["semaforo_id"],
            interseccion_id = data["interseccion_id"],
            calle_id = data["calle_id"],
            nuevo_estado = EstadoSemaforo(data["nuevo_estado"]),
            duracion_s = data["duracion_s"],
            motivo = data["motivo"],
            timestamp     = datetime.fromisoformat(data["timestamp"]),
        )

    # Representación en string del ComandoSemaforo
    def __repr__(self) -> str:
        return (
            f"ComandoSemaforo("
            f"{self.semaforo_id} → {self.nuevo_estado.value} "
            f"por {self.duracion_s}s | {self.motivo})"
        )
