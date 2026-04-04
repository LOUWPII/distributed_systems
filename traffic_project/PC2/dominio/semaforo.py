from datetime import datetime

from enums import EstadoSemaforo
from dtos import ComandoSemaforo

from .constantes import DURACION_NORMAL_S

class Semaforo:
    """
    Representa un semáforo físico ubicado en una intersección específica.

    Cada semáforo controla el paso de una sola calle en esa intersección.
    Por eso tiene tanto calle_id (qué calle controla) como interseccion_id
    (en qué cruce está ubicado físicamente).

    La clase Interseccion es la responsable de garantizar la exclusión mutua
    (nunca dos semáforos del mismo cruce en verde al mismo tiempo).
    """

    def __init__(
        self,
        semaforo_id: str,
        calle_id: str,
        interseccion_id: str,
        estado_inicial: EstadoSemaforo = EstadoSemaforo.ROJO,
    ):
        self.semaforo_id = semaforo_id
        self.calle_id = calle_id
        self.interseccion_id = interseccion_id
        self.estado = estado_inicial
        self.duracion_actual_s = DURACION_NORMAL_S
        self.ts_ultimo_cambio = datetime.now()

    # Cambia el estado del semáforo y registra el momento del cambio.
    def cambiar(self, nuevo_estado: EstadoSemaforo, duracion_s: int) -> None:
        self.estado = nuevo_estado
        self.duracion_actual_s = duracion_s
        self.ts_ultimo_cambio = datetime.now()
    
    # Crea el ComandoSemaforo que GestorSalida enviará al Control de Semáforos.
    def to_comando(self, motivo: str) -> ComandoSemaforo:
        return ComandoSemaforo(
            semaforo_id = self.semaforo_id,
            interseccion_id = self.interseccion_id,
            calle_id = self.calle_id,
            nuevo_estado = self.estado,
            duracion_s = self.duracion_actual_s,
            motivo = motivo,
        )

    # Estima cuanto tiempo falta para que cambie el estado actual.
    def tiempo_restante_s(self) -> int:
        transcurrido = (datetime.now() - self.ts_ultimo_cambio).total_seconds()
        restante = self.duracion_actual_s - transcurrido
        return max(0, int(restante))

    # Representación en string del semáforo.
    def __repr__(self) -> str:
        return (
            f"Semaforo({self.semaforo_id} | {self.estado.value} | "
            f"{self.calle_id} en {self.interseccion_id})"
        )