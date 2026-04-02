from typing import Optional

from enums import EstadoSemaforo

from .semaforo import Semaforo
from .constantes import DURACION_NORMAL_S

class Interseccion:
    """
    Representa el cruce físico entre una fila y una columna de la ciudad.

    Responsabilidad principal: garantizar la EXCLUSIÓN MUTUA.
    Nunca puede tener semaforo_fila y semaforo_columna en VERDE al mismo tiempo.
    Esta regla se aplica en set_verde_fila() y set_verde_columna(), que son los
    únicos puntos donde los semáforos cambian de estado.

    RulesEngine nunca llama directamente a semaforo.cambiar(). Siempre pasa
    por los métodos de Interseccion para que la exclusión mutua esté garantizada.

    Relaciones:
        - Compone dos Semaforo (semaforo_fila y semaforo_columna)
        - Compuesta por RulesEngine (el motor mantiene dict de intersecciones)
    """

    def __init__(
        self,
        interseccion_id: str,
        semaforo_fila: Semaforo,
        semaforo_columna: Semaforo,
    ):
        self.interseccion_id = interseccion_id
        self.semaforo_fila = semaforo_fila
        self.semaforo_columna = semaforo_columna

    def set_verde_fila(self, duracion_s: int = DURACION_NORMAL_S) -> None:
        """
        Pone en VERDE el semáforo de la fila y en ROJO el de la columna.
        Garantiza exclusión mutua.
        """
        self.semaforo_fila.cambiar(EstadoSemaforo.VERDE, duracion_s)
        self.semaforo_columna.cambiar(EstadoSemaforo.ROJO, duracion_s)

    def set_verde_columna(self, duracion_s: int = DURACION_NORMAL_S) -> None:
        """
        Pone en VERDE el semáforo de la columna y en ROJO el de la fila.
        Garantiza exclusión mutua.
        """
        self.semaforo_columna.cambiar(EstadoSemaforo.VERDE, duracion_s)
        self.semaforo_fila.cambiar(EstadoSemaforo.ROJO, duracion_s)

    def get_semaforo(self, calle_id: str) -> Optional[Semaforo]:
        """
        Retorna el semáforo que controla la calle indicada, o None si esta
        intersección no tiene semáforo para esa calle.
        Usado por RulesEngine.aplicar_ola_verde() para iterar intersecciones.
        """
        if self.semaforo_fila.calle_id == calle_id:
            return self.semaforo_fila
        if self.semaforo_columna.calle_id == calle_id:
            return self.semaforo_columna
        return None

    def get_semaforo_cruzado(self, calle_id: str) -> Optional[Semaforo]:
        """
        Retorna el semáforo de la calle opuesta al calle_id dado.
        Útil para saber qué semáforo queda en rojo cuando uno se pone en verde.
        """
        if self.semaforo_fila.calle_id == calle_id:
            return self.semaforo_columna
        if self.semaforo_columna.calle_id == calle_id:
            return self.semaforo_fila
        return None

    def hay_conflicto(self) -> bool:
        """
        Verifica si ambos semáforos están en VERDE al mismo tiempo.
        No debería ocurrir nunca si se usan set_verde_fila/columna correctamente.
        Útil para pruebas y logging defensivo.
        """
        return (
            self.semaforo_fila.estado == EstadoSemaforo.VERDE
            and self.semaforo_columna.estado == EstadoSemaforo.VERDE
        )

    def to_registro(self) -> dict:
        return {
            "interseccion_id": self.interseccion_id,
            "semaforo_fila": self.semaforo_fila.semaforo_id,
            "estado_fila": self.semaforo_fila.estado.value,
            "semaforo_columna": self.semaforo_columna.semaforo_id,
            "estado_columna": self.semaforo_columna.estado.value,
        }

    def __repr__(self) -> str:
        return (
            f"Interseccion({self.interseccion_id} | "
            f"fila={self.semaforo_fila.estado.value} | "
            f"col={self.semaforo_columna.estado.value})"
        )
