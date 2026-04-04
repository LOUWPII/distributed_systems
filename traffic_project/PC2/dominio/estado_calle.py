from datetime import datetime

from enums import EstadoTrafico, TipoCalle
from dtos import EventoSensor, EventoCamara, EventoEspira, EventoGPS

from .constantes import (
    COLA_CONGESTION,
    COLA_NORMAL,
    VEL_CONGESTION,
    VEL_NORMAL,
)

class EstadoCalle:
    """
    Representa el estado de tráfico actual de una calle completa (fila o columna).

    Una instancia existe por cada calle que tenga al menos un sensor.
    Sus atributos se actualizan cada vez que llega un EventoSensor de esa calle.
    El RulesEngine llama a evaluar_estado() después de cada actualización
    para decidir si el estado del tráfico cambió.

    Relaciones:
        - Agregada por Interseccion (una calle puede tener semáforos en N intersecciones)
        - Usada por RulesEngine para tomar decisiones
    """

    def __init__(self, calle_id: str, tipo: TipoCalle):
        self.calle_id            = calle_id
        self.tipo                = tipo
        self.nivel               = 0.0      # 0.0 (vacía) a 1.0 (bloqueada)
        self.velocidad_promedio  = 50.0     # km/h — arranca en velocidad libre
        self.ultima_cola         = 0        # número de vehículos en cola
        self.ultimo_conteo       = 0        # vehículos contados por espira
        self.nivel_congestion_gps = "BAJA"  # ALTA / NORMAL / BAJA
        self.estado              = EstadoTrafico.NORMAL
        self.ts_ultimo_evento    = datetime.now()

    def actualizar(self, evento: EventoSensor) -> None:
        """
        Actualiza los atributos de la calle con los datos del evento recibido.
        Cada tipo de sensor actualiza campos distintos pero todos comparten
        la misma instancia de EstadoCalle para la misma calle.
        """
        self.ts_ultimo_evento = evento.timestamp

        # Actualizar atributos según el tipo de sensor
        if isinstance(evento, EventoCamara):
            self.ultima_cola = evento.volumen
            self.velocidad_promedio = evento.velocidad_promedio

        elif isinstance(evento, EventoEspira):
            self.ultimo_conteo = evento.vehiculos_contados

        elif isinstance(evento, EventoGPS):
            self.nivel_congestion_gps = evento.nivel_congestion
            # El GPS también reporta velocidad, actualizamos si es más reciente
            self.velocidad_promedio   = evento.velocidad_promedio


    def evaluar_estado(self) -> EstadoTrafico:
        """
        Aplica las reglas de tráfico y retorna el nuevo EstadoTrafico.

        Reglas (en orden de prioridad, la primera que se cumple gana):
          CONGESTION: cola alta Y velocidad baja Y GPS reporta ALTA
          NORMAL:     cola baja Y velocidad alta Y GPS no reporta ALTA
          Si no cumple ninguna completamente → mantiene el estado actual
        """
        # Evaluar estado según las reglas
        if (
            self.ultima_cola > COLA_CONGESTION
            and self.velocidad_promedio < VEL_CONGESTION
            and self.nivel_congestion_gps == "ALTA"
        ):
            return EstadoTrafico.CONGESTION

        if (
            self.ultima_cola < COLA_NORMAL
            and self.velocidad_promedio > VEL_NORMAL
            and self.nivel_congestion_gps != "ALTA"
        ):
            return EstadoTrafico.NORMAL

        # No hay suficiente evidencia para cambiar → mantener estado actual
        return self.estado

    # Verificar si la calle esta congestionada
    def esta_congestionada(self) -> bool:
        return self.estado == EstadoTrafico.CONGESTION

    # Serializar el estado actual y persistirlo en la BD
    def to_registro(self) -> dict:
        return {
            "calle_id":             self.calle_id,
            "tipo":                 self.tipo.value,
            "nivel":                self.nivel,
            "velocidad_promedio":   self.velocidad_promedio,
            "ultima_cola":          self.ultima_cola,
            "ultimo_conteo":        self.ultimo_conteo,
            "nivel_congestion_gps": self.nivel_congestion_gps,
            "estado":               self.estado.value,
            "ts_ultimo_evento":     self.ts_ultimo_evento.isoformat(),
        }

    # Representar el estado actual de la calle
    def __repr__(self) -> str:
        return (
            f"EstadoCalle({self.calle_id} | {self.estado.value} | "
            f"cola={self.ultima_cola} | vel={self.velocidad_promedio:.1f}km/h)"
        )