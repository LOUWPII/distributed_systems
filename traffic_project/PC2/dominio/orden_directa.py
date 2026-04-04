from datetime import datetime, timedelta

from enums import EstadoTrafico

class OrdenDirecta:
    """
    Instrucción emitida por un usuario desde el Servicio de Monitoreo (PC3).
    Permite forzar un estado en una calle específica, ignorando las reglas
    automáticas mientras la orden esté activa.

    Caso: Paso de ambulancia → ola verde en fila_C por 60 segundos.
    """

    def __init__(
        self,
        calle_id: str,
        accion: EstadoTrafico,
        duracion_s: int,
        motivo: str,
    ):
        self.calle_id = calle_id
        self.accion = accion
        self.duracion_s = duracion_s
        self.motivo = motivo
        self.ts_inicio = datetime.now()
        self.ts_expiracion = self.ts_inicio + timedelta(seconds=duracion_s)

    # Retorna True si la orden todavía no ha expirado
    def esta_activa(self) -> bool:
        return datetime.now() < self.ts_expiracion

    # Retorna True si la orden ya expiró
    def esta_expirada(self) -> bool:
        return not self.esta_activa()

    # Serializar el estado actual para persistirlo en la BD
    def to_registro(self) -> dict:
        return {
            "calle_id": self.calle_id,
            "accion": self.accion.value,
            "duracion_s": self.duracion_s,
            "motivo": self.motivo,
            "ts_inicio": self.ts_inicio.isoformat(),
            "ts_expiracion": self.ts_expiracion.isoformat(),
        }

    # Representar el estado actual de la orden
    def __repr__(self) -> str:
        restante = (self.ts_expiracion - datetime.now()).total_seconds()
        return (
            f"OrdenDirecta({self.calle_id} | {self.accion.value} | "
            f"{self.motivo} | {max(0, restante):.0f}s restantes)"
        )
