"""
GestorSalida — Único punto de salida del Servicio de Analítica.

Administra tres canales ZMQ PUSH simultáneamente:
  1. sock_semaforos   → Control de Semáforos en PC2 (siempre)
  2. sock_bd_replica  → BD Réplica en PC2          (siempre)
  3. sock_bd_principal → BD Principal en PC3        (solo si PC3 disponible)

La lógica de failover está encapsulada aquí. RulesEngine y QueryHandler
llaman los métodos públicos sin saber a dónde van los datos realmente.

Comunicación ZMQ:
    - Patrón PUSH hacia Control de Semáforos: puerto config.semaforos_url
    - Patrón PUSH hacia BD Réplica:           puerto config.bd_replica_url
    - Patrón PUSH hacia BD Principal PC3:     puerto config.bd_principal_url
"""

import json

import zmq

from config import Config
from dtos import ComandoSemaforo, EventoSensor
from dominio import OrdenDirecta
from infrastructure.health_monitor import HealthMonitor


class GestorSalida:
    """
    Gestiona toda la comunicación saliente del Servicio de Analítica.

    No corre en su propio hilo porque PUSH es asíncrono en ZMQ:
    send() retorna inmediatamente sin esperar al receptor. Por eso
    no bloquea a quien lo llama (RulesEngine o QueryHandler).

    Si se necesitara envío paralelo intensivo, se podría convertir
    en thread, pero para el volumen del proyecto no es necesario.
    """

    def __init__(self, config: Config, health_monitor: HealthMonitor):
        self._config = config
        self._health = health_monitor
        self._contexto_zmq = zmq.Context.instance()

        # Socket hacia el Control de Semáforos (proceso local en PC2)
        self._sock_semaforos = self._contexto_zmq.socket(zmq.PUSH)
        self._sock_semaforos.connect(config.semaforos_url)

        # Socket hacia la BD Réplica (proceso local en PC2) — SIEMPRE activo
        self._sock_bd_replica = self._contexto_zmq.socket(zmq.PUSH)
        self._sock_bd_replica.connect(config.bd_replica_url)

        # Socket hacia la BD Principal (PC3) — condicional según health check
        self._sock_bd_principal = self._contexto_zmq.socket(zmq.PUSH)
        self._sock_bd_principal.connect(config.bd_principal_url)
        # SNDHWM: si PC3 está caído, limitar mensajes encolados para no consumir RAM
        self._sock_bd_principal.setsockopt(zmq.SNDHWM, 100)

        print("[GestorSalida] Conectado a semáforos, BD réplica y BD principal")

    # ------------------------------------------------------------------
    # Métodos públicos — llamados por RulesEngine y QueryHandler
    # ------------------------------------------------------------------

    def enviar_cmd(self, comando: ComandoSemaforo) -> None:
        """
        Envía un ComandoSemaforo al Control de Semáforos via PUSH.
        El servicio de control lo recibe por PULL y ejecuta el cambio.
        """
        try:
            self._sock_semaforos.send_string(comando.to_json(), zmq.NOBLOCK)
            print(f"[GestorSalida] → Semáforo: {comando}")
        except zmq.Again:
            print(f"[GestorSalida] ⚠ Cola de semáforos llena, comando descartado: {comando}")

    def persistir_evento(self, evento: EventoSensor) -> None:
        """
        Persiste el registro de un evento de sensor en las BDs.
        Siempre va a la réplica. Va a la principal solo si PC3 está disponible.
        """
        registro = {
            "tipo_registro": "EVENTO_SENSOR",
            "datos": evento.to_registro(),
        }
        self._dispatch_to_bd(registro)

    def persistir_cambio(self, calle_id: str, estado_anterior: str, estado_nuevo: str, motivo: str) -> None:
        """
        Persiste un cambio de estado de tráfico detectado por RulesEngine.
        Este registro es el que permite responder consultas históricas.
        """
        registro = {
            "tipo_registro": "CAMBIO_ESTADO",
            "datos": {
                "calle_id": calle_id,
                "estado_anterior": estado_anterior,
                "estado_nuevo": estado_nuevo,
                "motivo": motivo,
            },
        }
        self._dispatch_to_bd(registro)

    def persistir_orden(self, orden: OrdenDirecta) -> None:
        """
        Persiste una OrdenDirecta emitida por el usuario.
        Permite auditar qué órdenes se ejecutaron y cuándo.
        """
        registro = {
            "tipo_registro": "ORDEN_DIRECTA",
            "datos": orden.to_registro(),
        }
        self._dispatch_to_bd(registro)

    def cerrar(self) -> None:
        """Cierra los sockets ZMQ ordenadamente al apagar el servicio."""
        self._sock_semaforos.close()
        self._sock_bd_replica.close()
        self._sock_bd_principal.close()
        print("[GestorSalida] Sockets cerrados")

    # ------------------------------------------------------------------
    # Lógica interna de failover
    # ------------------------------------------------------------------

    def _get_bd_socket(self) -> zmq.Socket:
        """
        Retorna el socket correcto según disponibilidad de PC3.
        Este es el núcleo del fault masking: quien llama no sabe
        si los datos van a PC3 o a la réplica.
        """
        if self._health.is_pc3_disponible():
            return self._sock_bd_principal
        return self._sock_bd_replica

    def _dispatch_to_bd(self, registro: dict) -> None:
        """
        Envía el registro a la BD activa y siempre también a la réplica.
        La réplica recibe todo siempre para mantenerse sincronizada.
        """
        mensaje = json.dumps(registro)

        # Siempre escribir en la réplica
        try:
            self._sock_bd_replica.send_string(mensaje, zmq.NOBLOCK)
        except zmq.Again:
            print("[GestorSalida] ⚠ Cola BD réplica llena")

        # Escribir en principal solo si PC3 está disponible
        if self._health.is_pc3_disponible():
            try:
                self._sock_bd_principal.send_string(mensaje, zmq.NOBLOCK)
            except zmq.Again:
                print("[GestorSalida] ⚠ Cola BD principal llena — solo réplica")
