import json

import zmq

from config import Config
from dtos import ComandoSemaforo, EventoSensor
from dominio import OrdenDirecta
from infrastructure.health_monitor import HealthMonitor


# Gestiona toda la comunicación saliente del Servicio de Analítica.
# No corre en su propio hilo porque PUSH es asíncrono en ZMQ
class GestorSalida:

    def __init__(self, config: Config, health_monitor: HealthMonitor):
        self._config = config
        self._health = health_monitor
        self._contexto_zmq = zmq.Context.instance()

        # Socket hacia el Control de Semáforos (Proceso local en PC2)
        self._sock_semaforos = self._contexto_zmq.socket(zmq.PUSH)
        self._sock_semaforos.connect(config.semaforos_url)

        # Socket hacia la BD Réplica (Proceso local en PC2)
        self._sock_bd_replica = self._contexto_zmq.socket(zmq.PUSH)
        self._sock_bd_replica.connect(config.bd_replica_url)

        # Socket hacia la BD Principal (Externo en PC3)
        self._sock_bd_principal = self._contexto_zmq.socket(zmq.PUSH)
        self._sock_bd_principal.connect(config.bd_principal_url)

        # Si PC3 está caído, limitar mensajes encolados para no consumir demasiada RAM
        self._sock_bd_principal.setsockopt(zmq.SNDHWM, 100)

        print("[GestorSalida] Conectado a semáforos, BD réplica y BD principal")

    # Enviar comandos a los semáforos mediante PUSH
    def enviar_cmd(self, comando: ComandoSemaforo) -> None:
        try:
            # Enviar el comando al Control de Semáforos
            self._sock_semaforos.send_string(comando.to_json(), zmq.NOBLOCK)
            print(f"[GestorSalida] → Semáforo: {comando}")
            
            # Persistir el comando enviado en las BDs
            self._dispatch_to_bd({
                "tipo_registro": "semaforo",
                "datos": json.loads(comando.to_json())
            })
        
        # Si la cola está llena, descartar el comando
        except zmq.Again:
            print(f"[GestorSalida] ⚠ Cola de semáforos llena, comando descartado: {comando}")

    # Persistir eventos de sensores en las BDs
    def persistir_evento(self, evento: EventoSensor) -> None:
        registro = {
            "tipo_registro": "evento",
            "datos": evento.to_registro(),
        }
        self._dispatch_to_bd(registro)

    # Persistir cambios de estado de tráfico detectados por RulesEngine
    def persistir_cambio(self, calle_id: str, estado_anterior: str, estado_nuevo: str, motivo: str) -> None:
        registro = {
            "tipo_registro": "congestion",
            "datos": {
                "calle_id": calle_id,
                "estado_anterior": estado_anterior,
                "estado_nuevo": estado_nuevo,
                "motivo": motivo,
            },
        }
        self._dispatch_to_bd(registro)

    # Persistir órdenes directas emitidas por el usuario
    def persistir_orden(self, orden: OrdenDirecta) -> None:
        registro = {
            "tipo_registro": "priorizacion",
            "datos": orden.to_registro(),
        }
        self._dispatch_to_bd(registro)

    # Cierra los sockets ZMQ ordenadamente al apagar el servicio
    def cerrar(self) -> None:
        self._sock_semaforos.close()
        self._sock_bd_replica.close()
        self._sock_bd_principal.close()
        print("[GestorSalida] Sockets cerrados")

    # Retorna el socket correcto según disponibilidad de PC3
    def _get_bd_socket(self) -> zmq.Socket:
        # Si PC3 está disponible, retornar el socket de la BD principal
        if self._health.is_pc3_disponible():
            return self._sock_bd_principal
        return self._sock_bd_replica

    # Envía el registro a la BD activa y siempre también a la réplica
    def _dispatch_to_bd(self, registro: dict) -> None:
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
