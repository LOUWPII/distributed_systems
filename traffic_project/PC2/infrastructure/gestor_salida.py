import json
import queue
import threading
import zmq

from config import Config
from dtos import ComandoSemaforo, EventoSensor
from dominio import OrdenDirecta
from infrastructure.health_monitor import HealthMonitor

# Gestiona toda la comunicación saliente del Servicio de Analítica.
# Delega sus responsabilidades a tres hilos Workers dedicados (Semáforos, BD Réplica, BD Principal)
# de forma totalmente paralela y asíncrona para que un bloqueo en uno de los destinos
# no interrumpa a los demás envíos ni al RulesEngine principal.
class GestorSalida:

    def __init__(self, config: Config, health_monitor: HealthMonitor):
        self._config = config
        self._health = health_monitor
        self._contexto_zmq = zmq.Context.instance()
        self._activo = True
        self.name = "GestorSalida" # Utilizado en el print de main.py
        
        # 3 Colas independientes en memoria para los distintos receptores
        self._queue_semaforos = queue.Queue(maxsize=0)
        self._queue_replica = queue.Queue(maxsize=0)
        self._queue_principal = queue.Queue(maxsize=0)

        # Configuración de Sockets...
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

        # Creación de los hilos dedicados para cada receptor ZMQ
        self._worker_semaforos = threading.Thread(target=self._loop_semaforos, daemon=True)
        self._worker_replica = threading.Thread(target=self._loop_replica, daemon=True)
        self._worker_principal = threading.Thread(target=self._loop_principal, daemon=True)

        print("[GestorSalida] Conectado a semáforos, BD réplica y BD principal.")

    # Implementa un ".start()" manual emulando el Thread para no romper main.py
    def start(self) -> None:
        print("[GestorSalida] Arrancando workers en hilos paralelos...")
        self._worker_semaforos.start()
        self._worker_replica.start()
        self._worker_principal.start()

    # --- HILOS WORKERS ---
    
    def _loop_semaforos(self):
        while self._activo:
            try:
                msj = self._queue_semaforos.get(timeout=1.0)
                self._sock_semaforos.send_string(msj) # Envío bloqueante sólo para su red
            except queue.Empty:
                pass
            except zmq.ZMQError as e:
                if self._activo: print(f"[GestorSalida - Semáforos] Error de red: {e}")

    def _loop_replica(self):
        while self._activo:
            try:
                msj = self._queue_replica.get(timeout=1.0)
                self._sock_bd_replica.send_string(msj) # Envío bloqueante sólo para su red
            except queue.Empty:
                pass
            except zmq.ZMQError as e:
                if self._activo: print(f"[GestorSalida - Réplica] Error de red: {e}")

    def _loop_principal(self):
        while self._activo:
            try:
                msj = self._queue_principal.get(timeout=1.0)
                # Verifica el estado final antes de intentar enviarlo sobre una conexión fallida o bloqueada
                if self._health.is_pc3_disponible():
                    self._sock_bd_principal.send_string(msj)
            except queue.Empty:
                pass
            except zmq.ZMQError as e:
                if self._activo: print(f"[GestorSalida - Principal] Error de red: {e}")


    def enviar_cmd(self, comando: ComandoSemaforo) -> None:
        # 1. Encola asíncronamente al hilo que rige a los semáforos locales
        cmd_json = comando.to_json()
        self._queue_semaforos.put(cmd_json)

        # 2. Persistirlo asíncronamente en BD (replica y principal)
        comando_dic = json.loads(cmd_json)
        self._dispatch_to_bd({"tipo_registro": "semaforo", "datos": comando_dic})

    def persistir_evento(self, evento: EventoSensor) -> None:
        self._dispatch_to_bd({"tipo_registro": "evento", "datos": evento.to_registro()})

    def persistir_cambio(self, calle_id: str, estado_anterior: str, estado_nuevo: str, motivo: str) -> None:
        self._dispatch_to_bd({
            "tipo_registro": "congestion",
            "datos": {
                "calle_id": calle_id, "estado_anterior": estado_anterior,
                "estado_nuevo": estado_nuevo, "motivo": motivo
            }
        })
        
    def persistir_orden(self, orden: OrdenDirecta) -> None:
        self._dispatch_to_bd({"tipo_registro": "priorizacion", "datos": orden.to_registro()})



    def _dispatch_to_bd(self, registro: dict) -> None:
        mensaje = json.dumps(registro)
        # Se envía la petición hacia cada hilo gestor correspondiente.
        self._queue_replica.put(mensaje)

        # Al encolar ya filtramos si la principal está caída (ahorro de memoria)
        if self._health.is_pc3_disponible():
            self._queue_principal.put(mensaje)

    def detener(self) -> None:
        self._activo = False
        self.cerrar()

    def cerrar(self) -> None:
        self._sock_semaforos.close()
        self._sock_bd_replica.close()
        self._sock_bd_principal.close()
        print("[GestorSalida] Sockets cerrados")