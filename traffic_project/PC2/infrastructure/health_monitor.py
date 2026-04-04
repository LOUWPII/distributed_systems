import threading
import time

import zmq

from config import Config

# Monitorea la salud del PC3 mediante health checks periódicos
# Utiliza lock para evirtar un Race Condition
class HealthMonitor(threading.Thread):

    def __init__(self, config: Config):
        super().__init__(daemon=True, name="HealthMonitor")
        self._config = config
        self._pc3_disponible = True
        self._lock = threading.Lock()
        self._activo = True
        self._contexto_zmq = zmq.Context.instance()

    # Interfaz pública — llamada desde GestorSalida (hilo distinto)
    def is_pc3_disponible(self) -> bool:
        # El lock garantiza que se lee el valor más reciente.
        with self._lock:
            return self._pc3_disponible

    # Señala al hilo que debe terminar su ciclo.
    def detener(self) -> None:
        self._activo = False

    # Inicializa o recrea el socket REQ conectado a PC3.
    def _crear_socket(self) -> None:
        self._socket = self._contexto_zmq.socket(zmq.REQ)
        self._socket.setsockopt(zmq.RCVTIMEO, self._config.health_timeout_s * 1000)
        self._socket.setsockopt(zmq.LINGER, 0)
        self._socket.connect(self._config.pc3_health_url)

    # Ciclo principal del hilo. Se ejecuta hasta que detener() sea llamado.
    # En cada iteración: envía PING, esperando un PONG del PC3
    def run(self) -> None:
        print(f"[HealthMonitor] Iniciado — verificando PC3 cada {self._config.health_intervalo_s}s")
        
        self._crear_socket()

        # Mientras el monitor esté activo, verifica la salud del PC3
        while self._activo:
            resultado = self.check_health()
            self._actualizar_estado(resultado)
            time.sleep(self._config.health_intervalo_s)

        # Cierra el socket cuando el monitor se detiene
        if hasattr(self, '_socket'):
            self._socket.close()
        print("[HealthMonitor] Detenido")

    # Verifica la salud del PC3
    def check_health(self) -> bool:
        try:
            # Envía un PING al PC3
            self._socket.send_string("PING")
            # Espera un PONG del PC3
            respuesta = self._socket.recv_string()
            # Si la respuesta es PONG, el PC3 está disponible
            return respuesta == "PONG"
        except zmq.Again:
            # Timeout: PC3 no respondió. (Patrón Lazy Pirate: destruir y recrear)
            self._socket.close()
            self._crear_socket()
            return False
        # Si hay error ZMQ, cerrar y recrear el socket
        except zmq.ZMQError:
            self._socket.close()
            self._crear_socket()
            return False

    # Actualiza pc3_disponible con el resultado del check
    def _actualizar_estado(self, nuevo_estado: bool) -> None:
        # Usa lock para evitar Race conditions
        with self._lock:
            estado_anterior = self._pc3_disponible
            self._pc3_disponible = nuevo_estado

        # Imprime mensajes solo cuando el estado cambia
        if estado_anterior and not nuevo_estado:
            print(
                "[HealthMonitor] ⚠ PC3 NO DISPONIBLE — "
                "redirigiendo escrituras a BD Réplica"
            )
        # Si el PC3 se recupera, imprime un mensaje
        elif not estado_anterior and nuevo_estado:
            print(
                "[HealthMonitor] ✓ PC3 RECUPERADO — "
                "reanudando escrituras en BD Principal"
            )
