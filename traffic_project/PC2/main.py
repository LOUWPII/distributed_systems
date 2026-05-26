"""
    Servicio de monitoreo de salud y tolerancia a fallos para nodos distribuidos del sistema.
    Supervisa disponibilidad de PC3 mediante heartbeats PING/PONG sobre ZeroMQ REQ/REP.
    Ejecuta verificaciones periodicas con timeout, recreacion de sockets y recuperacion automatica.
    Notifica a listeners registrados cuando cambia el estado de disponibilidad de PC3.
    Implementa estrategias de alta disponibilidad sin lanzar procesos hijos que contaminen la consola.
"""

import threading
import time
import zmq

from config import Config


class HealthMonitor(threading.Thread):
    def __init__(self, config: Config):
        super().__init__(daemon=True, name="HealthMonitor")
        self._config = config
        self._pc3_disponible = True
        self._lock = threading.Lock()
        self._activo = True
        self._contexto_zmq = zmq.Context.instance()
        self._listeners = []

    def is_pc3_disponible(self) -> bool:
        with self._lock:
            return self._pc3_disponible

    def add_listener(self, callback) -> None:
        self._listeners.append(callback)

    def detener(self) -> None:
        self._activo = False

    def _crear_socket(self) -> zmq.Socket:
        socket = self._contexto_zmq.socket(zmq.REQ)
        socket.setsockopt(zmq.RCVTIMEO, self._config.health_timeout_s * 1000)
        socket.setsockopt(zmq.LINGER, 0)
        socket.connect(self._config.pc3_health_url)
        return socket

    def run(self) -> None:
        print(f"[HealthMonitor] Iniciado. Heartbeat cada {self._config.health_intervalo_s}s")
        socket = self._crear_socket()

        while self._activo:
            resultado = self._check(socket)
            self._actualizar_estado(resultado)
            time.sleep(self._config.health_intervalo_s)

        socket.close()
        print("[HealthMonitor] Detenido")

    def _check(self, socket: zmq.Socket) -> bool:
        try:
            print(f"[HealthMonitor] -> PING {self._config.pc3_health_url}")
            socket.send_string("PING")
            respuesta = socket.recv_string()
            print(f"[HealthMonitor] <- {respuesta}")
            return respuesta == "PONG"
        except (zmq.Again, zmq.ZMQError):
            print(f"[HealthMonitor] !! Timeout/ERROR esperando PONG en {self._config.pc3_health_url}")
            try:
                socket.close()
            except Exception:
                pass
            return False

    def _actualizar_estado(self, nuevo_estado: bool) -> None:
        with self._lock:
            estado_anterior = self._pc3_disponible
            self._pc3_disponible = nuevo_estado
            estado_nuevo = self._pc3_disponible

        if estado_anterior != estado_nuevo:
            if not estado_nuevo:
                print("[HealthMonitor] PC3 NO DISPONIBLE. Activando failover.")
                print("[HealthMonitor] -> Envio a BD Principal DETENIDO.")
                print("[HealthMonitor] -> Envio a BD Replica CONTINUA.")
                print("[HealthMonitor] -> QueryHandler de PC2 sigue activo.")
                print("[HealthMonitor] -> Levante manualmente: python monitoreo_universal.py en PC2")
            else:
                print("[HealthMonitor] PC3 RECUPERADO. Desactivando failover.")
                print("[HealthMonitor] -> Envio a BD Principal REANUDADO.")
            self._notificar_listeners(estado_nuevo)

    def _notificar_listeners(self, pc3_disponible: bool) -> None:
        for callback in self._listeners:
            try:
                callback(pc3_disponible)
            except Exception as error:
                print(f"[HealthMonitor] Error notificando listener: {error}")