import threading
import time
import os
import subprocess
import sys

import zmq

from config import Config


class HealthMonitor(threading.Thread):
    """
    Monitorea salud de PC3 via heartbeat PING/PONG.
    Expone estado y notifica cambios para activar/desactivar failover.
    """

    def __init__(self, config: Config):
        super().__init__(daemon=True, name="HealthMonitor")
        self._config = config
        self._pc3_disponible = True
        self._lock = threading.Lock()
        self._activo = True
        self._contexto_zmq = zmq.Context.instance()
        self._listeners = []
        self._failover_proc = None

    def is_pc3_disponible(self) -> bool:
        with self._lock:
            return self._pc3_disponible

    def add_listener(self, callback) -> None:
        self._listeners.append(callback)

    def detener(self) -> None:
        self._activo = False

    def _crear_socket(self) -> None:
        self._socket = self._contexto_zmq.socket(zmq.REQ)
        self._socket.setsockopt(zmq.RCVTIMEO, self._config.health_timeout_s * 1000)
        self._socket.setsockopt(zmq.LINGER, 0)
        self._socket.connect(self._config.pc3_health_url)

    def run(self) -> None:
        print(f"[HealthMonitor] Iniciado. Heartbeat cada {self._config.health_intervalo_s}s")
        self._crear_socket()

        while self._activo:
            resultado = self.check_health()
            self._actualizar_estado(resultado)
            time.sleep(self._config.health_intervalo_s)

        if hasattr(self, "_socket"):
            self._socket.close()
        print("[HealthMonitor] Detenido")

    def check_health(self) -> bool:
        try:
            self._socket.send_string("PING")
            respuesta = self._socket.recv_string()
            return respuesta == "PONG"
        except (zmq.Again, zmq.ZMQError):
            self._socket.close()
            self._crear_socket()
            return False

    def _actualizar_estado(self, nuevo_estado: bool) -> None:
        with self._lock:
            estado_anterior = self._pc3_disponible
            self._pc3_disponible = nuevo_estado
            estado_nuevo = self._pc3_disponible

        if estado_anterior != estado_nuevo:
            if not estado_nuevo:
                print("[HealthMonitor] PC3 NO DISPONIBLE. Activando failover.")
                self._activar_monitoreo_pc2()
            else:
                print("[HealthMonitor] PC3 RECUPERADO. Desactivando failover.")
                self._desactivar_monitoreo_pc2()
            self._notificar_listeners(estado_nuevo)

    def _notificar_listeners(self, pc3_disponible: bool) -> None:
        for callback in self._listeners:
            try:
                callback(pc3_disponible)
            except Exception as error:
                print(f"[HealthMonitor] Error notificando listener: {error}")

    def _activar_monitoreo_pc2(self) -> None:
        if self._failover_proc is not None and self._failover_proc.poll() is None:
            return
        script_path = os.path.join(os.path.dirname(__file__), "..", "monitoreo_consulta_failover.py")
        try:
            self._failover_proc = subprocess.Popen([sys.executable, script_path], cwd=os.path.join(os.path.dirname(__file__), ".."))
            print("[HealthMonitor] Monitoreo/consulta de PC2 levantado.")
        except Exception as error:
            print(f"[HealthMonitor] Error levantando monitoreo PC2: {error}")

    def _desactivar_monitoreo_pc2(self) -> None:
        if self._failover_proc is None:
            return
        if self._failover_proc.poll() is None:
            self._failover_proc.terminate()
            print("[HealthMonitor] Monitoreo/consulta de PC2 detenido.")
        self._failover_proc = None
