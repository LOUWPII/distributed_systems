"""
    Servicio de monitoreo de salud y tolerancia a fallos para nodos distribuidos del sistema.
    Supervisa disponibilidad de PC3 mediante heartbeats PING/PONG sobre ZeroMQ REQ/REP.
    Ejecuta verificaciones periódicas con timeout, recreación de sockets y recuperación automática.
    Activa mecanismos de failover iniciando servicios de monitoreo de respaldo en PC2.
    Restaura operación principal automáticamente cuando el nodo remoto vuelve a estar disponible.
    Implementa estrategias de alta disponibilidad y recuperación ante fallos distribuidos.
"""

import threading
import time
import os
import subprocess
import sys

import zmq

from config import Config


class HealthMonitor(threading.Thread):
    # HealthMonitor es un hilo dedicado que supervisa la disponibilidad de PC3 mediante heartbeats periódicos.
    def __init__(self, config: Config):
        super().__init__(daemon=True, name="HealthMonitor")
        self._config = config
        self._pc3_disponible = True
        self._lock = threading.Lock()
        self._activo = True
        self._contexto_zmq = zmq.Context.instance()
        self._listeners = []
        self._failover_proc = None

    # Método para que otros componentes consulten el estado actual de disponibilidad de PC3 de forma segura.
    def is_pc3_disponible(self) -> bool:
        with self._lock:
            return self._pc3_disponible

    #  Permite registrar callbacks que serán notificados cuando cambie el estado de disponibilidad de PC3.
    def add_listener(self, callback) -> None:
        self._listeners.append(callback)

    # Método principal del hilo que ejecuta el ciclo de monitoreo y failover.
    def detener(self) -> None:
        self._activo = False

    # Crea un socket REQ para enviar heartbeats a PC3 y configurar timeouts para detectar caídas.
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

        # Al detenerse, cierra el socket y termina cualquier proceso de failover activo.
        if hasattr(self, "_socket"):
            self._socket.close()
        print("[HealthMonitor] Detenido")

    # Método que envía un heartbeat PING a PC3 y espera un PONG como respuesta. Si no llega a tiempo, asume que PC3 está caído.
    def check_health(self) -> bool:
        try:
            print(f"[HealthMonitor] -> PING {self._config.pc3_health_url}")
            self._socket.send_string("PING")
            respuesta = self._socket.recv_string()
            print(f"[HealthMonitor] <- {respuesta}")
            return respuesta == "PONG"
        except (zmq.Again, zmq.ZMQError):
            print(f"[HealthMonitor] !! Timeout/ERROR esperando PONG en {self._config.pc3_health_url}")
            self._socket.close()
            self._crear_socket()
            return False

    # Actualiza el estado de disponibilidad de PC3 y notifica a los listeners registrados. 
    # Si PC3 se vuelve no disponible, activa el monitoreo de respaldo en PC2.
    # Si PC3 se recupera, desactiva el monitoreo de respaldo.
    def _actualizar_estado(self, nuevo_estado: bool) -> None:
        with self._lock:
            estado_anterior = self._pc3_disponible
            self._pc3_disponible = nuevo_estado
            estado_nuevo = self._pc3_disponible

        # Solo si el estado cambió, se activan o desactivan los mecanismos de failover y se notifican los listeners.
        if estado_anterior != estado_nuevo:
            if not estado_nuevo:
                print("[HealthMonitor] PC3 NO DISPONIBLE. Activando failover.")
                self._activar_monitoreo_pc2()
            else:
                print("[HealthMonitor] PC3 RECUPERADO. Desactivando failover.")
                self._desactivar_monitoreo_pc2()
            self._notificar_listeners(estado_nuevo)

    # Notifica a todos los listeners registrados sobre el cambio de estado de disponibilidad de PC3, manejando cualquier error en los callbacks.
    def _notificar_listeners(self, pc3_disponible: bool) -> None:
        for callback in self._listeners:
            try:
                callback(pc3_disponible)
            except Exception as error:
                print(f"[HealthMonitor] Error notificando listener: {error}")

    # Métodos para activar y desactivar el monitoreo de respaldo en PC2, que se ejecuta como un proceso separado para evitar bloqueos en el hilo principal.
    def _activar_monitoreo_pc2(self) -> None:
        if self._failover_proc is not None and self._failover_proc.poll() is None:
            return
        script_path = os.path.join(os.path.dirname(__file__), "..", "monitoreo_consulta_failover.py")
        try:
            create_flags = getattr(subprocess, "CREATE_NEW_CONSOLE", 0)
            self._failover_proc = subprocess.Popen(
                [sys.executable, script_path],
                cwd=os.path.join(os.path.dirname(__file__), ".."),
                creationflags=create_flags,
            )
            print("[HealthMonitor] Monitoreo/consulta de PC2 levantado en nueva terminal.")
        except Exception as error:
            print(f"[HealthMonitor] Error levantando monitoreo PC2: {error}")

    # El método de desactivación de monitoreo simplemente termina el proceso de failover si está activo, sin necesidad de comunicación adicional.
    def _desactivar_monitoreo_pc2(self) -> None:
        # Si el proceso de monitoreo de respaldo está activo, se termina para liberar recursos y evitar conflictos cuando PC3 se recupere.
        if self._failover_proc is None:
            return
        # Termina el proceso de monitoreo de respaldo si está activo. No es necesario enviar una señal específica, simplemente se termina el proceso para liberar recursos.
        if self._failover_proc.poll() is None:
            self._failover_proc.terminate()
            print("[HealthMonitor] Monitoreo/consulta de PC2 detenido.")
        self._failover_proc = None
