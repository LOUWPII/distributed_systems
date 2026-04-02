"""
HealthMonitor — Detector de fallas del PC3.

Corre en su propio hilo y verifica periódicamente si PC3 responde.
Cuando detecta una falla actualiza pc3_disponible = False, lo que hace
que GestorSalida deje de intentar escribir en la BD Principal.

Patrón de tolerancia a fallos: Health Check con failover automático.

Comunicación ZMQ:
    - Abre un socket REQ hacia PC3 en cada ciclo de verificación
    - Envía PING, espera PONG con timeout configurado
    - Si no responde → marca PC3 como no disponible
"""

import threading
import time

import zmq

from config import Config


class HealthMonitor(threading.Thread):
    """
    Hilo de monitoreo de salud del PC3.

    El atributo _lock protege pc3_disponible porque es leído por GestorSalida
    (otro hilo) mientras este hilo lo escribe. Sin el lock habría condición de
    carrera aunque bool sea un tipo simple en Python.

    Hereda de threading.Thread para poder hacer self.start() directamente.
    El flag daemon=True garantiza que si el proceso principal termina,
    este hilo termina también sin necesidad de join() explícito.
    """

    def __init__(self, config: Config):
        super().__init__(daemon=True, name="HealthMonitor")
        self._config = config
        self._pc3_disponible = True
        self._lock = threading.Lock()
        self._activo = True
        self._contexto_zmq = zmq.Context.instance()

    # ------------------------------------------------------------------
    # Interfaz pública — llamada desde GestorSalida (hilo distinto)
    # ------------------------------------------------------------------

    def is_pc3_disponible(self) -> bool:
        """
        Thread-safe. GestorSalida llama esto antes de cada envío a BD principal.
        El lock garantiza que se lee el valor más reciente.
        """
        with self._lock:
            return self._pc3_disponible

    def detener(self) -> None:
        """Señala al hilo que debe terminar su ciclo."""
        self._activo = False

    # ------------------------------------------------------------------
    # Lógica interna del hilo
    # ------------------------------------------------------------------

    def run(self) -> None:
        """
        Ciclo principal del hilo. Se ejecuta hasta que detener() sea llamado.
        En cada iteración: abre socket REQ, envía PING, espera PONG.
        """
        print(f"[HealthMonitor] Iniciado — verificando PC3 cada {self._config.health_intervalo_s}s")

        while self._activo:
            resultado = self.check_health()
            self._actualizar_estado(resultado)
            time.sleep(self._config.health_intervalo_s)

        print("[HealthMonitor] Detenido")

    def check_health(self) -> bool:
        """
        Intenta conectar a PC3 y enviar un PING.
        Usa un socket REQ con RCVTIMEO para no bloquearse indefinidamente.
        Crea y destruye el socket en cada llamada para evitar sockets
        en estado inconsistente después de un timeout.
        """
        socket = self._contexto_zmq.socket(zmq.REQ)
        # RCVTIMEO: tiempo máximo de espera para recv() en milisegundos
        socket.setsockopt(zmq.RCVTIMEO, self._config.health_timeout_s * 1000)
        # LINGER 0: al cerrar el socket, no esperar mensajes pendientes
        socket.setsockopt(zmq.LINGER, 0)

        try:
            socket.connect(self._config.pc3_health_url)
            socket.send_string("PING")
            respuesta = socket.recv_string()
            return respuesta == "PONG"
        except zmq.Again:
            # Timeout: PC3 no respondió en el tiempo configurado
            return False
        except zmq.ZMQError:
            return False
        finally:
            socket.close()

    def _actualizar_estado(self, nuevo_estado: bool) -> None:
        """
        Actualiza pc3_disponible con el resultado del check.
        Solo imprime mensajes cuando el estado cambia (no en cada ciclo).
        """
        with self._lock:
            estado_anterior = self._pc3_disponible
            self._pc3_disponible = nuevo_estado

        if estado_anterior and not nuevo_estado:
            print(
                "[HealthMonitor] ⚠ PC3 NO DISPONIBLE — "
                "redirigiendo escrituras a BD Réplica"
            )
        elif not estado_anterior and nuevo_estado:
            print(
                "[HealthMonitor] ✓ PC3 RECUPERADO — "
                "reanudando escrituras en BD Principal"
            )
