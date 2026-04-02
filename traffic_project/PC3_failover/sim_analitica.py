import zmq
import threading
import json
import time

# Lo ideal es leer estos puertos desde el archivo config.JSON
PULL_PORT_PRINCIPAL = 5560      # Puerto donde PC3 recibe datos (PULL)
REP_PORT_PRINCIPAL = 5562       # Puerto donde PC3 atiende health checks (REP)

# PC2 (Réplica) → usa este puerto cuando PC3 está caído
PULL_PORT_REPLICA = 5570        # Puerto donde la réplica recibe datos (PULL)



class AnaliticaSimulacion:
    """
        Simulador de servicio de Analítica.

        Este servicio corre en PC2 (simulado) y tiene dos responsabilidades:
        1. Enviar datos de tráfico a la base de datos principal (PC3)
        2. Detectar fallos de PC3 y cambiar automáticamente a una réplica (PC2)

        El patrón Lazy Pirate se usa para la detección de fallos:
        - Cada health check tiene timeout (no espera para siempre)
        - Si hay timeout, se asume que PC3 está caído
        - El socket REQ se recrea después de cada fallo para salir del estado inválido

        Los datos se envían por PUSH porque no se necesita esperar respuesta.
        La Analítica solo empuja los datos y sigue trabajando.
        """
    def __init__(self):
        """
               Inicializa el simulador de Analítica.

               Args:

               """
        self.context = zmq.Context()
        # Estado compartido: False = PC3 OK (Principal), True = PC3 Caído (Réplica)
        self.pc3_caido = False
        self._lock = threading.Lock()

    def _hilo_lazy_pirate(self):
        """
        Hilo que monitorea la salud de PC3 usando el patrón Lazy Pirate.

        Este hilo corre en segundo plano y nunca termina. Cada 5 segundos:
        1. Envía un "PING" a PC3
        2. Espera respuesta con timeout de 2 segundos
        3. Si recibe "PONG"  PC3 está vivo
        4. Si hay timeout  PC3 está muerto

        Cuando se detecta un fallo, se cierra y recrea el socket REQ.
        Esto es clave para salir del estado inválido que deja ZeroMQ cuando
        una petición REQ se queda sin respuesta.

        El socket se configura con LINGER=0 para que close() no se bloquee
        intentando enviar mensajes pendientes a un servidor muerto.
        """
        endpoint = f"tcp://localhost:{REP_PORT_PRINCIPAL}"

        # Crear socket REQ inicial
        health_socket = self.context.socket(zmq.REQ)
        health_socket.connect(endpoint)
        # Timeout de 2 segundos
        health_socket.setsockopt(zmq.RCVTIMEO, 2000)
        health_socket.setsockopt(zmq.LINGER, 0)

        print(f"[Health] Vigilando PC3 en {endpoint}...")

        while True:
            try:
                health_socket.send_string("PING")
                respuesta = health_socket.recv_string()

                if respuesta == "PONG":
                    with self._lock:
                        if self.pc3_caido:
                            print("[Health] PC3 recuperado. Volviendo a Principal.")
                        self.pc3_caido = False

            except zmq.Again:
                # Timeout detectado: PC3 no responde
                with self._lock:
                    if not self.pc3_caido:
                        print("[!!!] Falla detectada en PC3. Activando modo RÉPLICA.")
                    self.pc3_caido = True

                # REGLA LAZY PIRATE: Cerrar y recrear el socket tras falla
                health_socket.close()
                health_socket = self.context.socket(zmq.REQ)
                health_socket.connect(endpoint)
                health_socket.setsockopt(zmq.RCVTIMEO, 2000)
                health_socket.setsockopt(zmq.LINGER, 0)

            time.sleep(5)  # Frecuencia del chequeo

    def _hilo_push_datos(self):
        """
        Hilo que envía datos al destino activo (Principal o Réplica).

        Este hilo corre en segundo plano y nunca termina. Cada 3 segundos:
        1. Verifica el estado de PC3
        2. Envía un registro JSON al socket correspondiente

        Los sockets son PUSH

        El failover es automático:
        - PC3 vivo ,datos van a Principal (puerto pull_principal)
        - PC3 muerto,datos van a Réplica (puerto pull_replica)
        """
        push_principal = self.context.socket(zmq.PUSH)
        push_principal.connect(f"tcp://localhost:{PULL_PORT_PRINCIPAL}")

        push_replica = self.context.socket(zmq.PUSH)
        push_replica.connect(f"tcp://localhost:{PULL_PORT_REPLICA}")

        print("[Analítica] Hilo PUSH iniciado. Enviando datos de simulación...")

        while True:
            # Datos simples de simulación
            registro_simple = {
                    "sensor_id": "SIM-PC2",
                    "tipo_registro": "evento",
                    "dato": "Simulación de tráfico",
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ")
            }

            # Consultar estado de la red bajo lock
            with self._lock:
                en_falla = self.pc3_caido

            # Failover automático: enviar a quien corresponda
            if not en_falla:
                push_principal.send_json(registro_simple)
                print(f"[PUSH] Dato enviado a Principal (PC3) en puerto {PULL_PORT_PRINCIPAL}")
            else:
                push_replica.send_json(registro_simple)
                print(f"[PUSH] Dato enviado a RÉPLICA (PC2) en puerto {PULL_PORT_REPLICA}")

            time.sleep(3)  # Envío cada 3 segundos

    def iniciar(self):
        """Lanza la simulación con dos hilos independientes."""
        t_salud = threading.Thread(target=self._hilo_lazy_pirate, daemon=True)
        t_datos = threading.Thread(target=self._hilo_push_datos, daemon=True)

        t_salud.start()
        t_datos.start()

        print("[PC2] Simulador de Analítica activo con Failover Automático.")
        print(f"   - Health check a PC3 en puerto {REP_PORT_PRINCIPAL}")
        print(f"   - Envío a Principal en puerto {PULL_PORT_PRINCIPAL}")
        print(f"   - Envío a Réplica en puerto {PULL_PORT_REPLICA}")
        t_datos.join()


if __name__ == "__main__":
    app = AnaliticaSimulacion()
    app.iniciar()