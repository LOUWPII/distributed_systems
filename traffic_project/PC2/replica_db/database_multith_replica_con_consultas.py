import zmq
import threading
import json
from jsonl_storage import JSONLStorage
import os


PULL_PORT = 5570  # Puerto para recibir datos de Analítica (failover)
REP_PORT_DB = 5572 # Puerto para health checks y consultas en la réplica
DATA_FOLDER = "bd_replica_data" #En esta carpeta se guardan los archivos BD

"""
    Solamente falta añadir la lógica de responder a las consultas del servicio 
    de monitoreo y consulta.
    
    Claramente este proceso tendrá un puerto PULL específico
"""

class DatabaseReplicaService:
    def __init__(self):
        self.context = zmq.Context()

        os.makedirs(DATA_FOLDER, exist_ok=True)
        print(f"[DB] Carpeta de datos: {DATA_FOLDER}/")

        self.storages = {
            "evento": JSONLStorage(os.path.join(DATA_FOLDER, "eventos.jsonl")),
            "congestion": JSONLStorage(os.path.join(DATA_FOLDER, "congestiones.jsonl")),
            "priorizacion": JSONLStorage(os.path.join(DATA_FOLDER, "priorizaciones.jsonl")),
            "semaforo": JSONLStorage(os.path.join(DATA_FOLDER, "semaforos.jsonl"))
        }

    # Hilo que recibe los datos de Analítica y los guarda en la BD réplica
    def _loop_ingesta(self):
        # Crea un socket PULL para recibir datos de Analítica
        pull_socket = self.context.socket(zmq.PULL)
        pull_socket.bind(f"tcp://*:{PULL_PORT}")
        print(f"[Réplica] Hilo PULL activo en puerto {PULL_PORT} (modo failover)")

        # Mientras el hilo esté activo, recibe datos de Analítica
        while True:
            evento = pull_socket.recv_json()
            tipo = evento["tipo_registro"]
            if tipo in self.storages:
                self.storages[tipo].append_atomico(evento)

    def _loop_consultas(self):
        """
        Hilo que atiende consultas de historial o salud en la Réplica.
        Usado principalmente si PC3 principal cae irremediablemente.
        """
        rep_socket = self.context.socket(zmq.REP)
        rep_socket.bind(f"tcp://*:{REP_PORT_DB}")
        print(f"[Réplica-Consultas] Hilo REP activo en puerto {REP_PORT_DB} (modo failover)")

        while True:
            solicitud = rep_socket.recv_string()
            print(f"[Réplica-Consultas] Solicitud recibida: {solicitud}")

            # Implementación equivalente al patrón de salud (Lazy Pirate)
            if solicitud == "PING":
                rep_socket.send_string("PONG")
                print("[Réplica-Consultas] PONG enviado")
            else:
                # Respuesta a consultas de historial o monitoreo
                rep_socket.send_json({"status": "ok", "data": "Resultados de consulta en réplica..."})

    # Inicia el hilo de ingesta
    def iniciar(self):
        t_ingesta = threading.Thread(target=self._loop_ingesta, daemon=True)
        t_consultas = threading.Thread(target=self._loop_consultas, daemon=True)
        
        t_ingesta.start()
        t_consultas.start()

        print("[Réplica] Base de Datos Réplica (PC2) operando en modo failover absoluto.")
        print(f"   - Puerto PULL (ingesta failover): {PULL_PORT}")
        print(f"   - Puerto REP (consultas failover): {REP_PORT_DB}")

        t_ingesta.join()
        t_consultas.join()


if __name__ == "__main__":
    replica = DatabaseReplicaService()
    replica.iniciar()