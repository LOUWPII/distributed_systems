import zmq
import threading
import json
from jsonl_storage import JSONLStorage
import os


PULL_PORT_REPLICA = 5570  # Puerto para recibir datos de Analítica (failover)
DATA_FOLDER = "bd_replica_data" #En sta carpeta se guardan los archivos BD

"""
    Este programa tiene la misma funcionalidad que la BD principal, sin embargo
    los archivos JSON de persistencia se almacenan en una carpeta distinta.
    
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

    def _loop_ingesta(self):
        pull_socket = self.context.socket(zmq.PULL)
        pull_socket.bind(f"tcp://*:{PULL_PORT_REPLICA}")
        print(f"[Réplica] Hilo PULL activo en puerto {PULL_PORT_REPLICA} (modo failover)")

        while True:
            evento = pull_socket.recv_json()
            tipo = evento["tipo_registro"]
            if tipo in self.storages:
                self.storages[tipo].append_atomico(evento)

    def iniciar(self):
        t_ingesta = threading.Thread(target=self._loop_ingesta, daemon=True)
        t_ingesta.start()

        print("[Réplica] Base de Datos Réplica (PC2) operando en modo failover.")
        print(f"   - Puerto PULL (failover): {PULL_PORT_REPLICA}")

        t_ingesta.join()


if __name__ == "__main__":
    replica = DatabaseReplicaService()
    replica.iniciar()