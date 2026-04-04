import zmq
import threading
import json
from jsonl_storage import JSONLStorage
import os


PULL_PORT = 5570  # Puerto para recibir datos de Analítica (failover)
DATA_FOLDER = "bd_replica_data" #En sta carpeta se guardan los archivos BD

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

    # Inicia el hilo de ingesta
    def iniciar(self):
        t_ingesta = threading.Thread(target=self._loop_ingesta, daemon=True)
        t_ingesta.start()

        print("[Réplica] Base de Datos Réplica (PC2) operando en modo failover.")
        print(f"   - Puerto PULL (failover): {PULL_PORT}")

        t_ingesta.join()


if __name__ == "__main__":
    replica = DatabaseReplicaService()
    replica.iniciar()