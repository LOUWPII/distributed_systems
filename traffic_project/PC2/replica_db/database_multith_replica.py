import zmq
import threading
import json
from jsonl_storage import JSONLStorage
import os

# Determinar ruta dinámica a config.json asumiendo la estructura del proyecto
CONFIG_FILE = os.path.join(os.path.dirname(__file__), "..", "config", "config.json")

def load_urls():
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    red = data["red"]
    pull_url = red["bd_replica_analitica_url_PULL"]
    rep_url = red["bd_replica_consultas_url_REP"]

    return pull_url, rep_url

PULL_URL, REP_URL = load_urls()
DATA_FOLDER = "bd_replica_data" #En esta carpeta se guardan los archivos BD

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

        self._activo = True
        self._pull_socket = None
        self._rep_socket = None

    # Hilo que recibe los datos de Analítica y los guarda en la BD réplica
    def _loop_ingesta(self):
        # Crea un socket PULL para recibir datos de Analítica
        self._pull_socket = self.context.socket(zmq.PULL)
        self._pull_socket.bind(PULL_URL)
        print(f"[Réplica] Hilo PULL activo en {PULL_URL}")

        # Mientras el hilo esté activo, recibe datos de Analítica
        while self._activo:
            try:
                evento = self._pull_socket.recv_json(flags=zmq.NOBLOCK)
                tipo = evento["tipo_registro"]
                if tipo in self.storages:
                    self.storages[tipo].append_atomico(evento)
                    print(f"[Réplica-Ingesta] Evento persistido ({tipo})")
            except zmq.Again:
                pass  # No hay mensaje, continuar
            except Exception as e:
                print(f"[Réplica] Error en ingesta: {e}")


    def _loop_consultas(self):
        self._rep_socket = self.context.socket(zmq.REP)
        self._rep_socket.bind(REP_URL)
        print(f"[Réplica-Consultas] Hilo REP activo en {REP_URL}")

        while self._activo:
            try:
                solicitud = self._rep_socket.recv_string(flags=zmq.NOBLOCK)
                print(f"[Réplica-Consultas] Solicitud recibida: {solicitud}")
                self._rep_socket.send_json({"status": "ok", "data": "Resultados de consulta en réplica..."})
            except zmq.Again:
                pass  # No hay mensaje, continuar
            except Exception as e:
                print(f"[Réplica-Consultas] Error en consultas: {e}")

    # Inicia los hilos
    def iniciar(self):
        t_ingesta = threading.Thread(target=self._loop_ingesta, daemon=True)
        t_consultas = threading.Thread(target=self._loop_consultas, daemon=True)
        t_ingesta.start()
        t_consultas.start()

        print("[Réplica] Base de Datos Réplica (PC2) operando con Failover")
        print(f"   - Ingesta failover (PULL): {PULL_URL}")
        # print(f"   - Consultas failover (REP): {REP_URL}")

        try:
            while True:
                t_ingesta.join(timeout=1)
                t_consultas.join(timeout=1)
        except KeyboardInterrupt:
            print("\n[DB] Finalizando réplica.")
            self._activo = False
            self._cerrar_sockets()
            print("[DB] Réplica cerrada correctamente.")

    def _cerrar_sockets(self):
        if self._pull_socket:
            self._pull_socket.close()
        if self._rep_socket:
            self._rep_socket.close()
        self.context.term()


if __name__ == "__main__":
    replica = DatabaseReplicaService()
    replica.iniciar()