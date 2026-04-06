import zmq
import threading
import json
from jsonl_storage import JSONLStorage
import os

CONFIG_FILE = os.path.join(os.path.dirname(__file__), "..", "PC2", "config", "config.json")

def load_urls():
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    red = data["red"]
    pull_url = red["bd_principal_analitica_url_PULL"]
    rep_health_url = red["health_analitica_url_REP"]
    rep_monitoreo_url = red.get("bd_principal_monitoreo_url_REP", rep_health_url)
    return pull_url, rep_health_url, rep_monitoreo_url

PULL_URL, REP_HEALTH_URL, REP_MONITOREO_URL = load_urls()
DATA_FOLDER = "bd_principal_data"


class DatabaseService:

    def __init__(self):
        self.context = zmq.Context()
        self.running = True  # Control de ejecución

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
        pull_socket.bind(PULL_URL)

        # Permite salir del recv
        pull_socket.setsockopt(zmq.RCVTIMEO, 1000)

        print(f"[DB-Ingesta] Hilo PULL activo en {PULL_URL}")

        while self.running:
            try:
                evento = pull_socket.recv_json()
                tipo = evento["tipo_registro"]
                if tipo in self.storages:
                    self.storages[tipo].append_atomico(evento)
                    print(f"[DB-Ingesta] Evento persistido ({tipo})")
            except zmq.Again:
                continue

        pull_socket.close()
        print("[DB-Ingesta] Hilo detenido")

    def _loop_consultas(self):
        rep_socket = self.context.socket(zmq.REP)
        rep_socket.bind(REP_HEALTH_URL)

        if REP_MONITOREO_URL != REP_HEALTH_URL:
            rep_socket.bind(REP_MONITOREO_URL)

        # Permite salir del recv
        rep_socket.setsockopt(zmq.RCVTIMEO, 1000)

        print(f"[DB-Consultas] Hilo REP activo en {REP_HEALTH_URL} y {REP_MONITOREO_URL}")

        while self.running:
            try:
                solicitud = rep_socket.recv_string()

                if solicitud == "PING":
                    rep_socket.send_string("PONG")
                    print("[DB-Consultas] Solicitud PING, retornando PONG")
                else:
                    print(f"[DB-Consultas] Solicitud recibida: {solicitud}")
                    rep_socket.send_json({
                        "status": "ok",
                        "data": "Resultados de consulta..."
                    })

            except zmq.Again:
                continue

        rep_socket.close()
        print("[DB-Consultas] Hilo detenido")

    def iniciar(self):
        t_ingesta = threading.Thread(target=self._loop_ingesta)
        t_consultas = threading.Thread(target=self._loop_consultas)

        t_ingesta.start()
        t_consultas.start()

        print("[DB] Base de Datos Principal (PC3) operando con hilos independientes.")
        print(f"   - Ingesta datos (PULL): {PULL_URL}")
        print(f"   - Healh y monitoreo (REP): {REP_HEALTH_URL} | {REP_MONITOREO_URL}")

        try:
            t_ingesta.join()
            t_consultas.join()

        except KeyboardInterrupt:
            print("\n[DB] Cerrando servicio...")

            # Detener loops
            self.running = False

            # Esperar cierre de hilos
            t_ingesta.join()
            t_consultas.join()

            # Liberar recursos ZMQ
            self.context.term()

            print("[DB] Servicio detenido correctamente.")


if __name__ == "__main__":
    db = DatabaseService()
    db.iniciar()