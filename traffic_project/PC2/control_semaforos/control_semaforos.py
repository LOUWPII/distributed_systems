import zmq
import json
import threading
import os
import time

CONFIG_FILE = os.path.join(os.path.dirname(__file__), "..", "config", "config.json")

def load_pull_url():
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    url = data["red"]["semaforos_analitica_url_PULL"]
    if url.startswith("tcp:*"):
        url = url.replace("tcp:*", "tcp://*")
    return url

PULL_URL = load_pull_url()
STATE_FILE = "estado_semaforos.json"

class ControlSemaforos:
    def __init__(self):
        self.context = zmq.Context()
        self.estados = self._cargar_estado()
        self._activo = True
        self._lock = threading.Lock()
        self._pull_socket = None

    def _cargar_estado(self):
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, "r") as f:
                    return json.load(f)
            except json.JSONDecodeError:
                pass
        return {}

    def _guardar_estado(self):
        with open(STATE_FILE, "w") as f:
            json.dump(self.estados, f, indent=4)

    def iniciar(self):
        self._pull_socket = self.context.socket(zmq.PULL)
        self._pull_socket.bind(PULL_URL)
        print(f"[Control Semáforos] Iniciado. Esperando comandos en {PULL_URL}...")
        print("-" * 60)

        poller = zmq.Poller()
        poller.register(self._pull_socket, zmq.POLLIN)

        try:
            while self._activo:
                # Poll con timeout de 1000 ms
                events = dict(poller.poll(timeout=1000))
                if self._pull_socket in events:
                    mensaje = self._pull_socket.recv_string()
                    self._procesar_mensaje(mensaje)
        except KeyboardInterrupt:
            print("\n[Control Semáforos] Ctrl+C detectado. Cerrando...")
        finally:
            self._cerrar()

    def _procesar_mensaje(self, mensaje):
        try:
            comando = json.loads(mensaje)
            if comando.get("comando") == "DETENER":
                print("[Control Semáforos] Comando DETENER recibido. Cerrando servicio...")
                self._activo = False
                return

            semaforo_id = comando.get("semaforo_id", "DESCONOCIDO")
            nuevo_estado = comando.get("nuevo_estado", "DESCONOCIDO")
            duracion = comando.get("duracion_s", 0)
            motivo = comando.get("motivo", "Automático")

            with self._lock:
                self.estados[semaforo_id] = {
                    "estado": nuevo_estado,
                    "ultima_actualizacion": comando.get("timestamp")
                }
                self._guardar_estado()

            print(f"[ACCIÓN SEMÁFORO]: {semaforo_id}")
            if nuevo_estado == "VERDE":
                print(f"🟢 CAMBIO A VERDE (Por {duracion}s)")
            elif nuevo_estado == "ROJO":
                print(f"🔴 CAMBIO A ROJO")
            print(f"Motivo: {motivo}")
            print("-" * 60)

        except json.JSONDecodeError:
            print("[Error] Mensaje mal formado recibido.")

    def _cerrar(self):
        if self._pull_socket:
            self._pull_socket.close(0)
        if self.context:
            self.context.term()
        print("[Control Semáforos] Servicio cerrado correctamente.")

if __name__ == "__main__":
    servicio = ControlSemaforos()
    servicio.iniciar()