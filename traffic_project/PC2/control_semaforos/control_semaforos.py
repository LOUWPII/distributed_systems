"""
    Desarollado por: Juan Felipe Gomez, Sebastian Gaibor y David Beltran Gomez
    Servicio encargado de ejecutar comandos de control sobre semáforos distribuidos.
    Consume instrucciones desde sockets PULL utilizando mensajería asíncrona con ZeroMQ.
    Mantiene y persiste localmente el estado operativo actual de cada semáforo.
    Procesa cambios de luz, eventos de sincronización y comandos administrativos.
    Implementa un polling no bloqueante para garantizar capacidad de respuesta y tolerancia a fallos.
    Integra persistencia ligera y cierre seguro de recursos y sockets distribuidos.
"""


import zmq
import json
import threading
import os
import time

CONFIG_FILE = os.path.join(os.path.dirname(__file__), "..", "config", "config.json")

# Función para cargar la URL de conexión desde el archivo de configuración, con manejo de errores para asegurar que el servicio tenga la información necesaria para operar.
def load_pull_url():
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    url = data["red"]["semaforos_analitica_url_PULL"]
    if url.startswith("tcp:*"):
        url = url.replace("tcp:*", "tcp://*")
    return url

def load_pub_url():
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    url = data["red"]["semaforos_notificacion_url_PUB"]
    if url.startswith("tcp:*"):
        url = url.replace("tcp:*", "tcp://*")
    return url

PULL_URL = load_pull_url()
PUB_URL = load_pub_url()
STATE_FILE = "estado_semaforos.json"

class ControlSemaforos:
    def __init__(self):
        self.context = zmq.Context()
        self.estados = self._cargar_estado()
        self._activo = True
        self._lock = threading.Lock()
        self._pull_socket = None

    # El método _validar_mensaje se encarga de verificar que el mensaje recibido a través del socket PULL tenga la estructura y los campos necesarios
    # para ser procesado correctamente, asegurando que los comandos de control sobre los semáforos sean válidos antes de su ejecución.
    def _validar_mensaje(self, mensaje):
        try:
            comando = json.loads(mensaje)
            required_fields = ["semaforo_id", "nuevo_estado"]
            return all(field in comando for field in required_fields)
        except json.JSONDecodeError:
            return False

    # El método _procesar_mensaje se encarga de interpretar el comando recibido, actualizar el estado del semáforo correspondiente, persistir el nuevo estado
    # y mostrar una representación visual del cambio de luz en la consola, facilitando la monitorización de las acciones realizadas sobre los semáforos.
    def _cargar_estado(self):
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, "r") as f:
                    return json.load(f)
            except json.JSONDecodeError:
                pass
        return {}

    # El método _guardar_estado se encarga de persistir el estado actual de los semáforos en un archivo JSON, asegurando que la información se mantenga entre reinicios del servicio
    # y facilitando la recuperación del estado operativo en caso de fallos.
    def _guardar_estado(self):
        with open(STATE_FILE, "w") as f:
            json.dump(self.estados, f, indent=4)

    # El método _procesar_mensaje se encarga de interpretar el comando recibido, actualizar el estado del semáforo correspondiente, persistir el nuevo estado
    # y mostrar una representación visual del cambio de luz en la consola, facilitando la monitorización de las acciones realizadas sobre los semáforos.
    def iniciar(self):
        self._pull_socket = self.context.socket(zmq.PULL)
        self._pull_socket.bind(PULL_URL)
        self._pub_socket = self.context.socket(zmq.PUB)
        self._pub_socket.bind(PUB_URL)
        print(f"[Control Semáforos] Iniciado. Esperando comandos en {PULL_URL}...")
        print(f"[Control Semáforos] Notificaciones PUB en {PUB_URL}")
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

    # El método _validar_mensaje se encarga de verificar que el mensaje recibido a través del socket PULL tenga la estructura y los campos necesarios
    # para ser procesado correctamente, asegurando que los comandos de control sobre los semáforos sean válidos antes de su ejecución.
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

            self._pub_socket.send_json({
                "tipo": "CAMBIO_SEMAFORO",
                "semaforo_id": semaforo_id,
                "nuevo_estado": nuevo_estado,
                "timestamp": comando.get("timestamp"),
            })

            print(f"[ACCIÓN SEMÁFORO]: {semaforo_id}")
            if nuevo_estado == "VERDE":
                print(f"🟢 CAMBIO A VERDE (Durante {duracion}s)")
            elif nuevo_estado == "ROJO":
                print(f"🔴 CAMBIO A ROJO")
            print(f"Motivo: {motivo}")
            print("-" * 60)

        except json.JSONDecodeError:
            print("[Error] Mensaje mal formado recibido.")

    # El método _cerrar se encarga de cerrar de manera segura los recursos utilizados por el servicio, incluyendo el socket PULL y el contexto de ZeroMQ,
    # asegurando que el servicio se detenga correctamente sin dejar conexiones abiertas o recursos bloqueados.
    def _cerrar(self):
        if self._pull_socket:
            self._pull_socket.close(0)
        if self._pub_socket:
            self._pub_socket.close(0)
        if self.context:
            self.context.term()
        print("[Control Semáforos] Servicio cerrado correctamente.")

if __name__ == "__main__":
    servicio = ControlSemaforos()
    servicio.iniciar()