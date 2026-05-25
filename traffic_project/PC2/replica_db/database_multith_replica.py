"""
    Desarollado por: Juan Felipe Gomez, Sebastian Gaibor y David Beltran Gomez
    Servicio de persistencia réplica desplegado en PC2 para soporte de failover distribuido.
    Recibe registros asíncronos desde Analítica mediante sockets PULL sobre ZeroMQ.
    Almacena eventos en archivos JSONL segmentados por dominio funcional.
    Expone consultas síncronas y heartbeats mediante interfaz REQ/REP independiente.
    Ejecuta hilos desacoplados para ingesta de datos y atención concurrente de consultas.
    Se comunica con Analítica, monitoreo de respaldo y clientes autorizados del sistema.
    Implementa replicación activa, tolerancia a fallos y separación lógica de lectura/escritura.
"""

import json
import os
import threading

import zmq

from jsonl_storage import JSONLStorage

CONFIG_FILE = os.path.join(os.path.dirname(__file__), "..", "config", "config.json")
DATA_FOLDER = "bd_replica_data"


def load_urls():
    with open(CONFIG_FILE, "r", encoding="utf-8") as file:
        data = json.load(file)
    red = data["red"]
    pull_url = red["bd_replica_analitica_url_PULL"]
    rep_url = red["bd_replica_consultas_url_REP"]
    return pull_url, rep_url

PULL_URL, REP_URL = load_urls()

class DatabaseReplicaService:
    # Inicializa el servicio de base de datos réplica, creando las estructuras de almacenamiento necesarias y preparando los sockets de comunicación.
    def __init__(self):
        self.context = zmq.Context()
        self.running = True

        os.makedirs(DATA_FOLDER, exist_ok=True)
        print(f"[DB Replica] Carpeta de datos: {DATA_FOLDER}/")

        self.storages = {
            "evento": JSONLStorage(os.path.join(DATA_FOLDER, "eventos.jsonl")),
            "congestion": JSONLStorage(os.path.join(DATA_FOLDER, "congestiones.jsonl")),
            "priorizacion": JSONLStorage(os.path.join(DATA_FOLDER, "priorizaciones.jsonl")),
            "semaforo": JSONLStorage(os.path.join(DATA_FOLDER, "semaforos.jsonl")),
        }

    # Método seguro para leer todos los registros de una categoría específica, manejando cualquier error de lectura y devolviendo una lista vacía en caso de problemas.
    def _safe_read_all(self, storage_key: str):
        storage = self.storages.get(storage_key)
        if storage is None:
            return []
        try:
            return storage.read_all()
        except Exception as error:
            print(f"[DB Replica-Consultas] Error leyendo {storage_key}: {error}")
            return []

    # Método que atiende consultas recibidas a través del socket REP, interpretando la solicitud JSON 
    # y devolviendo una respuesta estructurada según el tipo de consulta solicitada.
    def _atender_consulta(self, solicitud_raw: str):
        try:
            solicitud = json.loads(solicitud_raw)
        except json.JSONDecodeError:
            return {"estado": "ERROR", "mensaje": "Solicitud no es JSON valido"}

        tipo = solicitud.get("tipo")
        # Aquí se pueden agregar más tipos de consulta según las necesidades del sistema, cada uno accediendo a los datos almacenados y formateando la respuesta adecuadamente.
        if tipo == "CONSULTA_FECHAS_CONGESTIONES":
            congestiones = self._safe_read_all("congestion")
            detalle = []
            # Para cada registro de congestión, se extraen los datos relevantes (calle_id, estado_nuevo, fecha_hora) 
            # y se agregan a la lista de detalle que se incluirá en la respuesta.
            for registro in congestiones:
                datos = registro.get("datos", {})
                detalle.append({
                    "calle_id": datos.get("calle_id", "DESCONOCIDA"),
                    "estado_nuevo": datos.get("estado_nuevo", "DESCONOCIDO"),
                    "fecha_hora": datos.get("timestamp") or "SIN_FECHA",
                })
            return {
                "estado": "OK",
                "tipo_consulta": tipo,
                "total_congestiones": len(detalle),
                "congestiones": detalle,
            }

        # El tipo de consulta "CONSULTA_CAMBIOS_SEMAFOROS" devuelve el total de cambios de semáforo registrados, accediendo a los datos almacenados en la categoría "semaforo".
        if tipo == "CONSULTA_CAMBIOS_SEMAFOROS":
            semaforos = self._safe_read_all("semaforo")
            return {
                "estado": "OK",
                "tipo_consulta": tipo,
                "total_cambios_color": len(semaforos),
            }

        # El tipo de consulta "CONSULTA_PRIORIZACIONES_AMBULANCIA" devuelve un detalle de las priorizaciones realizadas para ambulancias, 
        # incluyendo calle_id, motivo y fecha_hora, accediendo a los datos almacenados en la categoría "priorizacion".
        if tipo == "CONSULTA_PRIORIZACIONES_AMBULANCIA":
            priorizaciones = self._safe_read_all("priorizacion")
            detalle = []
            # Para cada registro de priorización, se extraen los datos relevantes (calle_id, motivo, fecha_hora) 
            # y se agregan a la lista de detalle que se incluirá en la respuesta.
            for registro in priorizaciones:
                datos = registro.get("datos", {})
                detalle.append({
                    "calle_id": datos.get("calle_id", "DESCONOCIDA"),
                    "motivo": datos.get("motivo", "SIN_MOTIVO"),
                    "fecha_hora": datos.get("ts_inicio") or datos.get("timestamp") or "SIN_FECHA",
                })
            return {
                "estado": "OK",
                "tipo_consulta": tipo,
                "total_priorizaciones": len(detalle),
                "priorizaciones": detalle,
            }

        return {"estado": "ERROR", "mensaje": f"Tipo de consulta no soportado: {tipo}"}

    # El loop de ingesta se encarga de recibir eventos asíncronos desde Analítica a través de un socket PULL, 
    # persistiendo cada evento en la categoría correspondiente según su tipo_registro.
    def _loop_ingesta(self):
        pull_socket = self.context.socket(zmq.PULL)
        try:
            pull_socket.bind(PULL_URL)
        except zmq.ZMQError as error:
            self.running = False
            print(f"[DB Replica-Ingesta] ERROR al bindear {PULL_URL}: {error}")
            pull_socket.close()
            return

        pull_socket.setsockopt(zmq.RCVTIMEO, 1000)
        print(f"[DB Replica-Ingesta] Hilo PULL activo en {PULL_URL}")

        # El loop principal de ingesta se ejecuta continuamente mientras el servicio esté activo, intentando recibir eventos del socket PULL.
        while self.running:
            try:
                evento = pull_socket.recv_json()
                tipo = evento.get("tipo_registro")
                if tipo in self.storages:
                    self.storages[tipo].append_atomico(evento)
                    print(f"[DB Replica-Ingesta] Evento persistido ({tipo})")
            except zmq.Again:
                continue

        pull_socket.close()
        print("[DB Replica-Ingesta] Hilo detenido")

    # El loop de consultas se encarga de atender solicitudes síncronas recibidas a través de un socket REP, 
    # interpretando cada solicitud y devolviendo una respuesta estructurada.
    def _loop_consultas(self):
        rep_socket = self.context.socket(zmq.REP)
        try:
            rep_socket.bind(REP_URL)
        except zmq.ZMQError as error:
            self.running = False
            print(f"[DB Replica-Consultas] ERROR al bindear {REP_URL}: {error}")
            rep_socket.close()
            return

        rep_socket.setsockopt(zmq.RCVTIMEO, 1000)
        print(f"[DB Replica-Consultas] Hilo REP activo en {REP_URL}")

        while self.running:
            try:
                solicitud = rep_socket.recv_string()
                if solicitud == "PING":
                    rep_socket.send_string("PONG")
                    continue
                respuesta = self._atender_consulta(solicitud)
                rep_socket.send_json(respuesta)
            except zmq.Again:
                continue

        rep_socket.close()
        print("[DB Replica-Consultas] Hilo detenido")

    # El método iniciar se encarga de lanzar los hilos de ingesta y consultas, manteniendo el servicio activo hasta que se reciba una señal de interrupción,
    # momento en el cual se detienen ambos hilos y se cierra el contexto de ZeroMQ.
    def iniciar(self):
        t_ingesta = threading.Thread(target=self._loop_ingesta, daemon=True)
        t_consultas = threading.Thread(target=self._loop_consultas, daemon=True)
        t_ingesta.start()
        t_consultas.start()

        print("[DB Replica] Servicio activo.")
        print(f"   - Ingesta (PULL): {PULL_URL}")
        print(f"   - Consultas (REP): {REP_URL}")

        try:
            t_ingesta.join()
            t_consultas.join()
        except KeyboardInterrupt:
            self.running = False
            t_ingesta.join()
            t_consultas.join()
            self.context.term()
            print("[DB Replica] Servicio detenido.")


if __name__ == "__main__":
    DatabaseReplicaService().iniciar()
