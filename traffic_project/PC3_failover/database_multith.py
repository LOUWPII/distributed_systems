import json
import os
import threading

import zmq

from jsonl_storage import JSONLStorage

CONFIG_FILE = os.path.join(os.path.dirname(__file__), "..", "PC2", "config", "config.json")
DATA_FOLDER = "bd_principal_data"


def load_urls():
    with open(CONFIG_FILE, "r", encoding="utf-8") as file:
        data = json.load(file)
    red = data["red"]
    pull_url = red["bd_principal_analitica_url_PULL"]
    rep_monitoreo_url = red["bd_principal_monitoreo_url_REP"]
    rep_health_url = red.get("bd_principal_health_url_REP", rep_monitoreo_url)
    return pull_url, rep_health_url, rep_monitoreo_url


PULL_URL, REP_HEALTH_URL, REP_MONITOREO_URL = load_urls()


class DatabaseService:
    def __init__(self):
        self.context = zmq.Context()
        self.running = True

        os.makedirs(DATA_FOLDER, exist_ok=True)
        print(f"[DB] Carpeta de datos: {DATA_FOLDER}/")

        self.storages = {
            "evento": JSONLStorage(os.path.join(DATA_FOLDER, "eventos.jsonl")),
            "congestion": JSONLStorage(os.path.join(DATA_FOLDER, "congestiones.jsonl")),
            "priorizacion": JSONLStorage(os.path.join(DATA_FOLDER, "priorizaciones.jsonl")),
            "semaforo": JSONLStorage(os.path.join(DATA_FOLDER, "semaforos.jsonl")),
        }

    def _safe_read_all(self, storage_key: str):
        storage = self.storages.get(storage_key)
        if storage is None:
            return []
        try:
            return storage.read_all()
        except Exception as error:
            print(f"[DB-Consultas] Error leyendo {storage_key}: {error}")
            return []

    def _atender_consulta(self, solicitud_raw: str):
        try:
            solicitud = json.loads(solicitud_raw)
        except json.JSONDecodeError:
            return {"estado": "ERROR", "mensaje": "Solicitud no es JSON valido"}

        tipo = solicitud.get("tipo")

        if tipo == "CONSULTA_FECHAS_CONGESTIONES":
            congestiones = self._safe_read_all("congestion")
            fechas = []
            for registro in congestiones:
                datos = registro.get("datos", {})
                fechas.append({
                    "calle_id": datos.get("calle_id", "DESCONOCIDA"),
                    "estado_nuevo": datos.get("estado_nuevo", "DESCONOCIDO"),
                    "fecha_hora": datos.get("timestamp") or "SIN_FECHA",
                })
            return {
                "estado": "OK",
                "tipo_consulta": "CONSULTA_FECHAS_CONGESTIONES",
                "total_congestiones": len(fechas),
                "congestiones": fechas,
                "mensaje": "Registros antiguos pueden venir sin fecha (SIN_FECHA).",
            }

        if tipo == "CONSULTA_CAMBIOS_SEMAFOROS":
            semaforos = self._safe_read_all("semaforo")
            return {
                "estado": "OK",
                "tipo_consulta": "CONSULTA_CAMBIOS_SEMAFOROS",
                "total_cambios_color": len(semaforos),
            }

        if tipo == "CONSULTA_PRIORIZACIONES_AMBULANCIA":
            priorizaciones = self._safe_read_all("priorizacion")
            detalle = []
            for registro in priorizaciones:
                datos = registro.get("datos", {})
                detalle.append({
                    "calle_id": datos.get("calle_id", "DESCONOCIDA"),
                    "motivo": datos.get("motivo", "SIN_MOTIVO"),
                    "fecha_hora": datos.get("ts_inicio") or datos.get("timestamp") or "SIN_FECHA",
                })
            return {
                "estado": "OK",
                "tipo_consulta": "CONSULTA_PRIORIZACIONES_AMBULANCIA",
                "total_priorizaciones": len(detalle),
                "priorizaciones": detalle,
            }

        return {"estado": "ERROR", "mensaje": f"Tipo de consulta no soportado: {tipo}"}

    def _loop_ingesta(self):
        pull_socket = self.context.socket(zmq.PULL)
        try:
            pull_socket.bind(PULL_URL)
        except zmq.ZMQError as error:
            self.running = False
            print(f"[DB-Ingesta] ERROR al bindear {PULL_URL}: {error}")
            print("[DB-Ingesta] Puerto en uso. Cierre el proceso anterior o cambie el puerto.")
            pull_socket.close()
            return

        pull_socket.setsockopt(zmq.RCVTIMEO, 1000)
        print(f"[DB-Ingesta] Hilo PULL activo en {PULL_URL}")

        while self.running:
            try:
                evento = pull_socket.recv_json()
                tipo = evento.get("tipo_registro")
                if tipo in self.storages:
                    self.storages[tipo].append_atomico(evento)
                    print(f"[DB-Ingesta] Evento persistido ({tipo})")
            except zmq.Again:
                continue

        pull_socket.close()
        print("[DB-Ingesta] Hilo detenido")

    def _loop_consultas(self):
        rep_socket = self.context.socket(zmq.REP)
        try:
            rep_socket.bind(REP_HEALTH_URL)
        except zmq.ZMQError as error:
            self.running = False
            print(f"[DB-Consultas] ERROR al bindear {REP_HEALTH_URL}: {error}")
            print("[DB-Consultas] Puerto en uso. Cierre el proceso anterior o cambie el puerto.")
            rep_socket.close()
            return

        if REP_MONITOREO_URL != REP_HEALTH_URL:
            try:
                rep_socket.bind(REP_MONITOREO_URL)
            except zmq.ZMQError as error:
                self.running = False
                print(f"[DB-Consultas] ERROR al bindear {REP_MONITOREO_URL}: {error}")
                print("[DB-Consultas] Puerto en uso. Cierre el proceso anterior o cambie el puerto.")
                rep_socket.close()
                return

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
                    respuesta = self._atender_consulta(solicitud)
                    print(f"[DB-Consultas] Respuesta enviada: {json.dumps(respuesta, ensure_ascii=False)[:300]}...")
                    rep_socket.send_json(respuesta)
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
        print(f"   - Health y monitoreo (REP): {REP_HEALTH_URL} | {REP_MONITOREO_URL}")

        try:
            t_ingesta.join()
            t_consultas.join()
        except KeyboardInterrupt:
            print("\n[DB] Cerrando servicio...")
            self.running = False
            t_ingesta.join()
            t_consultas.join()
            self.context.term()
            print("[DB] Servicio detenido correctamente.")


if __name__ == "__main__":
    DatabaseService().iniciar()
