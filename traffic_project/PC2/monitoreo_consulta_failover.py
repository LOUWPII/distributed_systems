import json
import os
import sys
from datetime import datetime

import zmq

CONFIG_FILE = os.path.join(os.path.dirname(__file__), "config", "config.json")


def load_urls():
    if not os.path.exists(CONFIG_FILE):
        print(f"[Monitoreo-PC2] Error: No se encontro config en {CONFIG_FILE}")
        sys.exit(1)
    with open(CONFIG_FILE, "r", encoding="utf-8") as file:
        data = json.load(file)
    red = data["red"]
    return red["analitica_monitoreo_url_REQ"], red["bd_replica_consultas_url_REP"]


class MonitoreoConsultaPC2:
    def __init__(self):
        self.context = zmq.Context()
        self.req_analitica_url, self.req_db_replica_url = load_urls()
        self.socket_analitica = self.context.socket(zmq.REQ)
        self.socket_analitica.connect(self.req_analitica_url)
        self.socket_db = self.context.socket(zmq.REQ)
        db_req = self.req_db_replica_url.replace("://*:", "://localhost:")
        self.socket_db.connect(db_req)
        self.socket_analitica.setsockopt(zmq.RCVTIMEO, 3000)
        self.socket_db.setsockopt(zmq.RCVTIMEO, 3000)
        print(f"[Monitoreo-PC2] Failover activo. Analitica={self.req_analitica_url} DBReplica={db_req}")

    def _ts(self):
        return datetime.now().isoformat(timespec="seconds")

    def _send(self, socket, req, destino):
        print(f"[{self._ts()}][Monitoreo-PC2][REQ->{destino}] {json.dumps(req)}")
        socket.send_string(json.dumps(req))
        try:
            rep = json.loads(socket.recv_string())
            print(f"[{self._ts()}][Monitoreo-PC2][REP<-{destino}] OK")
            print(json.dumps(rep, indent=2, ensure_ascii=False))
        except Exception as err:
            print(f"[{self._ts()}][Monitoreo-PC2][ERROR] {destino}: {err}")

    def menu(self):
        while True:
            print("\n==== MONITOREO PC2 (FAILOVER) ====")
            print(" [1] Estado de todas las calles")
            print(" [2] Fechas de congestiones")
            print(" [3] Cambios de semaforos")
            print(" [4] Priorizaciones ambulancia")
            print(" [5] Salir")
            op = input("Seleccione opcion: ").strip()
            if op == "1":
                self._send(self.socket_analitica, {"tipo": "CONSULTA_TODOS_ESTADOS"}, "ANALITICA")
            elif op == "2":
                self._send(self.socket_db, {"tipo": "CONSULTA_FECHAS_CONGESTIONES"}, "DB_REPLICA")
            elif op == "3":
                self._send(self.socket_db, {"tipo": "CONSULTA_CAMBIOS_SEMAFOROS"}, "DB_REPLICA")
            elif op == "4":
                self._send(self.socket_db, {"tipo": "CONSULTA_PRIORIZACIONES_AMBULANCIA"}, "DB_REPLICA")
            elif op == "5":
                break


if __name__ == "__main__":
    MonitoreoConsultaPC2().menu()
