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
    # En failover (PC2), Monitoreo debe consultar la Analitica LOCAL (REP) y la BD Replica LOCAL (REP).
    analitica_rep_url = red["monitoreo_analitica_url_REP"]
    bd_replica_rep_url = red["bd_replica_consultas_url_REP"]
    return analitica_rep_url, bd_replica_rep_url


class MonitoreoConsultaPC2:
    def __init__(self):
        self.context = zmq.Context()
        self.analitica_rep_url, self.db_replica_rep_url = load_urls()
        self.req_analitica_url = self.analitica_rep_url.replace("://*:", "://localhost:")
        self.db_req_url = self.db_replica_rep_url.replace("://*:", "://localhost:")
        self.socket_analitica = self.context.socket(zmq.REQ)
        self.socket_analitica.connect(self.req_analitica_url)
        self.socket_db = self.context.socket(zmq.REQ)
        self.socket_db.connect(self.db_req_url)
        self.socket_analitica.setsockopt(zmq.RCVTIMEO, 3000)
        self.socket_db.setsockopt(zmq.RCVTIMEO, 3000)
        print(f"[Monitoreo-PC2] Failover activo. Analitica={self.req_analitica_url} DBReplica={self.db_req_url}")

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
            try:
                if socket is self.socket_analitica:
                    self.socket_analitica.close()
                    self.socket_analitica = self.context.socket(zmq.REQ)
                    self.socket_analitica.setsockopt(zmq.RCVTIMEO, 3000)
                    self.socket_analitica.connect(self.req_analitica_url)
                elif socket is self.socket_db:
                    self.socket_db.close()
                    self.socket_db = self.context.socket(zmq.REQ)
                    self.socket_db.setsockopt(zmq.RCVTIMEO, 3000)
                    self.socket_db.connect(self.db_req_url)
            except Exception:
                pass

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
