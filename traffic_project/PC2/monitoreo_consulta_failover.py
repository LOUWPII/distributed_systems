import json
import os
import sys
from datetime import datetime
from urllib.parse import urlparse

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
        self.db_req_url = self.req_db_replica_url.replace("://*:", "://localhost:")
        self.socket_analitica = self.context.socket(zmq.REQ)
        self.socket_analitica.connect(self.req_analitica_url)
        self.socket_db = self.context.socket(zmq.REQ)
        self.socket_db.connect(self.db_req_url)
        self.analitica_fallback_url = self._build_local_fallback(self.req_analitica_url)
        self.socket_analitica.setsockopt(zmq.RCVTIMEO, 3000)
        self.socket_db.setsockopt(zmq.RCVTIMEO, 3000)
        print(f"[Monitoreo-PC2] Failover activo. Analitica={self.req_analitica_url} DBReplica={self.db_req_url}")
        if self.analitica_fallback_url:
            print(f"[Monitoreo-PC2] Fallback Analitica habilitado en {self.analitica_fallback_url}")

    def _build_local_fallback(self, url):
        try:
            parsed = urlparse(url)
            if parsed.scheme != "tcp" or parsed.port is None:
                return None
            host = parsed.hostname or ""
            if host in ("localhost", "127.0.0.1"):
                return None
            return f"tcp://localhost:{parsed.port}"
        except Exception:
            return None

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
            if destino == "ANALITICA" and self.analitica_fallback_url:
                print(f"[{self._ts()}][Monitoreo-PC2][RETRY] Reintentando ANALITICA via {self.analitica_fallback_url}")
                retry_socket = self.context.socket(zmq.REQ)
                retry_socket.setsockopt(zmq.RCVTIMEO, 3000)
                retry_socket.connect(self.analitica_fallback_url)
                try:
                    retry_socket.send_string(json.dumps(req))
                    rep = json.loads(retry_socket.recv_string())
                    print(f"[{self._ts()}][Monitoreo-PC2][REP<-ANALITICA fallback] OK")
                    print(json.dumps(rep, indent=2, ensure_ascii=False))
                except Exception as retry_err:
                    print(f"[{self._ts()}][Monitoreo-PC2][ERROR] Retry ANALITICA: {retry_err}")
                finally:
                    retry_socket.close()

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
