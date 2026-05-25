"""
    Desarollado por: Juan Felipe Gomez, Sebastian Gaibor y David Beltran Gomez
    Cliente interactivo de monitoreo de respaldo ejecutado en PC2 durante escenarios de failover.
    Establece conexiones ZeroMQ REQ hacia Analítica y hacia la base de datos réplica.
    Construye solicitudes JSON estructuradas y registra trazabilidad completa de operaciones REQ/REP.
    Consulta estado operativo en tiempo real y datos persistidos desde almacenamiento secundario.
    Reconstruye sockets automáticamente ante timeouts o fallos de comunicación distribuidos.
    Funciona como interfaz integrada al esquema de alta disponibilidad del sistema.
    Implementa patrón Request-Reply para supervisión operativa distribuida.
"""

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
    # El constructor de la clase MonitoreoConsultaPC2 se encarga de cargar las URLs de los servicios de Analítica y la base de datos réplica desde el archivo de configuración,
    # estableciendo conexiones REQ hacia ambos servicios y configurando timeouts para garantizar la capacidad de respuesta del monitoreo incluso en escenarios de fallo.
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

    # El método _ts se utiliza para generar una marca de tiempo formateada que se incluye en los logs de monitoreo, facilitando la trazabilidad de las operaciones realizadas.
    def _ts(self):
        return datetime.now().isoformat(timespec="seconds")

    # El método _send se encarga de enviar una solicitud JSON a través del socket especificado, registrando la operación en los logs 
    # y manejando cualquier error de comunicación que pueda ocurrir,
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

    # El método menu presenta un menú interactivo al usuario, permitiendo seleccionar diferentes consultas para monitorear el estado del sistema durante el failover,
    # y utilizando el método _send para enviar las solicitudes correspondientes a Analítica o a la base de datos réplica según la opción seleccionada,
    # registrando todas las operaciones en los logs para facilitar la trazabilidad y el diagnóstico.
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
