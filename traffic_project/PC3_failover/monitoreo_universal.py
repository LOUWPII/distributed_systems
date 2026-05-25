"""
    Desarrollado por: Juan Felipe Gomez, Sebastian Gaibor y David Beltran Gomez
    Cliente universal de monitoreo y consulta ejecutable desde cualquier nodo (PC2 o PC3).
    Detecta automaticamente si PC3 esta disponible; si no, conmuta transparentemente a PC2.
    Mantiene todas las consultas originales y el menu interactivo del monitoreo central.
    Implementa reconstruccion de sockets ante timeouts y failover en caliente.
    Utiliza patron Request-Reply sobre ZeroMQ para arquitectura distribuida.
"""

import json
import os
import sys
from datetime import datetime

import zmq

CONFIG_CANDIDATES = [
    os.path.join(os.path.dirname(__file__), "config.json"),
    os.path.join(os.path.dirname(__file__), "..", "PC2", "config", "config.json"),
    os.path.join(os.path.dirname(__file__), "..", "config", "config.json"),
]


def load_config():
    config_path = None
    for candidate in CONFIG_CANDIDATES:
        if os.path.exists(candidate):
            config_path = candidate
            break
    if config_path is None:
        print("[Monitoreo] Error: No se encontro archivo de configuracion.")
        sys.exit(1)

    with open(config_path, "r", encoding="utf-8") as file:
        data = json.load(file)

    red = data["red"]
    url_analitica_pc3 = red["analitica_monitoreo_url_REQ"]
    url_db_pc3 = red["monitoreo_bd_principal_url_REQ"]
    url_analitica_pc2 = red.get("monitoreo_analitica_url_REQ", url_analitica_pc3)
    url_db_pc2 = red.get("monitoreo_bd_replica_url_REQ", url_db_pc3)
    return url_analitica_pc3, url_db_pc3, url_analitica_pc2, url_db_pc2, config_path


class MonitoreoUniversal:
    def __init__(self):
        self.context = zmq.Context()
        (
            self.url_analitica_pc3,
            self.url_db_pc3,
            self.url_analitica_pc2,
            self.url_db_pc2,
            self.config_path,
        ) = load_config()
        self.timeout_ms = 3000
        self.modo = "PC3"
        self.socket_analitica = None
        self.socket_db = None
        self._detectar_nodo_activo()
        print(f"[Monitoreo] Inicializado. Modo activo: {self.modo}")
        print(f"[Monitoreo] Config usada: {self.config_path}")

    def _crear_socket_req(self, url):
        socket = self.context.socket(zmq.REQ)
        socket.setsockopt(zmq.RCVTIMEO, self.timeout_ms)
        socket.setsockopt(zmq.LINGER, 0)
        socket.connect(url)
        return socket

    def _detectar_nodo_activo(self):
        try:
            test = self._crear_socket_req(self.url_analitica_pc3)
            test.send_string('{"tipo":"CONSULTA_TODOS_ESTADOS"}')
            test.recv_string()
            test.close()
            self.modo = "PC3"
        except Exception:
            self.modo = "PC2"
        self._reconectar_sockets()

    def _reconectar_sockets(self):
        if self.socket_analitica:
            try:
                self.socket_analitica.close()
            except Exception:
                pass
        if self.socket_db:
            try:
                self.socket_db.close()
            except Exception:
                pass
        url_a = self.url_analitica_pc3 if self.modo == "PC3" else self.url_analitica_pc2
        url_d = self.url_db_pc3 if self.modo == "PC3" else self.url_db_pc2
        self.socket_analitica = self._crear_socket_req(url_a)
        self.socket_db = self._crear_socket_req(url_d)
        print(f"[Monitoreo] Conectado a Analitica en {url_a}")
        print(f"[Monitoreo] Conectado a BD en {url_d}")

    def _timestamp(self):
        return datetime.now().isoformat(timespec="seconds")

    def _imprimir_resultado(self, titulo, payload):
        print(f"[{self._timestamp()}][Monitoreo][RESULTADO] {titulo}")
        print(json.dumps(payload, indent=2, ensure_ascii=False))

    def _enviar_peticion(self, socket_name, request, descripcion_operacion, destino):
        socket = getattr(self, socket_name)
        print(f"\n[{self._timestamp()}][Monitoreo][OP] {descripcion_operacion}")
        print(
            f"[{self._timestamp()}][Monitoreo][REQ -> {destino}({self.modo})] "
            f"{json.dumps(request, ensure_ascii=False)}"
        )
        socket.send_string(json.dumps(request))
        try:
            respuesta_str = socket.recv_string()
            respuesta = json.loads(respuesta_str)
            print(f"[{self._timestamp()}][Monitoreo][REP <- {destino}]")
            return respuesta
        except zmq.Again:
            print(
                f"[{self._timestamp()}][Monitoreo][ERROR] Timeout: {destino} no respondio."
            )
            if self.modo == "PC3" and socket_name == "socket_db":
                print("[Monitoreo][FAILOVER] Intentando conmutar a PC2...")
                try:
                    getattr(self, socket_name).close()
                except Exception:
                    pass
                self.modo = "PC2"
                self._reconectar_sockets()
                return self._enviar_peticion(
                    socket_name, request, descripcion_operacion, destino
                )
            try:
                getattr(self, socket_name).close()
            except Exception:
                pass
            url = (
                self.url_db_pc2
                if (socket_name == "socket_db" and self.modo == "PC2")
                else (
                    self.url_analitica_pc2
                    if self.modo == "PC2"
                    else self.url_analitica_pc3
                )
            )
            if socket_name == "socket_db":
                url = self.url_db_pc2 if self.modo == "PC2" else self.url_db_pc3
            setattr(self, socket_name, self._crear_socket_req(url))
            return None
        except Exception as error:
            print(
                f"[{self._timestamp()}][Monitoreo][ERROR] "
                f"Fallo en comunicacion con {destino}: {error}"
            )
            return None

    def consultar_estado_actual(self, calle_id):
        op = f"Consulta de estado trafico en tiempo real para calle {calle_id}"
        req = {"tipo": "CONSULTA_ESTADO_ACTUAL", "calle_id": calle_id}
        respuesta = self._enviar_peticion("socket_analitica", req, op, "ANALITICA")
        if respuesta:
            self._imprimir_resultado(f"Estado actual de '{calle_id}'", respuesta)

    def consultar_todos_estados(self):
        op = "Consulta global de estado en tiempo real"
        req = {"tipo": "CONSULTA_TODOS_ESTADOS"}
        respuesta = self._enviar_peticion("socket_analitica", req, op, "ANALITICA")
        if respuesta:
            self._imprimir_resultado("Estado global", respuesta)

    def consultar_interseccion_realtime(self, interseccion_id):
        op = f"Consulta de interseccion en tiempo real ({interseccion_id})"
        req = {"tipo": "CONSULTA_INTERSECCION", "interseccion_id": interseccion_id}
        respuesta = self._enviar_peticion("socket_analitica", req, op, "ANALITICA")
        if respuesta:
            self._imprimir_resultado(
                f"Interseccion '{interseccion_id}' en tiempo real", respuesta
            )

    def consultar_fechas_congestiones(self):
        op = "Consulta de fechas de congestiones"
        req = {"tipo": "CONSULTA_FECHAS_CONGESTIONES"}
        respuesta = self._enviar_peticion("socket_db", req, op, "BD_PRINCIPAL")
        if not respuesta:
            return
        if respuesta.get("estado") != "OK":
            self._imprimir_resultado("Error en consulta de congestiones", respuesta)
            return
        print(f"[{self._timestamp()}][Monitoreo][RESULTADO] Fechas de congestiones")
        print(f"Total congestiones: {respuesta.get('total_congestiones', 0)}")
        for i, item in enumerate(respuesta.get("congestiones", []), start=1):
            print(
                f"  {i:03d}. calle={item.get('calle_id', 'DESCONOCIDA')} | "
                f"estado={item.get('estado_nuevo', 'DESCONOCIDO')} | "
                f"fecha={item.get('fecha_hora', 'SIN_FECHA')}"
            )

    def consultar_cambios_semaforos(self):
        op = "Consulta de cambios de color de semaforos"
        req = {"tipo": "CONSULTA_CAMBIOS_SEMAFOROS"}
        respuesta = self._enviar_peticion("socket_db", req, op, "BD_PRINCIPAL")
        if not respuesta:
            return
        if respuesta.get("estado") != "OK":
            self._imprimir_resultado("Error en consulta de semaforos", respuesta)
            return
        print(
            f"[{self._timestamp()}][Monitoreo][RESULTADO] Cambios de color de semaforos"
        )
        print(f"Total de cambios de color: {respuesta.get('total_cambios_color', 0)}")

    def consultar_priorizaciones_ambulancia(self):
        op = "Consulta de priorizaciones de ambulancias"
        req = {"tipo": "CONSULTA_PRIORIZACIONES_AMBULANCIA"}
        respuesta = self._enviar_peticion("socket_db", req, op, "BD_PRINCIPAL")
        if not respuesta:
            return
        if respuesta.get("estado") != "OK":
            self._imprimir_resultado("Error en consulta de priorizaciones", respuesta)
            return
        print(
            f"[{self._timestamp()}][Monitoreo][RESULTADO] Priorizaciones de ambulancias"
        )
        total = respuesta.get("total_priorizaciones", 0)
        print(f"Cantidad total de priorizaciones: {total}")
        for i, item in enumerate(respuesta.get("priorizaciones", []), start=1):
            print(
                f"  {i:03d}. calle={item.get('calle_id', 'DESCONOCIDA')} | "
                f"fecha={item.get('fecha_hora', 'SIN_FECHA')} | "
                f"motivo={item.get('motivo', 'SIN_MOTIVO')}"
            )

    def enviar_comando_analitica(
        self, calle_id, accion="OLA_VERDE", duracion_s=60, motivo="ORDEN_USUARIO"
    ):
        req = {
            "tipo": "ORDEN_DIRECTA",
            "calle_id": calle_id,
            "accion": accion,
            "duracion_s": duracion_s,
            "motivo": motivo,
        }
        print(
            f"[{self._timestamp()}][Monitoreo][CMD -> ANALITICA] "
            f"tipo={req['tipo']}, accion={accion}, calle_id={calle_id}, "
            f"duracion_s={duracion_s}, motivo={motivo}"
        )
        op = f"Envio de instruccion de control ({accion}) para calle {calle_id}"
        respuesta = self._enviar_peticion("socket_analitica", req, op, "ANALITICA")
        if respuesta:
            self._imprimir_resultado("Comando a analitica", respuesta)

    def menu(self):
        while True:
            print("\n" + "=" * 64)
            print(f"         SISTEMA DE MONITOREO Y CONSULTA  [NODO: {self.modo}]")
            print("=" * 64)
            print(" [1] Enviar comando a analitica (Ej. Ambulancia)")
            print(" [2] Estado actual de una calle (Tiempo real)")
            print(" [3] Estado de TODAS las calles (Tiempo real)")
            print(" [4] Estado de interseccion puntual.")
            print(" [5] Consulta de fechas de congestiones.")
            print(" [6] Consulta de cambios de color de semaforos.")
            print(" [7] Consulta de priorizaciones de ambulancias.")
            print(" [8] Salir")
            print("=" * 64)
            opcion = input("Seleccione una opcion: ").strip()

            if opcion == "1":
                calle = input(
                    "-> ID de la calle o tramo a priorizar (ej. fila_C - col_3): "
                ).strip()
                accion = input("-> Accion (default OLA_VERDE): ").strip() or "OLA_VERDE"
                duracion_str = input("-> Duracion en segundos (Default 60s): ").strip()
                duracion = int(duracion_str) if duracion_str.isdigit() else 60
                motivo = (
                    input("-> Motivo (Default EMERGENCIA_AMBULANCIA): ").strip()
                    or "EMERGENCIA_AMBULANCIA"
                )
                self.enviar_comando_analitica(calle, accion, duracion, motivo)
            elif opcion == "2":
                calle = input(
                    "-> ID de la calle o tramo (ej. fila_C - col_3): "
                ).strip()
                self.consultar_estado_actual(calle)
            elif opcion == "3":
                self.consultar_todos_estados()
            elif opcion == "4":
                interseccion = input(
                    "-> ID de la interseccion (ej. INT_C2): "
                ).strip()
                self.consultar_interseccion_realtime(interseccion)
            elif opcion == "5":
                self.consultar_fechas_congestiones()
            elif opcion == "6":
                self.consultar_cambios_semaforos()
            elif opcion == "7":
                self.consultar_priorizaciones_ambulancia()
            elif opcion == "8":
                print("\n[Monitoreo] Cerrando servicio de Monitoreo y Consulta. Adios.")
                break
            else:
                print("\n[Monitoreo] Opcion invalida. Intente de nuevo.\n")


if __name__ == "__main__":
    try:
        monitoreo = MonitoreoUniversal()
        monitoreo.menu()
    except KeyboardInterrupt:
        print("\n[Monitoreo] Cierre por teclado detectado. Saliendo...")
    except Exception as error:
        print(f"\n[Monitoreo] Error fatal: {error}")
