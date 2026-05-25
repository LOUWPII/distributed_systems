"""
    Desarollado por: Juan Felipe Gomez, Sebastian Gaibor y David Beltran Gomez
    Interfaz central de monitoreo y control operativo ejecutada desde el nodo PC3.
    Establece comunicación síncrona ZeroMQ REQ hacia Analítica y la base de datos principal.
    Construye solicitudes JSON para consultas, monitoreo en tiempo real y control manual.
    Registra operaciones REQ/REP y presenta resultados para auditoría operativa.
    Implementa recuperación automática recreando sockets ante timeouts o errores de transporte.
    Se comunica con Analítica para control vial y con persistencia para consultas históricas.
    Utiliza patrón Request-Reply interactivo dentro de una arquitectura distribuida.
"""


import json
import os
import sys
from datetime import datetime

import zmq

CONFIG_CANDIDATES = [
    os.path.join(os.path.dirname(__file__), "config.json"),
    os.path.join(os.path.dirname(__file__), "..", "PC2", "config", "config.json"),
]

# La función load_urls se encarga de buscar el archivo de configuración en varias ubicaciones posibles,
# cargar las URLs de los servicios de Analítica y la base de datos principal,
def load_urls():
    config_path = None
    # Iterar sobre las rutas candidatas para encontrar el archivo de configuración, asegurando que el servicio pueda localizar la 
    # configuración necesaria incluso si se ejecuta desde diferentes ubicaciones dentro del proyecto.
    for candidate in CONFIG_CANDIDATES:
        if os.path.exists(candidate):
            config_path = candidate
            break
    # Si no se encuentra el archivo de configuración, se muestra un mensaje de error y se termina la ejecución del programa,
    # ya que la configuración es esencial para el funcionamiento del servicio.
    if config_path is None:
        print("[Monitoreo] Error: No se encontro archivo de configuracion.")
        sys.exit(1)

    # Cargar las URLs de los servicios desde el archivo de configuración encontrado,
    # asegurando que el servicio tenga la información necesaria para establecer las conexiones REQ.
    with open(config_path, "r", encoding="utf-8") as file:
        data = json.load(file)

    req_analitica_url = data["red"]["analitica_monitoreo_url_REQ"]
    req_db_url = data["red"]["monitoreo_bd_principal_url_REQ"]
    return req_analitica_url, req_db_url, config_path


class MonitoreoConsulta:
    
    # El constructor de la clase MonitoreoConsulta se encarga de cargar las URLs de los servicios de Analítica y la base de datos principal desde el archivo de configuración,
    # estableciendo conexiones REQ hacia ambos servicios y configurando timeouts para garantizar la capacidad de respuesta del monitoreo incluso en escenarios de fallo.
    def __init__(self):
        self.context = zmq.Context()
        self.req_analitica_url, self.req_db_url, self.config_path = load_urls()
        self.timeout_ms = 3000

        self.socket_analitica = self._crear_socket_req(self.req_analitica_url)
        self.socket_db = self._crear_socket_req(self.req_db_url)

        print(f"[Monitoreo] Inicializado. Conectado a Analitica en {self.req_analitica_url}")
        print(f"[Monitoreo] Conectado a BD Principal en {self.req_db_url}")
        print(f"[Monitoreo] Config usada: {self.config_path}")

    # El método _crear_socket_req se encarga de crear y configurar un socket REQ para la comunicación con los servicios de Analítica o la base de datos principal,
    # estableciendo los timeouts necesarios para garantizar que el monitoreo no se bloquee en caso de que alguno de los servicios no responda,
    # y asegurando que los recursos se manejen de manera eficiente.
    def _crear_socket_req(self, url):
        socket = self.context.socket(zmq.REQ)
        socket.setsockopt(zmq.RCVTIMEO, self.timeout_ms)
        socket.setsockopt(zmq.LINGER, 0)
        socket.connect(url)
        return socket

    # El método _verificar_conexion se utiliza para enviar un mensaje de prueba (PING) a través del socket REQ y esperar una respuesta (PONG),
    # verificando así que el servicio de destino está disponible y respondiendo correctamente, y manejando cualquier error de comunicación que
    # pueda ocurrir para mantener la robustez del monitoreo.
    def _timestamp(self):
        return datetime.now().isoformat(timespec="seconds")

    def _imprimir_resultado(self, titulo, payload):
        print(f"[{self._timestamp()}][Monitoreo][RESULTADO] {titulo}")
        print(json.dumps(payload, indent=2, ensure_ascii=False))

    # El método _enviar_peticion se encarga de enviar una solicitud JSON a través del socket especificado, registrando la operación en los logs
    # y manejando cualquier error de comunicación que pueda ocurrir, incluyendo timeouts o fall de transporte, para garantizar que el monitoreo
    # pueda recuperarse automáticamente recreando los sockets según sea necesario.
    def _enviar_peticion(self, socket_name, request, descripcion_operacion, destino):
        socket = getattr(self, socket_name)
        print(f"\n[{self._timestamp()}][Monitoreo][OP] {descripcion_operacion}")
        print(f"[{self._timestamp()}][Monitoreo][REQ -> {destino}] {json.dumps(request, ensure_ascii=False)}")

        socket.send_string(json.dumps(request))
        try:
            respuesta_str = socket.recv_string()
            respuesta = json.loads(respuesta_str)
            print(f"[{self._timestamp()}][Monitoreo][REP <- {destino}]")
            return respuesta
        except zmq.Again:
            print(f"[{self._timestamp()}][Monitoreo][ERROR] Timeout: {destino} no respondio.")
            try:
                getattr(self, socket_name).close()
            except Exception:
                pass
            setattr(self, socket_name, self._crear_socket_req(self.req_db_url if socket_name == "socket_db" else self.req_analitica_url))
            return None
        except Exception as error:
            print(f"[{self._timestamp()}][Monitoreo][ERROR] Fallo en comunicacion con {destino}: {error}")
            return None

    # El método consultar_estado_actual se encarga de construir una solicitud para consultar el estado actual del tráfico en una calle específica,
    # enviarla a Analítica a través del socket REQ, y presentar la respuesta de manera estructurada en los logs de monitoreo, facilitando la interpretación de la información recibida.
    def consultar_estado_actual(self, calle_id):
        op = f"Consulta de estado trafico en tiempo real para calle {calle_id}"
        req = {"tipo": "CONSULTA_ESTADO_ACTUAL", "calle_id": calle_id}
        respuesta = self._enviar_peticion("socket_analitica", req, op, "ANALITICA")
        if respuesta:
            self._imprimir_resultado(f"Estado actual de '{calle_id}'", respuesta)

    # El método consultar_todos_estados se encarga de construir una solicitud para consultar el estado actual del tráfico en todas las calles,
    # enviarla a Analítica a través del socket REQ, y presentar la respuesta de manera estructurada en los logs de monitoreo, facilitando la interpretación de la información recibida.
    def consultar_todos_estados(self):
        op = "Consulta global de estado en tiempo real"
        req = {"tipo": "CONSULTA_TODOS_ESTADOS"}
        respuesta = self._enviar_peticion("socket_analitica", req, op, "ANALITICA")
        if respuesta:
            self._imprimir_resultado("Estado global", respuesta)

    # El método consultar_interseccion_realtime se encarga de construir una solicitud para consultar el estado actual de una intersección específica,
    # enviarla a Analítica a través del socket REQ, y presentar la respuesta de manera estructurada en los logs de monitoreo, facilitando la interpretación de la información recibida.
    def consultar_interseccion_realtime(self, interseccion_id):
        op = f"Consulta de interseccion en tiempo real ({interseccion_id})"
        req = {"tipo": "CONSULTA_INTERSECCION", "interseccion_id": interseccion_id}
        respuesta = self._enviar_peticion("socket_analitica", req, op, "ANALITICA")
        if respuesta:
            self._imprimir_resultado(f"Interseccion '{interseccion_id}' en tiempo real", respuesta)

    # El método consultar_fechas_congestiones se encarga de construir una solicitud para consultar las fechas de congestiones registradas en la base de datos principal,
    # enviarla a través del socket REQ, y presentar la respuesta de manera estructurada en los logs de monitoreo, facilitando la interpretación de la información recibida.
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
        # Iterar sobre la lista de congestiones recibida en la respuesta y presentar cada una de ellas de manera estructurada, mostrando la calle,
        # el nuevo estado y la fecha/hora de cada congestión registrada.
        for i, item in enumerate(respuesta.get("congestiones", []), start=1):
            print(
                f"  {i:03d}. calle={item.get('calle_id', 'DESCONOCIDA')} | "
                f"estado={item.get('estado_nuevo', 'DESCONOCIDO')} | "
                f"fecha={item.get('fecha_hora', 'SIN_FECHA')}"
            )

    # El método consultar_cambios_semaforos se encarga de construir una solicitud para consultar los cambios de color de semáforos registrados en la base de datos principal,
    # enviarla a través del socket REQ, y presentar la respuesta de manera estructurada en los logs de monitoreo, facilitando la interpretación de la información recibida.
    def consultar_cambios_semaforos(self):
        op = "Consulta de cambios de color de semaforos"
        req = {"tipo": "CONSULTA_CAMBIOS_SEMAFOROS"}
        respuesta = self._enviar_peticion("socket_db", req, op, "BD_PRINCIPAL")
        if not respuesta:
            return
        if respuesta.get("estado") != "OK":
            self._imprimir_resultado("Error en consulta de semaforos", respuesta)
            return
        print(f"[{self._timestamp()}][Monitoreo][RESULTADO] Cambios de color de semaforos")
        print(f"Total de cambios de color: {respuesta.get('total_cambios_color', 0)}")

    # El método consultar_priorizaciones_ambulancia se encarga de construir una solicitud para consultar las priorizaciones realizadas para ambulancias registradas en la base de datos principal,
    # enviarla a través del socket REQ, y presentar la respuesta de manera estructurada en los logs de monitoreo, facilitando la interpretación de la información recibida.
    def consultar_priorizaciones_ambulancia(self):
        op = "Consulta de priorizaciones de ambulancias"
        req = {"tipo": "CONSULTA_PRIORIZACIONES_AMBULANCIA"}
        respuesta = self._enviar_peticion("socket_db", req, op, "BD_PRINCIPAL")
        if not respuesta:
            return
        if respuesta.get("estado") != "OK":
            self._imprimir_resultado("Error en consulta de priorizaciones", respuesta)
            return

        print(f"[{self._timestamp()}][Monitoreo][RESULTADO] Priorizaciones de ambulancias")
        total = respuesta.get("total_priorizaciones", 0)
        print(f"Cantidad total de priorizaciones: {total}")
        # Iterar sobre la lista de priorizaciones recibida en la respuesta y presentar cada una de ellas de manera estructurada, mostrando la calle, la fecha/hora y el motivo de cada priorización realizada para ambulancias.
        for i, item in enumerate(respuesta.get("priorizaciones", []), start=1):
            print(
                f"  {i:03d}. calle={item.get('calle_id', 'DESCONOCIDA')} | "
                f"fecha={item.get('fecha_hora', 'SIN_FECHA')} | "
                f"motivo={item.get('motivo', 'SIN_MOTIVO')}"
            )

    # El método enviar_comando_analitica se encarga de construir una solicitud para enviar un comando de control a Analítica, incluyendo la calle a priorizar, 
    # la acción a realizar, la duración y el motivo, enviarla a través del socket REQ, y presentar la respuesta de manera estructurada en los logs de monitoreo,
    # facilitando la interpretación de la información recibida y el seguimiento de las acciones realizadas sobre el sistema de control vial.
    def enviar_comando_analitica(self, calle_id, accion="OLA_VERDE", duracion_s=60, motivo="ORDEN_USUARIO"):
        req = {
            "tipo": "ORDEN_DIRECTA",
            "calle_id": calle_id,
            "accion": accion,
            "duracion_s": duracion_s,
            "motivo": motivo,
        }
        print(
            f"[{self._timestamp()}][Monitoreo][CMD -> ANALITICA] "
            f"tipo={req['tipo']}, accion={accion}, calle_id={calle_id}, duracion_s={duracion_s}, motivo={motivo}"
        )
        op = f"Envio de instruccion de control ({accion}) para calle {calle_id}"
        respuesta = self._enviar_peticion("socket_analitica", req, op, "ANALITICA")
        if respuesta:
            self._imprimir_resultado("Comando a analitica", respuesta)

    # El método consultar_tramo_critico_calle se encarga de construir una solicitud para consultar el tramo más crítico de una calle macro específica,
    # enviarla a Analítica a través del socket REQ, y presentar la respuesta de manera estructurada en los logs de monitoreo, facilitando la interpretación de la información recibida.
    def menu(self):
        while True:
            print("\n" + "=" * 64)
            print("         SISTEMA DE MONITOREO Y CONSULTA")
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
                calle = input("-> ID de la calle o tramo a priorizar (ej. fila_C - col_3): ").strip()
                accion = input("-> Accion (default OLA_VERDE): ").strip() or "OLA_VERDE"
                duracion_str = input("-> Duracion en segundos (Default 60s): ").strip()
                duracion = int(duracion_str) if duracion_str.isdigit() else 60
                motivo = input("-> Motivo (Default EMERGENCIA_AMBULANCIA): ").strip() or "EMERGENCIA_AMBULANCIA"
                self.enviar_comando_analitica(calle, accion, duracion, motivo)
            elif opcion == "2":
                calle = input("-> ID de la calle o tramo (ej. fila_C - col_3): ").strip()
                self.consultar_estado_actual(calle)
            elif opcion == "3":
                self.consultar_todos_estados()
            elif opcion == "4":
                interseccion = input("-> ID de la interseccion (ej. INT_C2): ").strip()
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
        monitoreo = MonitoreoConsulta()
        monitoreo.menu()
    except KeyboardInterrupt:
        print("\n[Monitoreo] Cierre por teclado detectado. Saliendo...")
    except Exception as error:
        print(f"\n[Monitoreo] Error fatal: {error}")
