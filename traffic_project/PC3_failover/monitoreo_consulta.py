import zmq
import json
import os
import sys

# Determinar ruta dinámica a config.json (está en PC2/config)
CONFIG_FILE = os.path.join(os.path.dirname(__file__), "..", "PC2", "config", "config.json")

def load_urls():
    if not os.path.exists(CONFIG_FILE):
        print(f"[Monitoreo] Error: No se encontró el config en {CONFIG_FILE}")
        sys.exit(1)
        
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
        
    req_analitica_url = data["red"]["analitica_monitoreo_url_REQ"]
    req_db_url = data["red"]["monitoreo_bd_principal_url_REQ"]
    return req_analitica_url, req_db_url

class MonitoreoConsulta:
    """
    Servicio de Monitoreo y Consulta (PC3).
    Permite al usuario interactuar con la Analítica (PC2) para ver estado o forzar luz verde,
    y con la Base de Datos (PC3) para consultas históricas.
    Usa REQ/REP sobre ZeroMQ.
    """
    def __init__(self):
        self.context = zmq.Context()
        self.req_analitica_url, self.req_db_url = load_urls()
        
        # Socket REQ para interactuar con Analítica (PC2)
        self.socket_analitica = self.context.socket(zmq.REQ)
        self.socket_analitica.connect(self.req_analitica_url)
        
        # Socket REQ para interactuar con la Base de Datos Históricas (PC3)
        self.socket_db = self.context.socket(zmq.REQ)
        # La DB levanta el puerto en modo bind() y nosotros hacemos connect()
        self.socket_db.connect(self.req_db_url)
        
        # Tiempo de espera (timeout) de 3 segundos para evitar bloqueos
        self.timeout_ms = 3000
        self.socket_analitica.setsockopt(zmq.RCVTIMEO, self.timeout_ms)
        self.socket_db.setsockopt(zmq.RCVTIMEO, self.timeout_ms)
        
        print(f"[Monitoreo] Inicializado. Conectado a Analítica en {self.req_analitica_url}")
        print(f"[Monitoreo] Conectado a BD Principal en {self.req_db_url}")

    def _enviar_peticion(self, socket, request, descripcion_operacion=""):
        # Imprime la operación de acuerdo a los requerimientos
        print(f"\n[Monitoreo][OP] >> {descripcion_operacion}")
        print(f"[Monitoreo][TX] Enviando: {request}")
        
        socket.send_string(json.dumps(request))
        try:
            respuesta_str = socket.recv_string()
            # Omitimos imprimir json gigante para legibilidad, pero parseamos
            respuesta = json.loads(respuesta_str)
            print("[Monitoreo][RX] Respuesta recibida exitosamente desde el servidor.")
            return respuesta
        except zmq.Again:
            print("[Monitoreo] ❌ Timeout: El servidor no respondió a la solicitud.")
            return None
        except Exception as e:
            print(f"[Monitoreo] ❌ Fallo en la comunicación: {e}")
            return None

    def consultar_estado_actual(self, calle_id):
        op = f"Consulta de estado tráfico en la calle {calle_id}"
        req = {"tipo": "CONSULTA_ESTADO_ACTUAL", "calle_id": calle_id}
        respuesta = self._enviar_peticion(self.socket_analitica, req, op)
        if respuesta:
            print(f"\n>> ESTADO '{calle_id}':\n{json.dumps(respuesta, indent=2)}\n")

    def consultar_todos_estados(self):
        op = "Consulta general del estado de todas las calles"
        req = {"tipo": "CONSULTA_TODOS_ESTADOS"}
        respuesta = self._enviar_peticion(self.socket_analitica, req, op)
        if respuesta:
            print(f"\n>> ESTADO GLOBAL:\n{json.dumps(respuesta, indent=2)}\n")

    def consultar_interseccion(self, int_id):
        op = f"Consulta puntual de información sobre la intersección {int_id}"
        req = {"tipo": "CONSULTA_INTERSECCION", "interseccion_id": int_id}
        respuesta = self._enviar_peticion(self.socket_analitica, req, op)
        if respuesta:
            print(f"\n>> INFO INTERSECCIÓN '{int_id}':\n{json.dumps(respuesta, indent=2)}\n")

    def forzar_estado_ambulancia(self, calle_id, duracion_s):
        op = f"Forzando OLA VERDE prioritaria para emergencia/ambulancia en {calle_id} por {duracion_s}s"
        req = {
            "tipo": "ORDEN_DIRECTA",
            "calle_id": calle_id,
            "accion": "OLA_VERDE",
            "duracion_s": duracion_s,
            "motivo": "Emergencia - Ambulancia"
        }
        respuesta = self._enviar_peticion(self.socket_analitica, req, op)
        if respuesta:
            print(f"\n>> RESULTADO FORZADO:\n{json.dumps(respuesta, indent=2)}\n")

    def consultar_historico_db(self, fecha_inicio, fecha_fin):
        op = f"Consulta histórica a la BD entre periodos: {fecha_inicio} y {fecha_fin}"
        req = {
            "tipo": "CONSULTA_HISTORICA",
            "inicio": fecha_inicio,
            "fin": fecha_fin
        }
        respuesta = self._enviar_peticion(self.socket_db, req, op)
        if respuesta:
            print(f"\n>> REPORTE HISTÓRICO:\n{json.dumps(respuesta, indent=2)}\n")

    def menu(self):
        while True:
            print("=" * 60)
            print("      SISTEMA DE MONITOREO Y CONSULTA INTERACTIVA  ")
            print("=" * 60)
            print(" [1] Consultar estado actual de una calle        (REQ Analítica)")
            print(" [2] Consultar estado de TODAS las calles        (REQ Analítica)")
            print(" [3] Consultar estado de una intersección física (REQ Analítica)")
            print(" [4] Priorizar Ambulancia (Forzar Ola Verde)     (REQ Analítica)")
            print(" [5] Consultar histórica base de datos           (REQ DB Principal)")
            print(" [6] Salir")
            print("=" * 60)
            opcion = input("Seleccione una opción: ")

            if opcion == "1":
                calle = input("-> ID de la calle o tramo (ej. fila_C): ")
                self.consultar_estado_actual(calle)
            elif opcion == "2":
                self.consultar_todos_estados()
            elif opcion == "3":
                int_id = input("-> ID de la intersección (ej. INT_C5): ")
                self.consultar_interseccion(int_id)
            elif opcion == "4":
                calle = input("-> ID de la calle para ola verde prioritaria (ej. fila_C): ")
                d_str = input("-> Duración de la ola verde en seg (default 60): ")
                duracion = int(d_str) if d_str.isdigit() else 60
                self.forzar_estado_ambulancia(calle, duracion)
            elif opcion == "5":
                print("-> Sugerencia de formato: '2026-04-01T00:00:00Z'")
                f_inicio = input("-> Fecha Inicio (ISO8601): ")
                f_fin = input("-> Fecha Fin    (ISO8601): ")
                self.consultar_historico_db(f_inicio, f_fin)
            elif opcion == "6":
                print("\n[Monitoreo] Cerrando CLI interactivo. ¡Adiós!")
                break
            else:
                print("\n[Error] Opción inválida. Intente de nuevo.\n")


if __name__ == "__main__":
    try:
        monitoreo = MonitoreoConsulta()
        monitoreo.menu()
    except KeyboardInterrupt:
        print("\n[Monitoreo] Cierre forzado via teclado detectado. Saliendo...")
    except Exception as e:
        print(f"\n[Monitoreo] Error fatal: {e}")
