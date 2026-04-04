import json
import threading
import zmq
from config import Config
# Imports desde dominio/application
from dominio.orden_directa import OrdenDirecta
from enums import EstadoTrafico
from application.rules_engine import RulesEngine

# Hilo que atiende solicitudes síncronas del usuario desde PC3.
class QueryHandler(threading.Thread):
    def __init__(
        self,
        config: Config,
        rules_engine: RulesEngine,
    ):
        super().__init__(daemon=True, name="QueryHandler")
        self._config = config
        self._rules_engine = rules_engine
        self._activo = True
        self._contexto_zmq = zmq.Context.instance()
        self._socket = None

    # Ciclo principal del hilo
    def run(self) -> None:
        # Crea un socket REP para recibir solicitudes del usuario
        self._socket = self._contexto_zmq.socket(zmq.REP)
        self._socket.bind(self._config.query_handler_url)
        print(f"[QueryHandler] Escuchando en {self._config.query_handler_url}")

        # Mientras el hilo esté activo, recibe solicitudes del usuario
        while self._activo:
            try:
                # recv() bloquea hasta que llegue una solicitud de PC3
                mensaje = self._socket.recv_string()
                solicitud = self._parse_request(mensaje)

                # Si la solicitud es inválida, retorna un error
                if solicitud is None:
                    respuesta = self._error("Solicitud mal formada")
                else:
                    respuesta = self.atender_consulta(solicitud)

                # En REP siempre hay que responder antes del siguiente recv()
                self._socket.send_string(json.dumps(respuesta))

            # Si hay un error ZMQ, lo imprime
            except zmq.ZMQError as e:
                if self._activo:
                    print(f"[QueryHandler] Error ZMQ: {e}")

        print("[QueryHandler] Detenido")

    # Detiene el hilo
    def detener(self) -> None:
        self._activo = False
        if self._socket:
            self._socket.close()

    # Atiende la solicitud y la enruta al manejador correcto según su tipo.
    def atender_consulta(self, solicitud: dict) -> dict:
        # Obtiene el tipo de solicitud
        tipo = solicitud.get("tipo", "")

        # Diccionario de manejadores
        manejadores = {
            "CONSULTA_ESTADO_ACTUAL": self._handle_estado_actual,
            "CONSULTA_TODOS_ESTADOS": self._handle_todos_estados,
            "CONSULTA_INTERSECCION": self._handle_interseccion,
            "ORDEN_DIRECTA": self._handle_orden_directa,
        }

        # Obtiene el manejador correspondiente al tipo de solicitud
        handler = manejadores.get(tipo)
        # Si no existe el manejador, retorna un error
        if handler is None:
            return self._error(f"Tipo de solicitud desconocido: {tipo}")

        try:
            return handler(solicitud)
        except Exception as e:
            return self._error(f"Error procesando solicitud: {e}")

    # Ejecuta una OrdenDirecta
    def ejecutar_orden(self, orden: OrdenDirecta) -> None:
        # Registra y ejecuta una OrdenDirecta.
        # Llamado internamente por _handle_orden_directa().
        self._rules_engine.registrar_orden(orden)

        # Manejador para consulta de estado actual
    def _handle_estado_actual(self, solicitud: dict) -> dict:
        """
        Retorna el estado de tráfico. 
        Ahora soporta IDs de tramos específicos (INT_XX_calle_YY) 
        o IDs de calles generales (calle_YY).
        """
        calle_id = solicitud.get("calle_id")
        if not calle_id:
            return self._error("Falta campo calle_id")

        # 1. Intentar buscar tramo específico en el diccionario de intersecciones
        # Formato esperado: "INT_C1_fila_C"
        for int_id, dic_calles in self._rules_engine._estados_interseccion.items():
            for key_calle, estado in dic_calles.items():
                if calle_id == f"{int_id}_{key_calle}":
                    print(f"[QueryHandler] Consulta tramo exacto: {calle_id}")
                    return self._ok({"calle": estado.to_registro()})

        # 2. Si es una calle general (ej: "fila_C"), buscar el tramo con PEOR tráfico
        # Esto mantiene compatibilidad con el dashboard macro de PC3
        tramos_calle = []
        for int_id, dic_calles in self._rules_engine._estados_interseccion.items():
            if calle_id in dic_calles:
                tramos_calle.append(dic_calles[calle_id])

        if not tramos_calle:
            return self._error(f"No se encontraron tramos para la calle: {calle_id}")

        # Ordenar por severidad: CONGESTION > OLA_VERDE > NORMAL
        # Devolvemos el tramo más crítico para avisar al usuario
        peor_tramo = sorted(
            tramos_calle, 
            key=lambda x: (x.estado == EstadoTrafico.CONGESTION, x.ultima_cola), 
            reverse=True
        )[0]

        print(f"[QueryHandler] Consulta calle macro {calle_id} -> Reportando tramo más crítico ({peor_tramo.calle_id})")
        return self._ok({"calle": peor_tramo.to_registro()})


    # Manejador para consulta de todos los estados
    def _handle_todos_estados(self, solicitud: dict) -> dict:
        # Retorna el estado actual de todas las calles del sistema.
        estados = self._rules_engine.get_todos_estados()
        print(f"[QueryHandler] Consulta todos los estados ({len(estados)} calles)")
        return self._ok({"calles": estados})

    # Manejador para consulta de intersección
    def _handle_interseccion(self, solicitud: dict) -> dict:
        # Retorna el estado de una intersección específica con sus semáforos.
        int_id = solicitud.get("interseccion_id")
        # Si no se proporciona interseccion_id, retorna un error
        if not int_id:
            return self._error("Falta campo interseccion_id")

        # Obtiene la intersección
        interseccion = self._rules_engine.get_interseccion(int_id)
        # Si no se encuentra la intersección, retorna un error
        if interseccion is None:
            return self._error(f"Intersección no encontrada: {int_id}")

        print(f"[QueryHandler] Consulta intersección: {interseccion}")
        return self._ok({"interseccion": interseccion.to_registro()})

    # Crea y ejecuta una OrdenDirecta
    def _handle_orden_directa(self, solicitud: dict) -> dict:
        calle_id = solicitud.get("calle_id")
        accion_str = solicitud.get("accion", "OLA_VERDE")
        duracion_s = int(solicitud.get("duracion_s", 60))
        motivo = solicitud.get("motivo", "ORDEN_USUARIO")

        # Si no se proporciona calle_id, retorna un error
        if not calle_id:
            return self._error("Falta campo calle_id en orden directa")

        # Convierte la acción a EstadoTrafico
        try:
            accion = EstadoTrafico(accion_str)
        except ValueError:
            return self._error(f"Acción desconocida: {accion_str}")

        # Crea la OrdenDirecta
        orden = OrdenDirecta(
            calle_id = calle_id,
            accion = accion,
            duracion_s = duracion_s,
            motivo = motivo,
        )

        # Ejecuta la OrdenDirecta
        self.ejecutar_orden(orden)

        # Retorna la respuesta
        print(f"[QueryHandler] Orden directa ejecutada: {orden}")
        return self._ok({
            "mensaje": f"OLA_VERDE activada en {calle_id} por {duracion_s}s",
            "orden": orden.to_registro(),
        })

    # Deserializa el JSON de la solicitud
    def _parse_request(self, mensaje: str) -> dict | None:
        try:
            return json.loads(mensaje)
        # Si hay error al deserializar, retorna un error
        except json.JSONDecodeError:
            print(f"[QueryHandler] ⚠ JSON inválido recibido: {mensaje[:100]}")
            return None

    # Retorna una respuesta OK
    def _ok(self, datos: dict) -> dict:
        return {"estado": "OK", **datos}

    # Retorna una respuesta de error
    def _error(self, mensaje: str) -> dict:
        print(f"[QueryHandler] Error: {mensaje}")
        return {"estado": "ERROR", "mensaje": mensaje}
