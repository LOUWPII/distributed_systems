"""
QueryHandler — Manejador de solicitudes del Servicio de Monitoreo (PC3).

Corre en su propio hilo escuchando un socket ZMQ REP.
Cuando llega una solicitud, la procesa y responde antes de aceptar la siguiente.

Tipos de solicitud que maneja:
  1. CONSULTA_ESTADO_ACTUAL  → retorna el estado actual de una calle
  2. CONSULTA_TODOS_ESTADOS  → retorna el estado de todas las calles
  3. CONSULTA_INTERSECCION   → retorna el estado de una intersección
  4. ORDEN_DIRECTA           → fuerza un estado en una calle (ej: ambulancia)

Comunicación ZMQ:
    - Patrón REP: recibe REQ del Monitoreo, responde, recibe el siguiente REQ
    - El socket REP en ZMQ es estrictamente alternante: recv → send → recv → send
    - Si se rompe el orden (dos recv seguidos) el socket queda en error
"""

import json
import threading
import zmq
from config import Config
# Imports desde dominio/application
from dominio.orden_directa import OrdenDirecta
from enums import EstadoTrafico
from application.rules_engine import RulesEngine


class QueryHandler(threading.Thread):
    """
    Hilo que atiende solicitudes síncronas del usuario desde PC3.

    Corre separado del RulesEngine para que una consulta lenta no bloquee
    el procesamiento de eventos de sensores.

    El protocolo de solicitud/respuesta es JSON en ambas direcciones.
    """

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

    # ------------------------------------------------------------------
    # Ciclo principal del hilo
    # ------------------------------------------------------------------

    def run(self) -> None:
        self._socket = self._contexto_zmq.socket(zmq.REP)
        self._socket.bind(self._config.query_handler_url)
        print(f"[QueryHandler] Escuchando en {self._config.query_handler_url}")

        while self._activo:
            try:
                # recv() bloquea hasta que llegue una solicitud de PC3
                mensaje = self._socket.recv_string()
                solicitud = self._parse_request(mensaje)

                if solicitud is None:
                    respuesta = self._error("Solicitud mal formada")
                else:
                    respuesta = self.atender_consulta(solicitud)

                # En REP siempre hay que responder antes del siguiente recv()
                self._socket.send_string(json.dumps(respuesta))

            except zmq.ZMQError as e:
                if self._activo:
                    print(f"[QueryHandler] Error ZMQ: {e}")

        print("[QueryHandler] Detenido")

    def detener(self) -> None:
        self._activo = False
        if self._socket:
            self._socket.close()

    # ------------------------------------------------------------------
    # Procesamiento de solicitudes
    # ------------------------------------------------------------------

    def atender_consulta(self, solicitud: dict) -> dict:
        """
        Enruta la solicitud al manejador correcto según su tipo.
        Retorna siempre un dict con al menos {"estado": "OK"} o {"estado": "ERROR"}.
        """
        tipo = solicitud.get("tipo", "")

        manejadores = {
            "CONSULTA_ESTADO_ACTUAL": self._handle_estado_actual,
            "CONSULTA_TODOS_ESTADOS": self._handle_todos_estados,
            "CONSULTA_INTERSECCION": self._handle_interseccion,
            "ORDEN_DIRECTA": self._handle_orden_directa,
        }

        handler = manejadores.get(tipo)
        if handler is None:
            return self._error(f"Tipo de solicitud desconocido: {tipo}")

        try:
            return handler(solicitud)
        except Exception as e:
            return self._error(f"Error procesando solicitud: {e}")

    def ejecutar_orden(self, orden: OrdenDirecta) -> None:
        """
        Registra y ejecuta una OrdenDirecta.
        Llamado internamente por _handle_orden_directa().
        """
        self._rules_engine.registrar_orden(orden)

    # ------------------------------------------------------------------
    # Manejadores específicos por tipo de solicitud
    # ------------------------------------------------------------------

    def _handle_estado_actual(self, solicitud: dict) -> dict:
        """
        Retorna el estado actual de una calle específica.
        Solicitud: { tipo, calle_id }
        """
        calle_id = solicitud.get("calle_id")
        if not calle_id:
            return self._error("Falta campo calle_id")

        estado = self._rules_engine.get_estado_calle(calle_id)
        if estado is None:
            return self._error(f"Calle no encontrada: {calle_id}")

        print(f"[QueryHandler] Consulta estado: {calle_id} → {estado}")
        return self._ok({"calle": estado.to_registro()})

    def _handle_todos_estados(self, solicitud: dict) -> dict:
        """Retorna el estado actual de todas las calles del sistema."""
        estados = self._rules_engine.get_todos_estados()
        print(f"[QueryHandler] Consulta todos los estados ({len(estados)} calles)")
        return self._ok({"calles": estados})

    def _handle_interseccion(self, solicitud: dict) -> dict:
        """
        Retorna el estado de una intersección específica con sus semáforos.
        Solicitud: { tipo, interseccion_id }
        """
        int_id = solicitud.get("interseccion_id")
        if not int_id:
            return self._error("Falta campo interseccion_id")

        interseccion = self._rules_engine.get_interseccion(int_id)
        if interseccion is None:
            return self._error(f"Intersección no encontrada: {int_id}")

        print(f"[QueryHandler] Consulta intersección: {interseccion}")
        return self._ok({"interseccion": interseccion.to_registro()})

    def _handle_orden_directa(self, solicitud: dict) -> dict:
        """
        Crea y ejecuta una OrdenDirecta.
        Solicitud: { tipo, calle_id, accion, duracion_s, motivo }
        """
        calle_id = solicitud.get("calle_id")
        accion_str = solicitud.get("accion", "OLA_VERDE")
        duracion_s = int(solicitud.get("duracion_s", 60))
        motivo = solicitud.get("motivo", "ORDEN_USUARIO")

        if not calle_id:
            return self._error("Falta campo calle_id en orden directa")

        try:
            accion = EstadoTrafico(accion_str)
        except ValueError:
            return self._error(f"Acción desconocida: {accion_str}")

        orden = OrdenDirecta(
            calle_id = calle_id,
            accion = accion,
            duracion_s = duracion_s,
            motivo = motivo,
        )

        self.ejecutar_orden(orden)

        print(f"[QueryHandler] Orden directa ejecutada: {orden}")
        return self._ok({
            "mensaje": f"OLA_VERDE activada en {calle_id} por {duracion_s}s",
            "orden": orden.to_registro(),
        })

    # ------------------------------------------------------------------
    # Helpers de serialización
    # ------------------------------------------------------------------

    def _parse_request(self, mensaje: str) -> dict | None:
        """Deserializa el JSON de la solicitud. Retorna None si es inválido."""
        try:
            return json.loads(mensaje)
        except json.JSONDecodeError:
            print(f"[QueryHandler] ⚠ JSON inválido recibido: {mensaje[:100]}")
            return None

    def _ok(self, datos: dict) -> dict:
        return {"estado": "OK", **datos}

    def _error(self, mensaje: str) -> dict:
        print(f"[QueryHandler] Error: {mensaje}")
        return {"estado": "ERROR", "mensaje": mensaje}
