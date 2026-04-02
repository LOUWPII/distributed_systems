"""
EventReceiver — Receptor de eventos del Broker ZMQ (PC1).

Corre en su propio hilo escuchando el socket SUB conectado al Broker de PC1.
Por cada mensaje recibido: identifica el tópico, deserializa el JSON,
construye el EventoSensor correcto y lo deposita en la event_queue.

No toma ninguna decisión sobre el tráfico. Solo recibe, convierte y encola.

Comunicación ZMQ:
    - Patrón SUB conectado al Broker de PC1
    - Se suscribe a los tópicos: "camara", "espira_inductiva", "gps"
    - El mensaje llega en dos partes: tópico (frame 1) + JSON (frame 2)
"""

import json
import queue
import threading

import zmq

from config import Config
from dtos import evento_desde_topico, EventoSensor


class EventReceiver(threading.Thread):
    """
    Hilo receptor de eventos de sensores.

    La event_queue es de tipo threading.Queue, que es thread-safe por diseño.
    EventReceiver hace put() y RulesEngine hace get() desde hilos distintos
    sin necesidad de locks explícitos.

    El socket SUB de ZMQ bloquea en recv() cuando no hay mensajes, lo cual
    es el comportamiento correcto para un receptor: duerme sin consumir CPU
    hasta que llega algo.
    """

    def __init__(self, config: Config, event_queue: queue.Queue):
        super().__init__(daemon=True, name="EventReceiver")
        self._config = config
        self._event_queue = event_queue
        self._activo = True
        self._contexto_zmq = zmq.Context.instance()
        self._socket = None

    def run(self) -> None:
        """
        Ciclo principal. Se conecta al broker y escucha indefinidamente.
        Cada mensaje recibido pasa por _deserialize() antes de encolarse.
        """
        self._socket = self._contexto_zmq.socket(zmq.SUB)

        # Suscribirse a los tres tópicos de sensores
        topicos = [
            self._config.topico_camara,
            self._config.topico_espira,
            self._config.topico_gps,
        ]
        for topico in topicos:
            self._socket.setsockopt_string(zmq.SUBSCRIBE, topico)

        self._socket.connect(self._config.broker_url)
        print(f"[EventReceiver] Conectado a broker {self._config.broker_url}")
        print(f"[EventReceiver] Suscrito a tópicos: {topicos}")

        while self._activo:
            try:
                # recv_multipart: recibe [tópico_bytes, json_bytes]
                partes = self._socket.recv_multipart()
                if len(partes) < 2:
                    continue

                topico = partes[0].decode("utf-8")
                cuerpo = json.loads(partes[1].decode("utf-8"))

                evento = self._deserialize(topico, cuerpo)
                if evento is not None:
                    self._event_queue.put(evento)

            except zmq.ZMQError as e:
                if self._activo:
                    print(f"[EventReceiver] Error ZMQ: {e}")

    def detener(self) -> None:
        """
        Señala al hilo que debe terminar y cierra el socket para
        desbloquear el recv() bloqueante.
        """
        self._activo = False
        if self._socket:
            self._socket.close()
        print("[EventReceiver] Detenido")

    def _deserialize(self, topico: str, data: dict) -> EventoSensor | None:
        """
        Convierte el dict JSON en el EventoSensor correcto según el tópico.
        Si el evento no es válido o el tópico no se reconoce, retorna None
        y lo descarta sin propagar excepciones al ciclo principal.
        """
        try:
            evento = evento_desde_topico(topico, data)
            if evento is None:
                print(f"[EventReceiver] ⚠ Evento inválido descartado | tópico={topico}")
            return evento
        except (KeyError, ValueError) as e:
            print(f"[EventReceiver] ⚠ Error deserializando evento: {e} | data={data}")
            return None
