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

    # Hilo principal que recibe los eventos del broker
    def run(self) -> None:
        # Crear socket SUB
        self._socket = self._contexto_zmq.socket(zmq.SUB)

        # Suscribirse a los tres tópicos de sensores
        topicos = [
            self._config.topico_camara,
            self._config.topico_espira,
            self._config.topico_gps,
        ]
        # Suscribirse a los tópicos
        for topico in topicos:
            self._socket.setsockopt_string(zmq.SUBSCRIBE, topico)

        # Conectarse al broker
        self._socket.connect(self._config.broker_url)
        print(f"[EventReceiver] Conectado a Broker {self._config.broker_url}")
        print(f"[EventReceiver] Suscrito a Tópicos: {topicos}")

        # Ciclo para recibir eventos mientras el hilo esté activo
        while self._activo:
            try:
                # recibe [tópico_bytes, json_bytes]
                partes = self._socket.recv_multipart()
                # Si el mensaje tiene menos de 2 partes, continuar
                if len(partes) < 2:
                    continue

                # Decodificar el tópico y el cuerpo 
                topico = partes[0].decode("utf-8")
                cuerpo = json.loads(partes[1].decode("utf-8"))

                # Crear el evento
                evento = self._deserialize(topico, cuerpo)
                # Si el evento es válido, agregarlo a la cola
                if evento is not None:
                    self._event_queue.put(evento)

            # Manejo de errores
            except zmq.ZMQError as e:
                if self._activo:
                    print(f"[EventReceiver] Error ZMQ: {e}")

    # Detener el hilo
    def detener(self) -> None:
        
        #Señala al hilo que debe terminar y cierra el socket para desbloquear el recv() bloqueante.
        self._activo = False
        # Cerrar el socket
        if self._socket:
            self._socket.close()
        print("[EventReceiver] Detenido")

    # Deserializar el evento según el tópico
    def _deserialize(self, topico: str, data: dict) -> EventoSensor | None:
        try:
            evento = evento_desde_topico(topico, data)
            # Si el evento no es válido, descartarlo
            if evento is None:
                print(f"[EventReceiver] ⚠ Evento inválido descartado | tópico={topico}")
            return evento
        # Si hay error al deserializar, descartar el evento
        except (KeyError, ValueError) as e:
            print(f"[EventReceiver] ⚠ Error deserializando evento: {e} | data={data}")
            return None