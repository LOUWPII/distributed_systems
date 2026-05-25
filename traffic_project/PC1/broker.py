"""
    Desarollado por: Juan Felipe Gomez, Sebastian Gaibor y David Beltran Gomez
    Implementa un Broker basado en ZeroMQ para enrutamiento de eventos.
    Usando una comunicación de PUB/SUB desacoplando sensores físicos y módulo Analítico.
    Consume eventos desde sensores, valida integridad estructural y reenvía mensajes.
    Aplica validaciones sobre tópicos, payloads y consistencia física básica.
    Enriquece cada evento con metadatos para trazabilidad.
    Mantiene procesamiento continuo mediante loop síncrono de recepción y publicación.
    Cuenta con un cierre controlado de sockets y contexto ZMQ.
"""

import zmq
import json
import multiprocessing
from datetime import datetime, timezone

# ── Constantes internas ─────────────────────────────────────
INTERNAL_RECEIVER_WORKER = "tcp://127.0.0.1:5565"
INTERNAL_WORKER_PUBLISHER = "tcp://127.0.0.1:5566"
DEFAULT_NUM_WORKERS = 4


# ── Funciones de validación / enriquecimiento (module-level) ──

# Valida la estructura y coherencia básica del evento recibido, asegurando que cumpla con el formato esperado.
def _validar_evento(topico, evento, topicos):
    """Validación del JSON (espera un DICCIONARIO)."""
    if topico not in topicos:
        return False
    if 'sensor_id' not in evento:
        return False
    if topico in ('espira_inductiva', 'camara'):
        if 'interseccion' not in evento:
            return False
    return True


# Verifica que los datos recibidos tengan coherencia con las expectativas del mundo real, aplicando reglas simples basadas en la física del tráfico.
def _validar_sentido_fisico(topico, evento):
    """Verifica coherencia básica según Greenshields."""
    try:
        if topico == 'camara':
            if evento.get('volumen', 0) < 0:
                return False
            if evento.get('velocidad_promedio', 0) > 60:
                return False
        elif topico == 'gps':
            v = evento.get('velocidad_promedio', 0)
            cat = evento.get('nivel_congestion', '')
            if cat == 'ALTA' and v >= 20:
                return False
            if cat == 'BAJA' and v <= 35:
                return False
        return True
    except:
        return False


# Agrega una marca de tiempo al evento para facilitar la trazabilidad y el análisis de latencia en el sistema.
def _enriquecer_evento(evento):
    """Agrega timestamp para medir latencia sensor-broker."""
    evento['broker_timestamp'] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S") + "Z"
    return evento


# ── Funciones de proceso (multiprocesos) ─────────────────────

def _receiver_process(sensor_url, worker_url, topicos, shutdown_event):
    context = zmq.Context()
    sub_socket = context.socket(zmq.SUB)
    sub_socket.bind(sensor_url)
    for t in topicos:
        sub_socket.setsockopt_string(zmq.SUBSCRIBE, t)
    sub_socket.setsockopt(zmq.RCVTIMEO, 1000)

    push_socket = context.socket(zmq.PUSH)
    push_socket.bind(worker_url)

    print(f"[Receiver] SUB conectado a {sensor_url} | PUSH bindeado en {worker_url}")
    while not shutdown_event.is_set():
        try:
            partes = sub_socket.recv_multipart()
            if len(partes) >= 2:
                push_socket.send_multipart(partes[:2])
        except zmq.Again:
            continue

    sub_socket.close()
    push_socket.close()
    context.term()
    print("[Receiver] Finalizado.")


def _worker_process(worker_id, receiver_url, publisher_url, topicos, shutdown_event):
    context = zmq.Context()
    pull_socket = context.socket(zmq.PULL)
    pull_socket.connect(receiver_url)

    push_socket = context.socket(zmq.PUSH)
    push_socket.connect(publisher_url)

    pull_socket.setsockopt(zmq.RCVTIMEO, 1000)

    print(f"[Worker-{worker_id}] PULL conectado a {receiver_url} | PUSH conectado a {publisher_url}")
    while not shutdown_event.is_set():
        try:
            partes = pull_socket.recv_multipart()
        except zmq.Again:
            continue
        if len(partes) < 2:
            continue

        topico = partes[0].decode('utf-8')
        try:
            evento = json.loads(partes[1].decode('utf-8'))
        except json.JSONDecodeError:
            print(f"[Worker-{worker_id}] JSON inválido en {topico}")
            continue

        if not _validar_evento(topico, evento, topicos) or not _validar_sentido_fisico(topico, evento):
            print(f"[Worker-{worker_id}] Mensaje de {evento.get('sensor_id', '???')} DESCARTADO por validación.")
            continue

        evento = _enriquecer_evento(evento)

        sid = evento.get('sensor_id', '???')
        if topico == 'espira_inductiva':
            info = f"Flujo: {evento.get('vehiculos_contados', 0):<3} veh/int"
        elif topico == 'camara':
            info = f"Cola: {evento.get('volumen', 0):<3} veh | Vel: {evento.get('velocidad_promedio', 0):.1f} km/h"
        elif topico == 'gps':
            info = f"Est: {evento.get('nivel_congestion', '???'):<8} | Vel: {evento.get('velocidad_promedio', 0):.1f} km/h"
        else:
            info = "Datos recibidos"
        print(f"[Worker-{worker_id}] {topico:<18} | ID: {sid:<8} | {info}")

        payload_final = json.dumps(evento)
        push_socket.send_multipart([topico.encode('utf-8'), payload_final.encode('utf-8')])

    pull_socket.close()
    push_socket.close()
    context.term()
    print(f"[Worker-{worker_id}] Finalizado.")


def _publisher_process(worker_url, analitica_url, shutdown_event):
    context = zmq.Context()
    pull_socket = context.socket(zmq.PULL)
    pull_socket.bind(worker_url)

    pub_socket = context.socket(zmq.PUB)
    pub_socket.bind(analitica_url)

    pull_socket.setsockopt(zmq.RCVTIMEO, 1000)

    print(f"[Publisher] PULL bindeado en {worker_url} | PUB bindeado en {analitica_url}")
    while not shutdown_event.is_set():
        try:
            partes = pull_socket.recv_multipart()
            if len(partes) >= 2:
                pub_socket.send_multipart(partes[:2])
        except zmq.Again:
            continue

    pull_socket.close()
    pub_socket.close()
    context.term()
    print("[Publisher] Finalizado.")


# ── Clase BrokerZMQ ──────────────────────────────────────────

class BrokerZMQ:
    def __init__(self, config):
        self.config = config
        self.modo = config.get('modo_broker', 'simple')
        self.topicos = list(config['sensores_topicos'].values())
        self.contadores = {t: 0 for t in self.topicos}
        self._shutdown = multiprocessing.Event()

        if self.modo == 'simple':
            self._configurar_sockets()
        elif self.modo == 'multihilos':
            self.num_workers = config.get('broker_multihilos', {}).get('num_workers', DEFAULT_NUM_WORKERS)

    # Configura los sockets de ZMQ para el modo simple, estableciendo un socket SUB para recibir eventos de los sensores y un socket PUB para reenviar eventos a PC2 (Analítica).
    def _configurar_sockets(self):
        self.context = zmq.Context()
        self.sub_socket = self.context.socket(zmq.SUB)
        self.sub_socket.bind(self.config['red']['sensor_broker_url_PUB'])
        for t in self.topicos:
            self.sub_socket.setsockopt_string(zmq.SUBSCRIBE, t)
        self.sub_socket.setsockopt(zmq.RCVTIMEO, 1000)
        self.pub_socket = self.context.socket(zmq.PUB)
        self.pub_socket.bind(self.config['red']['broker_analitica_url_PUB'])

    # ── Dispatcher principal ──────────────────────────────────

    # Implementa el loop principal del broker: recibe eventos de sensores, los valida, enriquece con metadatos y los reenvía a PC2 (Analítica) vía ZMQ.
    def iniciar(self):
        if self.modo == 'simple':
            self._iniciar_simple()
        else:
            self._iniciar_multihilos()

    # ── Modo simple (1 proceso, sin paralelismo) ──────────────

    def _iniciar_simple(self):
        print(f"[Broker] Iniciando Modo Simple (1 hilo)...")
        print(f"[Broker] Escuchando sensores en: {self.config['red']['sensor_broker_url_PUB']}")
        print(f"[Broker] Tópicos suscritos: {self.topicos}")
        print("-" * 75)
        try:
            while True:
                try:
                    partes = self.sub_socket.recv_multipart()
                    if len(partes) < 2:
                        continue
                    topico = partes[0].decode('utf-8')
                    cuerpo_raw = partes[1].decode('utf-8')
                    try:
                        evento = json.loads(cuerpo_raw)
                    except json.JSONDecodeError:
                        print(f"[Broker] Error: JSON inválido en {topico}")
                        continue
                    if _validar_evento(topico, evento, self.topicos) and _validar_sentido_fisico(topico, evento):
                        evento_enriquecido = _enriquecer_evento(evento)
                        payload_final = json.dumps(evento_enriquecido)
                        self.pub_socket.send_multipart([
                            topico.encode('utf-8'),
                            payload_final.encode('utf-8')
                        ])
                        self._loguear_evento(topico, evento_enriquecido)
                    else:
                        print(f"[Broker] Mensaje de {evento.get('sensor_id', '???')} DESCARTADO por validación.")
                except zmq.Again:
                    continue
        except KeyboardInterrupt:
            print("\n[Broker] Finalizado por el usuario.")
        finally:
            self.sub_socket.close()
            self.pub_socket.close()
            self.context.term()
            total = sum(self.contadores.values())
            print(f"[Broker] Total eventos procesados: {total}")
            print(f"[Broker] Desglose: {dict(self.contadores)}")

    # Imprime un log legible y estructurado de los eventos procesados, mostrando información relevante según el tipo de sensor.
    def _loguear_evento(self, topico, evento):
        self.contadores[topico] += 1
        sid = evento.get('sensor_id', '???')
        if topico == 'espira_inductiva':
            info = f"Flujo: {evento.get('vehiculos_contados', 0):<3} veh/int"
        elif topico == 'camara':
            info = f"Cola: {evento.get('volumen', 0):<3} veh | Vel: {evento.get('velocidad_promedio', 0):.1f} km/h"
        elif topico == 'gps':
            info = f"Est: {evento.get('nivel_congestion', '???'):<8} | Vel: {evento.get('velocidad_promedio', 0):.1f} km/h"
        else:
            info = "Datos recibidos"
        print(f"[Broker] {topico:<18} | ID: {sid:<8} | {info}")

    # ── Modo multihilos (multiprocesos con ZMQ PUSH/PULL) ────

    def _iniciar_multihilos(self):
        print(f"[Broker] Iniciando Modo Multiprocesos ({self.num_workers} workers)...")
        print(f"[Broker] Escuchando sensores en: {self.config['red']['sensor_broker_url_PUB']}")
        print(f"[Broker] Tópicos suscritos: {self.topicos}")
        print("-" * 75)

        sensor_url = self.config['red']['sensor_broker_url_PUB']
        analitica_url = self.config['red']['broker_analitica_url_PUB']
        recv_worker_url = INTERNAL_RECEIVER_WORKER
        worker_pub_url = INTERNAL_WORKER_PUBLISHER

        processes = []

        p_recv = multiprocessing.Process(
            target=_receiver_process,
            args=(sensor_url, recv_worker_url, self.topicos, self._shutdown)
        )
        processes.append(("Receiver", p_recv))

        for i in range(self.num_workers):
            p_worker = multiprocessing.Process(
                target=_worker_process,
                args=(i, recv_worker_url, worker_pub_url, self.topicos, self._shutdown)
            )
            processes.append((f"Worker-{i}", p_worker))

        p_pub = multiprocessing.Process(
            target=_publisher_process,
            args=(worker_pub_url, analitica_url, self._shutdown)
        )
        processes.append(("Publisher", p_pub))

        for nombre, p in processes:
            p.start()
            print(f"[Broker] {nombre} iniciado (PID: {p.pid}).")

        try:
            while not self._shutdown.is_set():
                self._shutdown.wait(1)
        except KeyboardInterrupt:
            print("\n[Broker] Finalizando procesos...")
            self._shutdown.set()

        for nombre, p in processes:
            p.join(timeout=3)
            if p.is_alive():
                p.terminate()
                p.join()
            print(f"[Broker] {nombre} terminado.")

        print("[Broker] Todos los procesos han finalizado.")


if __name__ == "__main__":
    import os
    config_path = os.path.join(os.path.dirname(__file__), 'config.json')
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        broker = BrokerZMQ(config)
        broker.iniciar()
    except FileNotFoundError:
        print(f"[Error] No se encontró 'config.json' en {config_path}")
