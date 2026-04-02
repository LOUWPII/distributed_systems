import zmq
import json
import threading
from datetime import datetime, timezone


class BrokerZMQ:
    def __init__(self, config):
        """Inicializa configuración y contadores ."""
        self.config = config
        self.modo = config['broker']['modo']  # 'simple' o 'multihilos'
        self.topicos = config['broker']['topicos']
        self.contadores = {t: 0 for t in self.topicos}

        # En modo simple, los sockets son globales al objeto [7]
        if self.modo == 'simple':
            self._configurar_sockets()

    def _configurar_sockets(self):
        """Configuración para el modo de un solo hilo."""
        self.context = zmq.Context()
        # Frontend: Recibe de sensores (SUB)
        self.sub_socket = self.context.socket(zmq.SUB)
        #self.sub_socket.bind(f"tcp://*:{self.config['broker']['sub_port']}")
        self.sub_socket.bind(f"tcp://*:5550")
        for t in self.topicos:
            self.sub_socket.setsockopt_string(zmq.SUBSCRIBE, t)

        # Backend: Publica hacia PC2 (PUB)
        self.pub_socket = self.context.socket(zmq.PUB)
        self.pub_socket.bind(f"tcp://*:{self.config['broker']['pub_port']}")

    def _validar(self, topico, evento):
        """Validación del JSON."""
        if topico not in self.topicos: return False
        if 'sensor_id' not in evento: return False
        # Espira y cámara requieren intersección obligatoriamente [8]
        if topico in ('sensor.espira_inductiva', 'sensor.camara'):
            if 'interseccion' not in evento: return False
        return True

    def _validar_sentido_fisico(self, topico, evento):
        """Verifica coherencia según Greenshields ."""
        try:
            if topico == 'sensor.camara':
                nivel_est = evento.get('volumen', 0) / 20.0
                v_esperada = 50 * (1 - nivel_est)
                if abs(evento.get('velocidad_promedio', 0) - v_esperada) > 8.0: return False
            elif topico == 'sensor.gps':
                v = evento.get('velocidad_promedio', 0)
                cat = evento.get('nivel_congestion', '')
                if cat == 'ALTA' and v >= 10: return False
                if cat == 'BAJA' and v <= 40: return False
            return True
        except:
            return False

    def _enriquecer(self, evento):
        """Agrega timestamp para medir latencia sensor-broker [2, 9]."""
        evento['broker_timestamp'] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S") + "Z"
        return evento

    def _loguear(self, topico, evento):
        """Imprime estado del tráfico con contadores [2, 10]."""
        self.contadores[topico] += 1
        total = self.contadores[topico]
        sid = evento['sensor_id']
        print(f"[Broker] {topico:<30} | {sid:<12} | total: {total}")

    def _procesar_mensaje(self, msg, pub_socket):
        """Flujo de 6 pasos definido en el diseño"""
        # PASO 1: Separar tópico y payload
        topico, _, payload = msg.partition(' ')

        # PASO 2: Validar JSON
        try:
            evento = json.loads(payload)
        except json.JSONDecodeError:
            print(f"[Broker] ERROR: JSON inválido en {topico}")
            return

        # PASO 3: Validar estructura y sentido físico [8]
        if self._validar(topico, evento) and self._validar_sentido_fisico(topico, evento):
            # PASO 4: Enriquecer con timestamp
            evento = self._enriquecer(evento)
            # PASO 5: Reconstruir y reenviar a PC2
            msg_out = f"{topico} {json.dumps(evento)}"
            pub_socket.send_string(msg_out)
            # PASO 6: Loguear
            self._loguear(topico, evento)
        else:
            print(f"[Broker] Mensaje de {evento.get('sensor_id', '???')} DESCARTADO.")

    def _loop_simple(self):
        """Procesamiento secuencial."""
        print("[Broker] Iniciando Modo Simple (1 hilo)...")
        while True:
            msg = self.sub_socket.recv_string()
            self._procesar_mensaje(msg, self.pub_socket)

    def _worker_topico(self, topico):
        """Hilo trabajador: Respeta 'un socket por hilo'."""
        ctx = zmq.Context.instance()
        # Cada hilo crea sus propios sockets locales
        sub = ctx.socket(zmq.SUB)
        sub.connect(f"tcp://localhost:{self.config['broker']['sub_port']}")
        sub.setsockopt_string(zmq.SUBSCRIBE, topico)

        pub = ctx.socket(zmq.PUB)
        pub.connect(f"tcp://{self.config['red']['pc2_ip']}:{self.config['broker']['pub_port']}")

        print(f"[Broker-Worker] Hilo para {topico} listo.")
        while True:
            msg = sub.recv_string()
            self._procesar_mensaje(msg, pub)

    def _loop_multihilos(self):
        """Procesamiento paralelo para experimentos de la Tabla 1."""
        print("[Broker] Iniciando Modo Multihilos (1 hilo por tópico)...")
        for t in self.topicos:
            thread = threading.Thread(target=self._worker_topico, args=(t,), daemon=True)
            thread.start()
        threading.Event().wait()  # Bloquea el hilo principal

    def iniciar(self):
        """Punto de entrada principal."""
        if self.modo == 'simple':
            self._loop_simple()
        else:
            self._loop_multihilos()


if __name__ == "__main__":
    with open('config.json', 'r') as f:
        config = json.load(f)
    broker = BrokerZMQ(config)
    broker.iniciar()