"""
    Clase base abstracta para la jerarquía de sensores de tráfico.
    Define el flujo de adquisición, procesamiento y publicación de eventos.
    Obtiene métricas viales desde CityManager, aplica ruido y normaliza datos.
    Publica eventos estructurados en una cola thread-safe compartida con el publicador ZMQ.
    Sirve como contrato base para sensores de cámara, espira electromagnética y GPS.
    Integra concurrencia mediante hilos y desacoplamiento productor-consumidor.
"""
import zmq
import json
from abc import ABC, abstractmethod
import random
import time

class SensorBase(ABC):

    def __init__(self, config_sensor, city_manager_ref, cola, intervalo):
        """
        Args:
            config_sensor (dict): Configuracion del sensor
            city_manager_ref (None): Ya no se usa, el sensor ahora es un proceso
            cola (None): Ya no se usa, el sensor publica directamente
            intervalo (int): Segundos entre cada generación
        """
        self.config = config_sensor
        self.interseccion = config_sensor.get('interseccion', '')
        self.sensor_id = config_sensor.get('sensor_id', '')
        self.direccion = config_sensor['direccion']
        self.intervalo_s = intervalo
        self.contador_eventos = 0
        self.calle = self.config.get('calle_id', f"{self.direccion}_{self.interseccion[-2 if self.direccion == 'fila' else -1]}")

        # Configuración de red para el nuevo proceso
        self.context = zmq.Context()
        # Socket para consultar al CityManager
        self.req_socket = self.context.socket(zmq.REQ)
        self.req_socket.connect("tcp://localhost:5555")
        
        # Socket para publicar eventos al Broker
        self.pub_socket = self.context.socket(zmq.PUB)
        self.pub_socket.connect("tcp://localhost:5550")

    # Este metodo se encarga de añadir algo de ruido a la lectura de los sensores para simular el tráfico real
    def _aplicar_ruido(self, nivel_base):
        sigma_v = random.uniform(0.05, 0.20)
        ruido = random.gauss(0, nivel_base * sigma_v)
        return max(0.0, min(1.0, nivel_base + ruido))

    # Cada subclase implementará su propia fórmula de Greenshields para generar el evento específico del sensor
    @abstractmethod
    def generar_evento(self, nivel):
        pass

    # El método iniciar se encarga de ejecutar el ciclo de vida del sensor, consultando al CityManager, 
    # generando eventos con ruido y publicándolos directamente al Broker a través de ZMQ. 
    def iniciar(self):
        print(f"[Sensor {self.sensor_id}] Proceso iniciado en calle {self.calle}")
        while True:
            try:
                # 1. Consultar nivel al CityManager vía ZMQ
                self.req_socket.send_json({"action": "get_nivel", "calle": self.calle})
                respuesta = self.req_socket.recv_json()
                nivel_base = respuesta.get("nivel", 0.0)
                
                # 2. Aplicar ruido y generar evento
                nivel_efectivo = self._aplicar_ruido(nivel_base)
                self.contador_eventos += 1
                evento = self.generar_evento(nivel_efectivo)
                evento['id_evento'] = self.contador_eventos
                evento['sensor_id'] = self.sensor_id
                evento['tipo_sensor'] = self.config.get('tipo')

                # 3. Publicar evento directamente vía ZMQ
                topico = evento['tipo_sensor']
                self.pub_socket.send_multipart([
                    topico.encode('utf-8'),
                    json.dumps(evento).encode('utf-8')
                ])
                print(f"[Sensor {self.sensor_id} | {self.config.get('tipo').upper()}] Evento enviado: {json.dumps(evento)}")

                time.sleep(self.intervalo_s)

            except Exception as e:
                print(f"[Sensor {self.sensor_id}] Error: {e}")
                time.sleep(5)
