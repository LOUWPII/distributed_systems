from abc import ABC, abstractmethod
import random
import time


class SensorBase(ABC):
    """
         Es una clase abstracta que centraliza toda la lógica común de los tres tipos de sensores (Espiras, Cámaras y GPS).
         Su propósito es evitar la duplicación de código, manejando tareas como la comunicación con el CityManager, la aplicación de ruido y
         la gestión de la cola de salida, dejando únicamente la fórmula física de Greenshields específica a las subclases
    """

    def __init__(self, config_sensor, city_manager, cola, intervalo):
        """
            Inicializa la clase
            Args:
                config_sensor (dict): Configuracion del sensor
                city_manager (cityManager): Objeto cityManager para poder consultar el nivel de tráfico de la vía
                cola (queue.Queue): Cola thread-safe que maneja el patrón productor consumidor, en esta cola los sensores
                    depositan sus eventos
                intervalo (int): Segundos entre cada generación de datos del sensor
        """
        self.sensor_id = config_sensor['sensor_id']
        self.interseccion = config_sensor['interseccion']
        self.direccion = config_sensor['direccion']
        self.city_manager = city_manager
        self.cola_salida = cola
        self.intervalo_s = intervalo
        self.contador_eventos = 0

        # Construye el nombre de la calle (ej: fila_C)
        self.calle = f"{self.direccion}_{self.interseccion[-2]}"

    def _aplicar_ruido(self, nivel_base):
        """
            Este metodo se encarga de añadir algo de ruido a la lectura de los sensores para simular el tráfico real
        """
        sigma_v = random.uniform(0.05, 0.20)
        ruido = random.gauss(0, nivel_base * sigma_v)
        return max(0.0, min(1.0, nivel_base + ruido))

    @abstractmethod
    def generar_evento(self, nivel):
        """
            Cada subclase implementará su fórmula de Greenshields
        """
        pass

    def iniciar(self):
        """
            Ciclo de vida del hilo del sensor
            Este metodo se ejecuta en un hilo independiente para cada sensor y sigue un bucle infinito.
            Cada hilo siempre consulta el nivel de su calle, aplica el ruido, ejecuta su medicion particular, deposita en la cola
                y duerme el intervalo de segundos de generación
        """
        while True:
            nivel_base = self.city_manager.get_nivel(self.calle)
            nivel_efectivo = self._aplicar_ruido(nivel_base)

            # Generar el JSON y ponerlo en la cola thread-safe [7, 8]
            self.contador_eventos += 1
            evento = self.generar_evento(nivel_efectivo)
            evento['id_evento'] = self.contador_eventos
            self.cola_salida.put(evento)

            time.sleep(self.intervalo_s)