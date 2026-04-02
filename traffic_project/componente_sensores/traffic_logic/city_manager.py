import threading
import time
import random

from .traffic_state import TrafficState


class CityManager:
    """
    El CityManager es el motor de simulación de la ciudad y actúa como el orquestador central dentro del proceso sensores.py.
    Su función principal es gestionar e instanciar cada vía (fila o coluna), gestionando de forma dinámica los estados de tráfico
    que los sensores consultarán posteriormente.
    """

    def __init__(self, config):
        """
            Inicializa la clase
            Lee del archivo JSON los valores de sus atributos
        """
        #Contiene el diccionario completo leído desde el config.json, incluyendo la lista de sensores y parámetros de red.
        self.config = config

        #Almacena objetos TrafficState, donde la clave es el nombre único de la calle (ej. "fila_F", "col_3").
        self.traffic_states = {}

        #Define que toda la ciudad se actualiza físicamente cada 5 segundos, esto se refiere al nivel de congestión de cada vía
        self.intervalo_evolucion = config['parametros_simulacion']['intervalo_evolucion_s']
        self._construir_states()

    def _construir_states(self):
        """
            Recorre la lista de sensores del config
            Identifica la calle (fila ocolumna) a la que pertenece cada sensor basandose en la interseccion y direccion
            Crea un único objeto TrafficState para ea calle si aún no existe
            Si hay un numero x de sensores en la fila X, entonces todos estos sensores serán parte del mismo objeto para manejar coherencia
        """
        #Se extrae el prob_shock del config
        prob_shock = self.config['parametros_simulacion']['probabilidad_shock']

        for s in self.config['sensores']:
            # Extraer identificador de calle (ej: INT_F3 + columna = col_3)
            num_calle = s['interseccion'][-1]
            id_calle = f"{s['direccion']}_{num_calle}"

            if id_calle not in self.traffic_states:
                # Nivel inicial aleatorio para la calle (entre 0.1 y 0.6)
                nivel_ini = random.uniform(0.1, 0.6)
                self.traffic_states[id_calle] = TrafficState(
                    nombre=id_calle,
                    nivel_inicial=nivel_ini,
                    prob_shock=prob_shock
                )

    def _hilo_evolucion(self):
        """
        Ejecuta un bucle infinito que invoca el metodo evolucionar del TrafficState
        Corre como hilo secundario (daemon) para no bloquear el proceso.
        Duerme el numero de segundos definido en el intervalo de evolucion en el config
        """
        while True:
            # Evolucionar todas las calles de la ciudad secuencialmente
            for ts in self.traffic_states.values():
                ts.evolucionar()

            # Esperar el intervalo de evolucion
            time.sleep(self.intervalo_evolucion)

    def iniciar(self):
        """
            Lanza el hilo de evolución de la ciudad
        """
        t = threading.Thread(target=self._hilo_evolucion, daemon=True)
        t.start()
        print(f"[CityManager] Simulación física iniciada.")
        print(f"[CityManager] Calles monitoreadas: {list(self.traffic_states.keys())}")

    def get_nivel(self, calle):
        """
        Interfaz para que los hilos de sensores consulten el nivel de su calle
        Retorna el nivel de densidad

        args:
            calle (str): Representa el identificador de la calle (ej. "fila_F", "col_3")
        """
        if calle in self.traffic_states:
            return self.traffic_states[calle].leer()
        return 0.0

    def set_estado_semaforo(self, calle, estado):
        """
        Permite cambiar el estado del semáforo (ROJO/VERDE) de una calle.
        Permite que al haber un semáforo en rojo, aumente la densidad/congestión de la calle

        args:
            calle (str): Identificador de la calle (ej. "fila_F", "col_3")
            estado (str): El estado tiene como dominio VERDE o ROJO
        """
        if calle in self.traffic_states:
            self.traffic_states[calle].semaforo = estado
            print(f"[CityManager] SEMÁFORO: {calle} cambiado a {estado}")