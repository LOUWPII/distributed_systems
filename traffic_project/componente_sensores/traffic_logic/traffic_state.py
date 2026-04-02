import threading
import random


class TrafficState:

    """
    TrafficState representa el estado de trafico de una sola calle, ya sea una fila o una columna de la cuadricula.
    Es la fuente del nivel de congestion para la vía. El CityManager la crea, y está representa la instancia de una vía, la evoluciona
    y los sensores que estén en esta vía leen directamente sus datos.
    """

    def __init__(self, nombre, nivel_inicial, prob_shock=0.05):
        """
               Inicializa la clase.

               Args:
                   nombre (tipo): Identificador único de la vía (ej. "fila_C" o "col_3"). Permite que los sensores
                   sepan de qué objeto deben leer el nivel

                   nivel_inicial (float): Es el valor del nivel inicial de congestión de la vía

                   prob_shock (float): Es el valor de la probabilidad de que ocurra un choque o una siuación extrema
                   que provoque un aumento drástico en la congestion
               """
        self.nombre = nombre

        #Valor de densidad vehicular normalizada que oscila entre 0.0 (vía vacía) y 1.0 (embotellamiento total)
        self.nivel = nivel_inicial

        #Estado de control de tráfico ("VERDE" o "ROJO"). En estado "ROJO", actúa como un shock que aumenta la densidad del tráfico
        self.semaforo = "VERDE"  # Estado inicial por defecto

        # Parámetros del modelo matemático

        #Bandera que indica si la calle está en proceso de volver a la normalidad tras un shock
        self.en_recuperacion = False

        #Almacena el nivel de densidad justo antes de un shock para que el sistema sepa hacia qué valor debe retornar la recuperación
        self.nivel_pre_shock = None

        #velocidad de recuperación. Determina lo que el nivel tardará en estabilizarse tras un shock
        self.alpha = 0.15

        #Desviación estándar del Random Walk. Controla el nivel de los cambios naturales del tráfico
        self.sigma = 0.05

        #Probabilidad de que ocurra un shock en cada ciclo de evolución del tráfico
        self.prob_shock = prob_shock

        #Garantiza que mientras el CityManager escribe el nivel, los hilos de los sensores no lean un dato incorrecto
        self._lock = threading.Lock()



    def evolucionar(self):
        """
            Llamado por el CityManager cada 5 segundos. Calcula el nuevo nivel sumando los tres deltas: random walk siempre,
            prob_shock y recuperacion solo si en_recuperacion es True. Aplica clip para mantener el nivel en [0.0, 1.0].
        """
        with self._lock:
            if self.semaforo == "ROJO":
                # Si el semáforo está en rojo, la densidad sube forzadamente
                # para simular el bloqueo de la calle.
                delta = 0.2
            else:
                # Comportamiento normal
                delta = self._random_walk()
                delta += self._shock()
                delta += self._recuperacion()

            # Aplicar clip para mantener nivel en el rango
            self.nivel = max(0.0, min(1.0, self.nivel + delta))

    def _random_walk(self):
        """
            Produce los cambios leves y continuos del tráfico para simular un tráfico real
        """
        return random.gauss(0, self.sigma)

    def _shock(self):
        """
            Simula incidentes imprevistos como accidentes o situaciones alternas. Si un número aleatorio es menor a
            prob_shock, el nivel cambia bruscamente en ±0.35 y se activa la fase de recuperación
        """
        if not self.en_recuperacion and random.random() < self.prob_shock:
            self.nivel_pre_shock = self.nivel
            self.en_recuperacion = True
            #ESTO DEBE REVISARSE POR EL TEMA DE LA OLA VERDE Y LA CONGESTIÓN FORZADA
            return random.choice([0.35, -0.35])
        return 0

    def _recuperacion(self):
        """
            Retorno gradual al nivel original tras un shock
        """
        if self.en_recuperacion:
            diferencia = self.nivel - self.nivel_pre_shock
            if abs(diferencia) < 0.03:
                self.en_recuperacion = False
                return 0
            return -self.alpha * diferencia
        return 0

    def leer(self):
        """
            Permite a los sensores obtener el nivel de la calle de forma segura.
            Utiliza el lock para asegurar que la lectura sea consistente y evitar race conds
        """
        with self._lock:
            return self.nivel