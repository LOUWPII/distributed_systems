import json
import zmq
import threading
import queue
import time
import os
import sys

# Solución para importaciones absolutas desde la raíz del proyecto
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from traffic_logic import city_manager  # Suponiendo archivos separados
from sensor_logic import sensor_espira, sensor_camara, sensor_gps


def hilo_publicador(config, cola_eventos):
    """
    Único componente que toca el socket ZMQ PUB.
    Saca eventos de la cola y los despacha al broker.
    """
    context = zmq.Context()
    pub_socket = context.socket(zmq.PUB)

    # Conexión al puerto SUB del broker (ej: 5550)
    #Comentamos para pruebas
    #broker_ip = config['red']['pc1_ip']
    #broker_port = config['broker']['sub_port']
    #pub_socket.connect(f"tcp://{broker_ip}:{broker_port}")
    pub_socket.connect("tcp://localhost:5550")

    #print(f"[PUB] Hilo iniciado. Conectado al broker en {broker_ip}:{broker_port}")
    print(f"[PUB] Hilo iniciado. Conectado al broker en localhost con el puerto 5550")

    while True:
        # Bloquea hasta que haya un evento disponible en la cola
        evento = cola_eventos.get()

        # Determinar tópico según tipo: sensor.espira_inductiva, sensor.camara, etc.
        topico = f"sensor.{evento['tipo_sensor']}"
        payload = json.dumps(evento)

        # Enviar mensaje con el formato 'topico payload'
        pub_socket.send_string(f"{topico} {payload}")

        # Loguear operación según requerimiento del enunciado
        print(f"[PUB] Enviado: {evento['sensor_id']} -> {topico}")
        cola_eventos.task_done()


def main():
    # 1. Leer configuración inicial
    with open('config.json', 'r') as f:
        config = json.load(f)

    # 2. Inicializar el motor de la ciudad
    city_manag = city_manager.CityManager(config)
    city_manag.iniciar()

    # 3. Crear cola thread-safe compartida
    # Canal de comunicación entre hilos de sensores y el publicador.
    cola_compartida = queue.Queue()

    # 4. Lanzar el hilo publicador ZMQ
    t_pub = threading.Thread(target=hilo_publicador, args=(config, cola_compartida), daemon=True)
    t_pub.start()

    # 5. Instanciar y lanzar hilos de sensores dinámicamente [3, 10]
    # Se crea un hilo por cada entrada en la lista 'sensores' del config.json.
    clases_sensores = {
        "espira_inductiva": sensor_espira.SensorEspira,
        "camara": sensor_camara.SensorCamara,
        "gps": sensor_gps.SensorGPS,
    }

    intervalo_global = config['parametros_simulacion']['intervalo_sensores_s']

    for s_cfg in config['sensores']:
        tipo = s_cfg['tipo_sensor']
        if tipo in clases_sensores:
            # Crear instancia pasándole referencias a memoria compartida
            sensor_inst = clases_sensores[tipo](s_cfg, city_manag, cola_compartida, intervalo_global)

            # Lanzar el hilo del sensor
            t_s = threading.Thread(target=sensor_inst.iniciar, daemon=True)
            t_s.start()
            print(f"[Main] Hilo lanzado para sensor: {s_cfg['sensor_id']}")

    # Mantener el proceso principal vivo
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[Main] Simulación finalizada por el usuario.")


if __name__ == "__main__":
    main()