"""
    Desarollado por: Juan Felipe Gomez, Sebastian Gaibor y David Beltran Gomez
    Punto de inicialización y orquestación del subsistema de sensores del PC1.
    Carga configuración global, inicializa CityManager y recursos compartidos del sistema.
    Instancia dinámicamente sensores según tipología definida en configuración JSON.
    Ejecuta cada sensor en hilos independientes para adquisición concurrente de eventos.
    Implementa patrón Productor-Consumidor mediante cola compartida y publicador dedicado.
    Publica eventos multipartes compatibles con el broker de mensajería ZeroMQ.
    Coordina el pipeline distribuido de captura y transmisión hacia Analítica.
"""

import json
import multiprocessing
import time
import os
import sys

# Solución para importaciones absolutas desde la raíz del proyecto
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from traffic_logic import city_manager
from sensor_logic import sensor_espira, sensor_camara, sensor_gps

# Funciones auxiliares para ejecutar el CityManager y los sensores en procesos separados, facilitando la concurrencia y el aislamiento de cada componente.
def run_city_manager(config):
    from traffic_logic import city_manager
    cm = city_manager.CityManager(config)
    cm.iniciar()

# Función genérica para ejecutar un sensor específico según su configuración, permitiendo la creación dinámica de sensores 
# de diferentes tipos (espira, cámara, GPS) con un intervalo de adquisición definido.
def run_sensor(s_cfg, intervalo):
    from sensor_logic import sensor_espira, sensor_camara, sensor_gps
    clases_sensores = {
        "espira_inductiva": sensor_espira.SensorEspira,
        "camara": sensor_camara.SensorCamara,
        "gps": sensor_gps.SensorGPS,
    }
    tipo = s_cfg.get('tipo') or s_cfg.get('tipo_sensor')
    # Validación básica de configuración del sensor
    if tipo in clases_sensores:
        sensor_inst = clases_sensores[tipo](s_cfg, None, None, intervalo)
        sensor_inst.iniciar()

def main():
    # 1. Leer configuración inicial con ruta absoluta
    config_path = os.path.join(os.path.dirname(__file__), 'config.json')
    with open(config_path, 'r') as f:
        config = json.load(f)

    # 2. Inicializar el CityManager como proceso independiente
    proc_city = multiprocessing.Process(target=run_city_manager, args=(config,), daemon=True)
    proc_city.start()
    print(f"[Main] Proceso CityManager iniciado (PID: {proc_city.pid})")

    # Esperar un momento para que el servidor del CityManager esté listo
    time.sleep(1)

    # 3. Instanciar y lanzar procesos de sensores
    intervalo_global = config['parametros_simulacion']['intervalo_sensores_s']
    procesos_sensores = []

    # Iterar sobre la configuración de sensores y lanzar un proceso para cada uno, pasando su configuración específica y el intervalo global definido.
    for s_cfg in config['sensores']:
        # Lanzar el proceso del sensor
        p = multiprocessing.Process(target=run_sensor, args=(s_cfg, intervalo_global), daemon=True)
        p.start()
        procesos_sensores.append(p)
        print(f"[Main] Proceso lanzado para sensor: {s_cfg['sensor_id']} (PID: {p.pid})")

    # Mantener el proceso principal vivo y gestionar terminación
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[Main] Finalizando simulación...")
        for p in procesos_sensores:
            p.terminate()
        proc_city.terminate()
        print("[Main] Todos los procesos finalizados.")

if __name__ == "__main__":
    main()