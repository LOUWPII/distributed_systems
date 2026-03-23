#!/usr/bin/env python3
"""
Taller: 3 Sensores + 1 Publicador
Multithreading con queue.Queue
"""

import threading
import queue
import time
import random
import json


# =============================================================================
# PARTE 1: LOS 3 HILOS SENSORES
# =============================================================================

def sensor_espira(cola):
    """
    Sensor de espira (bucle inductivo en la carretera)
    Detecta paso de vehículos
    """
    id_sensor = "ESP-001"

    while True:
        # Simular lectura de sensor
        vehiculos = random.randint(0, 5)  # 0-5 vehículos en el intervalo

        dato = {
            "tipo": "espira",
            "id": id_sensor,
            "vehiculos": vehiculos,
            "timestamp": time.strftime("%H:%M:%S")
        }

        # Poner en la cola compartida
        cola.put(dato)
        print(f"[ESPIRA] Detecté {vehiculos} vehículos → cola")

        time.sleep(1)  # Cada 1 segundo


def sensor_camara(cola):
    """
    Sensor de cámara (visión computacional)
    Detecta congestión visual
    """
    id_sensor = "CAM-002"

    while True:
        # Simular análisis de imagen
        congestion = random.choice(["baja", "media", "alta"])

        dato = {
            "tipo": "camara",
            "id": id_sensor,
            "congestion": congestion,
            "timestamp": time.strftime("%H:%M:%S")
        }

        cola.put(dato)
        print(f"[CAMARA] Congestión {congestion} → cola")

        time.sleep(2)  # Cada 2 segundos


def sensor_gps(cola):
    """
    Sensor GPS (vehículos conectados)
    Reporta velocidad promedio
    """
    id_sensor = "GPS-003"

    while True:
        # Simular datos de flota de vehículos
        velocidad = random.randint(20, 80)  # km/h

        dato = {
            "tipo": "gps",
            "id": id_sensor,
            "velocidad_promedio": velocidad,
            "timestamp": time.strftime("%H:%M:%S")
        }

        cola.put(dato)
        print(f"[GPS] Velocidad promedio {velocidad} km/h → cola")

        time.sleep(3)  # Cada 3 segundos


# =============================================================================
# PARTE 2: EL HILO PUBLICADOR (con ZMQ)
# =============================================================================

def publicador(cola):
    """
    Único hilo que toca ZMQ
    Saca de la cola y envía al broker
    """
    import zmq

    # Crear contexto y socket (solo en este hilo)
    ctx = zmq.Context()
    socket = ctx.socket(zmq.PUSH)
    socket.connect("tcp://localhost:5555")

    print("[PUB] Conectado al broker en puerto 5555")

    while True:
        # Sacar de la cola (bloquea si está vacía)
        dato = cola.get()

        # Enviar por ZMQ
        mensaje = json.dumps(dato)
        socket.send_string(mensaje)

        print(f"[PUB] Enviado al broker: {dato['tipo']}")


# =============================================================================
# PARTE 3: MAIN - CREAR Y LANZAR TODO
# =============================================================================

def main():
    print("=" * 50)
    print("SISTEMA DE SENSORES MULTIHILO")
    print("3 sensores → cola → 1 publicador ZMQ")
    print("=" * 50)

    # Crear la cola compartida (sin límite por ahora)
    cola = queue.Queue()

    # Crear los 4 hilos
    hilo_espira = threading.Thread(target=sensor_espira, args=(cola,), daemon=True)
    hilo_camara = threading.Thread(target=sensor_camara, args=(cola,), daemon=True)
    hilo_gps = threading.Thread(target=sensor_gps, args=(cola,), daemon=True)
    hilo_pub = threading.Thread(target=publicador, args=(cola,), daemon=True)

    # Lanzar todos
    print("\n[Lanzando hilos...]")
    hilo_espira.start()
    hilo_camara.start()
    hilo_gps.start()
    hilo_pub.start()

    print("[OK] 3 sensores + 1 publicador corriendo")
    print("\nPresiona Ctrl+C para detener\n")

    # Mantener programa vivo
    try:
        while True:
            time.sleep(1)
            # Mostrar estado de la cola cada segundo
            print(f"   [ESTADO] Cola tiene {cola.qsize()} mensajes pendientes")
    except KeyboardInterrupt:
        print("\n\nDeteniendo sistema...")


if __name__ == "__main__":
    main()