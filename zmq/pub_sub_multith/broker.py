#Codigo del broker
import zmq
import json

# !/usr/bin/env python3
"""
Broker simple - recibe y muestra
"""

import zmq
import json

ctx = zmq.Context()
socket = ctx.socket(zmq.PULL)  # PULL recibe de PUSH
socket.bind("tcp://*:5555")

print("[BROKER] Esperando sensores en puerto 5555...")
print("[BROKER] Ctrl+C para salir\n")

while True:
    try:
        mensaje = socket.recv_string()
        dato = json.loads(mensaje)

        print(f"[BROKER] Recibido: {dato['tipo']} | "
              f"id={dato['id']} | "
              f"hora={dato['timestamp']}")

        # Mostrar el dato específico según tipo
        if dato['tipo'] == 'espira':
            print(f"         → {dato['vehiculos']} vehículos detectados")
        elif dato['tipo'] == 'camara':
            print(f"         → congestión {dato['congestion']}")
        elif dato['tipo'] == 'gps':
            print(f"         → velocidad {dato['velocidad_promedio']} km/h")

    except KeyboardInterrupt:
        print("\n[BROKER] Cerrando...")
        break

socket.close()
ctx.term()

