#!/usr/bin/env python3
"""
Mini Taller Etapa 1 - Cliente Echo
Completa los 4 TODOs marcados
"""

import zmq
import time


def main():
    # TODO 1: Crear contexto ZMQ
    ctx = zmq.Context()
    # TODO 2: Crear socket REQ (hace requests)
    req = ctx.socket(zmq.REQ)
    # TODO 3: Conectar al servidor en localhost:5557
    req.connect('tcp://localhost:5557')
    # TODO 4: Configurar timeout de 2 segundos (RCVTIMEO)
    # Para no bloquear para siempre si el servidor muere
    req.setsockopt(zmq.RCVTIMEO, 2000)

    mensajes = [
        "hola mundo",
        "sistemas distribuidos",
        "javeriana 2026",
        "zmq es poderoso",
        "fin del taller"
    ]

    for msg in mensajes:
        print(f"\n[CLIENTE] Enviando: {msg}")

        try:
            # TODO 5: Enviar mensaje al servidor (send_string)
            req.send_string(msg)
            # TODO 6: Recibir respuesta (recv_string)
            # Guardar en variable 'respuesta'
            respuesta = req.recv_string()
            print(f"[CLIENTE] Recibido: {respuesta}")

        except zmq.Again:
            # Timeout - servidor no respondió en 2 segundos
            print("[CLIENTE] ERROR: Timeout, servidor no responde")
            print("[CLIENTE] ¿Olvidaste iniciar servidor_echo.py?")
            break  # Salir del loop

        time.sleep(1)

    # TODO 7: Cerrar socket y contexto


if __name__ == '__main__':
    main()