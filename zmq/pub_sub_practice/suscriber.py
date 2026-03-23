#!/usr/bin/env python3
"""
Lab 1 - Suscriptor (SUB)
Etapa 1: Fundamentos ZMQ

Este proceso simula el broker o analítica recibiendo eventos.
Demuestra: Context, socket SUB, connect, subscribe (¡CRÍTICO!), recv.
"""

import zmq
import json
import time


def main():
    # 1. CREAR CONTEXTO
    ctx = zmq.Context()

    # 2. CREAR SOCKET SUB - Solo puede recibir mensajes, nunca enviar
    sub = ctx.socket(zmq.SUB)

    # 3. CONNECT - El suscriptor se conecta al publicador
    sub.connect('tcp://localhost:5550')
    print("[SUB] Conectado a tcp://localhost:5550")

    # 4. SUSCRIBIRSE - ¡SIN ESTO NO RECIBES NADA!
    # La trampa #1 de ZMQ: olvidar subscribe()
    # '' = recibe todos los mensajes (wildcard)
    # 'sensor.espira' = recibe solo mensajes con ese prefijo
    sub.setsockopt_string(zmq.SUBSCRIBE, '')
    print("[SUB] Suscrito a todos los tópicos ('')")

    print("[SUB] Esperando mensajes... (Ctrl+C para salir)")

    try:
        while True:
            # 5. RECV - Bloquea hasta recibir un mensaje completo
            # ZMQ garantiza: un send() = exactamente un recv()
            # No hay fragmentación ni concatenación
            mensaje = sub.recv_string()

            # Parsear: separar tópico del payload JSON
            topico, _, payload_json = mensaje.partition(' ')
            evento = json.loads(payload_json)

            print(f"[SUB] Recibido: {topico}")
            print(f"       Evento #{evento['id_evento']}: {evento['tipo_sensor']} "
                  f"en {evento['interseccion']} = {evento['valor']}")
            print()

    except KeyboardInterrupt:
        print("\n[SUB] Cerrando...")
    finally:
        sub.setsockopt(zmq.LINGER, 0)
        sub.close()
        ctx.term()
        print("[SUB] Cerrado correctamente")


if __name__ == '__main__':
    main()