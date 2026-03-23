#!/usr/bin/env python3
"""
Lab 1 - Publicador (PUB)
Etapa 1: Fundamentos ZMQ

Este proceso simula un sensor de tráfico que publica eventos.
Demuestra: Context, socket PUB, bind, mensajes atómicos, reconexión automática.
"""

import zmq
import json
import time
import random


def main():
    # 1. CREAR CONTEXTO - Uno solo por proceso, es el núcleo de ZMQ
    # El contexto administra hilos C internos, colas de I/O, y todos los sockets
    ctx = zmq.Context()

    # 2. CREAR SOCKET PUB - Solo puede enviar mensajes, nunca recibir
    # PUB = Publicador del patrón PUB/SUB
    pub = ctx.socket(zmq.PUB)

    # 3. BIND - El publicador es el lado "estable", espera conexiones
    # *:5550 = escucha en todas las interfaces de red, puerto 5550
    pub.bind('tcp://*:5550')
    print("[PUB] Socket PUB creado y bind en tcp://*:5550")
    print("[PUB] Esperando suscriptores...")

    # 4. Esperar a que los suscriptores se conecten
    # En PUB/SUB, los mensajes enviados antes de que haya suscriptores se pierden
    # ZMQ no tiene memoria histórica
    time.sleep(1)

    contador = 0
    try:
        while True:
            contador += 1

            # Simular un evento de sensor de tráfico
            evento = {
                'id_evento': contador,
                'tipo_sensor': random.choice(['espira', 'camara', 'gps']),
                'interseccion': f'INT_{random.choice(["A1", "B2", "C3"])}',
                'timestamp': time.time(),
                'valor': random.randint(0, 100)  # Nivel de tráfico 0-100
            }

            # 5. CONSTRUIR MENSAJE: tópico + espacio + JSON
            # El tópico permite filtrado en el suscriptor
            # Usamos send_string para UTF-8 automático
            topico = f"sensor.{evento['tipo_sensor']}"
            mensaje = f"{topico} {json.dumps(evento)}"

            pub.send_string(mensaje)
            print(f"[PUB] Enviado: {topico} | Evento #{contador}")

            time.sleep(10)  # Un evento cada 2 segundos

    except KeyboardInterrupt:
        print("\n[PUB] Cerrando...")
    finally:
        # 6. LIMPIEZA: cerrar socket y terminar contexto
        # LINGER=0: no esperar a drenar colas, cerrar inmediatamente
        pub.setsockopt(zmq.LINGER, 0)
        pub.close()
        ctx.term()
        print("[PUB] Cerrado correctamente")


if __name__ == '__main__':
    main()