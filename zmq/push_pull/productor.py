#!/usr/bin/env python3
"""
Lab PUSH/PULL - Productor SIN consumidor conectado
"""

import zmq
import time


def main():
    ctx = zmq.Context()

    push = ctx.socket(zmq.PUSH)
    push.setsockopt(zmq.SNDHWM, 3)
    push.setsockopt(zmq.LINGER, 0)  # No esperar al cerrar
    push.bind('tcp://*:5555')

    print("[PRODUCTOR] HWM=3, NADIE conectado todavía")
    print("[PRODUCTOR] Esperando 5s para que te conectes el consumidor...")
    time.sleep(5)  # ← Consumidor no conectado aún

    print("[PRODUCTOR] Enviando 10 mensajes rápido...\n")

    for i in range(1, 11):
        mensaje = f"Tarea {i}"

        inicio = time.time()
        push.send_string(mensaje)
        tiempo = time.time() - inicio

        print(f"[PRODUCTOR] {mensaje}: {tiempo:.3f}s")
        time.sleep(0.1)


if __name__ == '__main__':
    main()