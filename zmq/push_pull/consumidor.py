#!/usr/bin/env python3
"""
Lab PUSH/PULL Sencillo - Consumidor RÁPIDO
Procesa inmediatamente
"""

import zmq
import time


def main():
    ctx = zmq.Context()

    # PULL: Recibe trabajos de la fila
    pull = ctx.socket(zmq.PULL)
    pull.connect('tcp://localhost:5555')

    print("[CONSUMIDOR-RÁPIDO] PULL conectado")
    print("[CONSUMIDOR-RÁPIDO] Procesando inmediatamente\n")

    recibidos = 0

    try:
        while True:
            # recv() BLOQUEA si no hay mensajes
            mensaje = pull.recv_string()
            recibidos += 1

            print(f"[CONSUMIDOR] Recibido: {mensaje} [#{recibidos}]")
            # Sin sleep: procesa inmediatamente

    except KeyboardInterrupt:
        print(f"\n[CONSUMIDOR] Total recibidos: {recibidos}")
        pull.close()
        ctx.term()


if __name__ == '__main__':
    main()