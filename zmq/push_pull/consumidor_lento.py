#!/usr/bin/env python3
"""
Lab PUSH/PULL Sencillo - Consumidor LENTO
Procesa 1 mensaje cada 2 segundos
"""

import zmq
import time


def main():
    ctx = zmq.Context()

    pull = ctx.socket(zmq.PULL)
    pull.connect('tcp://localhost:5555')

    print("[CONSUMIDOR-LENTO] PULL conectado")
    print("[CONSUMIDOR-LENTO] Procesando LENTO (1 cada 2s)\n")

    recibidos = 0

    try:
        while True:
            mensaje = pull.recv_string()
            recibidos += 1

            print(f"[CONSUMIDOR] Recibido: {mensaje} [#{recibidos}]")
            print(f"             Procesando... (duerme 2s)")

            time.sleep(5)  # LENTO

            print(f"             [OK] Terminado {mensaje}")

    except KeyboardInterrupt:
        print(f"\n[CONSUMIDOR] Total recibidos: {recibidos}")
        pull.close()
        ctx.term()


if __name__ == '__main__':
    main()