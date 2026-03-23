#!/usr/bin/env python3
"""
Mini Taller Etapa 1 - Servidor Echo
Completa los 3 TODOs marcados
"""

import zmq
import time


def main():
    # TODO 1: Crear contexto ZMQ
    ctx = zmq.Context()

    # TODO 2: Crear socket REP (responde a requests)
    rep = ctx.socket(zmq.REP)
    # TODO 3: Hacer bind en puerto 5557
    rep.bind('tcp://*:5557')

    rep.setsockopt(zmq.RCVTIMEO, 5000)
    print("[SERVIDOR] Echo server listo en tcp://*:5557")


    while True:
        try:
            # TODO 4: Recibir mensaje del cliente (recv_string)
            # Guardar en variable 'mensaje'
            mensaje = rep.recv()
            print(f"[SERVIDOR] Recibido: {mensaje}")

            # Simular procesamiento
            time.sleep(0.5)

            respuesta = mensaje.upper()

            # TODO 5: Enviar respuesta al cliente (send_string)
            rep.send(respuesta)
            print(f"[SERVIDOR] Respondido: {respuesta}")

        except KeyboardInterrupt:
            print("\n[SERVIDOR] Cerrando...")
            break

    # TODO 6: Cerrar socket y contexto limpiamente
    # Usar LINGER=0
    rep.setsockopt(zmq.LINGER, 0)
    rep.close()
    ctx.term()

if __name__ == '__main__':
    main()