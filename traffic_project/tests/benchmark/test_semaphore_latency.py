"""
test_semaphore_latency.py — Mide RTT de ORDEN_DIRECTA -> CAMBIO_SEMAFORO.

Uso:
    python tests/benchmark/test_semaphore_latency.py

No inicia procesos. Asume que main.py y control_semaforos.py ya estan corriendo.
Lee URLs de config.json -> benchmark -> tests -> semaphore_latency.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import load_benchmark_test
import measurements

TIMEOUT_S = 10


def main():
    cfg = load_benchmark_test('semaphore_latency')
    query_handler_url = cfg['query_handler_REQ']
    semaphore_pub_url = cfg['semaphore_pub_SUB']

    print(f"\n--- Semaphore Latency ---")
    print(f"  QueryHandler: {query_handler_url}")
    print(f"  Semaphore PUB: {semaphore_pub_url}")
    print(f"  Enviando ORDEN_DIRECTA y esperando CAMBIO_SEMAFORO...")

    lat_ms = measurements.measure_semaphore_latency(
        query_handler_url, semaphore_pub_url, 'fila_C', TIMEOUT_S
    )

    if lat_ms is not None:
        print(f"  Resultado: {lat_ms:.1f}ms")
        return 0
    else:
        print(f"  Resultado: None (timeout)")
        return 1


if __name__ == '__main__':
    sys.exit(main())
