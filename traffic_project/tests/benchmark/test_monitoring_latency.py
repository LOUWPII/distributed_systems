"""
test_monitoring_latency.py — Mide RTT de CONSULTA_TODOS_ESTADOS.

Uso:
    python tests/benchmark/test_monitoring_latency.py

No inicia procesos. Asume que main.py ya esta corriendo.
Lee URLs de config.json -> benchmark -> tests -> monitoring_latency.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import load_benchmark_test
import measurements

N_QUERIES = 10


def main():
    cfg = load_benchmark_test('monitoring_latency')
    query_handler_url = cfg['query_handler_REQ']

    print(f"\n--- Monitoring Latency ---")
    print(f"  QueryHandler: {query_handler_url}")
    print(f"  Enviando {N_QUERIES}x CONSULTA_TODOS_ESTADOS...")

    avg, mn, mx = measurements.measure_monitoring_latency(query_handler_url, N_QUERIES)

    print(f"  Resultado: avg={avg:.1f}ms  min={mn:.1f}ms  max={mx:.1f}ms")
    return 0


if __name__ == '__main__':
    sys.exit(main())
