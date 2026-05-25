"""
test_throughput.py — Mide eventos almacenados en BD Replica en 120s.

Uso:
    python tests/benchmark/test_throughput.py

No inicia procesos. Asume que todos los componentes ya estan corriendo
(PC3, ReplicaDB, PC2, Broker, Sensores).
Lee URLs de config.json -> benchmark -> tests -> throughput.
"""

import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import load_benchmark_test, clean_data_dirs, REPLICA_DATA_DIR, PRINCIPAL_DATA_DIR
import measurements

DURACION_S = 120


def main():
    cfg = load_benchmark_test('throughput')
    bd_replica_url = cfg['bd_replica_REQ']

    print(f"\n--- Throughput ---")
    print(f"  BD Replica: {bd_replica_url}")
    print(f"  Midiendo durante {DURACION_S}s...")

    tp = measurements.count_db_events(bd_replica_url, DURACION_S)

    print(f"  Resultado: {tp['count']} eventos ({tp['eps']:.2f} ev/s)")
    return 0


if __name__ == '__main__':
    sys.exit(main())
