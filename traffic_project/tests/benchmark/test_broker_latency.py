"""
test_broker_latency.py — Mide latencia del broker (PC1).

Uso:
    python tests/benchmark/test_broker_latency.py
    python tests/benchmark/test_broker_latency.py --modo simple
    python tests/benchmark/test_broker_latency.py --modo multihilos

Inicia sensores.py + broker.py localmente, mide latencia via archivo.
Ejecuta ambos modos por defecto.
"""

import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (
    load_benchmark_test, ProcessManager, build_pc1_config_base,
    write_pc1_config, read_latency_file, latency_stats,
    PC1_DIR, BENCHMARK_DIR
)

BROKER_TEST_SENSORES = [
    {"sensor_id": "CAM-C2", "tipo": "camara", "interseccion": "INT_C2", "calle_id": "fila_C", "direccion": "fila"},
    {"sensor_id": "ESP-C2", "tipo": "espira_inductiva", "interseccion": "INT_C2", "calle_id": "col_2", "direccion": "columna"},
    {"sensor_id": "GPS-B3", "tipo": "gps", "interseccion": "INT_B3", "calle_id": "fila_B", "direccion": "fila"},
    {"sensor_id": "CAM-B3", "tipo": "camara", "interseccion": "INT_B3", "calle_id": "col_3", "direccion": "columna"},
    {"sensor_id": "ESP-C4", "tipo": "espira_inductiva", "interseccion": "INT_C4", "calle_id": "fila_C", "direccion": "fila"},
    {"sensor_id": "CAM-C4", "tipo": "camara", "interseccion": "INT_C4", "calle_id": "col_4", "direccion": "columna"},
]

DURACION_S = 20
MODO_POR_DEFECTO = 'simple'


def test_broker_latency(modo):
    modo_desc = 'simple' if modo == 'simple' else 'multiprocesos'
    print(f"\n--- Broker Latency ({modo_desc}) ---")

    latency_file = os.path.join(BENCHMARK_DIR, f'latency_{modo}.txt')
    if os.path.exists(latency_file):
        os.remove(latency_file)

    env_broker = os.environ.copy()
    env_broker['BENCHMARK_LATENCY_FILE'] = latency_file

    config = build_pc1_config_base(modo)
    config['parametros_simulacion']['intervalo_sensores_s'] = 3
    config['sensores'] = BROKER_TEST_SENSORES
    write_pc1_config(config)

    with ProcessManager() as pm:
        pm.start('Sensores', 'sensores.py', PC1_DIR)
        time.sleep(2)
        pm.start('Broker', 'broker.py', PC1_DIR, env=env_broker)
        time.sleep(3)
        print(f"  Midiendo durante {DURACION_S}s...")
        time.sleep(DURACION_S)

    latencies = read_latency_file(latency_file)
    stats = latency_stats(latencies)
    print(f"  Resultado: avg={stats['avg']:.1f}us  min={stats['min']:.1f}us  max={stats['max']:.1f}us  (n={stats['n']})")

    write_pc1_config(build_pc1_config_base('multihilos'))
    return stats


def main():
    load_benchmark_test('broker_latency')

    modos = []
    if '--modo' in sys.argv:
        idx = sys.argv.index('--modo')
        if idx + 1 < len(sys.argv):
            modos.append(sys.argv[idx + 1])
    else:
        modos = ['simple', 'multihilos']

    resultados = []
    for modo in modos:
        r = test_broker_latency(modo)
        resultados.append((modo, r))
        time.sleep(2)

    print(f"\nResultados:")
    for modo, r in resultados:
        desc = 'simple' if modo == 'simple' else 'multiprocesos'
        print(f"  {desc}: avg={r['avg']:.1f}us  min={r['min']:.1f}us  max={r['max']:.1f}us  (n={r['n']})")


if __name__ == '__main__':
    main()
