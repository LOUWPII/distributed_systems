"""
main.py — Punto de entrada del Servicio de Analítica (PC2).

Responsabilidades:
  1. Cargar la configuración desde config.json
  2. Crear la event_queue compartida entre EventReceiver y RulesEngine
  3. Instanciar todos los componentes en el orden correcto (dependencias primero)
  4. Arrancar los hilos
  5. Esperar señal de interrupción (Ctrl+C) y apagar ordenadamente

Orden de instanciación (importante — las dependencias deben existir primero):
  Config → HealthMonitor → GestorSalida → RulesEngine → EventReceiver → QueryHandler
"""

import queue
import signal
import sys
import time

from config import Config
from infrastructure.health_monitor import HealthMonitor
from infrastructure.gestor_salida import GestorSalida
from application.rules_engine import RulesEngine
from infrastructure.event_receiver import EventReceiver
from application.query_handler import QueryHandler


def main():
    print("=" * 55)
    print("  Servicio de Analítica — PC2")
    print("  Sistema de Gestión de Tráfico Urbano")
    print("=" * 55)

    # 1. Cargar configuración
    try:
        config = Config("config/config.json")
        print(f"[Main] Configuración cargada: {config}")
    except FileNotFoundError as e:
        print(f"[Main] ERROR: {e}")
        sys.exit(1)

    # 2. Cola compartida entre EventReceiver y RulesEngine
    # maxsize=0 significa ilimitada — ZMQ ya tiene su propio buffer interno
    event_queue = queue.Queue(maxsize=0)

    # 3. Instanciar componentes en orden de dependencias
    health_monitor = HealthMonitor(config)
    gestor_salida = GestorSalida(config, health_monitor)
    rules_engine = RulesEngine(config, event_queue, gestor_salida)
    event_receiver = EventReceiver(config, event_queue)
    query_handler = QueryHandler(config, rules_engine)

    # 4. Arrancar todos los hilos
    hilos = [health_monitor, rules_engine, event_receiver, query_handler]
    for hilo in hilos:
        hilo.start()
        print(f"[Main] Hilo iniciado: {hilo.name}")

    print("[Main] Servicio de Analítica activo. Ctrl+C para detener.\n")

    # 5. Manejar Ctrl+C para apagado ordenado
    def apagar(sig, frame):
        print("\n[Main] Señal de interrupción recibida — apagando...")
        for hilo in [event_receiver, query_handler, rules_engine, health_monitor]:
            if hasattr(hilo, "detener"):
                hilo.detener()
        gestor_salida.cerrar()
        print("[Main] Servicio detenido.")
        sys.exit(0)

    signal.signal(signal.SIGINT,  apagar)
    signal.signal(signal.SIGTERM, apagar)

    # Mantener el hilo principal vivo
    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()
