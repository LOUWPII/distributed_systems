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
    print("  Sistema de Gestión de Tráfico")
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

    def _on_health_change(pc3_disponible: bool):
        if pc3_disponible:
            print("[Main][Failover] PC3 recuperado -> monitoreo respaldo desactivado.")
            print("[Main][Inserts] Destino activo: BD Principal + BD Réplica.")
        else:
            print("[Main][Failover] PC3 caído -> monitoreo respaldo activado.")
            print("[Main][Inserts] Destino activo: SOLO BD Réplica (redirección automática).")

    health_monitor.add_listener(_on_health_change)

    # 4. Arrancar todos los hilos
    hilos = [health_monitor, gestor_salida, rules_engine, event_receiver, query_handler]
    for hilo in hilos:
        hilo.start()
        print(f"[Main] Hilo iniciado: {hilo.name}")

    print("[Main] Servicio de Analítica activo.\n")
    print("[Main][Inserts] Destino inicial: BD Principal + BD Réplica.")

    def apagar(sig, frame):
        print("\n[Main] Señal de interrupción recibida — apagando...")
        for hilo in [event_receiver, query_handler, rules_engine, health_monitor, gestor_salida]:
            if hasattr(hilo, "detener"):
                hilo.detener()
        print("[Main] Servicio detenido.")
        sys.exit(0)

    signal.signal(signal.SIGINT,  apagar)
    signal.signal(signal.SIGTERM, apagar)

    # 5. Mantener el hilo principal vivo de forma interrumpible
    try:
        ultimo_ruteo = None
        ultimo_reporte = 0.0
        while True:
            time.sleep(1) # sleep permite que Python respire y detecte señales
            pc3_ok = health_monitor.is_pc3_disponible()
            ruteo = "PRINCIPAL+REPLICA" if pc3_ok else "SOLO_REPLICA"
            if ruteo != ultimo_ruteo:
                if pc3_ok:
                    print("[Main][Inserts] Envío habilitado a BD Principal y BD Réplica.")
                else:
                    print("[Main][Inserts] Falla en BD Principal. Envío redirigido a BD Réplica.")
                ultimo_ruteo = ruteo

            ahora = time.time()
            if ahora - ultimo_reporte >= 5:
                metricas = gestor_salida.obtener_metricas()
                print(
                    "[Main][Inserts][Estado] "
                    f"ruta={ruteo} | replica_enviados={metricas['replica_enviados']} "
                    f"| principal_enviados={metricas['principal_enviados']} "
                    f"| redirigidos_replica={metricas['principal_omitidos']}"
                )
                ultimo_reporte = ahora
    except KeyboardInterrupt:
        print("\n[Main] Apagando servicio de analítica (KeyboardInterrupt)...")
        apagar(None, None) # Llamamos a la función de limpieza


if __name__ == "__main__":
    main()
