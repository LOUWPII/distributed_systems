import zmq
import json

def validar_sentido_fisico(topico, evento):
    """
    Este es un codigo para simplemente validar si los senosres muestran datos válidos
    """
    try:
        # 1. Validación para Cámaras: Relación Cola vs Velocidad 
        if topico == 'sensor.camara':
            cola = evento.get('volumen', 0)
            vel = evento.get('velocidad_promedio', 0)
            # nivel = cola / 20. Velocidad esperada = 50 * (1 - nivel)
            nivel_est = cola / 20.0
            vel_esperada = 50 * (1 - nivel_est)
            # Margen de error por ruido gaussiano (aprox 7.5 km/h) 
            if abs(vel - vel_esperada) > 8.0:
                return False, f"Incoherencia física: Cola {cola} no coincide con Vel {vel} km/h"

        # 2. Validación para GPS: Categoría vs Velocidad 
        elif topico == 'sensor.gps':
            vel = evento.get('velocidad_promedio', 0)
            cat = evento.get('nivel_congestion', '')
            if cat == 'ALTA' and vel >= 10: return False, "Categoría ALTA con velocidad >= 10"
            if cat == 'NORMAL' and (vel < 10 or vel > 40): return False, "Categoría NORMAL fuera de rango 10-40"
            if cat == 'BAJA' and vel <= 40: return False, "Categoría BAJA con velocidad <= 40"

        # 3. Validación para Espiras: Flujo máximo
        elif topico == 'sensor.espira_inductiva':
            # Q = nivel * (1-nivel) * 40. El máximo físico es 10 veh en 30s 
            contados = evento.get('vehiculos_contados', 0)
            if contados > 12: # Margen por ruido
                return False, f"Flujo imposible: {contados} vehículos supera capacidad Q_max"

        return True, "Datos coherentes"
    except Exception as e:
        return False, f"Error en validación: {str(e)}"

def broker_prueba_inteligente():
    context = zmq.Context()
    subscriber = context.socket(zmq.SUB)
    subscriber.bind("tcp://*:5550") # Puerto de entrada 
    
    topicos = ["sensor.espira_inductiva", "sensor.camara", "sensor.gps"]
    for t in topicos:
        subscriber.setsockopt_string(zmq.SUBSCRIBE, t)
    
    print("[*] Broker de prueba con validación física iniciado...")

    while True:
        msg = subscriber.recv_string()
        topico, _, payload = msg.partition(' ')
        evento = json.loads(payload)
        
        # Ejecutar validación
        es_valido, motivo = validar_sentido_fisico(topico, evento)
        
        color = "\033[92m" if es_valido else "\033[91m" # Verde si ok, Rojo si mal
        reset = "\033[0m"
        
        print(f"{color}[{topico}] Sensor: {evento['sensor_id']} | {motivo}{reset}")
        if not es_valido:
            print(f"    DETALLE: {payload}")

if __name__ == "__main__":
    broker_prueba_inteligente()