import zmq
import json
def broker_prueba():
    # 1. Crear el contexto y el socket de suscripción (SUB)
    context = zmq.Context()
    subscriber = context.socket(zmq.SUB)

    # Usamos el puerto 5550 que definimos en el config.json
    puerto = 5550
    subscriber.bind(f"tcp://*:{puerto}")
    
    # 3. Suscribirse a todos los tópicos de sensores 
    topicos = ["sensor.espira_inductiva", "sensor.camara", "sensor.gps"]
    for t in topicos:
        subscriber.setsockopt_string(zmq.SUBSCRIBE, t)
    
    print(f"[*] Broker de prueba iniciado en el puerto {puerto}...")
    print(f"[*] Escuchando tópicos: {topicos}\n")

    try:
        while True:
            # 4. Recibir el mensaje (bloqueante)
            mensaje_raw = subscriber.recv_string()
            
            # 5. Separar tópico y contenido para mejor visualización 
            topico, _, payload = mensaje_raw.partition(' ')
            
            print(f"--- NUEVO MENSAJE RECIBIDO ---")
            print(f"TÓPICO:  {topico}")
            print(f"PAYLOAD: {payload}")
            print(f"------------------------------\n")
            
    except KeyboardInterrupt:
        print("\n[*] Broker de prueba detenido.")

if __name__ == "__main__":
    broker_prueba()