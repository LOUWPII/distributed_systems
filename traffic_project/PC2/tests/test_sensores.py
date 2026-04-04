import zmq
import time
import json
from datetime import datetime

context = zmq.Context()
socket = context.socket(zmq.PUB)
# Tu config.json dice que el Broker debe estar en tcp://192.168.1.10:5555. 
# Si lo pruebas en Localhost temporalmente, cambia esa IP en tu config.json a tcp://127.0.0.1:5555
socket.bind("tcp://127.0.0.1:5555") 

print("Iniciando simulador de sensores (esperando que PC2 se conecte)...")
time.sleep(1)  # Dar tiempo a que el suscriptor se conecte

# Evento GPS falso (Tópico "gps") indicando CONGESTION en la calle "col_3"
evento_congestion = {
    "sensor_id": "GPS-B3",
    "interseccion": "INT_B3",
    "calle_id": "col_3",
    "timestamp": datetime.now().isoformat() + "Z", # Formato ISO estricto de tu parser
    "nivel_congestion": "ALTA",
    "velocidad_promedio": 5.5
}

# Tópico en primer frame, JSON en el segundo
socket.send_multipart([b"gps", json.dumps(evento_congestion).encode('utf-8')])
print("Evento despachado: Congestión en col_3.")
