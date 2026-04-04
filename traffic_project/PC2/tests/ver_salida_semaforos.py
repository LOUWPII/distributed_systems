import zmq

context = zmq.Context()
socket = context.socket(zmq.PULL)
socket.bind("tcp://127.0.0.1:5556") # Puerto PUSH de tu config hacia los semáforos locales

print("Escuchando órdenes a Semáforos...")
while True:
    mensaje = socket.recv_string()
    print(f"COMANDO SEMÁFORO RECIBIDO:\n {mensaje}\n")
