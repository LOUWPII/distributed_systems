import zmq
import json

context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect("tcp://localhost:5560")  # Puerto de QueryHandler según config.json

# --- Prueba 1: Consultar todas las calles ---
solicitud_estados = {"tipo": "CONSULTA_TODOS_ESTADOS"}
socket.send_string(json.dumps(solicitud_estados))
respuesta = socket.recv_string()
print(f"Respuesta a la consulta global: \n{json.dumps(json.loads(respuesta), indent=2)}\n")

# --- Prueba 2: Enviar una Orden Directa (Ambulancia en fila_C) ---
orden_ambulancia = {
    "tipo": "ORDEN_DIRECTA",
    "calle_id": "fila_C",
    "accion": "OLA_VERDE",
    "duracion_s": 45,
    "motivo": "EMERGENCIA_AMBULANCIA"
}
socket.send_string(json.dumps(orden_ambulancia))
respuesta_orden = socket.recv_string()
print(f"Respuesta a la orden enviada: \n{json.dumps(json.loads(respuesta_orden), indent=2)}")
