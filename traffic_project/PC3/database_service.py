import zmq
import json
from zmq_socket import ZMQSocket
from jsonl_storage import JSONLStorage


class DatabaseService:
    def __init__(self, endpoint: str, filepath: str):
        self.endpoint = endpoint

        self.context = zmq.Context()
        self.socket = ZMQSocket(self.context, zmq.PULL)
        self.socket.bind(endpoint)

        self.storage = JSONLStorage(filepath)
        self.running = True

    def start(self):
        print(f"[DatabaseService] Escuchando en {self.endpoint}")

        while self.running:
            try:
                msg = self.socket.receive()

                if self._is_valid_json(msg):
                    self.storage.append(msg)
                    print("[DatabaseService] Guardado:", msg)
                else:
                    print("[DatabaseService] JSON inválido ignorado")

            except Exception as e:
                print("[DatabaseService] Error:", e)

    def _is_valid_json(self, msg: str) -> bool:
        try:
            json.loads(msg)
            return True
        except:
            return False


# ENTRY POINT DEL PROCESO
if __name__ == "__main__":
    service = DatabaseService(
        endpoint="tcp://*:5555",
        filepath="database.jsonl"
    )
    service.start()