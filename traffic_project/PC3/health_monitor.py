import zmq
import time
from zmq_socket import ZMQSocket


class HealthMonitor:
    def __init__(self, endpoint: str, interval: float = 2.0):
        self.endpoint = endpoint
        self.interval = interval

        self.context = zmq.Context()
        self.socket = ZMQSocket(self.context, zmq.PUB)
        self.socket.bind(endpoint)

        self.running = True

    def start(self):
        print(f"[HealthMonitor] Enviando heartbeat en {self.endpoint}")

        while self.running:
            try:
                self.socket.send("HEARTBEAT")
                print("[HealthMonitor] HEARTBEAT enviado")

                time.sleep(self.interval)

            except Exception as e:
                print("[HealthMonitor] Error:", e)

    def stop(self):
        self.running = False
        self.context.term()


# ENTRY POINT (proceso independiente)
if __name__ == "__main__":
    monitor = HealthMonitor(
        endpoint="tcp://*:5556",
        interval=2.0
    )
    monitor.start()