import zmq


class ZMQSocket:
    def __init__(self, context, socket_type):
        self.socket = context.socket(socket_type)

    def bind(self, endpoint: str):
        self.socket.bind(endpoint)

    def connect(self, endpoint: str):
        self.socket.connect(endpoint)

    def send(self, msg: str):
        self.socket.send_string(msg)

    def receive(self) -> str:
        return self.socket.recv_string()

    def subscribe(self, topic: str = ""):
        self.socket.setsockopt_string(zmq.SUBSCRIBE, topic)