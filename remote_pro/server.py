import grpc
from concurrent import futures
import logging
import interfaz_pb2
import interfaz_pb2_grpc


class Estudiante:
    def __init__(self, nombre, apellido, grupo, quiz, taller):
        self.nombre = nombre
        self.apellido = apellido
        self.grupo = grupo
        self.quiz = quiz
        self.taller = taller


class ConsultaService(interfaz_pb2_grpc.ConsultaServicer):
    Estudiantes = [
        Estudiante("Maria", "Perez", "GRT 1", 4.5, 4),
        Estudiante("Jose", "Montealegre", "GRT 1", 4.25, 4),
        Estudiante("Juan", "Sanchez Burbano", "GRT 2", 5, 4.5),
        Estudiante("Mariana", "Tellez Vallejo", "GRT 2", 5, 4.5),
        Estudiante("Miguel", "Castiblanco", "GRT 3", 5, 4.4),
        Estudiante("Thamara", "Ospina", "GRT 3", 5, 4.4),
        Estudiante("Pedro", "Berrizbeitia", "GRT 4", 4.75, 0),
        Estudiante("Samira", "Morales", "GRT 4", 4.9, 3),
        Estudiante("Thomas Alberto", "Sarmiento", "GRT 5", 4.5, 4),
        Estudiante("Lucia", "Montenegro", "GRT 5", 0, 4),
        Estudiante("Juan", "Madrigal Luz", "GRT 5", 4.5, 4),
        Estudiante("Nicolas", "Morales Sanchez", "GRT 6", 5, 2.8),
        Estudiante("Daniela", "Bohorquez", "GRT 7", 4.8, 2.3),
        Estudiante("Mariana", "Diaz Sanjuan", "GRT 7", 4.8, 2.3),
        Estudiante("Alejandro", "Parrado Cruz", "GRT 8", 4.9, 4.3),
        Estudiante("Silvestre", "Vargas Fonseca", "GRT 8", 4.9, 4.3),
        Estudiante("Juliana", "Araque Rojas", "GRT 9", 5, 0),
        Estudiante("Juan Ignacio", "Quintero", "GRT 9", 0, 3),
        Estudiante("Monica", "Jimenez", "GRT 9", 5, 3),
    ]

    def ConsultarNotas(self, request, context):
        for estudiante in self.Estudiantes:
            if estudiante.nombre == request.estudiante or estudiante.apellido == request.estudiante:
                return interfaz_pb2.NotasReply(promedio=(estudiante.quiz + estudiante.taller) / 2)

    def ConsultarGrupo(self, request, context):
        for estudiante in self.Estudiantes:
            if estudiante.nombre == request.estudiante or estudiante.apellido == request.estudiante:
                return interfaz_pb2.GrupoReply(grupo=estudiante.grupo)

    def ConsultarEvaluaciones(self, request, context):
        for estudiante in self.Estudiantes:
            if estudiante.nombre == request.estudiante or estudiante.apellido == request.estudiante:
                return interfaz_pb2.EvaluacionesReply(notaQuiz=estudiante.quiz, notaTaller=estudiante.taller)


def serve():
    port = "50051"

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10)
    )

    interfaz_pb2_grpc.add_ConsultaServicer_to_server(
        ConsultaService(),
        server
    )

    server.add_insecure_port('localhost:50051')
    server.start()

    print(f"Servidor gRPC escuchando en el puerto {port}")
    server.wait_for_termination()


if __name__ == "__main__":
    logging.basicConfig()
    serve()
