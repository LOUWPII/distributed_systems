import grpc
from concurrent import futures
import logging
import interfaz_pb2
import interfaz_pb2_grpc

# La clase Estudiante se utiliza como estructura de datos para almacenar la informacion de un estudiante
class Estudiante:
    def __init__(self, nombre, apellido, grupo, quiz, taller):
        self.nombre = nombre
        self.apellido = apellido
        self.grupo = grupo
        self.quiz = quiz
        self.taller = taller

# La clase ConsultaService implementa el servicio definido en interfaz.proto
class ConsultaService(interfaz_pb2_grpc.ConsultaServicer):
    # Lista de estudiantes con su respectiva informacion
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
    # Metodo que permite consultar el promedio de notas de un estudiante por nombre o apellido
    def ConsultarNotas(self, request, context):
        for estudiante in self.Estudiantes:
            if estudiante.nombre == request.estudiante or estudiante.apellido == request.estudiante:
                # Se retorna el promedio como respuestas al cliente
                return interfaz_pb2.NotasReply(promedio=(estudiante.quiz + estudiante.taller) / 2)
    # Metodo que permite consultar el grupo de un estudiante por nombre o apellido
    def ConsultarGrupo(self, request, context):
        for estudiante in self.Estudiantes:
            if estudiante.nombre == request.estudiante or estudiante.apellido == request.estudiante:
                # Se retorna el grupo como respuestas al cliente
                return interfaz_pb2.GrupoReply(grupo=estudiante.grupo)
    # Metodo que permite consultar las notas del quiz y taller de un estudiante por nombre o apellido
    def ConsultarEvaluaciones(self, request, context):
        for estudiante in self.Estudiantes:
            if estudiante.nombre == request.estudiante or estudiante.apellido == request.estudiante:
                # Se retornan las notas del quiz y taller como respuestas al cliente
                return interfaz_pb2.EvaluacionesReply(notaQuiz=estudiante.quiz, notaTaller=estudiante.taller)

# La funcion encargada de iniciar el servidor
def serve():
    # puerto en el que el servidor escuchara las solicitudes del cliente
    port = "50051"

    #Se crea el servidor
    # ThreadPoolExecutor(max_workers=10) indica que se pueden manejar hasta 10 solicitudes concurrentes 
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10)
    )

    # Se registra el servicio ConsultaService en el servidor
    interfaz_pb2_grpc.add_ConsultaServicer_to_server(
        ConsultaService(),
        server
    )

    #Direccion y puerto en el que el servidor escuchara las solicitudes del cliente
    server.add_insecure_port('[::]:50051')
    #Se inicia el servidor
    server.start()
    
    print(f"Servidor gRPC escuchando en el puerto {port}")
    #Se mantiene el servidor hasta que se detenga manualmente o ocurra un error
    server.wait_for_termination()

# entrada al programa
if __name__ == "__main__":
    logging.basicConfig()
    serve()