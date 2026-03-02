import grpc
import interfaz_pb2
import interfaz_pb2_grpc

def ejecutar_client():
    # Creamos el canal (el tubo de comunicación)
    with grpc.insecure_channel('192.168.80.59:50051') as canal:

        stub = interfaz_pb2_grpc.ConsultaStub(canal)
        
        peticion1 = interfaz_pb2.NotasRequest(estudiante="Maria")
        """
        Para la primera solicitud de consultar notas el cliente envía una petición con el nombre o apellido del estudiante, el servidor responde
        con el promedio de las notas del quiz y taller del estudiante. La definición de la función en el archivo .proto es la siguiente:

        rpc ConsultarNotas ( NotasRequest) returns (NotasReply) {}

        message NotasRequest{
            string estudiante = 1; //nombre o apellido del estudiante
        }

        message NotasReply{
            float promedio = 1;
        }

        """
        #Se envía como argumento la petición y se recibe la respuesta del servidor en una variable de tipo NotasReply
        respuesta1 = stub.ConsultarNotas(peticion1)
        
        print("El servidor dice:", respuesta1.promedio)

        peticion2 = interfaz_pb2.GrupoRequest(estudiante="Montealegre")
        """
        Para la segunda solicitud de consultar grupo el cliente envía una petición con el nombre o apellido del estudiante, el servidor responde
        con el grupo al que pertenece el estudiante. La definición de la función en el archivo .proto es la siguiente:

        rpc ConsultarGrupo ( GrupoRequest) returns (GrupoReply) {}

        message GrupoRequest{
            string estudiante = 1; //nombre o apellido del estudiante
        }

        message GrupoReply{
            string grupo = 1;
        }

        """
        #Se envía como argumento la petición y se recibe la respuesta delservidor en una variable de tipo GrupoReply
        respuesta2 = stub.ConsultarGrupo(peticion2)
        print("El servidor dice:", respuesta2.grupo)

        peticion3 = interfaz_pb2.EvaluacionesRequest(estudiante="Tellez Vallejo")
        """
        Para la tercer solicitud de consultar grupo el cliente envía una petición con el nombre o apellido del estudiante, el servidor responde
        con la nota del quiz y taller del estudiante. La definición de la función en el archivo .proto es la siguiente:

        rpc ConsultarEvaluaciones (EvaluacionesRequest) returns ( EvaluacionesReply) {}

        message EvaluacionesRequest{
            string estudiante = 1; //nombre o apellido del estudiante
        }
        message EvaluacionesReply{
            float notaQuiz = 1;
            float notaTaller = 2;
        }
        """
        respuesta3 = stub.ConsultarEvaluaciones(peticion3)
        print("El servidor dice: nota quiz:", respuesta3.notaQuiz, "nota taller:", respuesta3.notaTaller)

if __name__ == "__main__":
    ejecutar_client()