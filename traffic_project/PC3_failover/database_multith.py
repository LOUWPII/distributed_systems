import zmq
import threading
import json
from jsonl_storage import JSONLStorage
import os

#Estos son los puertos donde escucha la BD main
#Lo ideal es que estos puertos se puedan leer desde el archivo config.json para hacer el sistema modificable sin cambiar codigo
PULL_PORT = 5560  # Puerto para recibir datos de Analítica (PUSH → PULL)
REP_PORT = 5562  # Puerto para health checks (REQ → REP)

DATA_FOLDER = "bd_principal_data" #En sta carpeta se guardan los archivos BD


class DatabaseService:
    """
       Servidor de Base de Datos Principal.

       Este servicio corre en PC3 y tiene dos responsabilidades:
       1. Recibir datos de la Analítica y guardarlos en archivos separados por tipo
       2. Responder a health checks para que la Analítica sepa si el servicio está vivo

       Los datos se clasifican en 4 categorías, cada una con su propio archivo:
       - evento: eventos generales del sistema
       - congestion: alertas de congestión vehicular
       - priorizacion: solicitudes de prioridad (ambulancias, bomberos, etc.)
       - semaforo: cambios de estado de semáforos

       El guardado es atómico: si el programa se cae en medio de una escritura,
       el archivo original no se corrompe (primero se escribe en .tmp y luego se renombra).

       Ademas se estan utilizando dos hilos, el hilo que tiene el socket PULL para recibir los eventos desde analítica, y
       el otro hilo que tiene un socket REP para poder responder a los mansajes PING de analítica o el sericio de monitoreo
       """
    def __init__(self):
        """
                Inicializa el servicio de Base de Datos.

                Args:
                    Lo ideal es que los puertos e ips se puedan recibir desde el JSON
                """
        self.context = zmq.Context()

        #Esto genera una carpeta donde van los archivos de persistencia que crea este programa
        os.makedirs(DATA_FOLDER, exist_ok=True)
        print(f"[DB] Carpeta de datos: {DATA_FOLDER}/")

        # Diccionario que mapea cada tipo de registro a su archivo JSONL
        self.storages = {
            "evento": JSONLStorage(os.path.join(DATA_FOLDER, "eventos.jsonl")),
            "congestion": JSONLStorage(os.path.join(DATA_FOLDER, "congestiones.jsonl")),
            "priorizacion": JSONLStorage(os.path.join(DATA_FOLDER, "priorizaciones.jsonl")),
            "semaforo": JSONLStorage(os.path.join(DATA_FOLDER, "semaforos.jsonl"))
        }

    def _loop_ingesta(self):
        """
            Hilo que recibe datos de la Analítica.

            Este hilo corre en segundo plano y nunca termina. Se queda esperando
            mensajes en el puerto PULL. Cada mensaje es un registro JSON con un
            campo "tipo_registro" que indica a qué archivo debe ir.

            El socket es PULL porque la Analítica hace PUSH
        """
        # Creación del socket PULL local al hilo
        pull_socket = self.context.socket(zmq.PULL)
        pull_socket.bind(f"tcp://*:{PULL_PORT}")
        print(f"[DB-Ingesta] Hilo PULL activo en puerto {PULL_PORT}")

        while True:
            evento = pull_socket.recv_json()
            tipo = evento["tipo_registro"]
            if tipo in self.storages:
                self.storages[tipo].append_atomico(evento)

    def _loop_consultas(self):
        """
            Hilo que atiende health checks y consultas.

            Este hilo responde a dos tipos de mensajes:
            - "PING": el cliente pregunta si este proceso está vivo y se responde "PONG"
            - Otros: se tratan como consultas de datos del monitoreo y consulta

            El socket es REP (Reply) porque se sigue el patrón REQ/REP con lazy pirate:
        """
        # Creación del socket REP local al hilo
        rep_socket = self.context.socket(zmq.REP)
        rep_socket.bind(f"tcp://*:{REP_PORT}")
        print(f"[DB-Consultas] Hilo REP activo en puerto {REP_PORT}")

        while True:
            solicitud = rep_socket.recv_string()
            print(f"[DB-Consultas] Solicitud recibida: {solicitud}")

            # Implementación del patrón de Salud (Lazy Pirate)
            if solicitud == "PING":
                rep_socket.send_string("PONG")
                print("[DB-Consultas] PONG enviado")
            else:
                # Respuesta a consultas de monitoreo - esto no se ha implementado todavía
                rep_socket.send_json({"status": "ok", "data": "Resultados de consulta..."})

    def iniciar(self):
        """
            Lanza el servicio y lo mantiene corriendo.

            Crea dos hilos en segundo plano (daemon=True) para que se detengan
            automáticamente cuando el programa principal termine. Luego se queda
            esperando a que ambos hilos terminen pero en realidad no pasa porqe
            hay while true en las rutinas de los hilos
        """
        t_ingesta = threading.Thread(target=self._loop_ingesta, daemon=True)
        t_consultas = threading.Thread(target=self._loop_consultas, daemon=True)

        t_ingesta.start()
        t_consultas.start()

        print("[DB] Base de Datos Principal (PC3) operando con hilos independientes.")
        print(f"   - Puerto PULL (datos): {PULL_PORT}")
        print(f"   - Puerto REP (health): {REP_PORT}")

        # Mantener el proceso principal bloqueado
        t_ingesta.join()
        t_consultas.join()


if __name__ == "__main__":
    db = DatabaseService()
    db.iniciar()