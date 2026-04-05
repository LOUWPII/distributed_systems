import zmq
import json
import threading
from datetime import datetime, timezone
class BrokerZMQ:
    def __init__(self, config):
        """Inicializa configuración y contadores ."""
        self.config = config
        self.modo = config.get('modo_broker', 'simple')  # 'simple' o 'multihilos'
        self.topicos = list(config['sensores_topicos'].values())
        self.contadores = {t: 0 for t in self.topicos}
        # En el diseño actual usamos el Modo Simple como base robusta
        if self.modo == 'simple':
            self._configurar_sockets()
    def _configurar_sockets(self):
        """Configuración para el modo de un solo hilo."""
        self.context = zmq.Context()
        # Frontend: Recibe de sensores (SUB)
        self.sub_socket = self.context.socket(zmq.SUB)
        self.sub_socket.bind(self.config['red']['sensor_broker_url_PUB'])
        for t in self.topicos:
            self.sub_socket.setsockopt_string(zmq.SUBSCRIBE, t)
        # Backend: Publica hacia PC2 (PUB)
        self.pub_socket = self.context.socket(zmq.PUB)
        self.pub_socket.bind(self.config['red']['broker_analitica_url_PUB'])
    def _validar(self, topico, evento):
        """
        Validación del JSON (espera un DICCIONARIO).
        """
        if topico not in self.topicos: 
            return False
        if 'sensor_id' not in evento: 
            return False
        
        # Validación estructural mínima
        if topico in ('espira_inductiva', 'camara'):
            if 'interseccion' not in evento: 
                return False
        return True
    def _validar_sentido_fisico(self, topico, evento):
        """Verifica coherencia básica según Greenshields."""
        try:
            if topico == 'camara':
                # El volumen no puede ser negativo
                if evento.get('volumen', 0) < 0: return False
                # La velocidad no puede ser mayor que la de flujo libre de forma exagerada
                if evento.get('velocidad_promedio', 0) > 60: return False
            elif topico == 'gps':
                v = evento.get('velocidad_promedio', 0)
                cat = evento.get('nivel_congestion', '')
                if cat == 'ALTA' and v >= 20: return False
                if cat == 'BAJA' and v <= 35: return False
            return True
        except:
            return False
    def _enriquecer(self, evento):
        """Agrega timestamp para medir latencia sensor-broker."""
        evento['broker_timestamp'] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S") + "Z"
        return evento
    def _loguear_evento(self, topico, evento):
        """Imprime un log legible según el tipo de sensor."""
        self.contadores[topico] += 1
        sid = evento.get('sensor_id', '???')
        
        # Fusión de campos según tipo de sensor para el log centralizado
        if topico == 'espira_inductiva':
            info = f"Flujo: {evento.get('vehiculos_contados', 0):<3} veh/int"
        elif topico == 'camara':
            info = f"Cola: {evento.get('volumen', 0):<3} veh | Vel: {evento.get('velocidad_promedio', 0):.1f} km/h"
        elif topico == 'gps':
            info = f"Est: {evento.get('nivel_congestion', '???'):<8} | Vel: {evento.get('velocidad_promedio', 0):.1f} km/h"
        else:
            info = "Datos recibidos"
        print(f"[Broker] {topico:<18} | ID: {sid:<8} | {info}")
    def iniciar(self):
        # Configurar timeout para poder capturar Ctrl+C en Windows
        self.sub_socket.setsockopt(zmq.RCVTIMEO, 1000)
        
        print(f"[Broker] Iniciando Modo Simple (1 hilo)...")
        print(f"[Broker] Escuchando sensores en: {self.config['red']['sensor_broker_url_PUB']}")
        print(f"[Broker] Tópicos suscritos: {self.topicos}")
        print("-" * 75)
        
        try:
            while True:
                try:
                    # 1. Recibir el mensaje multiparte: [Tópico, Cuerpo JSON]
                    partes = self.sub_socket.recv_multipart()
                    if len(partes) < 2:
                        continue
                    
                    topico = partes[0].decode('utf-8')
                    cuerpo_raw = partes[1].decode('utf-8')
                    # 2. Parsear el JSON una sola vez
                    try:
                        evento = json.loads(cuerpo_raw)
                    except json.JSONDecodeError:
                        print(f"[Broker] ⚠ Error: JSON inválido en {topico}")
                        continue
                    # 3. Validar y Enriquecer
                    if self._validar(topico, evento) and self._validar_sentido_fisico(topico, evento):
                        evento_enriquecido = self._enriquecer(evento)
                        
                        # PASO 4: Reconstruir el mensaje y reenviar a PC2 (Analítica)
                        payload_final = json.dumps(evento_enriquecido)
                        self.pub_socket.send_multipart([
                            topico.encode('utf-8'),
                            payload_final.encode('utf-8')
                        ])
                        
                        # PASO 5: Loguear localmente lo que está pasando
                        self._loguear_evento(topico, evento_enriquecido)
                    else:
                        print(f"[Broker] ⚠ Mensaje de {evento.get('sensor_id', '???')} DESCARTADO por validación.")
                        
                except zmq.Again:
                    # Pausa de 1s para permitir interrupción (Ctrl+C)
                    continue
                    
        except KeyboardInterrupt:
            print("\n[Broker] Finalizado por el usuario.")
        finally:
            self.sub_socket.close()
            self.pub_socket.close()
            self.context.term()
if __name__ == "__main__":
    try:
        with open('config.json', 'r') as f:
            config = json.load(f)
        broker = BrokerZMQ(config)
        broker.iniciar()
    except FileNotFoundError:
        print("[Error] No se encontró el archivo 'config.json' en el directorio actual.")
