from typing import Optional

from enums import TipoSensor

from .evento_sensor import EventoSensor
from .evento_camara import EventoCamara
from .evento_espira import EventoEspira
from .evento_gps import EventoGPS


# Fábrica de eventos: dado el tópico ZMQ y el dict JSON, construye el EventoSensor correcto.
def evento_desde_topico(topico: str, data: dict) -> Optional[EventoSensor]:

    # Diccionario de fábricas
    fabricas = {
        "camara": EventoCamara.from_json,
        "espira": EventoEspira.from_json,
        "espira_inductiva": EventoEspira.from_json,
        "gps": EventoGPS.from_json,
    }

    # Obtener la el tipo de evento del JSON para fabricar un eventoSensor correspondiente al tópico
    fabrica = fabricas.get(topico)
    if fabrica is None:
        return None

    # Crear el evento
    evento = fabrica(data)

    # Validar que los datos del evento sean correctos
    if not evento.validar():
        print(f"[factory] ⚠ Evento {topico} inválido por validación interna")
        return None

    # Retornar el evento
    return evento