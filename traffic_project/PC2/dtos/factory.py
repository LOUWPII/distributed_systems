from typing import Optional

from enums import TipoSensor

from .evento_sensor import EventoSensor
from .evento_camara import EventoCamara
from .evento_espira import EventoEspira
from .evento_gps import EventoGPS


# Fábrica de eventos: dado el tópico ZMQ y el dict JSON, construye el EventoSensor correcto.
def evento_desde_topico(topico: str, data: dict) -> Optional[EventoSensor]:

    fabricas = {
        TipoSensor.CAMARA.value: EventoCamara.from_json,
        TipoSensor.ESPIRA.value: EventoEspira.from_json,
        TipoSensor.GPS.value: EventoGPS.from_json,
    }

    fabrica = fabricas.get(topico)
    if fabrica is None:
        return None

    evento = fabrica(data)

    if not evento.validar():
        return None

    return evento