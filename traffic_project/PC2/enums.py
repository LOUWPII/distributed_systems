from enum import Enum


class EstadoTrafico(Enum):
    NORMAL = "NORMAL"
    CONGESTION = "CONGESTION"
    OLA_VERDE = "OLA_VERDE"


class EstadoSemaforo(Enum):
    VERDE = "VERDE"
    ROJO = "ROJO"


class TipoSensor(Enum):
    CAMARA = "camara"
    ESPIRA = "espira_inductiva"
    GPS = "gps"


class TipoCalle(Enum):
    FILA = "fila"
    COLUMNA = "columna"
