from enum import Enum

# Estados del tráfico
class EstadoTrafico(Enum):
    NORMAL = "NORMAL"
    CONGESTION = "CONGESTION"
    OLA_VERDE = "OLA_VERDE"

# Estados de los semáforos
class EstadoSemaforo(Enum):
    VERDE = "VERDE"
    ROJO = "ROJO"

# Tipos de sensores
class TipoSensor(Enum):
    CAMARA = "camara"
    ESPIRA = "espira_inductiva"
    GPS = "gps"

# Tipos de calles
class TipoCalle(Enum):
    FILA = "fila"
    COLUMNA = "columna"
