# ---------------------------------------------------------------------------
# Constantes de las reglas de tráfico
# Definidas aquí para que sean fáciles de modificar sin tocar la lógica
# ---------------------------------------------------------------------------

COLA_CONGESTION      = 10      # vehículos — umbral para detectar congestión
COLA_NORMAL          = 5       # vehículos — umbral para confirmar tráfico normal
VEL_CONGESTION       = 20.0    # km/h — por debajo = congestión
VEL_NORMAL           = 35.0    # km/h — por encima = normal
DURACION_NORMAL_S    = 15      # segundos — ciclo estándar verde/rojo
DURACION_CONGESTION_S = 45     # segundos — verde extendido por congestión
DURACION_OLA_VERDE_S = 60      # segundos — duración por defecto de ola verde