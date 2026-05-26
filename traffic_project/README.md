# Sistema de Gestión Inteligente de Tráfico Urbano

> Plataforma distribuida de monitoreo, análisis y control de tráfico urbano en tiempo real.
> Desarrollada sobre **ZeroMQ** con arquitectura de tres nodos físicos.

**Autores:** Juan Felipe Gomez · Sebastian Gaibor · David Beltran Gomez
**Asignatura:** Introducción a Sistemas Distribuidos — Pontificia Universidad Javeriana, 2026-10

---

## Tabla de Contenidos

1. [Descripción General del Proyecto](#1-descripción-general-del-proyecto)
2. [Archivos Principales y Ejecución](#2-archivos-principales-y-ejecución)
3. [Comunicación entre Componentes](#3-comunicación-entre-componentes)
4. [Configuración de la Cuadrícula](#4-configuración-de-la-cuadrícula)
5. [Tolerancia a Fallos y Protección ante Caída de PC3](#5-tolerancia-a-fallos-y-protección-ante-caída-de-pc3)
6. [Estructura del Proyecto](#6-estructura-del-proyecto)
7. [Requisitos e Instalación](#7-requisitos-e-instalación)
8. [Flujo Completo del Sistema](#8-flujo-completo-del-sistema)

---
## 1. Descripción General del Proyecto

El sistema simula y gestiona el tráfico de una ciudad representada como una **cuadrícula NxM de intersecciones**.
Cada intersección tiene semáforos inteligentes controlados en tiempo real a partir de datos generados por tres tipos de sensores físicos.

### Objetivo

- Recopilar y almacenar información de tráfico desde sensores distribuidos.
- Detectar congestión vehicular mediante reglas automáticas.
- Controlar semáforos de forma autónoma y responder a órdenes manuales (ej. priorizar ambulancias).
- Persistir eventos históricos y exponer consultas interactivas al operador.
- Garantizar continuidad operativa ante la caída de cualquier nodo.

### Distribución en Tres Nodos

| Nodo | IP Real | Rol Principal |
|------|---------|---------------|
| **PC1** | 10.43.99.112 | Simulación de ciudad, sensores y broker de mensajería |
| **PC2** | 10.43.98.246 | Analítica, control de semáforos, BD réplica y failover |
| **PC3** | 10.43.99.93 | BD principal, monitoreo interactivo y consultas históricas |

### Flujo de Alto Nivel

`
Sensores (PC1) --> Broker ZMQ (PC1) --> Analítica/RulesEngine (PC2) --> BD Principal (PC3)
                                                  |                           |
                                         Control Semáforos (PC2)      Monitoreo/Consulta (PC3)
                                                  |
                                          BD Réplica (PC2) <-- failover si PC3 cae
`

---
## 2. Archivos Principales y Ejecución

### Orden de Ejecución

> **IMPORTANTE:** Respetar el orden. PC3 debe estar activo antes de PC2, y PC2 antes de PC1.

`
Paso 1: PC3 — BD Principal + Monitoreo
Paso 2: PC2 — BD Réplica + Control Semáforos + Analítica (main.py)
Paso 3: PC1 — Broker + Sensores
`

---

### PC3 — Base de Datos Principal y Monitoreo

Directorio de trabajo: PC3_failover/

#### Terminal 1 — Base de Datos Principal

`ash
cd PC3_failover
python database_multith.py
`

**Qué hace:** Levanta dos hilos independientes:
- **Hilo PULL** (:5558): recibe eventos asíncronos desde PC2 y los persiste en archivos JSONL.
- **Hilo REP** (:5562 / :5564): atiende health checks (PING/PONG) y consultas históricas síncronas.

**Archivos generados en d_principal_data/:**

| Archivo | Contenido |
|---------|-----------|
| eventos.jsonl | Todos los eventos de sensores recibidos |
| congestiones.jsonl | Cambios de estado de tráfico (NORMAL/CONGESTION) |
| semaforos.jsonl | Comandos de cambio de semáforo ejecutados |
| priorizaciones.jsonl | Órdenes directas (ej. ambulancias) |

#### Terminal 2 — Monitoreo Interactivo

`ash
cd PC3_failover
python monitoreo_universal.py
`

**Qué hace:** Presenta un menú CLI interactivo para el operador. Detecta automáticamente si PC3 está disponible; si no, conmuta a PC2 de forma transparente.

`
==============================================================
         SISTEMA DE MONITOREO Y CONSULTA  [NODO: PC3]
==============================================================
 [1] Enviar comando a analitica (Ej. Ambulancia)
 [2] Estado actual de una calle (Tiempo real)
 [3] Estado de TODAS las calles (Tiempo real)
 [4] Estado de interseccion puntual
 [5] Consulta de fechas de congestiones
 [6] Consulta de cambios de color de semaforos
 [7] Consulta de priorizaciones de ambulancias
 [8] Salir
==============================================================
`

---
### PC2 — Analítica, Control de Semáforos y BD Réplica

Directorio de trabajo: PC2/

#### Terminal 1 — BD Réplica

`ash
cd PC2/replica_db
python database_multith_replica.py
`

**Qué hace:** Espejo local de la BD principal. Siempre activo, independiente del estado de PC3.
- **Hilo PULL** (:5557): recibe todos los eventos que Analítica envía en paralelo a la BD principal.
- **Hilo REP** (:5560): atiende consultas históricas cuando PC3 no está disponible.

#### Terminal 2 — Control de Semáforos

`ash
cd PC2/control_semaforos
python control_semaforos.py
`

**Qué hace:** Recibe comandos de cambio de semáforo desde Analítica vía PUSH/PULL (:5556), actualiza estado_semaforos.json y publica notificaciones PUB (:5570).

Salida de ejemplo:
`
[ACCIÓN SEMÁFORO]: SEM-F-INT_C2
🟢 CAMBIO A VERDE (Durante 45s)
Motivo: Regla automática micro: NORMAL -> CONGESTION
------------------------------------------------------------
`

#### Terminal 3 — Servicio de Analítica (proceso principal de PC2)

`ash
cd PC2
python main.py
`

**Qué hace:** Orquesta todos los componentes de PC2 en un único proceso multihilo:

| Componente | Tipo | Función |
|------------|------|---------|
| EventReceiver | Hilo SUB | Consume eventos del broker (:5551) |
| RulesEngine | Hilo principal | Evalúa estados, controla semáforos, gestiona órdenes |
| GestorSalida | 3 hilos workers | Despacha a semáforos, BD réplica y BD principal |
| HealthMonitor | Hilo daemon | Heartbeat PING/PONG hacia PC3 cada 5s |
| QueryHandler | Hilo REP | Atiende consultas síncronas desde PC3 (:5563) |

---

### PC1 — Broker y Sensores

Directorio de trabajo: PC1/

#### Terminal 1 — Broker ZMQ

`ash
cd PC1
python broker.py
`

**Qué hace:** Intermediario entre sensores y PC2. Recibe eventos PUB de los sensores (:5550), los valida, enriquece con roker_timestamp y los reenvía a PC2 (:5551).

Soporta dos modos configurables en config.json:

| Modo | Descripción | Cuándo usar |
|------|-------------|-------------|
| simple | 1 proceso, loop síncrono | Pruebas, baja carga |
| multihilos | 1 Receiver + N Workers + 1 Publisher (multiprocesos) | Benchmarks, alta carga |

#### Terminal 2 — Sensores

`ash
cd PC1
python sensores.py
`

**Qué hace:** Lanza el CityManager y todos los sensores configurados como procesos independientes (multiprocessing).

| Proceso | Función |
|---------|---------|
| CityManager | Servidor REQ/REP (:5555). Simula el nivel de tráfico de cada calle con random walk + shocks |
| SensorCamara | Genera eventos de cola vehicular y velocidad (modelo Greenshields) |
| SensorEspira | Genera eventos de conteo vehicular por intervalo |
| SensorGPS | Genera eventos de velocidad y nivel de congestión (ALTA/NORMAL/BAJA) |

---
## 3. Comunicacion entre Componentes

Toda la comunicacion usa **ZeroMQ (pyzmq)**. No hay HTTP, sockets TCP crudos ni brokers externos.

### Tabla de Canales de Comunicacion

| Canal | Patron ZMQ | Puerto | Origen | Destino | Proposito |
|-------|-----------|--------|--------|---------|-----------|
| Sensores -> Broker | PUB/SUB | `:5550` | PC1 Sensores | PC1 Broker | Publicacion de eventos de trafico |
| Broker -> Analitica | PUB/SUB | `:5551` | PC1 Broker | PC2 EventReceiver | Reenvio de eventos validados |
| Sensores <-> CityManager | REQ/REP | `:5555` | PC1 Sensores | PC1 CityManager | Consulta del nivel de trafico por calle |
| Analitica -> ControlSemaforos | PUSH/PULL | `:5556` | PC2 GestorSalida | PC2 ControlSemaforos | Comandos de cambio de semaforo |
| ControlSemaforos -> Clientes | PUB | `:5570` | PC2 ControlSemaforos | Suscriptores | Notificaciones de cambio de luz |
| Analitica -> BD Replica | PUSH/PULL | `:5557` | PC2 GestorSalida | PC2 BD Replica | Persistencia local de todos los eventos |
| Analitica -> BD Principal | PUSH/PULL | `:5558` | PC2 GestorSalida | PC3 BD Principal | Persistencia remota (si PC3 disponible) |
| BD Replica <-> Monitoreo | REQ/REP | `:5560` | PC3/PC2 Monitoreo | PC2 BD Replica | Consultas historicas en failover |
| HealthMonitor <-> BD Principal | REQ/REP | `:5562` | PC2 HealthMonitor | PC3 BD Principal | Heartbeat PING/PONG |
| Monitoreo <-> Analitica | REQ/REP | `:5563` | PC3 Monitoreo | PC2 QueryHandler | Consultas en tiempo real y ordenes directas |
| Monitoreo <-> BD Principal | REQ/REP | `:5564` | PC3 Monitoreo | PC3 BD Principal | Consultas historicas directas |

### Descripcion por Patron

#### PUB/SUB - Publicacion asincrona desacoplada

Usado para el flujo de eventos de sensores. El publicador no conoce a los suscriptores.
Los mensajes son **multipart**: `[topico_bytes | json_bytes]`.

```
Sensor PUB  -->  topico=camara | payload={sensor_id, volumen, velocidad_promedio, ...}
Broker SUB  <--  filtra por topico: camara, espira_inductiva, gps
Broker PUB  -->  reenvio a PC2 con broker_timestamp anadido
PC2 SUB     <--  EventReceiver deserializa y encola en event_queue
```

**Topicos suscritos:**

| Topico | Sensor | Campos clave |
|--------|--------|-------------|
| `camara` | SensorCamara | `volumen`, `velocidad_promedio`, `interseccion` |
| `espira_inductiva` | SensorEspira | `vehiculos_contados`, `intervalo_segundos` |
| `gps` | SensorGPS | `nivel_congestion` (ALTA/NORMAL/BAJA), `velocidad_promedio` |

#### PUSH/PULL - Pipeline asincrono unidireccional

Usado para persistencia y control de semaforos. El emisor no espera respuesta.
`GestorSalida` mantiene **3 colas `queue.Queue` independientes** para no bloquear el `RulesEngine`.

```
RulesEngine
    |
    +--> queue_semaforos --> Worker hilo --> PUSH :5556 --> ControlSemaforos PULL
    +--> queue_replica   --> Worker hilo --> PUSH :5557 --> BD Replica PULL
    +--> queue_principal --> Worker hilo --> PUSH :5558 --> BD Principal PULL (solo si PC3 disponible)
```

#### REQ/REP - Comunicacion sincrona con respuesta garantizada

Usado para consultas interactivas, health checks y comunicacion sensor-CityManager.

```
PC3 Monitoreo REQ  -->  {"tipo": "CONSULTA_TODOS_ESTADOS"}
PC2 QueryHandler REP  <--  {"estado": "OK", "calles": {...}}

PC2 HealthMonitor REQ  -->  "PING"
PC3 BD Principal REP   <--  "PONG"
```

### Tipos de Consulta soportados por QueryHandler (PC2)

| Tipo de solicitud | Descripcion | Respuesta |
|-------------------|-------------|-----------|
| `CONSULTA_ESTADO_ACTUAL` | Estado de una calle especifica | `{"calle": {...}}` |
| `CONSULTA_TODOS_ESTADOS` | Estado global de todas las calles | `{"calles": {...}}` |
| `CONSULTA_INTERSECCION` | Estado de semaforos en una interseccion | `{"interseccion": {...}}` |
| `ORDEN_DIRECTA` | Forzar ola verde en una calle (ej. ambulancia) | `{"orden": {...}}` |

---

## 4. Configuracion de la Cuadricula

### Donde se configura

Cada nodo tiene su propio `config.json`. Los tres deben ser consistentes entre si:

| Nodo | Ruta del archivo |
|------|-----------------|
| PC1 | `PC1/config.json` |
| PC2 | `PC2/config/config.json` |
| PC3 | `PC3_failover/config.json` |

### Parametros de la Ciudad

```json
"ciudad": {
  "filas": 4,
  "columnas": 4,
  "descripcion": "Cuadricula del caso de prueba"
}
```

La ciudad es una cuadricula de **4 filas (A-D) x 4 columnas (1-4)**. Las intersecciones se nombran `INT_XN` donde X es la letra de fila y N el numero de columna. Ejemplo: `INT_C2` = fila C, columna 2.

### Configuracion de Sensores

Cada sensor se define con los siguientes campos:

```json
"sensores": [
  {
    "sensor_id": "CAM-C2",
    "tipo": "camara",
    "interseccion": "INT_C2",
    "calle_id": "fila_C",
    "direccion": "fila"
  },
  {
    "sensor_id": "ESP-C2",
    "tipo": "espira_inductiva",
    "interseccion": "INT_C2",
    "calle_id": "col_2",
    "direccion": "columna"
  }
]
```

| Campo | Descripcion | Valores posibles |
|-------|-------------|-----------------|
| `sensor_id` | Identificador unico del sensor | `CAM-XX`, `ESP-XX`, `GPS-XX` |
| `tipo` | Tipo de sensor | `camara`, `espira_inductiva`, `gps` |
| `interseccion` | Interseccion donde esta ubicado | `INT_B3`, `INT_C2`, etc. |
| `calle_id` | Calle que monitorea | `fila_B`, `col_3`, etc. |
| `direccion` | Orientacion de la calle | `fila` o `columna` |

**Sensores activos en la configuracion actual:**

| Sensor ID | Tipo | Interseccion | Calle |
|-----------|------|-------------|-------|
| `CAM-C2` | Camara | INT_C2 | fila_C |
| `ESP-C2` | Espira | INT_C2 | col_2 |
| `GPS-B3` | GPS | INT_B3 | fila_B |
| `CAM-B3` | Camara | INT_B3 | col_3 |
| `ESP-C4` | Espira | INT_C4 | fila_C |
| `CAM-C4` | Camara | INT_C4 | col_4 |

### Configuracion de Intersecciones

```json
"intersecciones": [
  {
    "interseccion_id": "INT_C2",
    "calle_fila": "fila_C",
    "calle_columna": "col_2"
  }
]
```

Cada interseccion define el cruce entre una calle fila y una calle columna. El `RulesEngine` crea automaticamente dos semaforos por interseccion: uno para la fila (`SEM-F-INT_XX`) y otro para la columna (`SEM-C-INT_XX`).

### Parametros de Simulacion

```json
"parametros_simulacion": {
  "intervalo_evolucion_s": 5,
  "intervalo_sensores_s": 3,
  "volatilidad_ruido": 0.15,
  "probabilidad_shock": 0.0
}
```

| Parametro | Descripcion | Efecto |
|-----------|-------------|--------|
| `intervalo_evolucion_s` | Cada cuantos segundos evoluciona el trafico | Menor = trafico mas dinamico |
| `intervalo_sensores_s` | Cada cuantos segundos genera un evento cada sensor | Menor = mas eventos, mas carga |
| `volatilidad_ruido` | Desviacion estandar del ruido gaussiano | Mayor = lecturas mas variables |
| `probabilidad_shock` | Probabilidad de incidente por ciclo | `0.0` = sin shocks, `0.1` = 10% por ciclo |

### Parametros de Health Check

```json
"health_check": {
  "intervalo_s": 5,
  "timeout_s": 2
}
```

- `intervalo_s`: cada cuantos segundos PC2 envia un PING a PC3.
- `timeout_s`: tiempo maximo de espera del PONG antes de declarar PC3 caido.

### Reglas de Trafico (constantes.py en PC2)

```python
COLA_CONGESTION      = 10   # vehiculos — umbral para detectar congestion
COLA_NORMAL          = 5    # vehiculos — umbral para confirmar trafico normal
VEL_CONGESTION       = 20.0 # km/h — por debajo = congestion
VEL_NORMAL           = 35.0 # km/h — por encima = normal
DURACION_NORMAL_S    = 15   # segundos — ciclo estandar verde/rojo
DURACION_CONGESTION_S = 45  # segundos — verde extendido por congestion
DURACION_OLA_VERDE_S = 60   # segundos — duracion por defecto de ola verde
```

---

## 5. Tolerancia a Fallos y Proteccion ante Caida de PC3

### Arquitectura de Alta Disponibilidad

El sistema implementa un esquema de **doble persistencia activa** y **failover automatico** que garantiza continuidad operativa ante la caida de PC3.

```
                    +------------------+
                    |   GestorSalida   |
                    |  (PC2 - siempre) |
                    +--------+---------+
                             |
              +--------------+--------------+
              |                             |
    +---------v---------+       +-----------v---------+
    |   BD Replica      |       |   BD Principal      |
    |   PC2 :5557       |       |   PC3 :5558         |
    |   SIEMPRE activa  |       |   Solo si disponible|
    +-------------------+       +---------------------+
```

### Mecanismo de Deteccion: HealthMonitor

`HealthMonitor` es un hilo daemon en PC2 que ejecuta el siguiente ciclo cada 5 segundos:

```
1. Envia "PING" a PC3 (REQ :5562) con timeout de 2s
2. Si recibe "PONG" --> PC3 disponible, operacion normal
3. Si timeout/error --> PC3 no disponible, activa failover
4. Recrea el socket ZMQ para evitar estados corruptos
5. Notifica a todos los listeners registrados (GestorSalida)
```

### Comportamiento durante el Failover

#### Cuando PC3 cae:

| Componente | Comportamiento |
|------------|---------------|
| `GestorSalida._loop_principal` | Descarta mensajes hacia BD principal (contador `principal_omitidos`) |
| `GestorSalida._loop_replica` | **Continua sin cambios** — todos los eventos se persisten en BD replica |
| `QueryHandler` (PC2) | Sigue activo en `:5563` — las consultas del operador se redirigen a PC2 |
| `HealthMonitor` | Imprime aviso y notifica listeners. En la version de produccion lanza `monitoreo_consulta_failover.py` |
| `GestorSalida._sock_bd_principal` | `SNDHWM=100` limita mensajes encolados para no consumir RAM |

#### Cuando PC3 se recupera:

```
1. HealthMonitor recibe PONG exitoso
2. Marca _pc3_disponible = True
3. Notifica listeners
4. GestorSalida._loop_principal reanuda envios a BD principal
5. Los eventos generados durante la caida quedan en BD replica (no se sincronizan automaticamente)
```

### Failover en el Cliente de Monitoreo

`monitoreo_universal.py` implementa deteccion y conmutacion transparente:

```python
# Al iniciar, prueba conectarse a PC3
try:
    test.send_string('{"tipo":"CONSULTA_TODOS_ESTADOS"}')
    test.recv_string()  # Si responde -> modo PC3
except:
    self.modo = "PC2"   # Si falla -> conmuta a PC2
```

Si durante una consulta PC3 no responde:
1. Detecta timeout en `recv_string()`
2. Cierra el socket actual
3. Cambia `self.modo = "PC2"`
4. Reconecta sockets a las URLs de PC2
5. Reintenta la misma consulta automaticamente

### Timeout de Sensores (RulesEngine)

Si un tramo lleva mas de **300 segundos** sin recibir eventos (sensor caido o desconectado):

```
[RulesEngine] Timeout en tramo INT_C2_fila_C (silencio de 312s) -> Reset a NORMAL
```

El estado se restaura a NORMAL y se aplica el ciclo estandar de semaforos para ese tramo.

### Persistencia Atomica (JSONLStorage)

Cada escritura en los archivos JSONL usa el patron **copy-on-write atomico**:

```
1. Copiar archivo actual a archivo.jsonl.tmp
2. Agregar nuevo registro al .tmp
3. fsync() -- forzar escritura fisica en disco
4. os.replace(tmp, original) -- operacion atomica del SO
```

Esto garantiza que ante un crash del proceso, el archivo nunca quede en estado corrupto o a medias.

---

## 6. Estructura del Proyecto

```
traffic_project/
|
+-- PC1/                            # Nodo 1: Simulacion y mensajeria
|   +-- broker.py                   # Broker ZMQ (modo simple o multihilos)
|   +-- sensores.py                 # Orquestador: lanza CityManager + sensores
|   +-- config.json                 # Configuracion de red, sensores e intersecciones
|   +-- sensor_logic/
|   |   +-- sensor_base.py          # Clase abstracta base (Template Method)
|   |   +-- sensor_camara.py        # Sensor de camara (cola + velocidad)
|   |   +-- sensor_espira.py        # Sensor de espira inductiva (conteo)
|   |   +-- sensor_gps.py           # Sensor GPS (velocidad + nivel congestion)
|   +-- traffic_logic/
|       +-- city_manager.py         # Motor de simulacion + servidor REQ/REP :5555
|       +-- traffic_state.py        # Estado de trafico por calle (random walk + shocks)
|
+-- PC2/                            # Nodo 2: Analitica, control y replica
|   +-- main.py                     # Punto de entrada: instancia y arranca todos los componentes
|   +-- enums.py                    # Enumeraciones: EstadoTrafico, EstadoSemaforo, TipoCalle
|   +-- monitoreo_consulta_failover.py  # Cliente de monitoreo de respaldo (failover)
|   +-- requirements.txt            # Dependencias Python (pyzmq)
|   +-- config/
|   |   +-- config.json             # Configuracion de red, sensores e intersecciones
|   |   +-- __init__.py             # Clase Config: carga y expone todos los parametros
|   +-- application/
|   |   +-- rules_engine.py         # Motor de reglas: evalua estados, controla semaforos
|   |   +-- query_handler.py        # Servidor REP para consultas sincronas desde PC3
|   +-- infrastructure/
|   |   +-- event_receiver.py       # Consumidor SUB del broker
|   |   +-- gestor_salida.py        # Despacho asincrono a semaforos y BDs (3 workers)
|   |   +-- health_monitor.py       # Heartbeat PING/PONG hacia PC3
|   +-- dominio/
|   |   +-- constantes.py           # Umbrales y duraciones de semaforo
|   |   +-- estado_calle.py         # Estado de trafico por calle + reglas de evaluacion
|   |   +-- interseccion.py         # Exclusion mutua de semaforos en un cruce
|   |   +-- semaforo.py             # Semaforo fisico con estado y tiempo restante
|   |   +-- orden_directa.py        # Orden manual con TTL (ej. ambulancia)
|   +-- dtos/
|   |   +-- evento_sensor.py        # Clase base abstracta para eventos
|   |   +-- evento_camara.py        # DTO para eventos de camara
|   |   +-- evento_espira.py        # DTO para eventos de espira
|   |   +-- evento_gps.py           # DTO para eventos de GPS
|   |   +-- comando_semaforo.py     # DTO para comandos de semaforo
|   |   +-- factory.py              # Factory: crea el DTO correcto segun topico ZMQ
|   +-- control_semaforos/
|   |   +-- control_semaforos.py    # Ejecuta comandos PULL, persiste estado, publica PUB
|   |   +-- estado_semaforos.json   # Estado actual de cada semaforo (persistido)
|   +-- replica_db/
|   |   +-- database_multith_replica.py  # BD replica: PULL ingesta + REP consultas
|   |   +-- jsonl_storage.py        # Persistencia atomica en archivos JSONL
|   |   +-- bd_replica_data/        # Archivos JSONL de la replica
|   +-- tests/
|       +-- test_monitoreo.py       # Pruebas del cliente de monitoreo
|       +-- test_sensores.py        # Pruebas de sensores
|       +-- ver_salida_semaforos.py # Utilidad para ver notificaciones PUB de semaforos
|
+-- PC3_failover/                   # Nodo 3: BD principal y monitoreo
|   +-- database_multith.py         # BD principal: PULL ingesta + REP health/consultas
|   +-- jsonl_storage.py            # Persistencia atomica en archivos JSONL
|   +-- monitoreo_consulta.py       # Interfaz interactiva de monitoreo (menu CLI)
|   +-- monitoreo_universal.py      # Version universal con failover automatico a PC2
|   +-- config.json                 # Configuracion de red (misma estructura que PC1/PC2)
|   +-- bd_principal_data/
|       +-- eventos.jsonl           # Todos los eventos de sensores
|       +-- congestiones.jsonl      # Cambios de estado de trafico
|       +-- semaforos.jsonl         # Comandos de semaforo ejecutados
|       +-- priorizaciones.jsonl    # Ordenes directas (ambulancias, etc.)
|
+-- diagrams/                       # Diagramas de arquitectura
|   +-- PC1/                        # Diagramas de componentes, secuencia y broker de PC1
|   +-- PC2/                        # Diagramas de clases, componentes y secuencia de PC2
|   +-- PC3/                        # Diagramas de clases, componentes y secuencia de PC3
|
+-- tests/benchmark/                # Suite de pruebas de rendimiento
|   +-- test_broker_latency.py      # Latencia del broker (simple vs multihilos)
|   +-- test_semaphore_latency.py   # Latencia usuario -> cambio de semaforo
|   +-- test_monitoring_latency.py  # Latencia de consultas de monitoreo
|   +-- test_throughput.py          # Throughput de la BD (registros en 2 minutos)
|   +-- measurements.py             # Utilidades de medicion
|   +-- doc.md                      # Documentacion de las pruebas
|
+-- config_files/
|   +-- config.json                 # Configuracion de referencia centralizada
|
+-- README.md                       # Este archivo
```

---

## 7. Requisitos e Instalacion

### Requisitos de Software

| Componente | Version recomendada |
|------------|-------------------|
| Python | 3.10 o superior |
| pyzmq | >= 25.0.0 |
| Sistema operativo | Windows 10/11, Ubuntu 20.04+ o macOS |

### Dependencias

El unico paquete externo requerido es **pyzmq**. Toda la concurrencia, serializacion y logica de red usa la biblioteca estandar de Python.

```
pyzmq>=25.0.0
```

Archivo `PC2/requirements.txt` incluido en el repositorio.

### Instalacion

#### Opcion 1 — Instalacion directa

```bash
pip install pyzmq
```

#### Opcion 2 — Entorno virtual (recomendado)

```bash
# Crear entorno virtual
python -m venv venv

# Activar (Windows)
venv\Scripts\activate

# Activar (Linux/macOS)
source venv/bin/activate

# Instalar dependencias
pip install -r PC2/requirements.txt
```

#### Verificar instalacion

```bash
python -c "import zmq; print('ZMQ version:', zmq.__version__)"
```

### Configuracion de Red

Antes de ejecutar en multiples PCs, actualizar las IPs en los tres `config.json`:

```json
"red": {
  "analitica_broker_url_SUB":      "tcp://IP_PC1:5551",
  "analitica_bd_principal_url_PUSH": "tcp://IP_PC3:5558",
  "analitica_health_url_REQ":      "tcp://IP_PC3:5562",
  "analitica_monitoreo_url_REQ":   "tcp://IP_PC2:5563",
  "monitoreo_bd_principal_url_REQ": "tcp://IP_PC3:5564"
}
```

> Las URLs con `tcp://*:XXXX` son sockets que **escuchan** (bind) y no necesitan IP especifica.
> Las URLs con `tcp://IP:XXXX` son sockets que **conectan** (connect) y requieren la IP del nodo remoto.

### Verificacion de Puertos

Asegurarse de que los siguientes puertos esten abiertos en el firewall de cada maquina:

| PC | Puertos que debe exponer |
|----|--------------------------|
| PC1 | 5550, 5551, 5555 |
| PC2 | 5556, 5557, 5560, 5563, 5570 |
| PC3 | 5558, 5562, 5564 |

---

## 8. Flujo Completo del Sistema

### Paso 1 — Arranque de PC3 (BD Principal)

```bash
# Terminal 1 en PC3
cd PC3_failover
python database_multith.py
```

PC3 levanta dos hilos:
- **Hilo PULL** escucha en `:5558` esperando eventos de PC2.
- **Hilo REP** escucha en `:5562` y `:5564` para health checks y consultas.

```
[DB] Carpeta de datos: bd_principal_data/
[DB-Ingesta] Hilo PULL activo en tcp://*:5558
[DB-Consultas] Hilo REP activo en tcp://*:5562 y tcp://*:5564
[DB] Base de Datos Principal (PC3) operando con hilos independientes.
```

### Paso 2 — Arranque de PC2 (Analitica + Servicios)

#### Terminal 1: BD Replica

```bash
cd PC2/replica_db
python database_multith_replica.py
```

#### Terminal 2: Control de Semaforos

```bash
cd PC2/control_semaforos
python control_semaforos.py
```

#### Terminal 3: Servicio de Analitica

```bash
cd PC2
python main.py
```

Al arrancar `main.py`, ocurre lo siguiente en orden:

```
1. Carga Config desde config/config.json
2. Instancia HealthMonitor --> inicia hilo daemon de heartbeat hacia PC3
3. Instancia GestorSalida --> conecta sockets PUSH a semaforos, BD replica y BD principal
4. Inicia workers de GestorSalida (3 hilos paralelos)
5. Instancia EventReceiver --> conecta socket SUB al broker de PC1
6. Instancia RulesEngine --> carga intersecciones y estados desde config
   --> envia comandos iniciales de semaforos (INICIO_SISTEMA)
   --> inicia hilo de ciclo automatico
7. Instancia QueryHandler --> bind socket REP en :5563
8. Arranca todos los hilos: EventReceiver, RulesEngine, QueryHandler
```

Salida esperada:
```
[HealthMonitor] Iniciado. Heartbeat cada 5s
[GestorSalida] Conectado a semaforos, BD replica y BD principal.
[GestorSalida] Arrancando workers en hilos paralelos...
[EventReceiver] Conectado a Broker tcp://10.43.99.112:5551
[EventReceiver] Suscrito a Topicos: ['camara', 'espira_inductiva', 'gps']
[RulesEngine] Estados de 3 intersecciones cargados
[RulesEngine] 3 intersecciones fisicas cargadas
[RulesEngine] Iniciado -- esperando eventos
[QueryHandler] Escuchando en tcp://*:5563
[HealthMonitor] -> PING tcp://10.43.99.93:5562
[HealthMonitor] <- PONG
```

### Paso 3 — Arranque de PC1 (Broker + Sensores)

#### Terminal 1: Broker

```bash
cd PC1
python broker.py
```

El broker se suscribe a los topicos `camara`, `espira_inductiva`, `gps` en `:5550` y publica en `:5551`.

#### Terminal 2: Sensores

```bash
cd PC1
python sensores.py
```

`sensores.py` lanza los siguientes procesos independientes:

```
[Main] Proceso CityManager iniciado (PID: XXXX)
[Main] Proceso lanzado para sensor: CAM-C2 (PID: XXXX)
[Main] Proceso lanzado para sensor: ESP-C2 (PID: XXXX)
[Main] Proceso lanzado para sensor: GPS-B3 (PID: XXXX)
[Main] Proceso lanzado para sensor: CAM-B3 (PID: XXXX)
[Main] Proceso lanzado para sensor: ESP-C4 (PID: XXXX)
[Main] Proceso lanzado para sensor: CAM-C4 (PID: XXXX)
```

### Paso 4 — Flujo de un Evento (de extremo a extremo)

```
1. CityManager evoluciona el nivel de trafico de fila_C cada 5s (random walk)

2. SensorCamara CAM-C2 consulta al CityManager:
   REQ --> {"action": "get_nivel", "calle": "fila_C"}
   REP <-- {"nivel": 0.73}

3. Sensor aplica ruido gaussiano: nivel_efectivo = 0.73 + gauss(0, 0.73*0.12) = 0.78

4. Sensor genera evento con modelo Greenshields:
   volumen = int(0.78 * 20) = 15 vehiculos
   velocidad = (1 - 0.78) * 50 = 11.0 km/h

5. Sensor publica via PUB :5550:
   topico="camara" | payload={"sensor_id":"CAM-C2","volumen":15,"velocidad_promedio":11.0,...}

6. Broker recibe, valida (volumen>=0, velocidad<=60), enriquece con broker_timestamp
   y reenvía via PUB :5551

7. EventReceiver en PC2 recibe el mensaje, llama a factory.evento_desde_topico("camara", data)
   --> crea EventoCamara y lo encola en event_queue

8. RulesEngine.procesar_evento(evento):
   a. Actualiza EstadoCalle de INT_C2/fila_C: ultima_cola=15, velocidad=11.0
   b. Persiste el evento: GestorSalida.persistir_evento() --> BD replica + BD principal
   c. Evalua estado: cola(15) > COLA_CONGESTION(10) --> CONGESTION
   d. Estado cambio NORMAL -> CONGESTION
   e. Llama a aplicar_verde_micro(INT_C2, fila_C, 45s)

9. Interseccion.set_verde_fila(45s):
   SEM-F-INT_C2 --> VERDE (45s)
   SEM-C-INT_C2 --> ROJO  (45s)  [exclusion mutua garantizada]

10. GestorSalida.enviar_cmd(ComandoSemaforo):
    --> queue_semaforos --> PUSH :5556 --> ControlSemaforos
    --> queue_replica   --> PUSH :5557 --> BD Replica
    --> queue_principal --> PUSH :5558 --> BD Principal (si PC3 disponible)

11. ControlSemaforos recibe el comando:
    Actualiza estado_semaforos.json
    Publica notificacion PUB :5570
    Imprime: "SEM-F-INT_C2 -- CAMBIO A VERDE (Durante 45s)"
```

### Paso 5 — Consulta desde PC3

```bash
# En PC3, seleccionar opcion 3 del menu
[3] Estado de TODAS las calles (Tiempo real)
```

```
PC3 Monitoreo REQ --> {"tipo": "CONSULTA_TODOS_ESTADOS"}
PC2 QueryHandler REP <-- {
  "estado": "OK",
  "calles": {
    "INT_C2_fila_C": {"estado": "CONGESTION", "ultima_cola": 15, "velocidad_promedio": 11.0, ...},
    "INT_C2_col_2":  {"estado": "NORMAL", "ultima_cola": 2, "velocidad_promedio": 42.0, ...},
    ...
  }
}
```

### Paso 6 — Orden Directa (Ambulancia)

```bash
# En PC3, seleccionar opcion 1 del menu
[1] Enviar comando a analitica (Ej. Ambulancia)
-> ID de la calle: fila_C
-> Accion: OLA_VERDE
-> Duracion en segundos: 60
-> Motivo: EMERGENCIA_AMBULANCIA
```

```
PC3 REQ --> {"tipo":"ORDEN_DIRECTA","calle_id":"fila_C","accion":"OLA_VERDE","duracion_s":60,"motivo":"EMERGENCIA_AMBULANCIA"}
PC2 QueryHandler --> RulesEngine.registrar_orden(OrdenDirecta)
RulesEngine.aplicar_ola_verde("fila_C", 60s):
  --> INT_C2: SEM-F-INT_C2=VERDE, SEM-C-INT_C2=ROJO
  --> INT_C4: SEM-F-INT_C4=VERDE, SEM-C-INT_C4=ROJO
  (todas las intersecciones donde fila_C tiene semaforo)
PC2 REP --> {"estado":"OK","mensaje":"OLA_VERDE activada en fila_C por 60s"}
```

La orden expira automaticamente a los 60s y el ciclo normal se restaura.

### Buenas Practicas para Ejecucion en Multiples PCs

1. **Verificar conectividad** antes de arrancar: `ping IP_PC1`, `ping IP_PC2`, `ping IP_PC3`
2. **Arrancar siempre en orden**: PC3 -> PC2 -> PC1
3. **Abrir puertos en el firewall** de cada maquina (ver seccion de requisitos)
4. **Usar la misma version de Python** en los tres nodos
5. **Sincronizar los config.json** — los tres deben tener las mismas IPs y puertos
6. **Monitorear los logs** de cada terminal para detectar errores de conexion
7. **Para detener el sistema**: Ctrl+C en cada terminal, en orden inverso (PC1 -> PC2 -> PC3)
8. **Para pruebas de rendimiento**: usar los scripts en `tests/benchmark/` con el modo `multihilos` en el broker

---

## Patrones de Diseno Implementados

| Patron | Clase / Modulo | Descripcion |
|--------|---------------|-------------|
| **Template Method** | `SensorBase` | Define el flujo de adquisicion; subclases implementan `generar_evento()` |
| **Factory Method** | `dtos/factory.py` | Crea el DTO correcto (EventoCamara/Espira/GPS) segun el topico ZMQ |
| **Observer** | `HealthMonitor.add_listener()` | Notifica callbacks al cambiar el estado de disponibilidad de PC3 |
| **Producer-Consumer** | `GestorSalida` | 3 `queue.Queue` + 3 hilos workers independientes |
| **State Machine** | `EstadoCalle.evaluar_estado()` | Transiciones NORMAL <-> CONGESTION <-> OLA_VERDE |
| **Strategy** | `BrokerZMQ` | Modo `simple` vs `multihilos` seleccionable por config |
| **DTO** | `dtos/` | EventoCamara, EventoEspira, EventoGPS, ComandoSemaforo |
| **Repository** | `JSONLStorage` | Abstrae la escritura atomica en archivos JSONL |
| **Facade** | `GestorSalida` | Unifica el despacho a semaforos, BD replica y BD principal |