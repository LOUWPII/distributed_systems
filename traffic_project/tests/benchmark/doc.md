# Benchmark de Rendimiento — Sistema de Gestión de Tráfico

## 📋 Índice

- [Estructura del Benchmark](#estructura-del-benchmark)
- [Máquinas e IPs](#máquinas-e-ips)
- [Arquitectura de Tests](#arquitectura-de-tests)
  - [test_broker_latency.py](#test_broker_latencypy)
  - [test_semaphore_latency.py](#test_semaphore_latencypy)
  - [test_monitoring_latency.py](#test_monitoring_latencypy)
  - [test_throughput.py](#test_throughputpy)
- [Sistema de Configuración](#sistema-de-configuración)
  - [benchmark.tests en cada PC](#benchmarktests-en-cada-pc)
- [Funciones de Medición (measurements.py)](#funciones-de-medición-measurementspy)
- [Módulo Compartido (common.py)](#módulo-compartido-commonpy)
- [Instrumentación del Broker](#instrumentación-del-broker)
- [Modos de Uso](#modos-de-uso)
  - [PC1 — Broker Latency](#pc1--broker-latency)
  - [PC2 — Semáforo, Monitoreo, Throughput](#pc2--semáforo-monitoreo-throughput)
  - [PC3 — Semáforo, Monitoreo, Throughput](#pc3--semáforo-monitoreo-throughput)
  - [Desarrollo Local](#desarrollo-local)
- [Bugs Encontrados y Corregidos](#bugs-encontrados-y-corregidos)
- [Interpretación de Resultados](#interpretación-de-resultados)

---

## Estructura del Benchmark

```
tests/benchmark/
├── common.py                      ← Módulo compartido (rutas, ProcessManager, helpers)
├── test_broker_latency.py         ← PC1: inicia sensores+broker, mide latencia
├── test_semaphore_latency.py      ← PC2/PC3: solo mide RTT semáforo
├── test_monitoring_latency.py     ← PC2/PC3: solo mide RTT monitoreo
├── test_throughput.py             ← PC2/PC3: solo mide throughput
├── measurements.py                ← Funciones ZMQ (conexión a servicios)
├── doc.md                         ← Esta documentación
└── __init__.py
```

**Principio clave**: cada test es **independiente** y se ejecuta en la máquina que corresponde. Los tests de medición **no inician procesos** — asumen que los componentes ya están corriendo.

---

## Máquinas e IPs

| Máquina | IP | Componentes | Tests que ejecuta |
|---|---|---|---|
| **PC1** | `10.43.99.112` | broker, sensores | `test_broker_latency.py` (inicia procesos) |
| **PC2** | `10.43.98.246` | analítica, semáforos, réplica DB, query handler | `test_semaphore_latency.py`, `test_monitoring_latency.py`, `test_throughput.py` (solo mide) |
| **PC3** | `10.43.99.93` | BD principal, monitoreo | `test_semaphore_latency.py`, `test_monitoring_latency.py`, `test_throughput.py` (conecta a PC2) |

Los 3 servicios de medición corren en **PC2**:

| Puerto | Servicio | Quién bindea |
|---|---|---|
| `:5560` | ReplicaDB REP | PC2 |
| `:5563` | QueryHandler REP | PC2 |
| `:5570` | ControlSemáforos PUB | PC2 |

---

## Arquitectura de Tests

### test_broker_latency.py

**Propósito**: Medir tiempo de procesamiento interno del broker (validación + enriquecimiento + publicación).

**Comportamiento**:
1. Verifica `benchmark.tests.broker_latency.enabled` en `config.json`
2. Inicia `sensores.py` (incluye CityManager)
3. Inicia `broker.py` con `BENCHMARK_LATENCY_FILE`
4. Espera 20s (6 sensores, intervalo 3s → ~42 eventos)
5. Lee archivo de latencia, calcula avg/min/max/n
6. Restaura `config.json`

**Uso**:
```bash
# Ejecuta ambos modos (simple + multihilos):
python tests/benchmark/test_broker_latency.py

# Un solo modo:
python tests/benchmark/test_broker_latency.py --modo simple
python tests/benchmark/test_broker_latency.py --modo multihilos
```

**Solo corre en PC1** (broker_latency.enabled = true en PC1, false en PC2/PC3).

### test_semaphore_latency.py

**Propósito**: Medir RTT desde ORDEN_DIRECTA hasta CAMBIO_SEMAFORO.

**Comportamiento**:
1. Verifica `benchmark.tests.semaphore_latency.enabled`
2. Lee `query_handler_REQ` y `semaphore_pub_SUB` del config
3. Envía ORDEN_DIRECTA (`calle_id=fila_C`, `OLA_VERDE`) vía ZMQ REQ
4. Escucha `CAMBIO_SEMAFORO` en PUB con `zmq.Poller` (precisión ~1ms)
5. Timeout: 10s

**No inicia procesos** — asume que `main.py` y `control_semaforos.py` ya corren.

**Uso**:
```bash
python tests/benchmark/test_semaphore_latency.py
```

### test_monitoring_latency.py

**Propósito**: Medir RTT del QueryHandler para CONSULTA_TODOS_ESTADOS.

**Comportamiento**:
1. Verifica `benchmark.tests.monitoring_latency.enabled`
2. Lee `query_handler_REQ` del config
3. Envía 10 consultas CONSULTA_TODOS_ESTADOS vía ZMQ REQ
4. Calcula avg/min/max del RTT

**No inicia procesos** — asume que `main.py` ya corre.

**Uso**:
```bash
python tests/benchmark/test_monitoring_latency.py
```

### test_throughput.py

**Propósito**: Medir eventos almacenados en BD Réplica en 120s.

**Comportamiento**:
1. Verifica `benchmark.tests.throughput.enabled`
2. Lee `bd_replica_REQ` del config
3. Envía CONSULTA_TOTAL_EVENTOS antes y después de 120s
4. Diferencia = throughput

**No inicia procesos** — asume que pipeline completo ya corre.

**Uso**:
```bash
python tests/benchmark/test_throughput.py
```

---

## Sistema de Configuración

Cada máquina tiene su `config.json` con una sección `benchmark.tests`. Las URLs siguen la convención de `red` (`_REQ`/`_REP`/`_PUB`/`_SUB`/`_PUSH`/`_PULL`).

### benchmark.tests en cada PC

**PC1** (`config.json`) — broker_latency activo, servicios apuntan a PC2:

```json
"benchmark": {
  "tests": {
    "broker_latency": {
      "enabled": true
    },
    "semaphore_latency": {
      "enabled": true,
      "query_handler_REQ": "tcp://10.43.98.246:5563",
      "query_handler_REP": "tcp://*:5563",
      "semaphore_pub_SUB": "tcp://10.43.98.246:5570",
      "semaphore_pub_PUB": "tcp://*:5570"
    },
    "monitoring_latency": {
      "enabled": true,
      "query_handler_REQ": "tcp://10.43.98.246:5563",
      "query_handler_REP": "tcp://*:5563"
    },
    "throughput": {
      "enabled": true,
      "bd_replica_REQ": "tcp://10.43.98.246:5560",
      "bd_replica_REP": "tcp://*:5560"
    }
  }
}
```

**PC2** (`config.json`) — broker_latency inactivo, servicios locales:

```json
"benchmark": {
  "tests": {
    "broker_latency": {
      "enabled": false
    },
    "semaphore_latency": {
      "enabled": true,
      "query_handler_REQ": "tcp://127.0.0.1:5563",
      "query_handler_REP": "tcp://*:5563",
      "semaphore_pub_SUB": "tcp://127.0.0.1:5570",
      "semaphore_pub_PUB": "tcp://*:5570"
    },
    "monitoring_latency": {
      "enabled": true,
      "query_handler_REQ": "tcp://127.0.0.1:5563",
      "query_handler_REP": "tcp://*:5563"
    },
    "throughput": {
      "enabled": true,
      "bd_replica_REQ": "tcp://127.0.0.1:5560",
      "bd_replica_REP": "tcp://*:5560"
    }
  }
}
```

**PC3** (`config.json`) — misma estructura que PC1, apunta a PC2:

| Clave | PC1 | PC2 | PC3 |
|---|---|---|---|
| `broker_latency.enabled` | `true` | `false` | `false` |
| `semaphore_latency.query_handler_REQ` | `10.43.98.246` | `127.0.0.1` | `10.43.98.246` |
| `semaphore_latency.semaphore_pub_SUB` | `10.43.98.246` | `127.0.0.1` | `10.43.98.246` |
| `monitoring_latency.query_handler_REQ` | `10.43.98.246` | `127.0.0.1` | `10.43.98.246` |
| `throughput.bd_replica_REQ` | `10.43.98.246` | `127.0.0.1` | `10.43.98.246` |

---

## Funciones de Medición (measurements.py)

Tres funciones independientes que reciben URLs como parámetros:

### `count_db_events(db_url, duration_s)`

```python
def count_db_events(db_url, duration_s) -> dict:
    """
    Args:
        db_url: URL del REP de BD Réplica (ej: tcp://127.0.0.1:5560)
        duration_s: segundos entre mediciones
    Returns:
        {'count': int, 'eps': float}
    """
```

### `measure_semaphore_latency(qh_url, pub_url, calle_id, timeout_s)`

```python
def measure_semaphore_latency(qh_url, pub_url, calle_id, timeout_s=15) -> float | None:
    """
    Args:
        qh_url: URL del QueryHandler REP (ej: tcp://127.0.0.1:5563)
        pub_url: URL del PUB de ControlSemáforos (ej: tcp://127.0.0.1:5570)
        calle_id: calle para OLA_VERDE (ej: fila_C)
        timeout_s: timeout total en segundos
    Returns:
        Latencia en ms, o None si timeout
    """
    # 1. SUB al semaphore_pub
    # 2. REQ al query_handler con ORDEN_DIRECTA
    # 3. zmq.Poller espera CAMBIO_SEMAFORO
    # 4. time.perf_counter() → latencia
```

### `measure_monitoring_latency(qh_url, n_queries)`

```python
def measure_monitoring_latency(qh_url, n_queries=10) -> tuple:
    """
    Args:
        qh_url: URL del QueryHandler REP
        n_queries: número de consultas
    Returns:
        (avg_ms, min_ms, max_ms)
    """
```

---

## Módulo Compartido (common.py)

`common.py` provee la infraestructura compartida entre todos los tests:

| Elemento | Descripción |
|---|---|
| `PROJECT_ROOT`, `PC1_DIR`, etc. | Rutas absolutas del proyecto |
| `load_benchmark_test(test_name)` | Lee `config.json.benchmark.tests.<test>` |
| `ProcessManager` | Context manager para subprocesos |
| `clean_data_dirs()` | Limpia `bd_replica_data/` y `bd_principal_data/` |
| `build_pc1_config_base(modo)` | Construye config de PC1 para modo dado |
| `write_pc1_config(config)` | Escribe config.json de PC1 |
| `read_latency_file(path)` | Lee archivo de latencias |
| `latency_stats(latencies)` | Calcula avg/min/max/n |

### ProcessManager

```python
with ProcessManager() as pm:
    pm.start('Broker', 'broker.py', PC1_DIR, env=env_broker)
    pm.start('Sensores', 'sensores.py', PC1_DIR)
    # ... test ...
# → kill_all() automático al salir
```

| Característica | Detalle |
|---|---|
| stdout/stderr | `DEVNULL` — evita bloqueo por pipe lleno |
| PYTHONIOENCODING | `utf-8` — evita crash por UnicodeEncodeError |
| Cleanup Windows | `taskkill /F /T /PID` — mata árbol completo |
| Cleanup Linux | `os.kill(pid, SIGTERM)` |
| Orden | LIFO (último en entrar, primero en morir) |

---

## Instrumentación del Broker

El broker escribe latencia por evento cuando la variable `BENCHMARK_LATENCY_FILE` está definida.

### Modo Simple (`PC1/broker.py:223-240`)

```python
t_start = time.perf_counter()
# validación + enriquecimiento + publicación
latency_us = (time.perf_counter() - t_start) * 1_000_000
# escribe a archivo si BENCHMARK_LATENCY_FILE existe
```

### Modo Multiprocesos (`PC1/broker.py:105-132`)

```python
t_start = time.perf_counter()
# validación + enriquecimiento del worker
latency_us = (time.perf_counter() - t_start) * 1_000_000
# escribe a archivo, luego envía al Publisher
```

---

## Modos de Uso

### PC1 — Broker Latency

```bash
# Los sensores y broker corren localmente en PC1:

python tests/benchmark/test_broker_latency.py
# Output:
#   Broker Latency (simple) → avg=74.2 min=44.3 max=138.4 us (n=42)
#   Broker Latency (multiprocesos) → avg=42.8 min=27.1 max=77.1 us (n=35)
```

### PC2 — Semáforo, Monitoreo, Throughput

```bash
# Primero iniciar servicios manualmente:
#   python PC2/main.py
#   python PC2/control_semaforos/control_semaforos.py
#   python PC2/replica_db/database_multith_replica.py

# Luego ejecutar tests (solo miden):
python tests/benchmark/test_semaphore_latency.py
python tests/benchmark/test_monitoring_latency.py
python tests/benchmark/test_throughput.py
```

### PC3 — Semáforo, Monitoreo, Throughput

```bash
# Primero iniciar BD principal manualmente:
#   python PC3_failover/database_multith.py

# (PC1 y PC2 también deben estar corriendo)

# Luego ejecutar tests (conectan a PC2):
python tests/benchmark/test_semaphore_latency.py
python tests/benchmark/test_monitoring_latency.py
python tests/benchmark/test_throughput.py
```

### Desarrollo Local

```bash
# En una sola máquina, config.json con 127.0.0.1:

# 1. Broker Latency (inicia sensores+broker):
python tests/benchmark/test_broker_latency.py

# 2. Tests de medición (iniciar servicios primero):
#    python PC2/main.py
#    python PC2/control_semaforos/control_semaforos.py
#    python PC2/replica_db/database_multith_replica.py
#    python PC3_failover/database_multith.py
#    python PC1/sensores.py
#    python PC1/broker.py

python tests/benchmark/test_semaphore_latency.py
python tests/benchmark/test_monitoring_latency.py
python tests/benchmark/test_throughput.py
```

---

## Bugs Encontrados y Corregidos

| # | Bug | Síntoma | Causa | Fix |
|---|---|---|---|---|
| 1 | **Pipe blocking** | Pocos eventos | `stdout=PIPE` se llenaba y bloqueaba | `DEVNULL` |
| 2 | **Unicode crash** | Semáforo timeout | `print("🟢...")` fallaba con cp1252 | `PYTHONIOENCODING=utf-8` |
| 3 | **Orphan processes** | Puertos ocupados | Procesos hijos no se limpiaban | `taskkill /F /T` |
| 4 | **Env var no propagada** | Broker no escribía latencia | `env` no se pasaba a `Popen` | Agregar parámetro `env` |
| 5 | **PUB notificación perdida** | CAMBIO_SEMAFORO no llegaba | Crash post-send cerraba socket | Unicode fix (bug #2) |
| 6 | **Typo ZMQ** | Posible error conexión | `tcp:*:5556` sin `//` | Corregido a `tcp://*:5556` |
| 7 | **PUB hardcodeado** | Puerto 5570 no configurable | `SEMAFORO_PUB_URL` en clase | Leer de `config.json` |

---

## Interpretación de Resultados

Ejemplo de output:

```
--- Broker Latency (simple) ---
  Resultado: avg=83.6us  min=49.0us  max=141.7us  (n=42)

--- Broker Latency (multiprocesos) ---
  Resultado: avg=32.9us  min=26.8us  max=46.9us  (n=42)
```

| Métrica | Interpretación |
|---|---|
| **avg** | Latencia promedio — la métrica principal |
| **min** | Mínimo observado — caso ideal sin contención |
| **max** | Máximo observado — pico bajo condiciones adversas |
| **n** | Número de muestras — valida que llegaron los eventos esperados |
| **Simple vs Multiprocesos** | Multiprocesos suele ser 2-3x más rápido en latencia |

Para throughput:

```
--- Throughput ---
  Resultado: 144 eventos (1.20 ev/s)
```

144 eventos = 6 sensores × 24 ciclos en 120s (con intervalo 5s).
