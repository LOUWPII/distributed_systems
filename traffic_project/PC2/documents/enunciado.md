Claro. Aquí tienes el contenido del PDF convertido a un formato Markdown limpio y estructurado, ideal para ser usado como insumo por un LLM o agente.

```markdown
# Proyecto: Gestión Inteligente de Tráfico Urbano (40%)

**Pontificia Universidad Javeriana - Facultad de Ingeniería - Departamento Ingeniería de Sistemas**  
**Proyecto Introducción a Sistemas Distribuidos - Período Académico 2026-10**

## Objetivos

- Desarrollar una solución a un problema de estructura distribuida.
- Utilizar patrones de comunicación síncronos y asíncronos.
- Resolver problemas que se presentan en sistemas distribuidos, tales como fallas en los componentes y persistencia de datos.
- Reconocer atributos de calidad (ej. desempeño, resiliencia) asociados a la implementación de un sistema distribuido.

## Descripción del Sistema a Desarrollar

El objetivo es diseñar e implementar una plataforma distribuida para la gestión inteligente del tráfico urbano. El sistema simula una ciudad con intersecciones controladas por semáforos inteligentes y una red de sensores que generan eventos en tiempo real (volumen vehicular, velocidad promedio, nivel de ocupación).

- **Representación de la ciudad**: Matriz o cuadrícula de NxM, donde N es la fila (letra) y M la columna (número). Notación: `INT_CK` (ej. `INT_C5` = fila C, columna 5). Los sensores están ubicados en varias intersecciones.
- **Comunicación**: Obligatorio usar ZeroMQ (ZMQ): https://zeromq.org/
- **Propósito del procesamiento**:
  - Recopilar y almacenar información de tráfico.
  - Analizar condiciones de congestión vehicular.
  - Tomar decisiones de control sobre semáforos.
  - Consultar situaciones de tráfico en un momento determinado.
  - Emitir acciones de cambio de luz en los semáforos.

## Funcionamiento del Sistema y Supuestos

- **Supuestos**:
  - Todas las vías tienen un único sentido.
  - Los semáforos cambian solo entre verde y rojo (no hay amarillo).
- **Distribución**: Los sensores, eventos y servicios se ubican en tres máquinas (PC1, PC2, PC3).

### Componentes PC1

- **Sensores de tráfico**: Procesos simulados que generan eventos periódicos (variables aleatorias). Envían datos de forma asíncrona a un **broker ZeroMQ** mediante patrón **PUB/SUB**.
- **Broker ZeroMQ**: Intermediario que se suscribe a los tópicos de los 3 tipos de sensores y reenvía los eventos al nodo de procesamiento y control (PC2).
- **Tipos de sensores**:

  1.  **Cámara (EVENTO_LONGITUD_COLA - Lq)** - Ejemplo:
      ```json
      {
        "sensor_id": "CAM-C5",
        "tipo_sensor": "camara",
        "interseccion": "INT-C5",
        "volumen": 10,
        "velocidad_promedio": 25,
        "timestamp": "2026-02-09T15:10:00Z"
      }
      ```
  2.  **Espira inductiva (EVENTO_CONTEO_VEHICULAR - Cv)** - Ejemplo:
      ```json
      {
        "sensor_id": "ESP-C5",
        "tipo_sensor": "espira_inductiva",
        "interseccion": "INT-C5",
        "vehiculos_contados": 12,
        "intervalo_segundos": 30,
        "timestamp_inicio": "2026-02-09T15:20:00Z",
        "timestamp_fin": "2026-02-09T15:20:30Z"
      }
      ```
  3.  **GPS (EVENTO_DENSIDAD_DE_TRAFICO - Dt)** - Ejemplo:
      ```json
      {
        "sensor_id": "GPS-C5",
        "nivel_congestion": "ALTA",
        "velocidad_promedio": 18,
        "timestamp": "2026-02-09T15:20:10Z"
      }
      ```
      - *Nota: Nivel de congestión: ALTA (<10 km/h), NORMAL (11-39 km/h), BAJA (>40 km/h)*

- **Inicialización**: Los estudiantes definen posición del sensor, cuadrículas que abarca, tiempo entre eventos, etc.

### Componentes PC2

- **Servicio de analítica**:
  - Se suscribe a eventos vía broker (PUB/SUB).
  - Procesa datos para detectar congestión/anomalías (reglas simples).
  - Envía información a Base de Datos usando **PUSH/PULL**.
  - Genera eventos de control (ej. extender fase verde para ambulancia).
  - Se comunica de forma asíncrona con el servicio de control de semáforos.
  - Puede recibir indicaciones directas desde el módulo de Monitoreo (PC3) para forzar cambios.
  - Imprime en pantalla: estado del tráfico, acciones a tomar.
- **Servicio de control de semáforos**: Ajusta el estado de los semáforos (rojo/verde) según órdenes recibidas. Imprime las operaciones.
- **Base de datos réplica**: Actualizada constantemente de forma asíncrona para servir como backup en caso de fallo del PC3.

### Componentes PC3

- **Servicio de monitoreo y consulta**:
  - Permite al usuario consultar el estado del sistema o enviar indicaciones directas al módulo de analítica (ej. priorizar ambulancia).
  - Consultas históricas (ej. horas pico) y puntuales por intersección usando patrón **REQ/REP**.
  - Imprime todas las operaciones que realiza.

## Arquitectura General del Sistema

*(En el documento original hay una figura en la página 4 que no es texto extraíble)*

### Procesos y Computadores

La implementación debe corresponder a la arquitectura planteada con los patrones de comunicación indicados. Es obligatorio usar ZeroMQ. Si se cambia algún patrón, debe justificarse en la primera entrega.

**Reglas**: Cada grupo define las reglas para los 3 estados (Tráfico normal, congestión, priorización), sincronización de semáforos, consultas de usuario y operaciones para cambios de estado.

**Fallas**: Si el PC3 falla, todos los procesos deben usar inmediatamente la réplica de BD en PC2. La operación debe ser transparente y el sistema continuar de forma ininterrumpida.

**Evaluación (sustentación)**: Se debe poder observar:
- Estado de la BD (original y réplica) con los cambios.
- Operaciones realizadas en los diferentes servicios.
- Consulta de estados de congestión históricos y situaciones de priorización.

## Medidas de Rendimiento

El equipo realizará pruebas comparando el diseño original (descrito) con un **diseño modificado que utiliza hilos en el servicio Broker ZMQ**.

| Diseño solicitado en el proyecto | Diseño multihilos en el Broker ZeroMQ |
| :--- | :--- |
| **Variables independientes (factores)**:<br>- Número de sensores<br>- Tiempo entre generación de mediciones | **(Iguales)** |
| **Variables dependientes**:<br>- Cantidad de solicitudes almacenadas en la BD en 2 minutos.<br>- Tiempo desde que el usuario solicita una acción hasta que el semáforo cambia. | **(Iguales)** |
| **Configuración 1**: 1 sensor de cada tipo generando datos cada 10 seg. | **Configuración 1**: (igual) |
| **Configuración 2**: 2 sensores de cada tipo generando datos cada 5 seg. | **Configuración 2**: (igual) |

Rellenar tablas, realizar gráficos, comentar resultados y justificar qué diseño es más escalable.

## Primera Entrega (15%)

**Semana 10 (sustentar martes en horario de clase)**

**Informe donde se especifique**:
- **Modelos del sistema**: arquitectónico, interacción, fallos y seguridad. Cómo se aplican al proyecto.
- **Diseño de TODO el sistema**: Diagrama de despliegue, Diagrama de componentes, Diagrama de clases, Diagrama de Secuencia. Incluir componentes para enmascarar fallas.
- **Explicación de**: a) Cómo los procesos obtienen la definición inicial (número/tipo de sensores, tamaño matriz, número de semáforos). b) Reglas. c) Tipos de consulta de usuarios. d) Ejemplos de indicaciones directas del Monitoreo a Analítica.
- **Protocolo de pruebas** para la entrega final (énfasis en pruebas de desempeño).
- **Cómo obtener las métricas de desempeño** de la tabla.

**Implementación requerida**:
- Servicios del PC1 y PC2
- Actualizaciones a la BD principal en el PC3
- Código fuente de las funcionalidades implementadas.

**Sustentación**: 15 minutos por equipo.

## Segunda Entrega (15%)

**Semana 17 (sustentar en horario de clase)**

**Componentes**:
1.  **Código fuente** (.zip) + archivo `README` con instrucciones de ejecución.
2.  **Documentación complementaria** a la primera entrega. Código fuente documentado.
3.  **Video (máx. 10 minutos)** explicando:
    - Distribución de componentes en máquinas.
    - Parámetros de todos los tipos de procesos.
    - Distribución de la cuadrícula entre sensores y asignación de semáforos.
    - Librerías y patrones usados.
    - Tratamiento de la falla.
4.  **Informe (máx. 5 páginas)** con experimentos y resultados. Debe incluir: especificaciones HW/SW, herramientas de medición, tablas, gráficos y análisis de resultados.

**Equipos de Trabajo**: Grupos a designar. No hay replicación de documentos/código (plagio).

## Calificación

### Primera Entrega (Valor 15% - Total 5 ptos)

| Indicador | Valoración en puntos |
| :--- | :--- |
| Informe (presentación, completitud) | 1 |
| Diseño del Proyecto | 1.5 |
| Protocolo de pruebas | 0.5 |
| Modelos del Sistema (fallas, interacción, seguridad) | 0.25 |
| Obtención de métricas de rendimiento | 0.25 |
| Implementación Inicial | 1.5 |
| **Total** | **5** |

### Segunda Entrega (Valor 25%)

- **Informe de rendimiento**: 10% (evaluado sobre 5 ptos)
- **Resto (corrida, sustentación, etc.)**: 15% (10% sustentación + 5% examen parcial)

#### Rúbrica Informe de Rendimiento (5 ptos)

| Indicador | Valoración |
| :--- | :--- |
| Informe: presentación, ortografía y redacción | 1 |
| Presentación de datos (tablas y gráficos correctos) | 2 |
| Análisis de resultados (comentarios y conclusiones) | 2 |
| **Total** | **5** |

#### Rúbrica Funcionamiento del Sistema y Sustentación (5 ptos)

| Indicador | Valoración |
| :--- | :--- |
| Todos los sensores implementados correctamente | 0.5 |
| Servicios en PC1: ZeroMQ | 0.75 |
| Servicios PC2: Analítica y Control de semáforos | 0.75 |
| Servicio PC3: Consulta y monitoreo | 0.5 |
| Persistencia/actualización de réplicas | 0.25 |
| Corrida en 3 máquinas | 0.75 |
| Tratamiento de Fallas | 0.75 |
| Código | 0.5 |
| Sustentación/Video | 0.25 |
| **Total** | **5** |

## Descripciones Cualitativas de las Rúbricas

*(Se incluyen las descripciones de niveles: Excelente, Competente, Deficiente para cada indicador, tal como aparecen en las páginas 9-13 del PDF original. Estas se han resumido en la tabla anterior para brevedad, pero el texto original contenía detalles como:)*

- **Diseño del Proyecto (Excelente)**: Se presentan todos los artefactos de diseño exigidos (diagramas de componentes, clases, secuencia, despliegue). Se incluyen todos los componentes del sistema final, incluyendo tolerancia a fallas y persistencia. Los diagramas están correctos.
- **Implementación Inicial (Excelente)**: Todas las funcionalidades requeridas para la primera entrega están implementadas correctamente. El sistema funciona en más de un computador (físico o virtual).
- **Tratamiento de Fallas (Excelente)**: Ante la falla del PC3 todos los procesos se reconectan con la réplica. La falla se detecta automáticamente (ej. health check). Los estudiantes explican los patrones de resiliencia implementados.
- **Corrida en 3 máquinas (Excelente)**: El sistema funciona en al menos tres computadoras (o máquinas virtuales) según la arquitectura sugerida.
```