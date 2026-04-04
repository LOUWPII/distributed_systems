import queue
import threading
from datetime import datetime, timezone
from typing import Optional
from config import Config
# Imports desde tu nueva estructura de dominio
from dominio.estado_calle import EstadoCalle
from dominio.interseccion import Interseccion
from dominio.orden_directa import OrdenDirecta
from dominio.semaforo import Semaforo
from dominio.constantes import (
    DURACION_CONGESTION_S,
    DURACION_NORMAL_S,
    DURACION_OLA_VERDE_S,
)
# Imports de DTOs y Enums
from dtos.evento_sensor import EventoSensor # (Corrección respecto a "evento_base")
from enums import EstadoSemaforo, EstadoTrafico, TipoCalle
# Aquí asumo que dejarás GestorSalida en servicios o lo mandarás a infrastructure/
from infrastructure.gestor_salida import GestorSalida

# 
class RulesEngine(threading.Thread):
    """
    Hilo principal de procesamiento de eventos y toma de decisiones.

    estados_calle:  dict[calle_id → EstadoCalle]
        Una entrada por cada calle que aparece en el archivo de configuración.
        Se pobla al inicializar desde config, no dinámicamente.

    intersecciones: dict[interseccion_id → Interseccion]
        Una entrada por cada intersección con semáforos definidos en config.
        Cada Interseccion contiene dos Semaforo (fila y columna).

    ordenes_activas: list[OrdenDirecta]
        Órdenes del usuario que tienen prioridad sobre las reglas automáticas.
        Se limpian automáticamente cuando expiran.
    """

    def __init__(
        self,
        config: Config,
        event_queue: queue.Queue,
        gestor_salida: GestorSalida,
    ):
        super().__init__(daemon=True, name="RulesEngine")
        self._config = config
        self._event_queue = event_queue
        self._gestor = gestor_salida
        self._activo = True

        # Lock para ordenes_activas: es modificada por QueryHandler (hilo distinto)
        self._lock_ordenes = threading.Lock()

        # Estado del tráfico por interseccion y calle — se inicializa desde config
        self._estados_interseccion: dict[str, dict[str, EstadoCalle]] = {}
        self._intersecciones: dict[str, Interseccion] = {}
        self._ordenes_activas: list[OrdenDirecta] = []

        self._inicializar_desde_config()

    # Construye el estado inicial del sistema a partir del config.json
    def _inicializar_desde_config(self) -> None:
        # Crear diccionarios de EstadoCalle por cada intersección
        for sensor in self._config.sensores:
            calle_id = sensor["calle_id"]
            int_id = sensor.get("interseccion") or sensor.get("interseccion_id", "")
            tipo_str = sensor["direccion"]  # "fila" o "columna"
            
            if int_id not in self._estados_interseccion:
                self._estados_interseccion[int_id] = {}

            if calle_id not in self._estados_interseccion[int_id]:
                tipo = TipoCalle.FILA if tipo_str == "fila" else TipoCalle.COLUMNA
                self._estados_interseccion[int_id][calle_id] = EstadoCalle(calle_id, tipo)

        # Crear las Interseccion con sus dos Semaforo
        for item in self._config.intersecciones:
            int_id = item["interseccion_id"]
            calle_fila = item["calle_fila"]
            calle_col = item["calle_columna"]

            # Se crea un semaforo para la calle fila y otro para la calle columna
            semaforo_fila = Semaforo(
                semaforo_id = f"SEM-F-{int_id}",
                calle_id = calle_fila,
                interseccion_id = int_id,
                estado_inicial = EstadoSemaforo.VERDE,
            )
            semaforo_col = Semaforo(
                semaforo_id = f"SEM-C-{int_id}",
                calle_id = calle_col,
                interseccion_id = int_id,
                estado_inicial = EstadoSemaforo.ROJO,
            )
            # Se crea una interseccion con sus dos semaforos
            self._intersecciones[int_id] = Interseccion(
                interseccion_id = int_id,
                semaforo_fila = semaforo_fila,
                semaforo_columna = semaforo_col,
            )

        print(f"[RulesEngine] Dics de {len(self._estados_interseccion)} intersecciones cargados")
        print(f"[RulesEngine] {len(self._intersecciones)} intersecciones físicas cargadas")

    # Ciclo principal del hilo
    def run(self) -> None:
        print("[RulesEngine] Iniciado — esperando eventos")
        while self._activo:
            try:
                # get() bloquea hasta que haya un evento en la cola
                evento = self._event_queue.get(timeout=1.0)
                self.procesar_evento(evento)
            except queue.Empty:
                # Limpieza proactiva en tiempos muertos
                self._limpiar_ordenes_expiradas()
                self._limpiar_estados_por_timeout()
            except Exception as e:
                print(f"[RulesEngine] Error procesando evento: {e}")

    # Detiene el hilo
    def detener(self) -> None:
        self._activo = False

    # Procesa un evento recibido
    def procesar_evento(self, evento: EventoSensor) -> None:
        # 1. Limpiar órdenes expiradas
        self._limpiar_ordenes_expiradas()

        calle_id = evento.calle_id
        int_id = evento.interseccion_id

        # 2. Verificar si la calle en esta intersección existe en el mapa
        if int_id not in self._estados_interseccion or calle_id not in self._estados_interseccion[int_id]:
            print(f"[RulesEngine] ⚠ Evento de tramo desconocido: INT={int_id} CALLE={calle_id}")
            return

        estado = self._estados_interseccion[int_id][calle_id]
        
        # 3. Actualizar el EstadoCalle del tramo específico con los datos del evento
        estado.actualizar(evento)
        
        # 4. Persistir el evento en las BDs
        self._gestor.persistir_evento(evento)

        print(
            f"[RulesEngine] Evento recibido: {evento} | "
            f"INT={int_id} cola={estado.ultima_cola} vel={estado.velocidad_promedio:.1f}km/h"
        )
        
        # 5. Evaluar si el estado del tráfico cambia (comportamiento micro)
        self.evaluar_micro(int_id, estado)


    # Cambiar orden 2,4,1,3,5

    # Evalúa las reglas a nivel micro (por intersección) y actúa si el estado cambió
    def evaluar_micro(self, interseccion_id: str, estado: EstadoCalle) -> None:
        # Prioridad 1: verificar si hay una orden directa activa para esta calle
        # (Las órdenes directas operan a nivel macro todavía)
        with self._lock_ordenes:
            orden_activa = next(
                (o for o in self._ordenes_activas if o.calle_id == estado.calle_id and o.esta_activa()),
                None,
            )

        if orden_activa is not None:
            # Hay orden directa activa en toda la avenida → no cambiar individualmente
            return

        # Prioridad 2: evaluar reglas automáticas
        estado_anterior = estado.estado
        estado_nuevo = estado.evaluar_estado()

        if estado_nuevo == estado_anterior:
            return  # Sin cambio → no hacer nada

        # El estado cambió → actualizar, actuar y persistir
        estado.estado = estado_nuevo
        motivo = f"Regla automática micro: {estado_anterior.value} → {estado_nuevo.value}"

        print(
            f"[RulesEngine] Cambio de estado MICRO: INT={interseccion_id} CALLE={estado.calle_id} | "
            f"{estado_anterior.value} → {estado_nuevo.value}"
        )

        # Determinar duración según el nuevo estado
        duracion = (
            DURACION_CONGESTION_S
            if estado_nuevo == EstadoTrafico.CONGESTION
            else DURACION_NORMAL_S
        )

        if estado_nuevo in (EstadoTrafico.CONGESTION, EstadoTrafico.OLA_VERDE):
            self.aplicar_verde_micro(interseccion_id, estado.calle_id, duracion, motivo)
        else:
            # NORMAL: restaurar ciclo estándar localmente
            self._aplicar_ciclo_normal_micro(interseccion_id, estado.calle_id, motivo)

        # Usamos tramo_id para la persistencia del cambio de estado específico
        tramo_id = f"{interseccion_id}_{estado.calle_id}"
        self._gestor.persistir_cambio(
            calle_id = tramo_id,
            estado_anterior = estado_anterior.value,
            estado_nuevo = estado_nuevo.value,
            motivo = motivo,
        )

    # Aplica luz verde a un semáforo específico (comportamiento Micro)
    def aplicar_verde_micro(
        self,
        interseccion_id: str,
        calle_id: str,
        duracion_s: int = DURACION_OLA_VERDE_S,
        motivo: str = "VERDE_MICRO",
    ) -> None:
        
        interseccion = self._intersecciones.get(interseccion_id)
        if not interseccion:
            return

        semaforo = interseccion.get_semaforo(calle_id)
        if not semaforo:
            return

        # Determinar si la calle es fila o columna localmente para exclusión mutua
        if semaforo.calle_id == interseccion.semaforo_fila.calle_id:
            interseccion.set_verde_fila(duracion_s)
        else:
            interseccion.set_verde_columna(duracion_s)

        # Enviar comandos de actualización de luces
        self._gestor.enviar_cmd(semaforo.to_comando(motivo))

        semaforo_cruzado = interseccion.get_semaforo_cruzado(calle_id)
        if semaforo_cruzado:
            self._gestor.enviar_cmd(semaforo_cruzado.to_comando(motivo))

        print(
            f"[RulesEngine] Verde micro aplicado en INT={interseccion_id} CALLE={calle_id} | "
            f"duración={duracion_s}s"
        )
        
    # Restaura ciclo normal en un semáforo específico (comportamiento Micro)
    def _aplicar_ciclo_normal_micro(
        self, 
        interseccion_id: str, 
        calle_id: str, 
        motivo: str
    ) -> None:
        
        interseccion = self._intersecciones.get(interseccion_id)
        if not interseccion:
            return

        semaforo = interseccion.get_semaforo(calle_id)
        if not semaforo:
            return

        if semaforo.calle_id == interseccion.semaforo_fila.calle_id:
            interseccion.set_verde_fila(DURACION_NORMAL_S)
        else:
            interseccion.set_verde_columna(DURACION_NORMAL_S)

        self._gestor.enviar_cmd(semaforo.to_comando(motivo))

    # Aplica una ola verde a TODA una calle (comportamiento Macro para órdenes manuales)
    def aplicar_ola_verde(
        self,
        calle_id: str,
        duracion_s: int = DURACION_OLA_VERDE_S,
        motivo: str = "OLA_VERDE",
    ) -> None:

        # Pone en VERDE todos los semáforos de la calle indicada en cada intersección donde esa calle tiene semáforo.
        # Las calles cruzadas quedan automáticamente en ROJO (exclusión mutua).
        comandos_enviados = 0
        for interseccion in self._intersecciones.values():
            semaforo = interseccion.get_semaforo(calle_id)
            if semaforo is None:
                continue  # Esta intersección no tiene la calle indicada

            # Determinar si la calle es fila o columna para llamar al método correcto de Interseccion (garantiza exclusión mutua)
            if semaforo.calle_id == interseccion.semaforo_fila.calle_id:
                interseccion.set_verde_fila(duracion_s)
            else:
                interseccion.set_verde_columna(duracion_s)

            # Generar y enviar el comando para este semáforo
            comando = semaforo.to_comando(motivo)
            self._gestor.enviar_cmd(comando)

            semaforo_cruzado = interseccion.get_semaforo_cruzado(calle_id)
            if semaforo_cruzado:
                cmd_cruzado = semaforo_cruzado.to_comando(motivo)
                self._gestor.enviar_cmd(cmd_cruzado)

            comandos_enviados += 2

        print(
            f"[RulesEngine] Ola verde Macro aplicada en {calle_id} | "
            f"{comandos_enviados} comandos enviados | duración={duracion_s}s"
        )

    # Aplica un ciclo normal a TODA una calle indicada
    def _aplicar_ciclo_normal(self, calle_id: str, motivo: str) -> None:
        # Aplica un ciclo normal a una calle indicada
        for interseccion in self._intersecciones.values():
            semaforo = interseccion.get_semaforo(calle_id)
            # Si la calle no tiene semáforo en esta intersección, continuar
            if semaforo is None:
                continue

            # Determinar si la calle es fila o columna para llamar
            if semaforo.calle_id == interseccion.semaforo_fila.calle_id:
                interseccion.set_verde_fila(DURACION_NORMAL_S)
            else:
                interseccion.set_verde_columna(DURACION_NORMAL_S)

            self._gestor.enviar_cmd(semaforo.to_comando(motivo))

    # Gestión de órdenes directas
    def registrar_orden(self, orden: OrdenDirecta) -> None:
        # Registra una OrdenDirecta y aplica inmediatamente la ola verde.
        # Thread-safe: usa _lock_ordenes.
        with self._lock_ordenes:
            self._ordenes_activas.append(orden)

        # Actualizar el estado en memoria para cada tramo de la calle en cada intersección
        for int_id, dic_calles in self._estados_interseccion.items():
            if orden.calle_id in dic_calles:
                dic_calles[orden.calle_id].estado = EstadoTrafico.OLA_VERDE

        # Aplicar inmediatamente los cambios en los semáforos (a nivel Macro)
        self.aplicar_ola_verde(orden.calle_id, orden.duracion_s, orden.motivo)
        self._gestor.persistir_orden(orden)

        print(f"[RulesEngine] Orden directa registrada: {orden}")

    # Elimina las órdenes que ya expiraron y restaura el ciclo normal
    def _limpiar_ordenes_expiradas(self) -> None:
        with self._lock_ordenes:
            expiradas = [o for o in self._ordenes_activas if o.esta_expirada()]
            self._ordenes_activas = [o for o in self._ordenes_activas if o.esta_activa()]

        # Restaurar ciclo normal en calles con órdenes expiradas
        for orden in expiradas:
            print(f"[RulesEngine] Orden expirada: {orden} → restaurando ciclo normal")
            
            # Actualizar el estado en memoria para cada tramo afectado
            for int_id, dic_calles in self._estados_interseccion.items():
                if orden.calle_id in dic_calles:
                    dic_calles[orden.calle_id].estado = EstadoTrafico.NORMAL
                    
            self._aplicar_ciclo_normal(orden.calle_id, "ORDEN_EXPIRADA")

    # Limpia estados de semáforos que llevan mucho tiempo sin recibir eventos
    def _limpiar_estados_por_timeout(self) -> None:
        # Obtiene la hora actual
        ahora = datetime.now(timezone.utc)
        # Recorre todos los tramos de todas las intersecciones
        for int_id, dic_calles in self._estados_interseccion.items():
            # Recorre todas las calles de la intersección
            for calle_id, estado in dic_calles.items():
                # Si el estado no es normal, verifica si ha pasado mucho tiempo
                if estado.estado != EstadoTrafico.NORMAL:
                    # Asegurarse de que ts_ultimo_evento también sea aware
                    if estado.ts_ultimo_evento.tzinfo is None:
                        estado.ts_ultimo_evento = estado.ts_ultimo_evento.replace(tzinfo=timezone.utc)
                    
                    transcurrido = (ahora - estado.ts_ultimo_evento).total_seconds()
                    # Si ha pasado más de 5 minutos, se restaura a NORMAL
                    if transcurrido > 300:
                        print(f"[RulesEngine] Timeout en tramo {int_id}_{calle_id} (silencio de {transcurrido:.0f}s) -> Reset a NORMAL")
                        estado.estado = EstadoTrafico.NORMAL
                        # Aplica ciclo normal al tramo específico
                        self._aplicar_ciclo_normal_micro(int_id, calle_id, "TIMEOUT_SENSOR")

    # Retorna el estado actual de una calle (primer tramo encontrado) o None
    def get_estado_calle(self, calle_id: str) -> Optional[EstadoCalle]:
        # Para compatibilidad con consultas legacy, retorna el primer estado encontrado para la calle
        for dic_calles in self._estados_interseccion.values():
            if calle_id in dic_calles:
                return dic_calles[calle_id]
        return None

    # Retorna el estado actual de todos los tramos de manera plana
    def get_todos_estados(self) -> dict:
        resultado = {}
        for int_id, dic_calles in self._estados_interseccion.items():
            for calle_id, estado in dic_calles.items():
                tramo_id = f"{int_id}_{calle_id}"
                resultado[tramo_id] = estado.to_registro()
        return resultado

    # Retorna una intersección específica
    def get_interseccion(self, interseccion_id: str) -> Optional[Interseccion]:
        return self._intersecciones.get(interseccion_id)