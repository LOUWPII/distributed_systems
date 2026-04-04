import queue
import threading
from datetime import datetime
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

        # Estado del tráfico por calle — se inicializa desde config
        self._estados_calle: dict[str, EstadoCalle] = {}
        self._intersecciones: dict[str, Interseccion] = {}
        self._ordenes_activas: list[OrdenDirecta] = []

        self._inicializar_desde_config()

    # Construye el estado inicial del sistema a partir del config.json
    def _inicializar_desde_config(self) -> None:
        # Crear un EstadoCalle por cada calle única que aparece en los sensores
        for sensor in self._config.sensores:
            calle_id = sensor["calle_id"]
            tipo_str = sensor["direccion"]  # "fila" o "columna"
            # Si no existe la calle, se crea
            if calle_id not in self._estados_calle:
                tipo = TipoCalle.FILA if tipo_str == "fila" else TipoCalle.COLUMNA
                self._estados_calle[calle_id] = EstadoCalle(calle_id, tipo)

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

        print(f"[RulesEngine] {len(self._estados_calle)} calles cargadas")
        print(f"[RulesEngine] {len(self._intersecciones)} intersecciones cargadas")

    # Ciclo principal del hilo
    def run(self) -> None:
        print("[RulesEngine] Iniciado — esperando eventos")
        while self._activo:
            try:
                # get() bloquea hasta que haya un evento en la cola
                # timeout=1s permite revisar self._activo periódicamente
                evento = self._event_queue.get(timeout=1.0)
                self.procesar_evento(evento)
            except queue.Empty:
                # No llegó nada en 1 segundo → limpiar órdenes expiradas
                self._limpiar_ordenes_expiradas()
            # Si hay un error, se imprime
            except Exception as e:
                print(f"[RulesEngine] Error procesando evento: {e}")

        print("[RulesEngine] Detenido")

    # Detiene el hilo
    def detener(self) -> None:
        self._activo = False

    # Procesa un evento recibido
    def procesar_evento(self, evento: EventoSensor) -> None:
        # 1. Limpiar órdenes expiradas
        self._limpiar_ordenes_expiradas()

        calle_id = evento.calle_id
        # 2. Verificar si la calle del evento existe en el mapa
        if calle_id not in self._estados_calle:
            print(f"[RulesEngine] ⚠ Evento de calle desconocida: {calle_id}")
            return

        estado = self._estados_calle[calle_id]
        # 3. Actualizar el EstadoCalle con los datos del evento
        estado.actualizar(evento)
        # 4. Persistir el evento en las BDs
        self._gestor.persistir_evento(evento)

        print(
            f"[RulesEngine] Evento recibido: {evento} | "
            f"cola={estado.ultima_cola} vel={estado.velocidad_promedio:.1f}km/h"
        )
        # 5. Evaluar si el estado del tráfico cambia
        self.evaluar_calle(estado)


    # Cambiar orden 2,4,1,3,5

    # Evalúa las reglas para una calle y actúa si el estado cambió
    def evaluar_calle(self, estado: EstadoCalle) -> None:
        # Prioridad 1: verificar si hay una orden directa activa para esta calle
        with self._lock_ordenes:
            orden_activa = next(
                (o for o in self._ordenes_activas if o.calle_id == estado.calle_id and o.esta_activa()),
                None,
            )

        if orden_activa is not None:
            # Hay orden directa activa → no cambiar el estado automáticamente
            return

        # Prioridad 2: evaluar reglas automáticas
        estado_anterior = estado.estado
        estado_nuevo = estado.evaluar_estado()

        if estado_nuevo == estado_anterior:
            return  # Sin cambio → no hacer nada

        # El estado cambió → actualizar, actuar y persistir
        estado.estado = estado_nuevo
        motivo = f"Regla automática: {estado_anterior.value} → {estado_nuevo.value}"

        print(
            f"[RulesEngine] Cambio de estado: {estado.calle_id} | "
            f"{estado_anterior.value} → {estado_nuevo.value}"
        )

        # Determinar duración según el nuevo estado
        duracion = (
            DURACION_CONGESTION_S
            if estado_nuevo == EstadoTrafico.CONGESTION
            else DURACION_NORMAL_S
        )

        if estado_nuevo in (EstadoTrafico.CONGESTION, EstadoTrafico.OLA_VERDE):
            self.aplicar_ola_verde(estado.calle_id, duracion, motivo)
        else:
            # NORMAL: restaurar ciclo estándar (verde en calle, rojo en cruzadas)
            self._aplicar_ciclo_normal(estado.calle_id, motivo)

        self._gestor.persistir_cambio(
            calle_id = estado.calle_id,
            estado_anterior = estado_anterior.value,
            estado_nuevo = estado_nuevo.value,
            motivo = motivo,
        )

    # Aplica una ola verde a una calle
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

            # También enviar comando para el semáforo cruzado (ROJO)
            semaforo_cruzado = interseccion.get_semaforo_cruzado(calle_id)
            if semaforo_cruzado:
                cmd_cruzado = semaforo_cruzado.to_comando(motivo)
                self._gestor.enviar_cmd(cmd_cruzado)

            comandos_enviados += 2

        print(
            f"[RulesEngine] Ola verde aplicada en {calle_id} | "
            f"{comandos_enviados} comandos enviados | duración={duracion_s}s"
        )

    # Aplica un ciclo normal a una calle indicada
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

        # Actualizar el estado en memoria
        if orden.calle_id in self._estados_calle:
            self._estados_calle[orden.calle_id].estado = EstadoTrafico.OLA_VERDE

        # Aplicar inmediatamente los cambios en los semáforos
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
            # Actualizar el estado en memoria
            if orden.calle_id in self._estados_calle:
                self._estados_calle[orden.calle_id].estado = EstadoTrafico.NORMAL
            self._aplicar_ciclo_normal(orden.calle_id, "ORDEN_EXPIRADA")

    # Retorna el estado actual de una calle específica
    def get_estado_calle(self, calle_id: str) -> Optional[EstadoCalle]:
        return self._estados_calle.get(calle_id)

    # Retorna el estado actual de todas las calles
    def get_todos_estados(self) -> dict:
        return {k: v.to_registro() for k, v in self._estados_calle.items()}

    # Retorna una intersección específica
    def get_interseccion(self, interseccion_id: str) -> Optional[Interseccion]:
        return self._intersecciones.get(interseccion_id)