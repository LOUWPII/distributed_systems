import zmq
import json
import time


def count_db_events(db_url, duration_s):
    """
    Cuenta eventos almacenados en BD (Réplica o Principal) consultando
    CONSULTA_TOTAL_EVENTOS antes y después de duration_s segundos.
    """
    ctx = zmq.Context()
    req = ctx.socket(zmq.REQ)
    req.connect(db_url)
    req.setsockopt(zmq.RCVTIMEO, 5000)

    req.send_string(json.dumps({"tipo": "CONSULTA_TOTAL_EVENTOS"}))
    resp = req.recv_json()
    count_inicio = resp.get("total_eventos", 0)

    time.sleep(duration_s)

    req.send_string(json.dumps({"tipo": "CONSULTA_TOTAL_EVENTOS"}))
    resp = req.recv_json()
    count_fin = resp.get("total_eventos", 0)

    req.close()
    ctx.term()

    total = count_fin - count_inicio
    return {
        'count': total,
        'eps': total / duration_s if duration_s > 0 else 0,
    }


def measure_semaphore_latency(query_handler_url, semaphore_pub_url, calle_id, timeout_s=15):
    """
    Mide el tiempo completo desde ORDEN_DIRECTA hasta que ControlSemáforos
    procesa el cambio. Usa PUB del ControlSemáforos (:5570) para detectar
    el momento exacto vía zmq.Poller (granularidad 1ms).

    Retorna latencia en ms, o None si timeout.
    """
    ctx = zmq.Context()

    sub = ctx.socket(zmq.SUB)
    sub.connect(semaphore_pub_url)
    sub.setsockopt_string(zmq.SUBSCRIBE, "")
    sub.setsockopt(zmq.RCVTIMEO, 100)

    req = ctx.socket(zmq.REQ)
    req.connect(query_handler_url)
    req.setsockopt(zmq.RCVTIMEO, 5000)

    orden = json.dumps({
        "tipo": "ORDEN_DIRECTA",
        "calle_id": calle_id,
        "accion": "OLA_VERDE",
        "duracion_s": 5,
        "motivo": "BENCHMARK"
    })

    req.send_string(orden)
    try:
        req.recv_json()
    except zmq.Again:
        sub.close()
        req.close()
        ctx.term()
        return None

    poller = zmq.Poller()
    poller.register(sub, zmq.POLLIN)

    start = time.perf_counter()
    deadline = start + timeout_s

    while time.perf_counter() < deadline:
        socks = dict(poller.poll(timeout=1))
        if sub in socks:
            try:
                notif = sub.recv_json()
                if notif.get("tipo") == "CAMBIO_SEMAFORO":
                    lat_ms = (time.perf_counter() - start) * 1000
                    sub.close()
                    req.close()
                    ctx.term()
                    return lat_ms
            except (zmq.Again, json.JSONDecodeError):
                continue

    sub.close()
    req.close()
    ctx.term()
    return None


def measure_monitoring_latency(query_handler_url, n_queries=10):
    """
    Envía CONSULTA_TODOS_ESTADOS n veces, mide RTT.
    Retorna (avg_ms, min_ms, max_ms).
    """
    ctx = zmq.Context()
    req = ctx.socket(zmq.REQ)
    req.connect(query_handler_url)
    req.setsockopt(zmq.RCVTIMEO, 5000)

    latencies = []
    for _ in range(n_queries):
        start = time.perf_counter()
        req.send_string(json.dumps({"tipo": "CONSULTA_TODOS_ESTADOS"}))
        try:
            req.recv_string()
            lat_ms = (time.perf_counter() - start) * 1000
            latencies.append(lat_ms)
        except zmq.Again:
            continue

    req.close()
    ctx.term()

    if not latencies:
        return 0, 0, 0
    return (
        sum(latencies) / len(latencies),
        min(latencies),
        max(latencies)
    )
