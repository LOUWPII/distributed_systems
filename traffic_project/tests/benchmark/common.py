import sys
import os
import json
import time
import subprocess
import shutil
import signal


# ── Rutas del proyecto ───────────────────────────────────────
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
PC1_DIR = os.path.join(PROJECT_ROOT, 'PC1')
PC2_DIR = os.path.join(PROJECT_ROOT, 'PC2')
PC3_DIR = os.path.join(PROJECT_ROOT, 'PC3_failover')
REPLICA_DIR = os.path.join(PC2_DIR, 'replica_db')
SEMAFORO_DIR = os.path.join(PC2_DIR, 'control_semaforos')
REPLICA_DATA_DIR = os.path.join(REPLICA_DIR, 'bd_replica_data')
PRINCIPAL_DATA_DIR = os.path.join(PC3_DIR, 'bd_principal_data')
BENCHMARK_DIR = os.path.dirname(os.path.abspath(__file__))


# ── Carga de config ──────────────────────────────────────────

def load_benchmark_test(test_name):
    cfg_path = os.path.join(PC1_DIR, 'config.json')
    with open(cfg_path, 'r') as f:
        cfg = json.load(f)
    test_cfg = cfg.get('benchmark', {}).get('tests', {}).get(test_name, {})
    if not test_cfg.get('enabled', False):
        print(f"[Error] El test '{test_name}' no esta habilitado en config.json")
        sys.exit(1)
    return test_cfg


# ── ProcessManager ───────────────────────────────────────────

class ProcessManager:
    def __init__(self):
        self.processes = []

    def start(self, name, script, cwd, env=None):
        if env is None:
            env = os.environ.copy()
        if 'PYTHONIOENCODING' not in env:
            env['PYTHONIOENCODING'] = 'utf-8'
        p = subprocess.Popen(
            [sys.executable, script],
            cwd=cwd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=env,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == 'win32' else 0,
        )
        self.processes.append((name, p))
        print(f"  [{name}] PID={p.pid}")
        return p

    def kill_all(self):
        print("  Deteniendo procesos...")
        for name, p in reversed(self.processes):
            if p.poll() is not None:
                continue
            print(f"    {name} (PID={p.pid})...")
            try:
                if sys.platform == 'win32':
                    subprocess.run(
                        ['taskkill', '/F', '/T', '/PID', str(p.pid)],
                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                        timeout=5
                    )
                else:
                    os.kill(p.pid, signal.SIGTERM)
            except Exception:
                pass
            try:
                p.wait(timeout=3)
            except Exception:
                pass
        self.processes = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.kill_all()


# ── Helpers de config ────────────────────────────────────────

def clean_data_dirs():
    for d in [REPLICA_DATA_DIR, PRINCIPAL_DATA_DIR]:
        if os.path.exists(d):
            shutil.rmtree(d)
            print(f"  Limpiado: {os.path.basename(d)}")


def build_pc1_config_base(modo):
    cfg_path = os.path.join(PC1_DIR, 'config.json')
    with open(cfg_path, 'r') as f:
        config = json.load(f)
    config['modo_broker'] = modo
    return config


def write_pc1_config(config):
    path = os.path.join(PC1_DIR, 'config.json')
    with open(path, 'w') as f:
        json.dump(config, f, indent=2)


# ── Lectura de latencias ─────────────────────────────────────

def read_latency_file(path):
    if not os.path.exists(path):
        return []
    with open(path, 'r') as f:
        return [float(line.strip()) for line in f if line.strip()]


def latency_stats(latencies):
    if not latencies:
        return {'avg': 0, 'min': 0, 'max': 0, 'n': 0}
    return {
        'avg': sum(latencies) / len(latencies),
        'min': min(latencies),
        'max': max(latencies),
        'n': len(latencies),
    }
