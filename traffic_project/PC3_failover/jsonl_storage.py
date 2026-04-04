import os
import json
import shutil

# Clase para persistencia atómica de datos en archivos JSONL
class JSONLStorage:
    def __init__(self, filepath: str):
        self.filepath = filepath
        # Crear el archivo original si no existe para evitar errores de lectura iniciales
        if not os.path.exists(self.filepath):
            with open(self.filepath, "w", encoding="utf-8") as f:
                pass

    def append_atomico(self, data_dict: dict):
        # Implementa persistencia atómica mediante el patrón .tmp + replace.
        # Garantiza la integridad del archivo ante fallos del sistema
        temp_path = self.filepath + ".tmp"

        try:
            # 1. Copiar el archivo actual al temporal para preservar registros previos
            if os.path.exists(self.filepath):
                shutil.copy2(self.filepath, temp_path)

            # 2. Agregar el nuevo registro al final del archivo temporal
            with open(temp_path, "a", encoding="utf-8") as f_temp:
                f_temp.write(json.dumps(data_dict) + "\n")
                # 3. Forzar el vaciado del buffer al sistema operativo
                f_temp.flush()
                # 4. Asegurar que los datos se escriban físicamente en el disco (fsync)
                os.fsync(f_temp.fileno())

            # 5. Operación Atómica: Reemplazar el original con el temporal corregido
            # En sistemas operativos modernos, esta operación es "todo o nada"
            os.replace(temp_path, self.filepath)

        except Exception as e:
            # Limpiar el temporal en caso de error para no dejar basura
            if os.path.exists(temp_path):
                os.remove(temp_path)
            print(f"[Storage] Error crítico de persistencia en {self.filepath}: {e}")

    # Retorna todos los registros parseados como objetos JSON
    def read_all(self):
        registros = []
        # Si el archivo no existe, retornar una lista vacía
        if not os.path.exists(self.filepath):
            return registros
        # Leer todos los registros del archivo
        with open(self.filepath, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    registros.append(json.loads(line))
        return registros