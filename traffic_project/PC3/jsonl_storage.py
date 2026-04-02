import os


class JSONLStorage:
    def __init__(self, filepath: str):
        self.filepath = filepath

        # Crear archivo si no existe
        if not os.path.exists(self.filepath):
            with open(self.filepath, "w") as f:
                pass

    def append(self, json_line: str):
        """
        Agrega una línea al archivo JSONL.
        Se asume que json_line ya es un JSON válido.
        """
        with open(self.filepath, "a", encoding="utf-8") as f:
            f.write(json_line.strip() + "\n")

    def read_all(self):
        """
        Retorna todas las líneas del archivo como lista de strings.
        """
        with open(self.filepath, "r", encoding="utf-8") as f:
            return [line.strip() for line in f if line.strip()]

    def clear(self):
        """
        Limpia completamente el archivo.
        """
        with open(self.filepath, "w", encoding="utf-8") as f:
            pass