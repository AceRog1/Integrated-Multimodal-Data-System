from rtree import index # Instalar rtree
from typing import List, Tuple, Any, Dict
import json
import os

class RTreeIndex:
    def __init__(self, filename: str = "rtree.dat"):
        p = index.Property()
        self.idx = index.Index(properties=p)
        self.data = {}  # id -> record_position
        self.filename = filename
        self.metadata_file = filename.replace('.dat', '_meta.json')

    def add(self, id: int, coords: Tuple[float, float], record_position: int):
        x, y = coords
        self.idx.insert(id, (x, y, x, y))
        self.data[id] = record_position

    def rangeSearch(self, point: Tuple[float, float], radius: float) -> List[int]:
        x, y = point
        box = (x - radius, y - radius, x + radius, y + radius)
        ids = list(self.idx.intersection(box))
        return [self.data[i] for i in ids]  # Retornar posiciones de registros

    def knnSearch(self, point: Tuple[float, float], k: int) -> List[int]:
        x, y = point
        ids = list(self.idx.nearest((x, y, x, y), k))
        return [self.data[i] for i in ids]  # Retornar posiciones de registros

    def save(self)->None:
        # Guardar metadata
        metadata = {
            "num_entries": len(self.data)
        }
        with open(self.metadata_file, 'w') as f:
            json.dump(metadata, f)
        
        # Guardar datos
        with open(self.filename, 'wb') as f:
            f.write(len(self.data).to_bytes(4, byteorder='little'))
            for id_val, record_pos in self.data.items():
                # Obtener coordenadas del indice
                bounds = self.idx.bounds(id_val)
                x, y = bounds[0], bounds[1]  # xmin, ymin
                f.write(id_val.to_bytes(4, byteorder='little'))
                f.write(x.to_bytes(8, byteorder='little'))  # float como double
                f.write(y.to_bytes(8, byteorder='little'))
                f.write(record_pos.to_bytes(4, byteorder='little'))

    def load(self) -> None:
        if not os.path.exists(self.filename) or not os.path.exists(self.metadata_file):
            return
            
        # Cargar metadata
        with open(self.metadata_file, 'r') as f:
            metadata = json.load(f)
        
        # Cargar datos
        with open(self.filename, 'rb') as f:
            num_entries = int.from_bytes(f.read(4), byteorder='little')
            self.data = {}
            
            for _ in range(num_entries):
                id_val = int.from_bytes(f.read(4), byteorder='little')
                x = float.from_bytes(f.read(8), byteorder='little')
                y = float.from_bytes(f.read(8), byteorder='little')
                record_pos = int.from_bytes(f.read(4), byteorder='little')
                
                self.data[id_val] = record_pos
                # Reconstruir indice
                self.idx.insert(id_val, (x, y, x, y))

    @staticmethod
    def parse_array_float(array_str: str) -> Tuple[float, float]:
        array_str = array_str.strip()
        if array_str.startswith('ARRAY[') and array_str.endswith(']'):
            array_str = array_str[6:-1]  # Remover
        
        # Dividir por comas y convertir a float
        parts = array_str.split(',')
        if len(parts) != 2:
            raise ValueError(f"Array debe tener exactamente 2 elementos: {array_str}")
        
        try:
            x = float(parts[0].strip())
            y = float(parts[1].strip())
            return (x, y)
        except ValueError as e:
            raise ValueError(f"Error parseando coordenadas: {array_str}") from e
