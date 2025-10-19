import os
import struct
from typing import Dict, Any, Optional, List
from .data_types import Column, DataType, serialize_value, deserialize_value

class RecordFile:
    def __init__(self, file_path: str, columns: List[Column]):
        self.file_path = file_path
        self.columns = columns
        self.record_size = sum(col.get_size() for col in columns)
        self.record_count = 0

        self.HEADER_SIZE = struct.calcsize('i') # num regs
        
        # Crear archivo si no existe
        if not os.path.exists(file_path):
            self._initialize_file()
        else:
            self._load_header()
    
    def _initialize_file(self)->None:
        with open(self.file_path, 'wb') as f:
            f.write(struct.pack('i', 0))
        self.record_count = 0
    
    def _load_header(self)->None:
        if os.path.getsize(self.file_path) < self.HEADER_SIZE:
            self._initialize_file()
            return
            
        with open(self.file_path, 'rb') as f:
            self.record_count = struct.unpack('i', f.read(self.HEADER_SIZE))[0]
    
    def _update_header(self)->None:
        with open(self.file_path, 'r+b') as f:
            f.seek(0)
            f.write(struct.pack('i', self.record_count))
    
    def insert(self, record: Dict[str, Any])->int:
        # Verificar que esten todas las columnas
        for col in self.columns:
            if col.name not in record:
                raise ValueError(f"Falta columna '{col.name}' en el registro")
        
        # Serializar
        record_bytes = self._serialize_record(record)
        
        # Agregar al final del archivo
        with open(self.file_path, 'ab') as f:
            position = self.record_count
            f.write(record_bytes)
            self.record_count += 1
        
        # Actualizar header
        self._update_header()
        
        return position
    
    def read(self, position: int)->Optional[Dict[str, Any]]:
        if position < 0 or position >= self.record_count:
            return None
        with open(self.file_path, 'rb') as f:
            f.seek(self.HEADER_SIZE + position * self.record_size)
            record_bytes = f.read(self.record_size)
            
            if len(record_bytes) < self.record_size:
                return None
            
            return self._deserialize_record(record_bytes)
    
    def read_multiple(self, positions: List[int])->List[Dict[str, Any]]:
        results = []
        for pos in positions:
            record = self.read(pos)
            if record:
                results.append(record)
        return results
    
    def update(self, position: int, record: Dict[str, Any])->bool:
        if position < 0 or position >= self.record_count:
            return False
        
        # Verificar que esten todas las columnas
        for col in self.columns:
            if col.name not in record:
                raise ValueError(f"Falta columna '{col.name}' en el registro")
        
        # Serializar
        record_bytes = self._serialize_record(record)

        with open(self.file_path, 'r+b') as f:
            f.seek(self.HEADER_SIZE + position * self.record_size)
            f.write(record_bytes)
        
        return True
    
    def delete(self, position: int)->bool:
        if position < 0 or position >= self.record_count:
            return False
        
        # Leer registro actual
        record = self.read(position)
        if not record:
            return False
        with open(self.file_path, 'r+b') as f:
            f.seek(self.HEADER_SIZE + position * self.record_size)
            f.write(b'\xff')  # Marca de eliminado
        return True
    
    def is_deleted(self, position: int)->bool:
        if position < 0 or position >= self.record_count:
            return True
        with open(self.file_path, 'rb') as f:
            f.seek(self.HEADER_SIZE + position * self.record_size)
            first_byte = f.read(1)
            return first_byte == b'\xff'
    
    def scan_all(self)->List[Dict[str, Any]]:
        results = []
        for pos in range(self.record_count):
            if not self.is_deleted(pos):
                record = self.read(pos)
                if record:
                    results.append(record)
        return results
    
    def get_count(self)->int:
        return self.record_count
    
    def get_active_count(self)->int:
        count = 0
        for pos in range(self.record_count):
            if not self.is_deleted(pos):
                count += 1
        return count
    
    def _serialize_record(self, record: Dict[str, Any])->bytes:
        record_bytes = b''
        for col in self.columns:
            value = record.get(col.name)
            value_bytes = serialize_value(value, col.data_type, col.size)
            record_bytes += value_bytes
        return record_bytes
    
    def _deserialize_record(self, record_bytes: bytes)->Dict[str, Any]:
        if record_bytes[0:1] == b'\xff':
            return None
        record = {}
        offset = 0
        for col in self.columns:
            col_size = col.get_size()
            col_bytes = record_bytes[offset:offset + col_size]
            try:
                value = deserialize_value(col_bytes, col.data_type, col.size)
                record[col.name] = value
            except Exception as e:
                print(f"Error deserializando columna '{col.name}': {str(e)}")
                record[col.name] = None
            offset += col_size
        return record
    
    def compact(self)->int:
        active_records = []
        for pos in range(self.record_count):
            if not self.is_deleted(pos):
                record = self.read(pos)
                if record:
                    active_records.append(record)
        
        # Reescribir archivo
        backup_path = self.file_path + '.bak'
        os.rename(self.file_path, backup_path)
        self._initialize_file()
        for record in active_records:
            self.insert(record)
        os.remove(backup_path)
        return len(active_records)
    
    def close(self)->None:
        self._update_header()
    
    def __repr__(self) ->str:
        return f"RecordFile({self.file_path}, {self.record_count} registros, {self.record_size} bytes/registro)"
