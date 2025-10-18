# Sequential Scan 
import os
import struct
from typing import List, Dict, Any, Optional, Callable
import json

class SequentialFile:
    def __init__(self, filename: str = "sequential.dat"):
        self.filename = filename
        self.metadata_file = filename.replace('.dat', '_meta.json')
        self.record_count = 0
        
    def insert(self, record: Dict[str, Any])->int:
        with open(self.filename, 'ab') as f:
            record_json = json.dumps(record)
            record_bytes = record_json.encode('utf-8')
            f.write(struct.pack('i', len(record_bytes)))
            f.write(record_bytes)
            position = self.record_count
            self.record_count += 1
            return position
    
    def read(self, position: int)->Optional[Dict[str, Any]]:
        if position >= self.record_count:
            return None
            
        with open(self.filename, 'rb') as f:
            # Saltar al registro deseado
            current_pos = 0
            while current_pos <= position:
                if current_pos == position:
                    # Leer longitud del registro
                    length_bytes = f.read(4)
                    if not length_bytes:
                        return None
                    length = struct.unpack('i', length_bytes)[0]
                    
                    # Leer registro
                    record_bytes = f.read(length)
                    if not record_bytes:
                        return None
                    
                    record_json = record_bytes.decode('utf-8')
                    return json.loads(record_json)
                else:
                    # Saltar este registro
                    length_bytes = f.read(4)
                    if not length_bytes:
                        return None
                    length = struct.unpack('i', length_bytes)[0]
                    f.seek(f.tell() + length)
                    current_pos += 1
        return None
    
    def scan_all(self)->List[Dict[str, Any]]:
        results = []
        with open(self.filename, 'rb') as f:
            while True:
                # Leer longitud del registro
                length_bytes = f.read(4)
                if not length_bytes:
                    break
                length = struct.unpack('i', length_bytes)[0]
                
                # Leer registro
                record_bytes = f.read(length)
                if not record_bytes:
                    break
                record_json = record_bytes.decode('utf-8')
                record = json.loads(record_json)
                results.append(record)
        return results
    
    def scan_where(self, condition: Callable[[Dict[str, Any]], bool])->List[Dict[str, Any]]:
        results = []
        with open(self.filename, 'rb') as f:
            while True:
                # Leer longitud del registro
                length_bytes = f.read(4)
                if not length_bytes:
                    break
                length = struct.unpack('i', length_bytes)[0]
                
                # Leer registro
                record_bytes = f.read(length)
                if not record_bytes:
                    break
                record_json = record_bytes.decode('utf-8')
                record = json.loads(record_json)
                
                # Condicion
                if condition(record):
                    results.append(record)
        
        return results
    
    def delete(self, position: int)->bool:
        record = self.read(position)
        if record is None:
            return False
        
        # Marcar como eliminado
        record['_deleted'] = True
        
        # Reescribir registro
        with open(self.filename, 'r+b') as f:
            current_pos = 0
            file_pos = 0
            while current_pos < position:
                length_bytes = f.read(4)
                if not length_bytes:
                    return False
                length = struct.unpack('i', length_bytes)[0]
                f.seek(f.tell() + length)
                file_pos = f.tell()
                current_pos += 1
            
            # Leer longitud actual
            f.seek(file_pos - 4)
            length_bytes = f.read(4)
            length = struct.unpack('i', length_bytes)[0]
            
            # Reescribir registro
            record_json = json.dumps(record)
            record_bytes = record_json.encode('utf-8')
            new_length = len(record_bytes)
            
            if new_length <= length:
                f.write(struct.pack('i', new_length))
                f.write(record_bytes)
                if new_length < length:
                    f.write(b'\x00' * (length - new_length))
            else:
                f.seek(file_pos - 4)
                f.write(struct.pack('i', 0))
                f.seek(0, 2)
                f.write(struct.pack('i', new_length))
                f.write(record_bytes)
        return True
    
    def update(self, position: int, record: Dict[str, Any])->bool:
        old_record = self.read(position)
        if old_record is None:
            return False
        
        # Mantener _deleted si existe
        if '_deleted' in old_record:
            record['_deleted'] = old_record['_deleted']
        
        # Reescribir registro
        with open(self.filename, 'r+b') as f:
            current_pos = 0
            file_pos = 0
            while current_pos < position:
                length_bytes = f.read(4)
                if not length_bytes:
                    return False
                length = struct.unpack('i', length_bytes)[0]
                f.seek(f.tell() + length)
                file_pos = f.tell()
                current_pos += 1
            
            # Leer longitud actual
            f.seek(file_pos - 4)
            length_bytes = f.read(4)
            length = struct.unpack('i', length_bytes)[0]
            
            # Reescribir registro
            record_json = json.dumps(record)
            record_bytes = record_json.encode('utf-8')
            new_length = len(record_bytes)
            
            if new_length <= length:
                f.write(struct.pack('i', new_length))
                f.write(record_bytes)
                if new_length < length:
                    f.write(b'\x00' * (length - new_length))
            else:
                f.seek(file_pos - 4)
                f.write(struct.pack('i', 0))
                f.seek(0, 2)
                f.write(struct.pack('i', new_length))
                f.write(record_bytes)
        return True
    
    def count(self)->int:
        count = 0
        with open(self.filename, 'rb') as f:
            while True:
                length_bytes = f.read(4)
                if not length_bytes:
                    break
                length = struct.unpack('i', length_bytes)[0]
                if length > 0:  # No eliminado
                    record_bytes = f.read(length)
                    if record_bytes:
                        record_json = record_bytes.decode('utf-8')
                        record = json.loads(record_json)
                        if not record.get('_deleted', False):
                            count += 1
                else:
                    pass # Registro eliminado, saltar
        return count
    
    def save_metadata(self)->None:
        metadata = {
            "record_count": self.record_count
        }
        with open(self.metadata_file, 'w') as f:
            json.dump(metadata, f)
    
    def load_metadata(self)->None:
        if os.path.exists(self.metadata_file):
            with open(self.metadata_file, 'r') as f:
                metadata = json.load(f)
                self.record_count = metadata.get("record_count", 0)
        else:
            # Contar registros existentes
            self.record_count = 0
            with open(self.filename, 'rb') as f:
                while True:
                    length_bytes = f.read(4)
                    if not length_bytes:
                        break
                    length = struct.unpack('i', length_bytes)[0]
                    if length > 0:
                        f.seek(f.tell() + length)
                        self.record_count += 1
                    else:
                        pass # Registro eliminado
