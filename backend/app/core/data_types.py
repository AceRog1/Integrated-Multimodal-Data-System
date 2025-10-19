from enum import Enum
from typing import Any, Optional
import struct
from datetime import datetime

class DataType(Enum):
    INT = "int"
    FLOAT = "float"
    VARCHAR = "varchar"
    DATE = "date"
    ARRAY_FLOAT = "array_float"

class Column:
    def __init__(
        self,
        name: str,
        data_type: DataType,
        size: Optional[int] = None,
        is_primary_key: bool = False,
        has_index: bool = False,
        index_type: Optional[str] = None
    ):
        self.name = name
        self.data_type = data_type
        self.size = size 
        self.is_primary_key = is_primary_key
        self.has_index = has_index
        self.index_type = index_type
        
        #validaciones
        if data_type == DataType.VARCHAR and size is None:
            raise ValueError("VARCHAR requiere especificar size")
        
        if data_type == DataType.ARRAY_FLOAT and has_index and index_type not in ['rtree', None]:
            raise ValueError("ARRAY_FLOAT solo puede usar indice RTree")
    
    def get_struct_format(self) -> str:
        if self.data_type == DataType.INT:
            return 'i'
        elif self.data_type == DataType.FLOAT:
            return 'f'
        elif self.data_type == DataType.VARCHAR:
            return f'{self.size}s'
        elif self.data_type == DataType.DATE:
            return 'q'  
        elif self.data_type == DataType.ARRAY_FLOAT:
            return 'ff'  # (x, y)
        else:
            raise ValueError(f"Tipo de dato no soportado - {self.data_type}")
    
    def get_size(self) -> int:
        return struct.calcsize(self.get_struct_format())
    
    def to_dict(self) -> dict:
        return {
            'name': self.name,
            'data_type': self.data_type.value,
            'size': self.size,
            'is_primary_key': self.is_primary_key,
            'has_index': self.has_index,
            'index_type': self.index_type
        }
    
    @staticmethod
    def from_dict(data: dict) -> 'Column':
        return Column(
            name=data['name'],
            data_type=DataType(data['data_type']),
            size=data.get('size'),
            is_primary_key=data.get('is_primary_key', False),
            has_index=data.get('has_index', False),
            index_type=data.get('index_type')
        )
    
    def __repr__(self) -> str:
        size_str = f"[{self.size}]" if self.size else ""
        key_str = " KEY" if self.is_primary_key else ""
        index_str = f" INDEX {self.index_type.upper()}" if self.has_index and self.index_type else ""
        return f"{self.name} {self.data_type.value.upper()}{size_str}{key_str}{index_str}"


def parse_value(value: str, data_type: DataType, size: Optional[int] = None) -> Any:
    if value is None or value == '':
        return None
    
    try:
        if data_type == DataType.INT:
            return int(value)
        elif data_type == DataType.FLOAT:
            return float(value)
        elif data_type == DataType.VARCHAR:
            return str(value)[:size] if size else str(value)
        elif data_type == DataType.DATE:
            if value.isdigit():
                return int(value)  
            else:
                dt = datetime.strptime(value, '%Y-%m-%d')
                return int(dt.timestamp())
        elif data_type == DataType.ARRAY_FLOAT:
            value = value.strip()
            if value.startswith('ARRAY['):
                value = value[6:-1] 
            elif value.startswith('['):
                value = value[1:-1] 
            
            parts = [p.strip() for p in value.split(',')]
            if len(parts) != 2:
                raise ValueError(f"ARRAY_FLOAT debe tener exactamente 2 elementos- {value}")
            
            x = float(parts[0])
            y = float(parts[1])
            return (x, y)
        else:
            raise ValueError(f"Tipo de dato no soportado: {data_type}")
    except Exception as e:
        raise ValueError(f"Error parseando valor '{value}' como {data_type}: {str(e)}")


def serialize_value(value: Any, data_type: DataType, size: Optional[int] = None) -> bytes:
    if value is None:
        # retonamos bytes vacios 
        if data_type == DataType.INT:
            return struct.pack('i', 0)
        elif data_type == DataType.FLOAT:
            return struct.pack('f', 0.0)
        elif data_type == DataType.VARCHAR:
            return b'\x00' * (size or 50)
        elif data_type == DataType.DATE:
            return struct.pack('q', 0)
        elif data_type == DataType.ARRAY_FLOAT:
            return struct.pack('ff', 0.0, 0.0)
    
    try:
        if data_type == DataType.INT:
            return struct.pack('i', int(value))
        elif data_type == DataType.FLOAT:
            return struct.pack('f', float(value))
        elif data_type == DataType.VARCHAR:
            value_str = str(value)
            max_size = size or 50
            value_bytes = value_str.encode('utf-8')[:max_size]
            return value_bytes.ljust(max_size, b'\x00')
        elif data_type == DataType.DATE:
            if isinstance(value, int):
                return struct.pack('q', value)
            elif isinstance(value, str):
                dt = datetime.strptime(value, '%Y-%m-%d')
                return struct.pack('q', int(dt.timestamp()))
            else:
                raise ValueError(f"Fecha invalida: {value}")
        elif data_type == DataType.ARRAY_FLOAT:
            if isinstance(value, (list, tuple)) and len(value) == 2:
                return struct.pack('ff', float(value[0]), float(value[1]))
            else:
                raise ValueError(f"ARRAY_FLOAT debe ser tupla o lista de 2 elementos - {value}")
        else:
            raise ValueError(f"Tipo de dato no soportado {data_type}")
    except Exception as e:
        raise ValueError(f"Error serializando valor '{value}' como {data_type}- {str(e)}")


def deserialize_value(data: bytes, data_type: DataType, size: Optional[int] = None) -> Any:
    try:
        if data_type == DataType.INT:
            return struct.unpack('i', data[:4])[0]
        elif data_type == DataType.FLOAT:
            return struct.unpack('f', data[:4])[0]
        elif data_type == DataType.VARCHAR:
            max_size = size or 50
            value_bytes = data[:max_size]
            return value_bytes.decode('utf-8').rstrip('\x00')
        elif data_type == DataType.DATE:
            timestamp = struct.unpack('q', data[:8])[0]
            if timestamp == 0:
                return None
            dt = datetime.fromtimestamp(timestamp)
            return dt.strftime('%Y-%m-%d')
        elif data_type == DataType.ARRAY_FLOAT:
            x, y = struct.unpack('ff', data[:8])
            return (x, y)
        else:
            raise ValueError(f"Tipo de dato no soportado {data_type}")
    except Exception as e:
        raise ValueError(f"Error deserializando datos - {data_type} {str(e)}")


def get_key_type_for_index(data_type: DataType) -> str:
    if data_type == DataType.INT:
        return "int"
    elif data_type == DataType.FLOAT:
        return "float"
    elif data_type == DataType.VARCHAR:
        return "string"
    elif data_type == DataType.DATE:
        return "int"  # Timestamps como int
    else:
        raise ValueError(f"Tipo de dato no soportado para indices {data_type}")
