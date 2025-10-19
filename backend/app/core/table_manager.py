import os
import shutil
import json
from typing import Dict, List, Optional
from .data_types import Column, DataType

class Table:    
    def __init__(
        self,
        name: str,
        columns: List[Column],
        primary_key: str,
        primary_index_type: str = "btree",
        data_dir: str = "data"
    ):
        self.name = name
        self.columns = columns
        self.primary_key = primary_key
        self.primary_index_type = primary_index_type
        self.data_dir = data_dir
        
        #rutas
        self.table_dir = os.path.join(data_dir, name)
        self.metadata_path = os.path.join(self.table_dir, "metadata.json")
        self.data_file_path = os.path.join(self.table_dir, f"{name}_data.dat")
        
        if not any(col.name == primary_key for col in columns):
            raise ValueError(f"Columna primary key '{primary_key}' no existe en la tabla")
        
        # marcamos columna primary key
        for col in self.columns:
            if col.name == primary_key:
                col.is_primary_key = True
                if not col.has_index:
                    col.has_index = True
                    col.index_type = primary_index_type
    
    def get_column(self, name: str) -> Optional[Column]:
        for col in self.columns:
            if col.name == name:
                return col
        return None
    
    def get_primary_key_column(self) -> Column:
        for col in self.columns:
            if col.is_primary_key:
                return col
        raise ValueError(f"No se encontro columna primary key en tabla {self.name}")
    
    def get_indexed_columns(self) -> List[Column]:
        return [col for col in self.columns if col.has_index]
    
    def get_record_size(self) -> int:
        return sum(col.get_size() for col in self.columns)
    
    def get_column_offset(self, column_name: str) -> int:
        offset = 0
        for col in self.columns:
            if col.name == column_name:
                return offset
            offset += col.get_size()
        raise ValueError(f"Columna '{column_name}' no encontrada en tabla {self.name}")
    
    def save_metadata(self) -> None:
        os.makedirs(self.table_dir, exist_ok=True)
        
        metadata = {
            'name': self.name,
            'columns': [col.to_dict() for col in self.columns],
            'primary_key': self.primary_key,
            'primary_index_type': self.primary_index_type,
            'record_size': self.get_record_size()
        }
        
        with open(self.metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
    
    @staticmethod
    def load_metadata(name: str, data_dir: str = "data") -> 'Table':
        table_dir = os.path.join(data_dir, name)
        metadata_path = os.path.join(table_dir, "metadata.json")
        
        if not os.path.exists(metadata_path):
            raise FileNotFoundError(f"Metadata de tabla '{name}' no encontrada")
        
        with open(metadata_path, 'r') as f:
            metadata = json.load(f)
        
        columns = [Column.from_dict(col_data) for col_data in metadata['columns']]
        
        return Table(
            name=metadata['name'],
            columns=columns,
            primary_key=metadata['primary_key'],
            primary_index_type=metadata.get('primary_index_type', 'btree'),
            data_dir=data_dir
        )
    
    def to_dict(self) -> dict:
        return {
            'name': self.name,
            'columns': [col.to_dict() for col in self.columns],
            'primary_key': self.primary_key,
            'primary_index_type': self.primary_index_type,
            'record_size': self.get_record_size(),
            'indexed_columns': [col.name for col in self.get_indexed_columns()]
        }
    
    def __repr__(self) -> str:
        cols_str = ", ".join(str(col) for col in self.columns)
        return f"Table({self.name}: {cols_str})"


class TableManager:    
    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self.tables: Dict[str, Table] = {}
        os.makedirs(data_dir, exist_ok=True)
        self.load_all_tables()
    
    def load_all_tables(self) -> None:
        if not os.path.exists(self.data_dir):
            return
        
        for item in os.listdir(self.data_dir):
            item_path = os.path.join(self.data_dir, item)
            if os.path.isdir(item_path):
                metadata_path = os.path.join(item_path, "metadata.json")
                if os.path.exists(metadata_path):
                    try:
                        table = Table.load_metadata(item, self.data_dir)
                        self.tables[table.name] = table
                        print(f"Tabla '{table.name}' cargada con exito")
                    except Exception as e:
                        print(f"Error cargando tabla '{item}': {str(e)}")
    
    def create_table(
        self,
        name: str,
        columns: List[Column],
        primary_key: str,
        primary_index_type: str = "btree"
    ) -> Table:
        if name in self.tables:
            raise ValueError(f"Tabla '{name}' ya existe")
        
        table = Table(name, columns, primary_key, primary_index_type, self.data_dir)
        table.save_metadata()
        self.tables[name] = table
        
        print(f"Tabla '{name}' creada con exito")
        return table
    
    def drop_table(self, name: str) -> bool:
        if name not in self.tables:
            raise ValueError(f"Tabla '{name}' no existe")
        
        table = self.tables[name]
        
        import shutil
        if os.path.exists(table.table_dir):
            shutil.rmtree(table.table_dir)
        
        del self.tables[name]
        
        print(f"Tabla '{name}' eliminada exitosamente")
        return True
    
    def get_table(self, name: str) -> Optional[Table]:
        return self.tables.get(name)
    
    def table_exists(self, name: str) -> bool:
        return name in self.tables
    
    def list_tables(self) -> List[str]:
        return list(self.tables.keys())
    
    def get_table_info(self, name: str) -> Optional[dict]:
        table = self.get_table(name)
        if table:
            return table.to_dict()
        return None
    
    def __repr__(self) -> str:
        return f"TableManager({len(self.tables)} tablas: {', '.join(self.tables.keys())})"
