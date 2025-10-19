import os
from typing import Dict, List, Any, Optional, Tuple
from .data_types import Column, DataType, get_key_type_for_index
from ..data_structures.avl_file import AVLFile, KeyType as AVLKeyType
from ..data_structures.bplus_tree import BPlusClustered, BPlusUnclustered
from ..data_structures.extendible_hashing import ExtendibleHashing, KeyType as HashKeyType
from ..data_structures.isam import ISAM2Index, KeyType as ISAMKeyType
from ..data_structures.spatial_rtree import RTreeIndex
from ..data_structures.sequential import SequentialFile

class IndexManager:    
    def __init__(self, table_name: str, columns: List[Column], table_dir: str):
        self.table_name = table_name
        self.columns = columns
        self.table_dir = table_dir
        self.indices: Dict[str, Any] = {} 
        
        #directorio de indices
        self.indices_dir = os.path.join(table_dir, "indices")
        os.makedirs(self.indices_dir, exist_ok=True)
        
        self._load_indices()
    
    def _get_key_type(self, column: Column) -> str:
        return get_key_type_for_index(column.data_type)
    
    def _get_index_filename(self, column_name: str, index_type: str) -> str:
        return os.path.join(self.indices_dir, f"{column_name}_{index_type}")
    
    def _load_indices(self) -> None:
        for col in self.columns:
            if col.has_index and col.index_type:
                try:
                    self._load_index(col)
                except Exception as e:
                    print(f"Error cargando indice en la columna '{col.name}' - {str(e)}")
    
    def _load_index(self, column: Column) -> None:
        index_type = column.index_type.lower()
        
        if index_type == "avl":
            self._create_avl_index(column)
        elif index_type == "btree":
            self._create_btree_index(column)
        elif index_type == "hash":
            self._create_hash_index(column)
        elif index_type == "isam":
            self._create_isam_index(column)
        elif index_type == "rtree":
            self._create_rtree_index(column)
        elif index_type == "seq":
            pass
        else:
            raise ValueError(f"Tipo de indice no soportado - {index_type}")
    
    def _create_avl_index(self, column: Column) -> None:
        filename = self._get_index_filename(column.name, "avl") + ".dat"
        key_type_str = self._get_key_type(column)
        
        # string a AVLKeyType
        key_type_map = {
            "int": AVLKeyType.INT,
            "float": AVLKeyType.FLOAT,
            "string": AVLKeyType.STRING
        }
        key_type = key_type_map.get(key_type_str, AVLKeyType.INT)
        
        max_length = column.size if column.data_type == DataType.VARCHAR else 50
        self.indices[column.name] = AVLFile(filename, key_type, max_length)
    
    def _create_btree_index(self, column: Column) -> None:
        filename = self._get_index_filename(column.name, "btree") + ".dat"
        
        if column.is_primary_key:
            self.indices[column.name] = BPlusClustered(order=8, filename=filename)
        else:
            self.indices[column.name] = BPlusUnclustered(order=8, filename=filename)
    
    def _create_hash_index(self, column: Column) -> None:
        dir_file = self._get_index_filename(column.name, "hash") + "_dir.bin"
        data_file = self._get_index_filename(column.name, "hash") + "_data.bin"
        key_type_str = self._get_key_type(column)
        
        #string a HashKeyType
        key_type_map = {
            "int": HashKeyType.INT,
            "float": HashKeyType.FLOAT,
            "string": HashKeyType.STRING
        }
        key_type = key_type_map.get(key_type_str, HashKeyType.INT)
        
        max_length = column.size if column.data_type == DataType.VARCHAR else 50
        self.indices[column.name] = ExtendibleHashing(dir_file, data_file, key_type, max_length)
    
    def _create_isam_index(self, column: Column) -> None:
        base_name = self._get_index_filename(column.name, "isam")
        key_type_str = self._get_key_type(column)
        
        #string a ISAMKeyType
        key_type_map = {
            "int": ISAMKeyType.INT,
            "float": ISAMKeyType.FLOAT,
            "string": ISAMKeyType.STRING
        }
        key_type = key_type_map.get(key_type_str, ISAMKeyType.INT)
        
        max_length = column.size if column.data_type == DataType.VARCHAR else 50
        self.indices[column.name] = ISAM2Index(base_name, key_type, max_length)
    
    def _create_rtree_index(self, column: Column) -> None:
        if column.data_type != DataType.ARRAY_FLOAT:
            raise ValueError("RTree solo soporta columnas con tipo ARRAY_FLOAT")
        
        filename = self._get_index_filename(column.name, "rtree") + ".dat"
        self.indices[column.name] = RTreeIndex(filename)
    
    def add_index(self, column_name: str, index_type: str) -> bool:
        column = None
        for col in self.columns:
            if col.name == column_name:
                column = col
                break
        
        if not column:
            raise ValueError(f"Columna '{column_name}' no encontrada")
        
        if column.has_index:
            raise ValueError(f"Columna '{column_name}' ya tiene indice")
        
        column.has_index = True
        column.index_type = index_type.lower()
        
        self._load_index(column)
        
        return True
    
    def remove_index(self, column_name: str) -> bool:
        if column_name not in self.indices:
            return False
        
        # eliminar archivos de indice
        # implementar limpieza de archivos dependiendo del indice / alejandro
        
        del self.indices[column_name]
        
        for col in self.columns:
            if col.name == column_name:
                col.has_index = False
                col.index_type = None
                break
        
        return True
    
    def get_index(self, column_name: str) -> Optional[Any]:
        return self.indices.get(column_name)
    
    def has_index(self, column_name: str) -> bool:
        return column_name in self.indices
    
    def insert(self, record: Dict[str, Any], record_position: int) -> None:
        for col in self.columns:
            if col.has_index and col.name in self.indices:
                key = record.get(col.name)
                if key is None:
                    continue
                
                index = self.indices[col.name]
                index_type = col.index_type.lower()
                
                try:
                    if index_type == "avl":
                        index.insert(key, record_position)
                    elif index_type == "btree":
                        if col.is_primary_key:
                            #BPlusClustered
                            index.add(key, record)
                        else:
                            #BPlusUnclustered
                            index.add(key, record)
                    elif index_type == "hash":
                        index.insert(key, record_position)
                    elif index_type == "isam":
                        index.insert(key, record_position)
                    elif index_type == "rtree":
                        if isinstance(key, (tuple, list)) and len(key) == 2:
                            index.add(record_position, key, record_position)
                except Exception as e:
                    print(f"Error insertando en indice '{col.name}' - {str(e)}")
    
    def search(self, column_name: str, key: Any) -> Optional[int]:
        if column_name not in self.indices:
            return None
        
        index = self.indices[column_name]
        column = next((col for col in self.columns if col.name == column_name), None)
        
        if not column:
            return None
        
        index_type = column.index_type.lower()
        
        try:
            if index_type == "avl":
                return index.find(key)
            elif index_type == "btree":
                results = index.search(key)
                if results:
                    if isinstance(results[0], dict):
                        # necesitamos obtener la posicion del registro
                        #por ahora estamos retornando el primer resultado
                        return 0  # TODO: mejorar esto / alejandro
                    else:
                        return results[0]
                return None
            elif index_type == "hash":
                return index.find(key)
            elif index_type == "isam":
                return index.search(key)
            elif index_type == "rtree":
                return None
        except Exception as e:
            print(f"Error buscando en indice '{column_name}': {str(e)}")
            return None
    
    def range_search(self, column_name: str, min_key: Any, max_key: Any) -> List[int]:
        if column_name not in self.indices:
            return []
        
        index = self.indices[column_name]
        column = next((col for col in self.columns if col.name == column_name), None)
        
        if not column:
            return []
        
        index_type = column.index_type.lower()
        
        try:
            if index_type == "avl":
                return index.range_search(min_key, max_key)
            elif index_type == "btree":
                results = index.range_search(min_key, max_key)
                positions = []
                for result in results:
                    if isinstance(result, tuple):
                        positions.append(result[1])  
                    elif isinstance(result, dict):
                        # TODO: Obtener posicion del diccionario /alejandro
                        pass
                return positions
            elif index_type == "isam":
                return index.range_search(min_key, max_key)
            elif index_type == "hash":
                return []
            elif index_type == "rtree":
                return []
        except Exception as e:
            print(f"Error en range search de indice '{column_name}': {str(e)}")
            return []
    
    def spatial_search(self, column_name: str, point: Tuple[float, float], radius: float) -> List[int]:
        if column_name not in self.indices:
            return []
        
        index = self.indices[column_name]
        column = next((col for col in self.columns if col.name == column_name), None)
        
        if not column or column.index_type.lower() != "rtree":
            return []
        
        try:
            return index.rangeSearch(point, radius)
        except Exception as e:
            print(f"Error en busqueda espacial de indice '{column_name}': {str(e)}")
            return []
    
    def delete(self, record: Dict[str, Any]) -> None:
        for col in self.columns:
            if col.has_index and col.name in self.indices:
                key = record.get(col.name)
                if key is None:
                    continue
                
                index = self.indices[col.name]
                index_type = col.index_type.lower()
                
                try:
                    if index_type == "avl":
                        index.remove(key)
                    elif index_type == "hash":
                        index.delete(key)
                    elif index_type == "isam":
                        index.remove(key)
                except Exception as e:
                    print(f"Error eliminando de indice '{col.name}': {str(e)}")
    
    def save_all(self) -> None:
        for col_name, index in self.indices.items():
            try:
                if hasattr(index, 'save'):
                    index.save()
            except Exception as e:
                print(f"Error guardando indice '{col_name}': {str(e)}")
    
    def __repr__(self) -> str:
        return f"IndexManager({self.table_name}, {len(self.indices)} indices: {list(self.indices.keys())})"


if __name__ == "__main__":
    # Pruebas
    print("Pruebas del IndexManager:\n")
    from data_types import Column, DataType
    import tempfile
    import shutil
    
    temp_dir = tempfile.mkdtemp()
    
    try:
        columns = [
            Column("id", DataType.INT, is_primary_key=True, has_index=True, index_type="hash"),
            Column("nombre", DataType.VARCHAR, size=30, has_index=True, index_type="btree"),
            Column("ubicacion", DataType.ARRAY_FLOAT, has_index=True, index_type="rtree")
        ]
        
        manager = IndexManager("test_table", columns, temp_dir)
        print(f"IndexManager creado: {manager}\n")
        
        print("Insertando registros")
        records = [
            {"id": 1, "nombre": "Restaurant A", "ubicacion": (-12.06, -77.03)},
            {"id": 2, "nombre": "Restaurant B", "ubicacion": (-12.08, -77.04)},
            {"id": 3, "nombre": "Restaurant C", "ubicacion": (-12.05, -77.06)}
        ]
        
        for i, record in enumerate(records):
            manager.insert(record, i)
            print(f"  Insertado: {record}")
        
       
        print("\nBusqueda por ID= 2")
        pos = manager.search("id", 2)
        print(f"  Posicion encontrada: {pos}")
        
        # Buscar por nombre, btree
        print("\nBusqueda por nombre = 'Restaurant B':")
        pos = manager.search("nombre", "Restaurant B")
        print(f"  Posicion encontrada: {pos}")
        
        # Busqueda espacial, rtree
        print("\nBusqueda espacial cerca de (-12.07, -77.05) con radio 0.03:")
        positions = manager.spatial_search("ubicacion", (-12.07, -77.05), 0.03)
        print(f"  Posiciones encontradas: {positions}")
        
        
    finally:
        shutil.rmtree(temp_dir)

