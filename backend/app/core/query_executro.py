import os
import time
from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass
import shutil
from .parser_sql import CreateTableStatement, InsertStatement, DeleteStatement, SelectStatement, SQLParser
from .table_manager import TableManager, Table
from .record_handler import RecordFile
from .index_manager import IndexManager
from .csv_loader import CSVLoader
from .data_types import Column, DataType

@dataclass
class QueryResult:
    success: bool
    data: Optional[List[Dict[str, Any]]] = None
    count: int = 0
    time: float = 0.0
    error: Optional[str] = None
    explain: Optional[str] = None

class QueryExecutor:    
    def __init__(self, table_manager: TableManager, data_dir: str = "data"):
        self.table_manager = table_manager
        self.data_dir = data_dir
        self.csv_loader = CSVLoader()
    
    def execute(self, statement: Union[CreateTableStatement, InsertStatement, DeleteStatement, SelectStatement]) -> QueryResult:
        start_time = time.time()
        
        try:
            if isinstance(statement, CreateTableStatement):
                return self._execute_create_table(statement, start_time)
            elif isinstance(statement, InsertStatement):
                return self._execute_insert(statement, start_time)
            elif isinstance(statement, DeleteStatement):
                return self._execute_delete(statement, start_time)
            elif isinstance(statement, SelectStatement):
                return self._execute_select(statement, start_time)
            else:
                return QueryResult(
                    success=False,
                    error=f"Tipo de statement no soportado - {type(statement)}",
                    time=time.time() - start_time
                )
        except Exception as e:
            return QueryResult(
                success=False,
                error=str(e),
                time=time.time() - start_time
            )
    
    def _execute_create_table(self, statement: CreateTableStatement, start_time: float) -> QueryResult:
        try:
            # Verificar si tabla ya existe
            if self.table_manager.table_exists(statement.table_name):
                return QueryResult(
                    success=False,
                    error=f"Tabla '{statement.table_name}' ya existe",
                    time=time.time() - start_time
                )
            
            table = self.table_manager.create_table(
                name=statement.table_name,
                columns=statement.columns,
                primary_key=statement.primary_key,
                primary_index_type=statement.primary_index_type
            )
            
            if statement.from_file:
                return self._load_from_csv(statement, table, start_time)
            
            # Tabla vacia creada con exito
            return QueryResult(
                success=True,
                data=[],
                count=0,
                time=time.time() - start_time,
                explain=f"Tabla '{statement.table_name}' creada con exito con {len(statement.columns)} columnas"
            )
            
        except Exception as e:
            return QueryResult(
                success=False,
                error=f"Error creando tabla - {str(e)}",
                time=time.time() - start_time
            )
    
    def _load_from_csv(self, statement: CreateTableStatement, table: Table, start_time: float) -> QueryResult:
        try:
            record_file = RecordFile(table.data_file_path, table.columns)
            index_manager = IndexManager(table.name, table.columns, table.table_dir)
            
            load_result = self.csv_loader.load_from_csv(
                csv_path=statement.from_file,
                create_statement=statement,
                table_manager=self.table_manager,
                index_manager=index_manager,
                record_handler=record_file
            )
            
            return QueryResult(
                success=True,
                data=[],
                count=load_result['inserted_count'],
                time=time.time() - start_time,
                explain=f"Tabla '{statement.table_name}' creada y cargada con {load_result['inserted_count']} registros desde CSV"
            )
            
        except Exception as e:
            return QueryResult(
                success=False,
                error=f"Error cargando csv: {str(e)}",
                time=time.time() - start_time
            )
    
    def _execute_insert(self, statement: InsertStatement, start_time: float) -> QueryResult:
        try:
            table = self.table_manager.get_table(statement.table_name)
            if not table:
                return QueryResult(
                    success=False,
                    error=f"Tabla '{statement.table_name}' no existe",
                    time=time.time() - start_time
                )
            
            #record handler e index manager
            record_file = RecordFile(table.data_file_path, table.columns)
            index_manager = IndexManager(table.name, table.columns, table.table_dir)
            
            inserted_count = 0
            errors = []
            
            # Insertar valores
            for values in statement.values:
                try:
                    record = self._create_record_from_values(values, table.columns, statement.columns)
                    record_position = record_file.insert(record)
                    index_manager.insert(record, record_position)
                    inserted_count += 1
                    
                except Exception as e:
                    errors.append(f"Error insertando valores {values}: {str(e)}")
            
            index_manager.save_all()
            record_file.close()
            
            return QueryResult(
                success=True,
                data=[],
                count=inserted_count,
                time=time.time() - start_time,
                explain=f"Registros insertados {inserted_count} en '{statement.table_name}'"
            )
            
        except Exception as e:
            return QueryResult(
                success=False,
                error=f"Error en INSERT: {str(e)}",
                time=time.time() - start_time
            )
    
    def _execute_delete(self, statement: DeleteStatement, start_time: float) -> QueryResult:
        try:
            table = self.table_manager.get_table(statement.table_name)
            if not table:
                return QueryResult(
                    success=False,
                    error=f"Tabla '{statement.table_name}' no existe",
                    time=time.time() - start_time
                )
            
            record_file = RecordFile(table.data_file_path, table.columns)
            index_manager = IndexManager(table.name, table.columns, table.table_dir)
            
            deleted_count = 0
            
            if statement.where_condition:
                deleted_count = self._delete_with_condition(
                    statement.where_condition, table, record_file, index_manager
                )
            else:
                deleted_count = self._delete_all_records(table, record_file, index_manager)
            
            index_manager.save_all()
            record_file.close()
            
            return QueryResult(
                success=True,
                data=[],
                count=deleted_count,
                time=time.time() - start_time,
                explain=f" {deleted_count} registros eliminados de '{statement.table_name}'"
            )
            
        except Exception as e:
            return QueryResult(
                success=False,
                error=f"Error en DELETE: {str(e)}",
                time=time.time() - start_time
            )
    
    def _execute_select(self, statement: SelectStatement, start_time: float) -> QueryResult:
        try:
            table = self.table_manager.get_table(statement.table_name)
            if not table:
                return QueryResult(
                    success=False,
                    error=f"Tabla '{statement.table_name}' no existe",
                    time=time.time() - start_time
                )
            
            record_file = RecordFile(table.data_file_path, table.columns)
            index_manager = IndexManager(table.name, table.columns, table.table_dir)
            
            if statement.where_condition:
                results = self._select_with_condition(
                    statement.where_condition, statement.columns, table, record_file, index_manager
                )
            else:
                results = self._select_all(statement.columns, table, record_file)
            
            return QueryResult(
                success=True,
                data=results,
                count=len(results),
                time=time.time() - start_time,
                explain=f"Consulta ejecutada en '{statement.table_name}', {len(results)} registros encontrados"
            )
            
        except Exception as e:
            return QueryResult(
                success=False,
                error=f"Error en SELECT: {str(e)}",
                time=time.time() - start_time
            )
    
    def _create_record_from_values(self, values: List[Any], columns: List, specified_columns: Optional[List[str]]) -> Dict[str, Any]:
        record = {}
        
        if specified_columns:
            # INSERT INTO tabla (col1, col2) VALUES (val1, val2)
            if len(values) != len(specified_columns):
                raise ValueError(f"Numero de valores ({len(values)}) no coincide con columnas ({len(specified_columns)})")
            
            for i, col_name in enumerate(specified_columns):
                column = next((c for c in columns if c.name == col_name), None)
                if not column:
                    raise ValueError(f"Columna '{col_name}' no existe en la tabla")
                
                record[col_name] = values[i]
        else:
            # INSERT INTO tabla VALUES (val1, val2, val3)
            if len(values) != len(columns):
                raise ValueError(f"Numero de valores ({len(values)}) no coincide con columnas de tabla ({len(columns)})")
            
            for i, column in enumerate(columns):
                record[column.name] = values[i]
        
        return record
    
    def _delete_with_condition(self, condition: Dict[str, Any], table: Table, record_file: RecordFile, index_manager: IndexManager) -> int:
        deleted_count = 0
        
        if condition['type'] == 'equal':
            column_name = condition['column']
            value = condition['value']
            
            if index_manager.has_index(column_name):
                position = index_manager.search(column_name, value)
                if position is not None:
                    record_file.delete(position)
                    deleted_count = 1
            else:
                # sequential scan
                all_records = record_file.scan_all()
                for i, record in enumerate(all_records):
                    if record.get(column_name) == value:
                        record_file.delete(i)
                        deleted_count += 1
        
        elif condition['type'] == 'between':
            # WHERE col BETWEEN min AND max
            column_name = condition['column']
            min_value = condition['min_value']
            max_value = condition['max_value']
            
            if index_manager.has_index(column_name):
                positions = index_manager.range_search(column_name, min_value, max_value)
                for position in positions:
                    record_file.delete(position)
                    deleted_count += 1
            else:
                all_records = record_file.scan_all()
                for i, record in enumerate(all_records):
                    value = record.get(column_name)
                    if min_value <= value <= max_value:
                        record_file.delete(i)
                        deleted_count += 1
        
        return deleted_count
    
    def _delete_all_records(self, table: Table, record_file: RecordFile, index_manager: IndexManager) -> int:
        count = record_file.get_count()
        
        # marcar como eliminados
        for i in range(count):
            record_file.delete(i)
        
        return count
    
    def _select_with_condition(self, condition: Dict[str, Any], columns: List[str], table: Table, record_file: RecordFile, index_manager: IndexManager) -> List[Dict[str, Any]]:
        results = []
        
        if condition['type'] == 'equal':
            column_name = condition['column']
            value = condition['value']
            
            if index_manager.has_index(column_name):
                position = index_manager.search(column_name, value)
                if position is not None:
                    record = record_file.read(position)
                    if record:
                        results.append(self._project_columns(record, columns, table.columns))
            else:
                all_records = record_file.scan_all()
                for record in all_records:
                    if record.get(column_name) == value:
                        results.append(self._project_columns(record, columns, table.columns))
        
        elif condition['type'] == 'between':
            # WHERE col BETWEEN min AND max
            column_name = condition['column']
            min_value = condition['min_value']
            max_value = condition['max_value']
            
            if index_manager.has_index(column_name):
                positions = index_manager.range_search(column_name, min_value, max_value)
                for position in positions:
                    record = record_file.read(position)
                    if record:
                        results.append(self._project_columns(record, columns, table.columns))
            else:
                all_records = record_file.scan_all()
                for record in all_records:
                    value = record.get(column_name)
                    if min_value <= value <= max_value:
                        results.append(self._project_columns(record, columns, table.columns))
        
        elif condition['type'] == 'spatial':
            # WHERE col IN (point, radius)
            column_name = condition['column']
            point = condition['point']
            radius = condition['radius']
            
            if index_manager.has_index(column_name):
                positions = index_manager.spatial_search(column_name, point, radius)
                for position in positions:
                    record = record_file.read(position)
                    if record:
                        results.append(self._project_columns(record, columns, table.columns))
            else:
                all_records = record_file.scan_all()
                for record in all_records:
                    location = record.get(column_name)
                    if location and self._point_in_radius(location, point, radius):
                        results.append(self._project_columns(record, columns, table.columns))
        
        return results
    
    def _select_all(self, columns: List[str], table: Table, record_file: RecordFile) -> List[Dict[str, Any]]:
        #SELECT * FROM tabla
        all_records = record_file.scan_all()
        results = []
        
        for record in all_records:
            results.append(self._project_columns(record, columns, table.columns))
        
        return results
    
    def _project_columns(self, record: Dict[str, Any], requested_columns: List[str], table_columns: List) -> Dict[str, Any]:
        if '*' in requested_columns:
            return record
        
        projected = {}
        for col_name in requested_columns:
            if col_name in record:
                projected[col_name] = record[col_name]
        
        return projected
    
    def _point_in_radius(self, point1: tuple, point2: tuple, radius: float) -> bool:
        import math
        distance = math.sqrt((point1[0] - point2[0])**2 + (point1[1] - point2[1])**2)
        return distance <= radius


# Test
if __name__ == "__main__":
    print("Pruebas - Query executor:\n")

    table_manager = TableManager(data_dir="test_data")
    executor = QueryExecutor(table_manager, data_dir="test_data")
    parser = SQLParser()
    
    try:
        print("CREATE TABLE")
        create_sql = """
        CREATE TABLE Restaurantes (
            id INT KEY INDEX HASH,
            nombre VARCHAR[20] INDEX BTREE,
            fechaRegistro DATE,
            ubicacion ARRAY INDEX RTREE
        )
        """
        
        create_stmt = parser.parse(create_sql)
        result = executor.execute(create_stmt)
        print(f"Resultado: {result.success}")
        print(f"Explicacion: {result.explain}")
        print(f"Tiempo: {result.time:.4f}s")
        
        print("\nINSERT")
        insert_sql = 'INSERT INTO Restaurantes VALUES (1, "Restaurant A", "2024-01-01", ARRAY[-12.06, -77.03])'
        
        insert_stmt = parser.parse(insert_sql)
        result = executor.execute(insert_stmt)
        print(f"Resultado: {result.success}")
        print(f"Registros insertados: {result.count}")
        print(f"Explicacion: {result.explain}")
        
        print("\nSELECT")
        select_sql = 'SELECT * FROM Restaurantes WHERE id = 1'
        
        select_stmt = parser.parse(select_sql)
        result = executor.execute(select_stmt)
        print(f"Resultado: {result.success}")
        print(f"Registros encontrados: {result.count}")
        print(f"Datos: {result.data}")
        
        
    except Exception as e:
        print(f"Error en pruebas: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        if os.path.exists("test_data"):
            shutil.rmtree("test_data")