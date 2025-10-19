import os
import time
import shutil
import os
from typing import Dict, List, Any, Optional
from .parser_sql import SQLParser
from .table_manager import TableManager
from .query_executor import QueryExecutor, QueryResult
from .query_optimizer import QueryOptimizer

class DatabaseEngine:    
    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self.parser = SQLParser()
        self.table_manager = TableManager(data_dir)
        self.query_executor = QueryExecutor(self.table_manager, data_dir)
        self.query_optimizer = QueryOptimizer()
        
        os.makedirs(data_dir, exist_ok=True)
        
        print(f"Database Engine inicializado en directorio: {data_dir}")
        print(f"Tablas cargadas: {len(self.table_manager.list_tables())}")
    
    def execute_query(self, sql: str) -> Dict[str, Any]:
        start_time = time.time()
        
        try:
            statement = self.parser.parse(sql)
            result = self.query_executor.execute(statement)
            total_time = time.time() - start_time
            
            return {
                'success': result.success,
                'data': result.data,
                'count': result.count,
                'time': total_time,
                'error': result.error,
                'explain': result.explain
            }
            
        except Exception as e:
            total_time = time.time() - start_time
            return {
                'success': False,
                'data': None,
                'count': 0,
                'time': total_time,
                'error': str(e),
                'explain': None
            }
    
    def explain_query(self, sql: str) -> Dict[str, Any]:
        try:
            statement = self.parser.parse(sql)
            
            # Solo soportamos EXPLAIN para SELECT por ahora
            if hasattr(statement, 'table_name'):
                table = self.table_manager.get_table(statement.table_name)
                if not table:
                    return {
                        'success': False,
                        'error': f"Tabla '{statement.table_name}' no existe",
                        'plan': None
                    }
                
                if hasattr(statement, 'where_condition'):
                    # Es un SELECT
                    plan = self.query_optimizer.optimize_select(statement, table)
                    explain_text = self.query_optimizer.get_explain_plan(statement, table)
                else:
                    # Es otro tipo de consulta
                    explain_text = f"Consulta de tipo {type(statement).__name__} - sin optimizacion "
                    plan = None
                
                return {
                    'success': True,
                    'error': None,
                    'plan': {
                        'operation': plan.operation if plan else 'unknown',
                        'cost': plan.estimated_cost if plan else 0,
                        'index_type': plan.index_type if plan else None,
                        'index_column': plan.index_column if plan else None,
                        'description': plan.description if plan else explain_text
                    },
                    'explain_text': explain_text
                }
            else:
                return {
                    'success': False,
                    'error': "EXPLAIN solo soportado para consultas SELECT",
                    'plan': None
                }
                
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'plan': None
            }
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        table = self.table_manager.get_table(table_name)
        if not table:
            return {
                'success': False,
                'error': f"Tabla '{table_name}' no existe"
            }
        
        try:
            from .record_handler import RecordFile
            record_file = RecordFile(table.data_file_path, table.columns)
            total_records = record_file.get_count()
            active_records = record_file.get_active_count()
            record_file.close()
        except:
            total_records = 0
            active_records = 0
        
        return {
            'success': True,
            'table_info': {
                'name': table.name,
                'columns': [col.to_dict() for col in table.columns],
                'primary_key': table.primary_key,
                'primary_index_type': table.primary_index_type,
                'record_size': table.get_record_size(),
                'total_records': total_records,
                'active_records': active_records,
                'indexed_columns': [col.name for col in table.get_indexed_columns()]
            }
        }
    
    def list_tables(self) -> Dict[str, Any]:
        tables = self.table_manager.list_tables()
        table_infos = []
        
        for table_name in tables:
            info = self.get_table_info(table_name)
            if info['success']:
                table_infos.append(info['table_info'])
        
        return {
            'success': True,
            'tables': table_infos,
            'count': len(table_infos)
        }
    
    def drop_table(self, table_name: str) -> Dict[str, Any]:
        try:
            success = self.table_manager.drop_table(table_name)
            return {
                'success': success,
                'message': f"Tabla '{table_name}' eliminada con exito" if success else f"Error eliminando tabla '{table_name}'"
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_system_stats(self) -> Dict[str, Any]:
        tables = self.table_manager.list_tables()
        total_tables = len(tables)
        total_records = 0
        total_size = 0
        
        for table_name in tables:
            try:
                info = self.get_table_info(table_name)
                if info['success']:
                    total_records += info['table_info']['active_records']
                    # Estimación de tamanio (registros x tamnio por registro)
                    total_size += info['table_info']['active_records'] * info['table_info']['record_size']
            except:
                pass
        
        return {
            'success': True,
            'stats': {
                'total_tables': total_tables,
                'total_records': total_records,
                'estimated_size_bytes': total_size,
                'estimated_size_mb': round(total_size / (1024 * 1024), 2),
                'data_directory': self.data_dir
            }
        }
    
    def validate_sql(self, sql: str) -> Dict[str, Any]:
        try:
            statement = self.parser.parse(sql)
            return {
                'success': True,
                'valid': True,
                'statement_type': type(statement).__name__,
                'message': "Sintaxis SQL valida"
            }
        except Exception as e:
            return {
                'success': True,
                'valid': False,
                'error': str(e),
                'message': "Sintaxis SQL invalida"
            }
    
    def close(self):
        print("Cerrando Database Engine")


if __name__ == "__main__":
    print("Pruebas:\n")
    engine = DatabaseEngine(data_dir="test_data")
    
    try:
        print("CREATE TABLE prueba ")
        create_sql = """
        CREATE TABLE Restaurantes (
            id INT KEY INDEX HASH,
            nombre VARCHAR[20] INDEX BTREE,
            fechaRegistro DATE,
            ubicacion ARRAY INDEX RTREE
        )
        """
        
        result = engine.execute_query(create_sql)
        print(f"Resultado: {result['success']}")
        print(f"Explicacion: {result['explain']}")
        print(f"Tiempo: {result['time']:.4f}s")
        
        # Prueba INSERT
        print("\nINSERT")
        insert_sql = 'INSERT INTO Restaurantes VALUES (1, "Restaurant A", "2024-01-01", ARRAY[-12.06, -77.03])'
        
        result = engine.execute_query(insert_sql)
        print(f"Resultado: {result['success']}")
        print(f"Registros insertados: {result['count']}")
        
        print("\nSELECT ")
        select_sql = 'SELECT * FROM Restaurantes WHERE id = 1'
        
        result = engine.execute_query(select_sql)
        print(f"Resultado: {result['success']}")
        print(f"Registros encontrados: {result['count']}")
        print(f"Datos: {result['data']}")
        
        print("\nEXPLAIN ")
        explain_result = engine.explain_query(select_sql)
        print(f"Plan: {explain_result['plan']}")
        
        print("\nLIST TABLES ")
        tables_result = engine.list_tables()
        print(f"Tablas: {tables_result['tables']}")
        
        print("\nTABLE INFO")
        info_result = engine.get_table_info("Restaurantes")
        print(f"Info: {info_result['table_info']}")
        
        print("\nSYSTEM STATS")
        stats_result = engine.get_system_stats()
        print(f"Stats: {stats_result['stats']}")
        
        
    except Exception as e:
        print(f"Error en pruebas: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        engine.close()
        if os.path.exists("test_data"):
            shutil.rmtree("test_data")
