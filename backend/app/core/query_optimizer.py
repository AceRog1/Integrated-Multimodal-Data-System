from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from .parser_sql import SelectStatement, DeleteStatement
from .table_manager import Table
from .data_types import DataType

@dataclass
class ExecutionPlan:
    operation: str
    index_type: Optional[str] = None
    index_column: Optional[str] = None
    estimated_cost: int = 0
    description: str = ""

class QueryOptimizer:
    def __init__(self):
        # Costos estimados para diferentes operaciones
        self.costs = {
            'sequential_scan': 1000, # Alto costo
            'hash_lookup': 1, # Muy bajo costo
            'btree_lookup': 3, # Bajo costo
            'btree_range': 10, # Costo medio
            'avl_lookup': 3, # Similar a B+Tree
            'avl_range': 10, # Similar a B+Tree
            'isam_lookup': 5, # Costo medio
            'isam_range': 15, # Costo medio-alto
            'rtree_spatial': 20, # Costo medio para espacial
            'sequential_filter': 500 # Costo alto pero mejor que scan completo
        }
    
    def optimize_select(self, statement:SelectStatement, table:Table)->ExecutionPlan:
        if not statement.where_condition:
            # SELECT * FROM tabla (sin WHERE)
            return ExecutionPlan(
                operation='sequential_scan',
                estimated_cost=self.costs['sequential_scan'],
                description=f"Sequential scan de tabla '{table.name}' ({table.get_record_size()} bytes/registro)"
            )
        
        condition = statement.where_condition
        condition_type = condition['type']
        column_name = condition['column']
        
        # Buscar columna en la tabla
        column = table.get_column(column_name)
        if not column:
            # por defecto sequential scan
            return ExecutionPlan(
                operation='sequential_scan',
                estimated_cost=self.costs['sequential_scan'],
                description=f"Sequential scan: columna '{column_name}' no encontrada"
            )
        
        # Seleccionar mejor indice segun tipo de condicion
        if condition_type == 'equal':
            return self._optimize_equal_condition(column, condition, table)
        elif condition_type == 'between':
            return self._optimize_range_condition(column, condition, table)
        elif condition_type == 'spatial':
            return self._optimize_spatial_condition(column, condition, table)
        else:
            # Condicion no soportada, usar sequential scan
            return ExecutionPlan(
                operation='sequential_scan',
                estimated_cost=self.costs['sequential_scan'],
                description=f"Sequential scan: condición '{condition_type}' no soportada"
            )
    
    def optimize_delete(self, statement:DeleteStatement, table:Table)->ExecutionPlan:
        if not statement.where_condition:
            # DELETE FROM tabla (sin WHERE) - eliminar todo
            return ExecutionPlan(
                operation='sequential_scan',
                estimated_cost=self.costs['sequential_scan'],
                description=f"Eliminar todos los registros de '{table.name}'"
            )
        
        # DELETE con WHERE - misma logica que SELECT
        condition = statement.where_condition
        condition_type = condition['type']
        column_name = condition['column']
        
        column = table.get_column(column_name)
        if not column:
            return ExecutionPlan(
                operation='sequential_scan',
                estimated_cost=self.costs['sequential_scan'],
                description=f"Sequential scan: columna '{column_name}' no encontrada"
            )
        
        if condition_type == 'equal':
            return self._optimize_equal_condition(column, condition, table)
        elif condition_type == 'between':
            return self._optimize_range_condition(column, condition, table)
        else:
            return ExecutionPlan(
                operation='sequential_scan',
                estimated_cost=self.costs['sequential_scan'],
                description=f"Sequential scan: condición '{condition_type}' no soportada"
            )
    
    def _optimize_equal_condition(self, column, condition:Dict[str, Any], table:Table)->ExecutionPlan:
        if not column.has_index:
            # Por defecto sequential scan
            return ExecutionPlan(
                operation='sequential_scan',
                estimated_cost=self.costs['sequential_filter'],
                description=f"Sequential scan con filtro en '{column.name}' (sin índice)"
            )
        
        index_type = column.index_type.lower()
        
        # Mejor indice para igualdad
        if index_type == 'hash':
            return ExecutionPlan(
                operation='index_scan',
                index_type='hash',
                index_column=column.name,
                estimated_cost=self.costs['hash_lookup'],
                description=f"Hash lookup en '{column.name}' para col = {condition['value']}"
            )
        elif index_type in ['btree', 'avl']:
            return ExecutionPlan(
                operation='index_scan',
                index_type=index_type,
                index_column=column.name,
                estimated_cost=self.costs['btree_lookup'],
                description=f"{index_type.upper()} lookup en '{column.name}' para col = {condition['value']}"
            )
        elif index_type == 'isam':
            return ExecutionPlan(
                operation='index_scan',
                index_type='isam',
                index_column=column.name,
                estimated_cost=self.costs['isam_lookup'],
                description=f"ISAM lookup en '{column.name}' para col = {condition['value']}"
            )
        else:
            return ExecutionPlan(
                operation='sequential_scan',
                estimated_cost=self.costs['sequential_filter'],
                description=f"Sequential scan: índice '{index_type}' no soportado para igualdad"
            )
    
    def _optimize_range_condition(self, column, condition:Dict[str, Any], table:Table)->ExecutionPlan:
        if not column.has_index:
            # Por defecto sequential scan
            return ExecutionPlan(
                operation='sequential_scan',
                estimated_cost=self.costs['sequential_filter'],
                description=f"Sequential scan con filtro en '{column.name}' (sin índice)"
            )
        
        index_type = column.index_type.lower()
        
        # Seleccionar mejor indice para rango
        if index_type in ['btree', 'avl']:
            return ExecutionPlan(
                operation='range_scan',
                index_type=index_type,
                index_column=column.name,
                estimated_cost=self.costs['btree_range'],
                description=f"{index_type.upper()} range scan en '{column.name}' BETWEEN {condition['min_value']} AND {condition['max_value']}"
            )
        elif index_type == 'isam':
            return ExecutionPlan(
                operation='range_scan',
                index_type='isam',
                index_column=column.name,
                estimated_cost=self.costs['isam_range'],
                description=f"ISAM range scan en '{column.name}' BETWEEN {condition['min_value']} AND {condition['max_value']}"
            )
        else:
            return ExecutionPlan(
                operation='sequential_scan',
                estimated_cost=self.costs['sequential_filter'],
                description=f"Sequential scan: índice '{index_type}' no soportado para rango"
            )
    
    def _optimize_spatial_condition(self, column, condition:Dict[str, Any], table:Table)->ExecutionPlan:
        if not column.has_index or column.index_type.lower() != 'rtree':
            # Por defecto sequential scan
            return ExecutionPlan(
                operation='sequential_scan',
                estimated_cost=self.costs['sequential_filter'],
                description=f"Sequential scan con filtro espacial en '{column.name}' (sin R-Tree)"
            )
        
        return ExecutionPlan(
            operation='spatial_scan',
            index_type='rtree',
            index_column=column.name,
            estimated_cost=self.costs['rtree_spatial'],
            description=f"R-Tree spatial scan en '{column.name}' cerca de {condition['point']} con radio {condition['radius']}"
        )
    
    def get_explain_plan(self, statement:SelectStatement, table:Table)->str:
        plan = self.optimize_select(statement, table)
        explain_lines = [
            f"EXPLAIN para consulta en tabla '{table.name}':",
            f"  Operacion: {plan.operation}",
            f"  Costo estimado: {plan.estimated_cost}",
            f"  Descripción: {plan.description}"
        ]
        
        if plan.index_type:
            explain_lines.append(f"  Indice usado: {plan.index_type}")
            explain_lines.append(f"  Columna indexada: {plan.index_column}")
        
        explain_lines.extend([
            f"  Tamaño de registro: {table.get_record_size()} bytes",
            f"  Columnas indexadas: {[col.name for col in table.get_indexed_columns()]}"
        ])
        
        return "\n".join(explain_lines)
    
    def compare_plans(self, plans:List[ExecutionPlan])->ExecutionPlan:
        if not plans:
            raise ValueError("No hay planes para comparar")
        
        # Seleccionar plan con menor costo
        best_plan = min(plans, key=lambda p: p.estimated_cost)
        
        return best_plan
    
    def estimate_selectivity(self, condition: Dict[str, Any], table: Table) -> float:
        if not condition:
            return 1.0  # Sin WHERE selecciona todo
        
        condition_type = condition['type']
        
        if condition_type == 'equal':
            # Igualdad: selectividad muy baja
            return 0.01 # 1% de los registros
        
        elif condition_type == 'between':
            # Rango: depende del tamaño del rango
            min_val = condition['min_value']
            max_val = condition['max_value']
            
            # Estimacion basada en el tipo de dato
            column = table.get_column(condition['column'])
            if column and column.data_type == DataType.INT:
                # Para enteros: estimacion basada en rango
                range_size = max_val - min_val
                if range_size < 100:
                    return 0.1 # 10%
                elif range_size < 1000:
                    return 0.3 # 30%
                else:
                    return 0.5 # 50%
            else:
                return 0.2 # 20% por defecto
        
        elif condition_type == 'spatial':
            # Espacial: depende del radio
            radius = condition['radius']
            if radius < 0.01:
                return 0.05 # 5%
            elif radius < 0.1:
                return 0.15 # 15%
            else:
                return 0.3 # 30%
        
        return 0.1 # por defecto

# test
if __name__ == "__main__":
    from table_manager import TableManager
    from data_types import Column, DataType
    from parser_sql import SQLParser

    table_manager = TableManager(data_dir="test_data")
    optimizer = QueryOptimizer()
    parser = SQLParser()
    
    try:
        columns = [
            Column("id", DataType.INT, is_primary_key=True, has_index=True, index_type="hash"),
            Column("nombre", DataType.VARCHAR, size=50, has_index=True, index_type="btree"),
            Column("precio", DataType.FLOAT, has_index=True, index_type="avl"),
            Column("ubicacion", DataType.ARRAY_FLOAT, has_index=True, index_type="rtree"),
            Column("fecha", DataType.DATE, has_index=True, index_type="isam"),
            Column("descripcion", DataType.VARCHAR, size=100) # Sin índice
        ]
        
        table = table_manager.create_table("productos", columns, "id", "hash")

        queries = [
            "SELECT * FROM productos WHERE id = 1",
            "SELECT * FROM productos WHERE nombre BETWEEN 'A' AND 'M'",
            "SELECT * FROM productos WHERE precio BETWEEN 10.0 AND 50.0",
            "SELECT * FROM productos WHERE ubicacion IN (ARRAY[-12.06, -77.03], 0.01)",
            "SELECT * FROM productos WHERE descripcion = 'test'",
            "SELECT * FROM productos"
        ]
        
        print("Planes de ejecucion\n")
        
        for sql in queries:
            print(f"Consulta: {sql}")
            try:
                statement = parser.parse(sql)
                plan = optimizer.optimize_select(statement, table)
                explain = optimizer.get_explain_plan(statement, table)
                
                print(f"  Plan: {plan.operation}")
                print(f"  Costo: {plan.estimated_cost}")
                print(f"  Índice: {plan.index_type or 'N/A'}")
                print(f"  Descripción: {plan.description}")
                print()
                
            except Exception as e:
                print(f"  Error: {e}")
                print()
        
        print("Pruebas listas")
        
    except Exception as e:
        print(f"Error en pruebas: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        import shutil
        import os
        if os.path.exists("test_data"):
            shutil.rmtree("test_data")
