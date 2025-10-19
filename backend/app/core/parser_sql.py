import re
from typing import List, Dict, Any, Optional, Union
from dataclasses import dataclass
from .data_types import Column, DataType

@dataclass
class CreateTableStatement:
    table_name: str
    columns: List[Column]
    primary_key: str
    primary_index_type: str
    from_file: Optional[str] = None
    using_index: Optional[str] = None

@dataclass
class InsertStatement:
    table_name: str
    columns: Optional[List[str]]
    values: List[List[Any]]

@dataclass
class DeleteStatement:
    table_name: str
    where_condition: Optional[Dict[str, Any]]

@dataclass
class SelectStatement:
    columns: List[str]  # * para todas las columnas
    table_name: str
    where_condition: Optional[Dict[str, Any]]


class SQLParser:
    
    def __init__(self):
        # patrones para tipos de consulta
        self.create_table_pattern = re.compile(
            r'CREATE\s+TABLE\s+(\w+)\s*\((.*?)\)(?:\s+FROM\s+FILE\s+"([^"]+)")?(?:\s+USING\s+INDEX\s+(\w+))?',
            re.IGNORECASE | re.DOTALL
        )
        self.insert_pattern = re.compile(
            r'INSERT\s+INTO\s+(\w+)(?:\s*\(([^)]+)\))?\s+VALUES\s*(\([^)]+\))',
            re.IGNORECASE
        )
        self.delete_pattern = re.compile(
            r'DELETE\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?',
            re.IGNORECASE
        )
        self.select_pattern = re.compile(
            r'SELECT\s+(.+?)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?',
            re.IGNORECASE
        )
    
    def parse(self, sql: str)->Union[CreateTableStatement, InsertStatement, DeleteStatement, SelectStatement]:
        sql = sql.strip()
        if sql.upper().startswith('CREATE TABLE'):
            return self._parse_create_table(sql)
        elif sql.upper().startswith('INSERT INTO'):
            return self._parse_insert(sql)
        elif sql.upper().startswith('DELETE FROM'):
            return self._parse_delete(sql)
        elif sql.upper().startswith('SELECT'):
            return self._parse_select(sql)
        else:
            raise ValueError(f"Tipo de consulta SQL no soportado: {sql[:20]}...")
    
    def _parse_create_table(self, sql: str)->CreateTableStatement:
        match = self.create_table_pattern.search(sql)
        if not match:
            raise ValueError("Sintaxis invalida para CREATE TABLE")
        table_name = match.group(1)
        columns_str = match.group(2)
        from_file = match.group(3)
        using_index = match.group(4)
        
        # Parsear columnas
        columns = self._parse_columns(columns_str)
        
        # Determinar primary key
        primary_key = None
        primary_index_type = "btree"  # Default
        for col in columns:
            if col.is_primary_key:
                primary_key = col.name
                primary_index_type = col.index_type or "btree"
                break
        if not primary_key:
            raise ValueError("Debe especificar una columna como PRIMARY KEY")
        
        return CreateTableStatement(
            table_name=table_name,
            columns=columns,
            primary_key=primary_key,
            primary_index_type=primary_index_type,
            from_file=from_file,
            using_index=using_index
        )
    
    def _parse_columns(self, columns_str: str)->List[Column]:
        columns = []
        # Dividir por comas, pero respetando parentesis
        col_defs = self._split_by_comma_respecting_parens(columns_str)
        for col_def in col_defs:
            col_def = col_def.strip()
            if not col_def:
                continue
            column = self._parse_column_definition(col_def)
            columns.append(column)
        return columns
    
    def _parse_column_definition(self, col_def: str)->Column:
        pattern = r'(\w+)\s+(\w+)(?:\[(\d+)\])?(?:\s+(KEY|PRIMARY\s+KEY))?(?:\s+INDEX\s+(\w+))?'
        match = re.search(pattern, col_def, re.IGNORECASE)
        if not match:
            raise ValueError(f"Definicion de columna invalida: {col_def}")
        name = match.group(1)
        type_str = match.group(2).upper()
        size = int(match.group(3)) if match.group(3) else None
        key_type = match.group(4)
        index_type = match.group(5)
        
        # Convertir string a DataType
        data_type = self._parse_data_type(type_str, size)
        
        # Determinar si es primary key
        is_primary_key = key_type and ('PRIMARY' in key_type.upper() or key_type.upper() == 'KEY')
        
        # Determinar si tiene indice
        has_index = bool(index_type) or is_primary_key
        
        return Column(
            name=name,
            data_type=data_type,
            size=size,
            is_primary_key=is_primary_key,
            has_index=has_index,
            index_type=index_type.lower() if index_type else None
        )
    
    def _parse_data_type(self, type_str: str, size: Optional[int])->DataType:
        type_str = type_str.upper()
        
        if type_str == 'INT':
            return DataType.INT
        elif type_str == 'FLOAT':
            return DataType.FLOAT
        elif type_str == 'VARCHAR':
            if not size:
                raise ValueError("VARCHAR requiere especificar tamaño [size]")
            return DataType.VARCHAR
        elif type_str == 'DATE':
            return DataType.DATE
        elif type_str == 'ARRAY':
            return DataType.ARRAY_FLOAT
        else:
            raise ValueError(f"Tipo de dato no soportado: {type_str}")
    
    def _parse_insert(self, sql: str)->InsertStatement:
        match = self.insert_pattern.search(sql)
        if not match:
            raise ValueError("Sintaxis invalida para INSERT INTO")
        table_name = match.group(1)
        columns_str = match.group(2)
        values_str = match.group(3)
        
        # Parsear columnas
        columns = None
        if columns_str:
            columns = [col.strip() for col in columns_str.split(',')]
        
        # Parsear valores
        values = self._parse_values(values_str)
        
        return InsertStatement(
            table_name=table_name,
            columns=columns,
            values=values
        )
    
    def _parse_values(self, values_str: str)->List[List[Any]]:
        # Dividir por comas, pero respetando paréntesis
        value_groups = self._split_by_comma_respecting_parens(values_str)
        values = []
        for group in value_groups:
            group = group.strip()
            if group.startswith('(') and group.endswith(')'):
                group = group[1:-1]  # Remover paréntesis
            
            # Dividir valores individuales
            individual_values = self._split_by_comma_respecting_parens(group)
            parsed_values = []
            for val in individual_values:
                val = val.strip()
                parsed_val = self._parse_value(val)
                parsed_values.append(parsed_val)
            values.append(parsed_values)
        return values
    
    def _parse_value(self, value: str)->Any:
        value = value.strip()

        # Parsear ARRAY[float, float]
        if value.upper().startswith('ARRAY[') and value.endswith(']'):
            array_content = value[6:-1]
            # Dividir por comas y parsear como floats
            try:
                float_values = [float(x.strip()) for x in array_content.split(',')]
                return float_values
            except ValueError:
                # Si no se puede parsear como floats, retornar como string
                return value
        
        # Remover comillas
        if value.startswith("'") and value.endswith("'"):
            return value[1:-1]
        elif value.startswith('"') and value.endswith('"'):
            return value[1:-1]
        
        # Intentar parsear como número
        try:
            if '.' in value:
                return float(value)
            else:
                return int(value)
        except ValueError:
            # Retornar como string
            return value
    
    def _parse_delete(self, sql: str)->DeleteStatement:
        match = self.delete_pattern.search(sql)
        if not match:
            raise ValueError("Sintaxis invalida para DELETE FROM")
        table_name = match.group(1)
        where_clause = match.group(2)
        
        # Parsear condicion WHERE
        where_condition = None
        if where_clause:
            where_condition = self._parse_where_condition(where_clause)
        
        return DeleteStatement(
            table_name=table_name,
            where_condition=where_condition
        )
    
    def _parse_select(self, sql: str)->SelectStatement:
        match = self.select_pattern.search(sql)
        if not match:
            raise ValueError("Sintaxis invalida para SELECT")
        columns_str = match.group(1)
        table_name = match.group(2)
        where_clause = match.group(3)
        
        # Parsear columnas
        if columns_str.strip() == '*':
            columns = ['*']
        else:
            columns = [col.strip() for col in columns_str.split(',')]
        
        # Parsear condicion WHERE
        where_condition = None
        if where_clause:
            where_condition = self._parse_where_condition(where_clause)
        
        return SelectStatement(
            columns=columns,
            table_name=table_name,
            where_condition=where_condition
        )
    
    def _parse_where_condition(self, where_clause: str)->Dict[str, Any]:
        where_clause = where_clause.strip()
        equal_pattern = r'(\w+)\s*=\s*(.+)'
        match = re.search(equal_pattern, where_clause, re.IGNORECASE)
        if match:
            return {
                'type': 'equal',
                'column': match.group(1),
                'value': self._parse_value(match.group(2))
            }
        
        # col BETWEEN x AND y
        between_pattern = r'(\w+)\s+BETWEEN\s+(.+?)\s+AND\s+(.+)'
        match = re.search(between_pattern, where_clause, re.IGNORECASE)
        if match:
            return {
                'type': 'between',
                'column': match.group(1),
                'min_value': self._parse_value(match.group(2)),
                'max_value': self._parse_value(match.group(3))
            }
        in_pattern = r'(\w+)\s+IN\s*\((.+?),\s*(.+?)\)'
        match = re.search(in_pattern, where_clause, re.IGNORECASE)
        if match:
            return {
                'type': 'spatial',
                'column': match.group(1),
                'point': self._parse_value(match.group(2)),
                'radius': self._parse_value(match.group(3))
            }
        raise ValueError(f"Condicion WHERE no soportada: {where_clause}")
    
    def _split_by_comma_respecting_parens(self, text: str)->List[str]:
        result = []
        current = ""
        paren_count = 0
        bracket_count = 0
        for char in text:
            if char == '(':
                paren_count += 1
            elif char == ')':
                paren_count -= 1
            elif char == '[':
                bracket_count += 1
            elif char == ']':
                bracket_count -= 1
            elif char == ',' and paren_count == 0 and bracket_count == 0:
                result.append(current.strip())
                current = ""
                continue
            current += char
        
        if current.strip():
            result.append(current.strip())
        return result


# Test
if __name__ == "__main__":
    print("Pruebas del SQL Parser:\n")
    
    parser = SQLParser()
    
    print("Crear tabla:")
    create_sql = """
    CREATE TABLE Restaurantes (
        id INT PRIMARY KEY INDEX HASH,
        nombre VARCHAR[20] INDEX BTREE,
        fechaRegistro DATE,
        ubicacion ARRAY INDEX RTREE
    )
    """
    try:
        create_stmt = parser.parse(create_sql)
        print(f"Tabla: {create_stmt.table_name}")
        print(f"Primary Key: {create_stmt.primary_key}")
        print(f"Primary Index Type: {create_stmt.primary_index_type}")
        print("Columnas:")
        for col in create_stmt.columns:
            print(f"  {col}")
    except Exception as e:
        print(f"Error: {e}")
    
    print("\nPrueba Insert:")
    insert_sql = 'INSERT INTO Restaurantes VALUES (1, "Restaurant A", "2024-01-01", ARRAY[-12.06, -77.03])'
    try:
        insert_stmt = parser.parse(insert_sql)
        print(f"Tabla: {insert_stmt.table_name}")
        print(f"Valores: {insert_stmt.values}")
    except Exception as e:
        print(f"Error: {e}")
    
    print("\nPrueba consultas con where:")
    select_sql = 'SELECT * FROM Restaurantes WHERE id = 1'
    try:
        select_stmt = parser.parse(select_sql)
        print(f"Columnas: {select_stmt.columns}")
        print(f"Tabla: {select_stmt.table_name}")
        print(f"Condicion: {select_stmt.where_condition}")
    except Exception as e:
        print(f"Error: {e}")

    print("\nPrueba consultas con between:")
    select_between_sql = 'SELECT * FROM Restaurantes WHERE nombre BETWEEN "A" AND "M"'
    try:
        select_stmt = parser.parse(select_between_sql)
        print(f"Condicion: {select_stmt.where_condition}")
    except Exception as e:
        print(f"Error: {e}")

    print("\nPrueba consulta espacial:")
    select_spatial_sql = 'SELECT * FROM Restaurantes WHERE ubicacion IN (ARRAY[-12.07, -77.05], 0.03)'
    try:
        select_stmt = parser.parse(select_spatial_sql)
        print(f"Condicion: {select_stmt.where_condition}")
    except Exception as e:
        print(f"Error: {e}")
    
    print("\nPrueba delete:")
    delete_sql = 'DELETE FROM Restaurantes WHERE id = 1'
    try:
        delete_stmt = parser.parse(delete_sql)
        print(f"Tabla: {delete_stmt.table_name}")
        print(f"Condicion: {delete_stmt.where_condition}")
    except Exception as e:
        print(f"Error: {e}")