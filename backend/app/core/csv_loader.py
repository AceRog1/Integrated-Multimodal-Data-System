import csv
import os
import tempfile
import shutil
from typing import List, Dict, Any, Optional
from .data_types import Column, DataType
from .parser_sql import CreateTableStatement
from datetime import datetime

class CSVLoader:    
    def __init__(self):
        pass
    
    def load_from_csv(
        self,
        csv_path: str,
        create_statement: CreateTableStatement,
        table_manager,
        index_manager,
        record_handler) -> Dict[str, Any]:
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"Archivo csv no encontrado - {csv_path}")
        
        csv_data = self._read_csv(csv_path)
        if not csv_data:
            raise ValueError("El archivo csv esta vacio")
        
        headers = csv_data[0]
        self._validate_headers(headers, create_statement.columns)
        
        table = table_manager.get_table(create_statement.table_name)
        if not table:
            raise ValueError(f"Tabla '{create_statement.table_name}' no encontrada. Debe ser creada primero.")
        
        record_file = record_handler  
        idx_manager = index_manager   
        
        inserted_count = 0
        error_count = 0
        errors = []
        
        for row_num, row in enumerate(csv_data[1:], start=2): 
            try:
                record = self._csv_row_to_record(row, headers, create_statement.columns)
                record_position = record_file.insert(record)
                idx_manager.insert(record, record_position)
                inserted_count += 1
                
                if inserted_count % 1000 == 0:
                    print(f"Registros procesados: {inserted_count} ")
                
            except Exception as e:
                error_count += 1
                error_msg = f"Fila {row_num}: {str(e)}"
                errors.append(error_msg)
                print(f"Error en fila {row_num}: {str(e)}")
        
        idx_manager.save_all()
        record_file.close()
        
        return {
            'success': True,
            'table_name': create_statement.table_name,
            'total_rows': len(csv_data) - 1,  
            'inserted_count': inserted_count,
            'error_count': error_count,
            'errors': errors[:10],  
            'primary_key': create_statement.primary_key,
            'indexed_columns': [col.name for col in create_statement.columns if col.has_index]
        }
    
    def _read_csv(self, csv_path: str) -> List[List[str]]:
        csv_data = []
        try:
            with open(csv_path, 'r', encoding='utf-8') as file:
                sample = file.read(1024)
                file.seek(0)
                sniffer = csv.Sniffer()
                delimiter = sniffer.sniff(sample).delimiter
                reader = csv.reader(file, delimiter=delimiter)
                csv_data = list(reader)
                
        except UnicodeDecodeError:
            with open(csv_path, 'r', encoding='latin-1') as file:
                reader = csv.reader(file)
                csv_data = list(reader)
        
        return csv_data
    
    def _validate_headers(self, csv_headers: List[str], table_columns: List[Column]) -> None:
        table_column_names = {col.name for col in table_columns}
        csv_column_names = {header.strip() for header in csv_headers}
        
        missing_columns = table_column_names - csv_column_names
        if missing_columns:
            raise ValueError(f"Columnas faltantes en csv: {missing_columns}")
        
        extra_columns = csv_column_names - table_column_names
        if extra_columns:
            print(f"Advertencia: Columnas extra en csv se ignoraran: {extra_columns}")
        
        required_columns = {col.name for col in table_columns if col.is_primary_key}
        missing_required = required_columns - csv_column_names
        if missing_required:
            raise ValueError(f"Columnas requeridas faltantes en csv: {missing_required}")
    
    def _csv_row_to_record(self, row: List[str], headers: List[str], columns: List[Column]) -> Dict[str, Any]:
        record = {}

        header_to_index = {header.strip(): i for i, header in enumerate(headers)}
    
        for col in columns:
            if col.name not in header_to_index:
                raise ValueError(f"Columna '{col.name}' no encontrada en csv")
            
            csv_index = header_to_index[col.name]
            if csv_index >= len(row):
                raise ValueError(f"Fila incompleta, falta columna '{col.name}'")
            raw_value = row[csv_index].strip()
            
            try:
                parsed_value = self._parse_csv_value(raw_value, col)
                record[col.name] = parsed_value
            except Exception as e:
                raise ValueError(f"Error parseando columna '{col.name}' con valor '{raw_value}': {str(e)}")
        
        return record
    
    def _parse_csv_value(self, raw_value: str, column: Column) -> Any:
        if not raw_value or raw_value.strip() == '' or raw_value.upper() == '\\N' or raw_value.upper() == 'NULL':
            return None
        
        raw_value = raw_value.strip()
        
        if column.data_type == DataType.INT:
            try:
                return int(raw_value)
            except ValueError:
                raise ValueError(f"Valor '{raw_value}' no es un entero válido")
        elif column.data_type == DataType.FLOAT:
            try:
                return float(raw_value)
            except ValueError:
                raise ValueError(f"Valor '{raw_value}' no es un número decimal válido")
        elif column.data_type == DataType.VARCHAR:
            value = str(raw_value)
            if column.size and len(value) > column.size:
                value = value[:column.size]
            return value
        elif column.data_type == DataType.DATE:
            try:
                dt = datetime.strptime(raw_value, '%Y-%m-%d')
                return int(dt.timestamp())
            except ValueError:
                for fmt in ['%d/%m/%Y', '%m/%d/%Y', '%Y-%m-%d %H:%M:%S']:
                    try:
                        dt = datetime.strptime(raw_value, fmt)
                        return int(dt.timestamp())
                    except ValueError:
                        continue
                raise ValueError(f"Formato de fecha no reconocido: {raw_value}")
        elif column.data_type == DataType.ARRAY_FLOAT:
            return self._parse_array_float(raw_value)
        else:
            raise ValueError(f"Tipo de dato no soportado - {column.data_type}")
    
    def _parse_array_float(self, raw_value: str) -> tuple:
        raw_value = raw_value.strip()

        if raw_value.upper().startswith('ARRAY['):
            raw_value = raw_value[6:-1]  
        elif raw_value.startswith('['):
            raw_value = raw_value[1:-1] 

        parts = [p.strip() for p in raw_value.split(',')]
        
        if len(parts) != 2:
            raise ValueError(f"Error - ARRAY_FLOAT debe tener solo 2 elementos: {raw_value}")
        
        try:
            x = float(parts[0])
            y = float(parts[1])
            return (x, y)
        except ValueError as e:
            raise ValueError(f"Error parseando ARRAY_FLOAT '{raw_value}': {str(e)}")
    
    def validate_csv_structure(self, csv_path: str, expected_columns: List[str]) -> Dict[str, Any]:
        if not os.path.exists(csv_path):
            return {'valid': False, 'error': f'Archivo no encontrado: {csv_path}'}
        
        try:
            csv_data = self._read_csv(csv_path)
            
            if not csv_data:
                return {'valid': False, 'error': 'Archivo CSV esta vacio'}
            
            headers = [h.strip() for h in csv_data[0]]
            expected_set = set(expected_columns)
            actual_set = set(headers)
            missing = expected_set - actual_set
            extra = actual_set - expected_set
            
            return {
                'valid': len(missing) == 0,
                'total_rows': len(csv_data) - 1,
                'headers': headers,
                'missing_columns': list(missing),
                'extra_columns': list(extra),
                'sample_row': csv_data[1] if len(csv_data) > 1 else None
            }
            
        except Exception as e:
            return {'valid': False, 'error': str(e)}
