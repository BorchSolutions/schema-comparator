# -*- coding: utf-8 -*-
"""
Worker para realizar la comparación de esquemas en un hilo separado.
"""

import traceback
import logging
from PyQt5.QtCore import QThread, pyqtSignal
import psycopg2
from core.schema_normalizer import SchemaNormalizer
from core.db_connector import connect_db

# Obtener el logger
logger = logging.getLogger('SchemaComparator')

class ComparisonWorker(QThread):
    """Ejecuta la comparación de esquemas en un hilo separado."""
    
    progress_signal = pyqtSignal(int)
    result_signal = pyqtSignal(list)
    error_signal = pyqtSignal(str)
    log_signal = pyqtSignal(str, int)  # mensaje, nivel
    completed_signal = pyqtSignal()
    
    def __init__(self, conn_params1, conn_params2):
        super().__init__()
        self.conn_params1 = conn_params1
        self.conn_params2 = conn_params2

        # Inicializar el normalizador de esquemas
        self.normalizer = SchemaNormalizer(conn_params1['schema'], conn_params2['schema'])
        
        # Log de inicialización
        self.log(f"Inicializado normalizador de esquemas para '{conn_params1['schema']}' y '{conn_params2['schema']}'", 
                 logging.INFO)
    
    def log(self, message, level=logging.INFO):
        """Método para enviar mensajes de log."""
        logger.log(level, message)
        self.log_signal.emit(message, level)
    
    def run(self):
        """Método principal que ejecuta la comparación."""
        try:
            # Conexión a las bases de datos
            self.log("Iniciando conexión a la primera base de datos...")
            self.progress_signal.emit(5)
            conn1 = self.connect_db(self.conn_params1)
            self.log(f"Conexión establecida a {self.conn_params1['dbname']} en {self.conn_params1['host']}")
            
            self.progress_signal.emit(10)
            self.log("Iniciando conexión a la segunda base de datos...")
            conn2 = self.connect_db(self.conn_params2)
            self.log(f"Conexión establecida a {self.conn_params2['dbname']} en {self.conn_params2['host']}")
            
            self.progress_signal.emit(15)
            
            # Verificar versiones de PostgreSQL
            self.verify_postgres_versions(conn1, conn2)
            
            # Verificar existencia de esquemas
            self.verify_schemas(conn1, conn2)
            
            # Comparar los diferentes objetos
            results = []
            
            # Comparar tablas y columnas
            self.log("Comparando tablas y columnas...")
            self.progress_signal.emit(25)
            results.extend(self.compare_tables(conn1, conn2))
            
            # Comparar funciones
            self.log("Comparando funciones...")
            self.progress_signal.emit(40)
            results.extend(self.compare_functions(conn1, conn2))
            
            # Comparar vistas
            self.log("Comparando vistas...")
            self.progress_signal.emit(55)
            results.extend(self.compare_views(conn1, conn2))
            
            # Comparar constraints
            self.log("Comparando constraints...")
            self.progress_signal.emit(70)
            results.extend(self.compare_constraints(conn1, conn2))
            
            # Comparar índices - Intentamos tres diferentes métodos
            self.log("Comparando índices...")
            self.progress_signal.emit(85)
            
            # Método 1 - Usando pg_class directamente
            try:
                index_results = self.compare_indexes(conn1, conn2)
                results.extend(index_results)
            except Exception as e1:
                self.log(f"Error en método principal para comparar índices: {str(e1)}", logging.WARNING)
                try:
                    # Método 2 - Consulta alternativa usando pg_catalog
                    self.log("Intentando método alternativo para comparar índices...", logging.WARNING)
                    index_results = self.compare_indexes_alternative(conn1, conn2)
                    results.extend(index_results)
                except Exception as e2:
                    self.log(f"Error en método alternativo para comparar índices: {str(e2)}", logging.WARNING)
                    # Método 3 - Última opción: omitir los índices
                    self.log("No se pudieron comparar los índices. Esta sección será omitida.", logging.WARNING)
            
            # Cerrar conexiones
            conn1.close()
            conn2.close()
            self.log("Conexiones cerradas correctamente")
            
            # Enviar resultados
            self.log(f"Comparación completada. Se encontraron {len(results)} diferencias.")
            self.result_signal.emit(results)
            self.progress_signal.emit(100)
            self.completed_signal.emit()
            
        except Exception as e:
            error_details = traceback.format_exc()
            self.log(f"Error en la comparación: {str(e)}\n{error_details}", logging.ERROR)
            self.error_signal.emit(str(e))
    
    def connect_db(self, params):
        """Conecta a la base de datos con manejo de errores mejorado"""
        try:
            return psycopg2.connect(
                host=params['host'],
                port=params['port'],
                dbname=params['dbname'],
                user=params['user'],
                password=params['password']
            )
        except psycopg2.OperationalError as e:
            error_msg = f"Error de conexión a {params['dbname']} en {params['host']}: {str(e)}"
            self.log(error_msg, logging.ERROR)
            raise Exception(error_msg)
        except Exception as e:
            error_msg = f"Error inesperado al conectar a {params['dbname']}: {str(e)}"
            self.log(error_msg, logging.ERROR)
            raise Exception(error_msg)
    
    def verify_postgres_versions(self, conn1, conn2):
        """Verificar versiones de PostgreSQL para asegurar compatibilidad"""
        try:
            cur1 = conn1.cursor()
            cur1.execute("SELECT version()")
            version1 = cur1.fetchone()[0]
            
            cur2 = conn2.cursor()
            cur2.execute("SELECT version()")
            version2 = cur2.fetchone()[0]
            
            # Extraer números de versión (como 12.4, 13.1, etc.)
            import re
            pg_version1 = re.search(r'PostgreSQL (\d+\.\d+)', version1).group(1)
            pg_version2 = re.search(r'PostgreSQL (\d+\.\d+)', version2).group(1)
            
            self.log(f"PostgreSQL versión 1: {pg_version1}")
            self.log(f"PostgreSQL versión 2: {pg_version2}")
            
            # Verificar compatibilidad de versiones
            major_version1 = int(pg_version1.split('.')[0])
            major_version2 = int(pg_version2.split('.')[0])
            
            if abs(major_version1 - major_version2) > 1:
                self.log(f"Advertencia: Las versiones de PostgreSQL difieren significativamente ({pg_version1} vs {pg_version2}). Pueden ocurrir errores de compatibilidad.", logging.WARNING)
            
        except Exception as e:
            self.log(f"Error al verificar versiones de PostgreSQL: {str(e)}", logging.WARNING)
    
    def verify_schemas(self, conn1, conn2):
        """Verificar que los esquemas existan en las bases de datos"""
        schema1 = self.conn_params1['schema']
        schema2 = self.conn_params2['schema']
        
        try:
            # Verificar esquema 1
            cur1 = conn1.cursor()
            cur1.execute("""
                SELECT EXISTS(
                    SELECT 1 FROM information_schema.schemata 
                    WHERE schema_name = %s
                )
            """, (schema1,))
            exists1 = cur1.fetchone()[0]
            
            if not exists1:
                error_msg = f"El esquema '{schema1}' no existe en la primera base de datos"
                self.log(error_msg, logging.ERROR)
                raise Exception(error_msg)
            
            # Verificar esquema 2
            cur2 = conn2.cursor()
            cur2.execute("""
                SELECT EXISTS(
                    SELECT 1 FROM information_schema.schemata 
                    WHERE schema_name = %s
                )
            """, (schema2,))
            exists2 = cur2.fetchone()[0]
            
            if not exists2:
                error_msg = f"El esquema '{schema2}' no existe en la segunda base de datos"
                self.log(error_msg, logging.ERROR)
                raise Exception(error_msg)
            
            self.log(f"Esquemas verificados: '{schema1}' y '{schema2}' existen")
            
        except Exception as e:
            if "no existe" not in str(e):
                self.log(f"Error al verificar esquemas: {str(e)}", logging.ERROR)
                raise Exception(f"Error al verificar esquemas: {str(e)}")
            else:
                raise
    
    def compare_tables(self, conn1, conn2):
        """Comparar tablas entre dos esquemas con manejo de errores mejorado"""
        results = []
        schema1 = self.conn_params1['schema']
        schema2 = self.conn_params2['schema']
        
        try:
            # Obtener tablas del primer esquema
            cur1 = conn1.cursor()
            cur1.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = %s AND table_type = 'BASE TABLE'
            """, (schema1,))
            tables1 = {row[0]: True for row in cur1.fetchall()}
            self.log(f"Obtenidas {len(tables1)} tablas del esquema '{schema1}'")
            
            # Obtener tablas del segundo esquema
            cur2 = conn2.cursor()
            cur2.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = %s AND table_type = 'BASE TABLE'
            """, (schema2,))
            tables2 = {row[0]: True for row in cur2.fetchall()}
            self.log(f"Obtenidas {len(tables2)} tablas del esquema '{schema2}'")
            
            # Comparar existencia de tablas
            all_tables = set(tables1.keys()) | set(tables2.keys())
            diff_count = 0
            identical_count = 0
            
            for table in all_tables:
                if table not in tables1:
                    results.append({
                        'tipo': 'TABLA',
                        'objeto': table,
                        'detalle': 'La tabla existe solo en el segundo esquema',
                        'esquema1': 'No existe',
                        'esquema2': f"{schema2}.{table}",
                        'estado': 'DIFERENTE'
                    })
                    diff_count += 1
                elif table not in tables2:
                    results.append({
                        'tipo': 'TABLA',
                        'objeto': table,
                        'detalle': 'La tabla existe solo en el primer esquema',
                        'esquema1': f"{schema1}.{table}",
                        'esquema2': 'No existe',
                        'estado': 'DIFERENTE'
                    })
                    diff_count += 1
                else:
                    # Comparar columnas si la tabla existe en ambos esquemas
                    try:
                        column_results = self.compare_columns(conn1, conn2, table)
                        
                        if not column_results:
                            # Si no hay diferencias en las columnas, la tabla es idéntica
                            results.append({
                                'tipo': 'TABLA',
                                'objeto': table,
                                'detalle': 'La tabla tiene la misma estructura en ambos esquemas',
                                'esquema1': f"{schema1}.{table}",
                                'esquema2': f"{schema2}.{table}",
                                'estado': 'IDÉNTICO'
                            })
                            identical_count += 1
                        else:
                            # Añadir las diferencias de columnas
                            results.extend(column_results)
                            diff_count += len(column_results)
                    except Exception as e:
                        self.log(f"Error al comparar columnas de la tabla '{table}': {str(e)}", logging.WARNING)
            
            self.log(f"Comparación de tablas completada. Se encontraron {diff_count} diferencias y {identical_count} tablas idénticas.")
            
        except Exception as e:
            self.log(f"Error al comparar tablas: {str(e)}", logging.ERROR)
            traceback_str = traceback.format_exc()
            self.log(f"Detalles del error:\n{traceback_str}", logging.DEBUG)
        
        return results
    
    def compare_columns(self, conn1, conn2, table):
        """Comparar columnas de una tabla entre dos esquemas"""
        results = []
        schema1 = self.conn_params1['schema']
        schema2 = self.conn_params2['schema']
        
        try:
            # Obtener columnas de la tabla en el primer esquema
            cur1 = conn1.cursor()
            cur1.execute("""
                SELECT column_name, data_type, character_maximum_length, is_nullable
                FROM information_schema.columns 
                WHERE table_schema = %s AND table_name = %s
            """, (schema1, table))
            columns1 = {row[0]: {'data_type': row[1], 'length': row[2], 'nullable': row[3]} 
                      for row in cur1.fetchall()}
            
            # Obtener columnas de la tabla en el segundo esquema
            cur2 = conn2.cursor()
            cur2.execute("""
                SELECT column_name, data_type, character_maximum_length, is_nullable
                FROM information_schema.columns 
                WHERE table_schema = %s AND table_name = %s
            """, (schema2, table))
            columns2 = {row[0]: {'data_type': row[1], 'length': row[2], 'nullable': row[3]} 
                      for row in cur2.fetchall()}
            
            # Comparar existencia y definición de columnas
            all_columns = set(columns1.keys()) | set(columns2.keys())
            for column in all_columns:
                if column not in columns1:
                    col_info = columns2[column]
                    results.append({
                        'tipo': 'COLUMNA',
                        'objeto': f"{table}.{column}",
                        'detalle': 'La columna existe solo en el segundo esquema',
                        'esquema1': 'No existe',
                        'esquema2': (f"{schema2}.{table}.{column} "
                                    f"({col_info['data_type']}"
                                    f"{f'({col_info['length']})' if col_info['length'] else ''}, "
                                    f"{col_info['nullable']})"),
                        'estado': 'DIFERENTE'
                    })
                elif column not in columns2:
                    col_info = columns1[column]
                    results.append({
                        'tipo': 'COLUMNA',
                        'objeto': f"{table}.{column}",
                        'detalle': 'La columna existe solo en el primer esquema',
                        'esquema1': (f"{schema1}.{table}.{column} "
                                    f"({col_info['data_type']}"
                                    f"{f'({col_info['length']})' if col_info['length'] else ''}, "
                                    f"{col_info['nullable']})"),
                        'esquema2': 'No existe',
                        'estado': 'DIFERENTE'
                    })
                else:
                    # Comparar definición de columnas
                    col1 = columns1[column]
                    col2 = columns2[column]
                    
                    if (col1['data_type'] != col2['data_type'] or
                        col1['length'] != col2['length'] or
                        col1['nullable'] != col2['nullable']):
                        results.append({
                            'tipo': 'COLUMNA',
                            'objeto': f"{table}.{column}",
                            'detalle': 'La definición de la columna es diferente',
                            'esquema1': (f"{schema1}.{table}.{column} "
                                        f"({col1['data_type']}"
                                        f"{f'({col1['length']})' if col1['length'] else ''}, "
                                        f"{col1['nullable']})"),
                            'esquema2': (f"{schema2}.{table}.{column} "
                                        f"({col2['data_type']}"
                                        f"{f'({col2['length']})' if col2['length'] else ''}, "
                                        f"{col2['nullable']})"),
                            'estado': 'DIFERENTE'
                        })
        
        except Exception as e:
            self.log(f"Error al comparar columnas de la tabla '{table}': {str(e)}", logging.ERROR)
            traceback_str = traceback.format_exc()
            self.log(traceback_str, logging.DEBUG)
            raise
        
        return results
    
    def compare_function_parameters(self, conn1, conn2):
        """Comparar parámetros de funciones entre dos esquemas - Versión optimizada"""
        results = []
        schema1 = self.conn_params1['schema']
        schema2 = self.conn_params2['schema']
        
        try:
            # Obtener parámetros de TODAS las funciones del primer esquema en una sola consulta
            cur1 = conn1.cursor()
            cur1.execute("""
                SELECT r.routine_name, p.parameter_name, p.data_type, p.parameter_mode
                FROM information_schema.routines r
                JOIN information_schema.parameters p ON r.specific_name = p.specific_name
                WHERE r.routine_schema = %s AND p.parameter_name IS NOT NULL
                ORDER BY r.routine_name, p.ordinal_position
            """, (schema1,))
            params1 = {}
            for row in cur1.fetchall():
                func, param, data_type, mode = row
                if func not in params1:
                    params1[func] = {}
                params1[func][param] = {'data_type': data_type, 'mode': mode}
            
            # Obtener parámetros de TODAS las funciones del segundo esquema en una sola consulta
            cur2 = conn2.cursor()
            cur2.execute("""
                SELECT r.routine_name, p.parameter_name, p.data_type, p.parameter_mode
                FROM information_schema.routines r
                JOIN information_schema.parameters p ON r.specific_name = p.specific_name
                WHERE r.routine_schema = %s AND p.parameter_name IS NOT NULL
                ORDER BY r.routine_name, p.ordinal_position
            """, (schema2,))
            params2 = {}
            for row in cur2.fetchall():
                func, param, data_type, mode = row
                if func not in params2:
                    params2[func] = {}
                params2[func][param] = {'data_type': data_type, 'mode': mode}
            
            self.log(f"Obtenidos parámetros para {len(params1)} funciones en el esquema 1 y {len(params2)} funciones en el esquema 2")
            
            # Comparar parámetros de funciones que existen en ambos esquemas
            common_funcs = set(params1.keys()) & set(params2.keys())
            for func in common_funcs:
                # Comparar existencia de parámetros
                all_params = set(params1[func].keys()) | set(params2[func].keys())
                for param in all_params:
                    if param not in params1[func]:
                        param_info = params2[func][param]
                        results.append({
                            'tipo': 'PARÁMETRO',
                            'objeto': f"{func}.{param}",
                            'detalle': 'El parámetro existe solo en el segundo esquema',
                            'esquema1': 'No existe',
                            'esquema2': f"{schema2}.{func}({param} {param_info['data_type']})",
                            'estado': 'DIFERENTE SIGNATURE'
                        })
                    elif param not in params2[func]:
                        param_info = params1[func][param]
                        results.append({
                            'tipo': 'PARÁMETRO',
                            'objeto': f"{func}.{param}",
                            'detalle': 'El parámetro existe solo en el primer esquema',
                            'esquema1': f"{schema1}.{func}({param} {param_info['data_type']})",
                            'esquema2': 'No existe',
                            'estado': 'DIFERENTE SIGNATURE'
                        })
                    elif (params1[func][param]['data_type'] != params2[func][param]['data_type'] or
                          params1[func][param]['mode'] != params2[func][param]['mode']):
                        results.append({
                            'tipo': 'PARÁMETRO',
                            'objeto': f"{func}.{param}",
                            'detalle': 'La definición del parámetro es diferente',
                            'esquema1': f"{schema1}.{func}({param} {params1[func][param]['data_type']})",
                            'esquema2': f"{schema2}.{func}({param} {params2[func][param]['data_type']})",
                            'estado': 'DIFERENTE SIGNATURE'
                        })
        
        except Exception as e:
            self.log(f"Error al comparar parámetros de funciones: {str(e)}", logging.ERROR)
            traceback_str = traceback.format_exc()
            self.log(f"Detalles del error:\n{traceback_str}", logging.DEBUG)
            raise
        
        return results            

    # Luego modificamos el método compare_functions
    def compare_functions(self, conn1, conn2):
        """Comparar funciones entre dos esquemas con normalización de referencias a esquemas"""
        results = []
        schema1 = self.conn_params1['schema']
        schema2 = self.conn_params2['schema']
        
        try:
            # Obtener funciones del primer esquema
            cur1 = conn1.cursor()
            cur1.execute("""
                SELECT r.routine_name, r.routine_definition,
                       pg_get_functiondef(p.oid) AS full_definition
                FROM information_schema.routines r
                JOIN pg_catalog.pg_proc p ON p.proname = r.routine_name
                JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
                WHERE r.routine_schema = %s AND n.nspname = %s
            """, (schema1, schema1))
            functions1 = {}
            for row in cur1.fetchall():
                # Normalizar la definición antes de guardarla
                normalized_def = self.normalizer.normalize_definition(row[1], schema1)
                normalized_full = self.normalizer.normalize_definition(row[2], schema1)
                functions1[row[0]] = {
                    'definition': row[1],
                    'normalized_definition': normalized_def,
                    'full_definition': row[2],
                    'normalized_full_definition': normalized_full
                }
            self.log(f"Obtenidas {len(functions1)} funciones del esquema '{schema1}'")
            
            # Obtener funciones del segundo esquema
            cur2 = conn2.cursor()
            cur2.execute("""
                SELECT r.routine_name, r.routine_definition,
                       pg_get_functiondef(p.oid) AS full_definition
                FROM information_schema.routines r
                JOIN pg_catalog.pg_proc p ON p.proname = r.routine_name
                JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
                WHERE r.routine_schema = %s AND n.nspname = %s
            """, (schema2, schema2))
            functions2 = {}
            for row in cur2.fetchall():
                # Normalizar la definición antes de guardarla
                normalized_def = self.normalizer.normalize_definition(row[1], schema2)
                normalized_full = self.normalizer.normalize_definition(row[2], schema2)
                functions2[row[0]] = {
                    'definition': row[1],
                    'normalized_definition': normalized_def,
                    'full_definition': row[2],
                    'normalized_full_definition': normalized_full
                }
            self.log(f"Obtenidas {len(functions2)} funciones del esquema '{schema2}'")
            
            # Comparar existencia y cuerpo de funciones
            all_functions = set(functions1.keys()) | set(functions2.keys())
            diff_count = 0
            identical_count = 0
            
            for func in all_functions:
                if func not in functions1:
                    results.append({
                        'tipo': 'FUNCIÓN',
                        'objeto': func,
                        'detalle': 'La función existe solo en el segundo esquema',
                        'esquema1': 'No existe',
                        'esquema2': f"{schema2}.{func}",
                        'estado': 'DIFERENTE',
                        'esquema1_full': 'No existe',
                        'esquema2_full': functions2[func]['full_definition'],
                        'esquema1_normalized': 'No existe',
                        'esquema2_normalized': functions2[func]['normalized_full_definition']
                    })
                    diff_count += 1
                elif func not in functions2:
                    results.append({
                        'tipo': 'FUNCIÓN',
                        'objeto': func,
                        'detalle': 'La función existe solo en el primer esquema',
                        'esquema1': f"{schema1}.{func}",
                        'esquema2': 'No existe',
                        'estado': 'DIFERENTE',
                        'esquema1_full': functions1[func]['full_definition'],
                        'esquema2_full': 'No existe',
                        'esquema1_normalized': functions1[func]['normalized_full_definition'],
                        'esquema2_normalized': 'No existe'
                    })
                    diff_count += 1
                # Comparar usando las definiciones normalizadas
                elif functions1[func]['normalized_definition'] != functions2[func]['normalized_definition']:
                    results.append({
                        'tipo': 'FUNCIÓN',
                        'objeto': func,
                        'detalle': 'El cuerpo de la función es diferente (ignorando referencias a esquemas)',
                        'esquema1': f"{schema1}.{func}",
                        'esquema2': f"{schema2}.{func}",
                        'estado': 'DIFERENTE CUERPO',
                        'esquema1_full': functions1[func]['full_definition'],
                        'esquema2_full': functions2[func]['full_definition'],
                        'esquema1_normalized': functions1[func]['normalized_full_definition'],
                        'esquema2_normalized': functions2[func]['normalized_full_definition']
                    })
                    diff_count += 1
                else:
                    # Las funciones son idénticas (considerando la normalización)
                    results.append({
                        'tipo': 'FUNCIÓN',
                        'objeto': func,
                        'detalle': 'La función es idéntica en ambos esquemas (ignorando referencias a esquemas)',
                        'esquema1': f"{schema1}.{func}",
                        'esquema2': f"{schema2}.{func}",
                        'estado': 'IDÉNTICO',
                        'esquema1_full': functions1[func]['full_definition'],
                        'esquema2_full': functions2[func]['full_definition'],
                        'esquema1_normalized': functions1[func]['normalized_full_definition'],
                        'esquema2_normalized': functions2[func]['normalized_full_definition']
                    })
                    identical_count += 1
            
            # Comparar parámetros de funciones - este paso se deja igual
            try:
                param_results = self.compare_function_parameters(conn1, conn2)
                results.extend(param_results)
                if param_results:
                    diff_count += len(param_results)
            except Exception as e:
                self.log(f"Error al comparar parámetros de funciones: {str(e)}", logging.WARNING)
            
            self.log(f"Comparación de funciones completada. Se encontraron {diff_count} diferencias y {identical_count} funciones idénticas.")
        
        except Exception as e:
            self.log(f"Error al comparar funciones: {str(e)}", logging.ERROR)
            traceback_str = traceback.format_exc()
            self.log(f"Detalles del error:\n{traceback_str}", logging.DEBUG)
        
        return results
    

    def compare_views(self, conn1, conn2):
        """Comparar vistas entre dos esquemas con normalización de referencias a esquemas"""
        results = []
        schema1 = self.conn_params1['schema']
        schema2 = self.conn_params2['schema']
        
        try:
            # Obtener vistas del primer esquema con definición completa
            cur1 = conn1.cursor()
            cur1.execute("""
                SELECT table_name, view_definition,
                       pg_get_viewdef(c.oid, true) AS full_definition
                FROM information_schema.views v
                JOIN pg_catalog.pg_class c ON c.relname = v.table_name
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE v.table_schema = %s AND n.nspname = %s
            """, (schema1, schema1))
            views1 = {}
            for row in cur1.fetchall():
                # Normalizar la definición antes de guardarla
                normalized_def = self.normalizer.normalize_definition(row[1], schema1)
                normalized_full = self.normalizer.normalize_definition(row[2], schema1)
                views1[row[0]] = {
                    'definition': row[1],
                    'normalized_definition': normalized_def,
                    'full_definition': row[2],
                    'normalized_full_definition': normalized_full
                }
            self.log(f"Obtenidas {len(views1)} vistas del esquema '{schema1}'")
            
            # Obtener vistas del segundo esquema con definición completa
            cur2 = conn2.cursor()
            cur2.execute("""
                SELECT table_name, view_definition,
                       pg_get_viewdef(c.oid, true) AS full_definition
                FROM information_schema.views v
                JOIN pg_catalog.pg_class c ON c.relname = v.table_name
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE v.table_schema = %s AND n.nspname = %s
            """, (schema2, schema2))
            views2 = {}
            for row in cur2.fetchall():
                # Normalizar la definición antes de guardarla
                normalized_def = self.normalizer.normalize_definition(row[1], schema2)
                normalized_full = self.normalizer.normalize_definition(row[2], schema2)
                views2[row[0]] = {
                    'definition': row[1],
                    'normalized_definition': normalized_def,
                    'full_definition': row[2],
                    'normalized_full_definition': normalized_full
                }
            self.log(f"Obtenidas {len(views2)} vistas del esquema '{schema2}'")
            
            # Comparar existencia y definición de vistas
            all_views = set(views1.keys()) | set(views2.keys())
            diff_count = 0
            identical_count = 0
            
            for view in all_views:
                if view not in views1:
                    results.append({
                        'tipo': 'VISTA',
                        'objeto': view,
                        'detalle': 'La vista existe solo en el segundo esquema',
                        'esquema1': 'No existe',
                        'esquema2': f"{schema2}.{view}",
                        'estado': 'DIFERENTE',
                        'esquema1_full': 'No existe',
                        'esquema2_full': views2[view]['full_definition'],
                        'esquema1_normalized': 'No existe',
                        'esquema2_normalized': views2[view]['normalized_full_definition']
                    })
                    diff_count += 1
                elif view not in views2:
                    results.append({
                        'tipo': 'VISTA',
                        'objeto': view,
                        'detalle': 'La vista existe solo en el primer esquema',
                        'esquema1': f"{schema1}.{view}",
                        'esquema2': 'No existe',
                        'estado': 'DIFERENTE',
                        'esquema1_full': views1[view]['full_definition'],
                        'esquema2_full': 'No existe',
                        'esquema1_normalized': views1[view]['normalized_full_definition'],
                        'esquema2_normalized': 'No existe'
                    })
                    diff_count += 1
                # Comparar usando las definiciones normalizadas
                elif views1[view]['normalized_definition'] != views2[view]['normalized_definition']:
                    results.append({
                        'tipo': 'VISTA',
                        'objeto': view,
                        'detalle': 'La definición de la vista es diferente (ignorando referencias a esquemas)',
                        'esquema1': f"{schema1}.{view}",
                        'esquema2': f"{schema2}.{view}",
                        'estado': 'DIFERENTE DEFINICIÓN',
                        'esquema1_full': views1[view]['full_definition'],
                        'esquema2_full': views2[view]['full_definition'],
                        'esquema1_normalized': views1[view]['normalized_full_definition'],
                        'esquema2_normalized': views2[view]['normalized_full_definition']
                    })
                    diff_count += 1
                else:
                    # Las vistas son idénticas (considerando la normalización)
                    results.append({
                        'tipo': 'VISTA',
                        'objeto': view,
                        'detalle': 'La vista es idéntica en ambos esquemas (ignorando referencias a esquemas)',
                        'esquema1': f"{schema1}.{view}",
                        'esquema2': f"{schema2}.{view}",
                        'estado': 'IDÉNTICO',
                        'esquema1_full': views1[view]['full_definition'],
                        'esquema2_full': views2[view]['full_definition'],
                        'esquema1_normalized': views1[view]['normalized_full_definition'],
                        'esquema2_normalized': views2[view]['normalized_full_definition']
                    })
                    identical_count += 1
            
            self.log(f"Comparación de vistas completada. Se encontraron {diff_count} diferencias y {identical_count} vistas idénticas.")
        
        except Exception as e:
            self.log(f"Error al comparar vistas: {str(e)}", logging.ERROR)
            traceback_str = traceback.format_exc()
            self.log(f"Detalles del error:\n{traceback_str}", logging.DEBUG)
        
        return results

    def compare_constraints(self, conn1, conn2):
        """Comparar constraints entre dos esquemas"""
        results = []
        schema1 = self.conn_params1['schema']
        schema2 = self.conn_params2['schema']
        
        try:
            # Obtener constraints del primer esquema
            cur1 = conn1.cursor()
            cur1.execute("""
                SELECT tc.table_name, tc.constraint_name, tc.constraint_type
                FROM information_schema.table_constraints tc
                WHERE tc.constraint_schema = %s
                AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE', 'FOREIGN KEY')
                ORDER BY tc.table_name, tc.constraint_name
            """, (schema1,))
            constraints1 = {}
            for row in cur1.fetchall():
                table, name, type_c = row
                key = f"{table}.{name}"
                constraints1[key] = {'table': table, 'name': name, 'type': type_c}
            self.log(f"Obtenidos {len(constraints1)} constraints del esquema '{schema1}'")
            
            # Obtener constraints del segundo esquema
            cur2 = conn2.cursor()
            cur2.execute("""
                SELECT tc.table_name, tc.constraint_name, tc.constraint_type
                FROM information_schema.table_constraints tc
                WHERE tc.constraint_schema = %s
                AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE', 'FOREIGN KEY')
                ORDER BY tc.table_name, tc.constraint_name
            """, (schema2,))
            constraints2 = {}
            for row in cur2.fetchall():
                table, name, type_c = row
                key = f"{table}.{name}"
                constraints2[key] = {'table': table, 'name': name, 'type': type_c}
            self.log(f"Obtenidos {len(constraints2)} constraints del esquema '{schema2}'")
            
            # Comparar existencia de constraints
            all_constraints = set(constraints1.keys()) | set(constraints2.keys())
            diff_count = 0
            identical_count = 0
            
            for key in all_constraints:
                if key not in constraints1:
                    c2 = constraints2[key]
                    results.append({
                        'tipo': c2['type'],
                        'objeto': key,
                        'detalle': f"El constraint {c2['type']} existe solo en el segundo esquema",
                        'esquema1': 'No existe',
                        'esquema2': f"{schema2}.{key}",
                        'estado': 'DIFERENTE'
                    })
                    diff_count += 1
                elif key not in constraints2:
                    c1 = constraints1[key]
                    results.append({
                        'tipo': c1['type'],
                        'objeto': key,
                        'detalle': f"El constraint {c1['type']} existe solo en el primer esquema",
                        'esquema1': f"{schema1}.{key}",
                        'esquema2': 'No existe',
                        'estado': 'DIFERENTE'
                    })
                    diff_count += 1
                elif constraints1[key]['type'] != constraints2[key]['type']:
                    c1, c2 = constraints1[key], constraints2[key]
                    results.append({
                        'tipo': 'CONSTRAINT',
                        'objeto': key,
                        'detalle': 'El tipo de constraint es diferente',
                        'esquema1': f"{schema1}.{key} ({c1['type']})",
                        'esquema2': f"{schema2}.{key} ({c2['type']})",
                        'estado': 'DIFERENTE TIPO'
                    })
                    diff_count += 1
                else:
                    # Para FOREIGN KEY, necesitamos verificar también las referencias
                    if constraints1[key]['type'] == 'FOREIGN KEY':
                        # Las referencias se verificarán en compare_foreign_keys, no marcamos como idénticas aquí
                        pass
                    else:
                        # Constraints idénticos (para PRIMARY KEY y UNIQUE)
                        c1 = constraints1[key]
                        results.append({
                            'tipo': c1['type'],
                            'objeto': key,
                            'detalle': f"El constraint {c1['type']} es idéntico en ambos esquemas",
                            'esquema1': f"{schema1}.{key}",
                            'esquema2': f"{schema2}.{key}",
                            'estado': 'IDÉNTICO'
                        })
                        identical_count += 1
            
            # Comparar foreign keys específicamente
            try:
                fk_results = self.compare_foreign_keys(conn1, conn2)
                
                # Añadir FKs idénticas (aquellas que son comunes y no tienen diferencias)
                common_fks = set(constraints1.keys()) & set(constraints2.keys())
                for key in common_fks:
                    if (constraints1[key]['type'] == 'FOREIGN KEY' and 
                        constraints2[key]['type'] == 'FOREIGN KEY'):
                        # Verificar si esta FK ya se marcó como diferente en fk_results
                        if not any(r['objeto'] == key for r in fk_results):
                            # Esta FK es idéntica en ambos esquemas
                            results.append({
                                'tipo': 'FOREIGN KEY',
                                'objeto': key,
                                'detalle': "La foreign key es idéntica en ambos esquemas",
                                'esquema1': f"{schema1}.{key}",
                                'esquema2': f"{schema2}.{key}",
                                'estado': 'IDÉNTICO'
                            })
                            identical_count += 1
                
                # Añadir las FKs con diferencias
                results.extend(fk_results)
                if fk_results:
                    diff_count += len(fk_results)
            except Exception as e:
                self.log(f"Error al comparar foreign keys: {str(e)}", logging.WARNING)
            
            self.log(f"Comparación de constraints completada. Se encontraron {diff_count} diferencias y {identical_count} constraints idénticos.")
        
        except Exception as e:
            self.log(f"Error al comparar constraints: {str(e)}", logging.ERROR)
            traceback_str = traceback.format_exc()
            self.log(f"Detalles del error:\n{traceback_str}", logging.DEBUG)
        
        return results

    
    def compare_foreign_keys(self, conn1, conn2):
        """Comparar foreign keys entre dos esquemas"""
        results = []
        schema1 = self.conn_params1['schema']
        schema2 = self.conn_params2['schema']
        
        try:
            # Obtener FKs del primer esquema con sus referencias
            cur1 = conn1.cursor()
            cur1.execute("""
                SELECT 
                    tc.table_name, tc.constraint_name, 
                    ccu.table_name AS referenced_table,
                    ccu.column_name AS referenced_column
                FROM information_schema.table_constraints tc
                JOIN information_schema.constraint_column_usage ccu 
                  ON tc.constraint_catalog = ccu.constraint_catalog 
                  AND tc.constraint_schema = ccu.constraint_schema
                  AND tc.constraint_name = ccu.constraint_name
                WHERE tc.constraint_schema = %s
                AND tc.constraint_type = 'FOREIGN KEY'
                ORDER BY tc.table_name, tc.constraint_name
            """, (schema1,))
            fks1 = {}
            for row in cur1.fetchall():
                table, name, ref_table, ref_col = row
                key = f"{table}.{name}"
                fks1[key] = {'table': table, 'name': name, 
                             'ref_table': ref_table, 'ref_col': ref_col}
            
            # Obtener FKs del segundo esquema con sus referencias
            cur2 = conn2.cursor()
            cur2.execute("""
                SELECT 
                    tc.table_name, tc.constraint_name, 
                    ccu.table_name AS referenced_table,
                    ccu.column_name AS referenced_column
                FROM information_schema.table_constraints tc
                JOIN information_schema.constraint_column_usage ccu 
                  ON tc.constraint_catalog = ccu.constraint_catalog 
                  AND tc.constraint_schema = ccu.constraint_schema
                  AND tc.constraint_name = ccu.constraint_name
                WHERE tc.constraint_schema = %s
                AND tc.constraint_type = 'FOREIGN KEY'
                ORDER BY tc.table_name, tc.constraint_name
            """, (schema2,))
            fks2 = {}
            for row in cur2.fetchall():
                table, name, ref_table, ref_col = row
                key = f"{table}.{name}"
                fks2[key] = {'table': table, 'name': name, 
                             'ref_table': ref_table, 'ref_col': ref_col}
            
            # Comparar referencias de FKs que existen en ambos esquemas
            # (Solo reportamos las que tienen diferencias, las idénticas se manejan en compare_constraints)
            common_fks = set(fks1.keys()) & set(fks2.keys())
            diff_count = 0
            
            for key in common_fks:
                fk1, fk2 = fks1[key], fks2[key]
                
                # Normalizar nombres de tablas referenciadas si corresponden a esquemas de clientes
                ref_table1 = self.normalizer.normalize_definition(fk1['ref_table'], schema1)
                ref_table2 = self.normalizer.normalize_definition(fk2['ref_table'], schema2)
                
                if (ref_table1 != ref_table2 or fk1['ref_col'] != fk2['ref_col']):
                    results.append({
                        'tipo': 'FOREIGN KEY',
                        'objeto': key,
                        'detalle': 'La referencia de la FK es diferente',
                        'esquema1': f"{schema1}.{key} -> {fk1['ref_table']}({fk1['ref_col']})",
                        'esquema2': f"{schema2}.{key} -> {fk2['ref_table']}({fk2['ref_col']})",
                        'estado': 'DIFERENTE REFERENCIA'
                    })
                    diff_count += 1
            
            self.log(f"Comparación detallada de foreign keys completada. Se encontraron {diff_count} diferencias en las referencias.")
        
        except Exception as e:
            self.log(f"Error al comparar foreign keys: {str(e)}", logging.ERROR)
            traceback_str = traceback.format_exc()
            self.log(f"Detalles del error:\n{traceback_str}", logging.DEBUG)
            raise
        
        return results


    def compare_indexes(self, conn1, conn2):
        """Comparar índices entre dos esquemas (consulta corregida para PostgreSQL 13)"""
        results = []
        schema1 = self.conn_params1['schema']
        schema2 = self.conn_params2['schema']
        
        try:
            # Consulta para obtener índices del primer esquema
            cur1 = conn1.cursor()
            cur1.execute("""
                SELECT 
                    t.relname AS tablename, 
                    ci.relname AS indexname,
                    pg_get_indexdef(ci.oid) AS indexdef
                FROM pg_catalog.pg_class ci
                JOIN pg_catalog.pg_index i ON ci.oid = i.indexrelid
                JOIN pg_catalog.pg_class t ON t.oid = i.indrelid
                JOIN pg_catalog.pg_namespace n ON n.oid = ci.relnamespace
                WHERE 
                    ci.relkind = 'i'
                    AND n.nspname = %s
                ORDER BY t.relname, ci.relname
            """, (schema1,))
            indexes1 = {}
            for row in cur1.fetchall():
                table, name, definition = row
                key = f"{table}.{name}"
                # Normalizar la definición para comparar correctamente
                normalized_def = self.normalizer.normalize_definition(definition, schema1)
                indexes1[key] = {
                    'table': table, 
                    'name': name, 
                    'definition': definition,
                    'normalized_definition': normalized_def
                }
            self.log(f"Obtenidos {len(indexes1)} índices del esquema '{schema1}'")
            
            # Consulta para obtener índices del segundo esquema
            cur2 = conn2.cursor()
            cur2.execute("""
                SELECT 
                    t.relname AS tablename, 
                    ci.relname AS indexname,
                    pg_get_indexdef(ci.oid) AS indexdef
                FROM pg_catalog.pg_class ci
                JOIN pg_catalog.pg_index i ON ci.oid = i.indexrelid
                JOIN pg_catalog.pg_class t ON t.oid = i.indrelid
                JOIN pg_catalog.pg_namespace n ON n.oid = ci.relnamespace
                WHERE 
                    ci.relkind = 'i'
                    AND n.nspname = %s
                ORDER BY t.relname, ci.relname
            """, (schema2,))
            indexes2 = {}
            for row in cur2.fetchall():
                table, name, definition = row
                key = f"{table}.{name}"
                # Normalizar la definición para comparar correctamente
                normalized_def = self.normalizer.normalize_definition(definition, schema2)
                indexes2[key] = {
                    'table': table, 
                    'name': name, 
                    'definition': definition,
                    'normalized_definition': normalized_def
                }
            self.log(f"Obtenidos {len(indexes2)} índices del esquema '{schema2}'")
            
            # Comparar existencia y definición de índices
            all_indexes = set(indexes1.keys()) | set(indexes2.keys())
            diff_count = 0
            identical_count = 0
            
            for key in all_indexes:
                if key not in indexes1:
                    idx2 = indexes2[key]
                    results.append({
                        'tipo': 'ÍNDICE',
                        'objeto': key,
                        'detalle': 'El índice existe solo en el segundo esquema',
                        'esquema1': 'No existe',
                        'esquema2': idx2['definition'],
                        'estado': 'DIFERENTE',
                        'esquema1_full': 'No existe',
                        'esquema2_full': idx2['definition'],
                        'esquema1_normalized': 'No existe',
                        'esquema2_normalized': idx2['normalized_definition']
                    })
                    diff_count += 1
                elif key not in indexes2:
                    idx1 = indexes1[key]
                    results.append({
                        'tipo': 'ÍNDICE',
                        'objeto': key,
                        'detalle': 'El índice existe solo en el primer esquema',
                        'esquema1': idx1['definition'],
                        'esquema2': 'No existe',
                        'estado': 'DIFERENTE',
                        'esquema1_full': idx1['definition'],
                        'esquema2_full': 'No existe',
                        'esquema1_normalized': idx1['normalized_definition'],
                        'esquema2_normalized': 'No existe'
                    })
                    diff_count += 1
                elif indexes1[key]['normalized_definition'] != indexes2[key]['normalized_definition']:
                    idx1, idx2 = indexes1[key], indexes2[key]
                    results.append({
                        'tipo': 'ÍNDICE',
                        'objeto': key,
                        'detalle': 'La definición del índice es diferente',
                        'esquema1': idx1['definition'],
                        'esquema2': idx2['definition'],
                        'estado': 'DIFERENTE DEFINICIÓN',
                        'esquema1_full': idx1['definition'],
                        'esquema2_full': idx2['definition'],
                        'esquema1_normalized': idx1['normalized_definition'],
                        'esquema2_normalized': idx2['normalized_definition']
                    })
                    diff_count += 1
                else:
                    # Los índices son idénticos (considerando la normalización)
                    idx1, idx2 = indexes1[key], indexes2[key]
                    results.append({
                        'tipo': 'ÍNDICE',
                        'objeto': key,
                        'detalle': 'El índice es idéntico en ambos esquemas (ignorando referencias a esquemas)',
                        'esquema1': idx1['definition'],
                        'esquema2': idx2['definition'],
                        'estado': 'IDÉNTICO',
                        'esquema1_full': idx1['definition'],
                        'esquema2_full': idx2['definition'],
                        'esquema1_normalized': idx1['normalized_definition'],
                        'esquema2_normalized': idx2['normalized_definition']
                    })
                    identical_count += 1
            
            self.log(f"Comparación de índices completada. Se encontraron {diff_count} diferencias y {identical_count} índices idénticos.")
        
        except Exception as e:
            self.log(f"Error al comparar índices: {str(e)}", logging.ERROR)
            traceback_str = traceback.format_exc()
            self.log(f"Detalles del error:\n{traceback_str}", logging.DEBUG)
            raise
        
        return results
    
    def compare_indexes_alternative(self, conn1, conn2):
        """Versión alternativa para comparar índices entre dos esquemas"""
        results = []
        schema1 = self.conn_params1['schema']
        schema2 = self.conn_params2['schema']
        
        try:
            # Enfoque más simple que usa menos joins pero debería ser confiable
            cur1 = conn1.cursor()
            cur1.execute("""
                SELECT 
                    c.relname AS tablename,
                    i.relname AS indexname,
                    pg_get_indexdef(i.oid) AS indexdef
                FROM 
                    pg_catalog.pg_namespace n,
                    pg_catalog.pg_class c,
                    pg_catalog.pg_index x,
                    pg_catalog.pg_class i
                WHERE 
                    c.relkind = 'r' AND
                    i.relkind = 'i' AND
                    n.oid = c.relnamespace AND
                    c.oid = x.indrelid AND
                    i.oid = x.indexrelid AND
                    n.nspname = %s
            """, (schema1,))
            indexes1 = {}
            for row in cur1.fetchall():
                table, name, definition = row
                key = f"{table}.{name}"
                indexes1[key] = {'table': table, 'name': name, 'definition': definition}
            self.log(f"Obtenidos {len(indexes1)} índices del esquema '{schema1}' (método alternativo)")
            
            # Consulta para el segundo esquema
            cur2 = conn2.cursor()
            cur2.execute("""
                SELECT 
                    c.relname AS tablename,
                    i.relname AS indexname,
                    pg_get_indexdef(i.oid) AS indexdef
                FROM 
                    pg_catalog.pg_namespace n,
                    pg_catalog.pg_class c,
                    pg_catalog.pg_index x,
                    pg_catalog.pg_class i
                WHERE 
                    c.relkind = 'r' AND
                    i.relkind = 'i' AND
                    n.oid = c.relnamespace AND
                    c.oid = x.indrelid AND
                    i.oid = x.indexrelid AND
                    n.nspname = %s
            """, (schema2,))
            indexes2 = {}
            for row in cur2.fetchall():
                table, name, definition = row
                key = f"{table}.{name}"
                indexes2[key] = {'table': table, 'name': name, 'definition': definition}
            self.log(f"Obtenidos {len(indexes2)} índices del esquema '{schema2}' (método alternativo)")
            
            # El resto de la función es igual que antes para la comparación
            all_indexes = set(indexes1.keys()) | set(indexes2.keys())
            diff_count = 0
            
            for key in all_indexes:
                if key not in indexes1:
                    idx2 = indexes2[key]
                    results.append({
                        'tipo': 'ÍNDICE',
                        'objeto': key,
                        'detalle': 'El índice existe solo en el segundo esquema',
                        'esquema1': 'No existe',
                        'esquema2': idx2['definition'],
                        'estado': 'DIFERENTE'
                    })
                    diff_count += 1
                elif key not in indexes2:
                    idx1 = indexes1[key]
                    results.append({
                        'tipo': 'ÍNDICE',
                        'objeto': key,
                        'detalle': 'El índice existe solo en el primer esquema',
                        'esquema1': idx1['definition'],
                        'esquema2': 'No existe',
                        'estado': 'DIFERENTE'
                    })
                    diff_count += 1
                elif indexes1[key]['definition'] != indexes2[key]['definition']:
                    idx1, idx2 = indexes1[key], indexes2[key]
                    results.append({
                        'tipo': 'ÍNDICE',
                        'objeto': key,
                        'detalle': 'La definición del índice es diferente',
                        'esquema1': idx1['definition'],
                        'esquema2': idx2['definition'],
                        'estado': 'DIFERENTE DEFINICIÓN'
                    })
                    diff_count += 1
            
            self.log(f"Comparación de índices completada (método alternativo). Se encontraron {diff_count} diferencias.")
        
        except Exception as e:
            self.log(f"Error al comparar índices (método alternativo): {str(e)}", logging.ERROR)
            traceback_str = traceback.format_exc()
            self.log(f"Detalles del error:\n{traceback_str}", logging.DEBUG)
            raise
        
        return results
