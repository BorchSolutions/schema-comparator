# -*- coding: utf-8 -*-
"""
Funciones para la conexión a bases de datos PostgreSQL.
"""

import psycopg2
import logging

# Obtener logger
logger = logging.getLogger('SchemaComparator')

def connect_db(params):
    """
    Conecta a la base de datos PostgreSQL con manejo de errores mejorado.
    
    Args:
        params: Diccionario con parámetros de conexión (host, port, dbname, user, password)
        
    Returns:
        Conexión a la base de datos
        
    Raises:
        Exception: Si hay errores de conexión
    """
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
        logger.error(error_msg)
        raise Exception(error_msg)
    except Exception as e:
        error_msg = f"Error inesperado al conectar a {params['dbname']}: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)

def verify_postgres_versions(conn1, conn2):
    """
    Verificar versiones de PostgreSQL para asegurar compatibilidad.
    
    Args:
        conn1: Conexión a la primera base de datos
        conn2: Conexión a la segunda base de datos
        
    Returns:
        Tupla con las versiones detectadas
    """
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
        
        logger.info(f"PostgreSQL versión 1: {pg_version1}")
        logger.info(f"PostgreSQL versión 2: {pg_version2}")
        
        # Verificar compatibilidad de versiones
        major_version1 = int(pg_version1.split('.')[0])
        major_version2 = int(pg_version2.split('.')[0])
        
        if abs(major_version1 - major_version2) > 1:
            logger.warning(f"Advertencia: Las versiones de PostgreSQL difieren significativamente ({pg_version1} vs {pg_version2}). Pueden ocurrir errores de compatibilidad.")
        
        return (pg_version1, pg_version2)
        
    except Exception as e:
        logger.warning(f"Error al verificar versiones de PostgreSQL: {str(e)}")
        return (None, None)