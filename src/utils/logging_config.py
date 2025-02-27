import logging
import os

def setup_logging():
    """Configuración centralizada del sistema de logging"""
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Crear directorio de logs si no existe
    os.makedirs('logs', exist_ok=True)
    
    # Configuración básica
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.FileHandler('logs/schema_comparator.log'),
            logging.StreamHandler()
        ]
    )
    
    # Obtener logger principal
    logger = logging.getLogger('SchemaComparator')
    
    return logger