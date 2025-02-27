# -*- coding: utf-8 -*-
"""
Utilidades para exportar resultados de la comparación a diferentes formatos.
"""

import json
import pandas as pd
from datetime import datetime
from PyQt5.QtWidgets import QMessageBox, QFileDialog
import logging

# Obtener logger
logger = logging.getLogger('SchemaComparator')

def export_to_excel(parent, file_path, results):
    """
    Exporta los resultados a un archivo Excel.
    
    Args:
        parent: Ventana padre para mostrar mensajes
        file_path: Ruta del archivo a crear
        results: Lista de resultados a exportar
    """
    try:
        df = pd.DataFrame(results)
        writer = pd.ExcelWriter(file_path, engine='xlsxwriter')
        df.to_excel(writer, sheet_name='Resultados', index=False)
        
        # Dar formato al archivo Excel
        workbook = writer.book
        worksheet = writer.sheets['Resultados']
        
        # Formato para encabezados
        header_format = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'valign': 'top',
            'fg_color': '#D7E4BC',
            'border': 1
        })
        
        # Escribir encabezados con formato
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, header_format)
            
        # Autoajustar columnas
        for i, col in enumerate(df.columns):
            column_width = max(df[col].astype(str).map(len).max(), len(col)) + 2
            worksheet.set_column(i, i, column_width)
        
        writer.close()
        QMessageBox.information(parent, "Exportación Exitosa", 
                               f"Los resultados se han exportado correctamente a:\n{file_path}")
    except Exception as e:
        logger.error(f"Error al exportar a Excel: {str(e)}")
        QMessageBox.critical(parent, "Error de Exportación", 
                           f"Error al exportar a Excel: {str(e)}")

def export_to_csv(parent, file_path, results):
    """Exporta los resultados a un archivo CSV."""
    try:
        df = pd.DataFrame(results)
        df.to_csv(file_path, index=False, encoding='utf-8')
        QMessageBox.information(parent, "Exportación Exitosa", 
                               f"Los resultados se han exportado correctamente a:\n{file_path}")
    except Exception as e:
        logger.error(f"Error al exportar a CSV: {str(e)}")
        QMessageBox.critical(parent, "Error de Exportación", 
                           f"Error al exportar a CSV: {str(e)}")

def export_to_html(parent, file_path, results):
    """Exporta los resultados a un archivo HTML."""
    try:
        df = pd.DataFrame(results)
        html_content = df.to_html(index=False, classes='table table-striped', border=0)
        
        # Añadir estilos CSS
        html_full = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <title>Comparación de Esquemas PostgreSQL</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .table {{ width: 100%; border-collapse: collapse; }}
                .table-striped tbody tr:nth-of-type(odd) {{ background-color: rgba(0,0,0,.05); }}
                th {{ text-align: left; background-color: #4CAF50; color: white; }}
                th, td {{ padding: 12px; border-bottom: 1px solid #ddd; }}
            </style>
        </head>
        <body>
            <h1>Comparación de Esquemas PostgreSQL</h1>
            <p>Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            {html_content}
        </body>
        </html>
        """
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(html_full)
        
        QMessageBox.information(parent, "Exportación Exitosa", 
                               f"Los resultados se han exportado correctamente a:\n{file_path}")
    except Exception as e:
        logger.error(f"Error al exportar a HTML: {str(e)}")
        QMessageBox.critical(parent, "Error de Exportación", 
                           f"Error al exportar a HTML: {str(e)}")

def export_to_json(parent, file_path, results):
    """Exporta los resultados a un archivo JSON."""
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=4)
        
        QMessageBox.information(parent, "Exportación Exitosa", 
                               f"Los resultados se han exportado correctamente a:\n{file_path}")
    except Exception as e:
        logger.error(f"Error al exportar a JSON: {str(e)}")
        QMessageBox.critical(parent, "Error de Exportación", 
                           f"Error al exportar a JSON: {str(e)}")