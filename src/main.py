#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Comparador de Esquemas PostgreSQL
---------------------------------
Esta aplicación permite comparar la estructura de dos esquemas de PostgreSQL.

Autor: Equipo de Desarrollo
Fecha: Febrero 2025
"""

import sys
from PyQt5.QtWidgets import QApplication
from ui.main_window import SchemaComparatorApp
from utils.logging_config import setup_logging

def main():
    # Configurar logging
    setup_logging()
    
    # Iniciar aplicación
    app = QApplication(sys.argv)
    app.setStyle('Fusion')
    ex = SchemaComparatorApp()
    sys.exit(app.exec_())

if __name__ == '__main__':
    main()