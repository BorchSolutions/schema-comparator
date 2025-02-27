# -*- coding: utf-8 -*-
"""
Widget de texto para mostrar logs en la interfaz.
"""

import logging
from PyQt5.QtCore import pyqtSignal, QObject
from PyQt5.QtWidgets import QTextEdit

class QTextEditLogger(QObject, logging.Handler):
    """Redirige mensajes de log a un widget QTextEdit."""
    
    log_signal = pyqtSignal(str)

    def __init__(self, parent):
        super().__init__()
        self.widget = QTextEdit(parent)
        self.widget.setReadOnly(True)
        self.log_signal.connect(self.widget.append)
        self.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

    def emit(self, record):
        """Emite el registro de log al widget."""
        self.log_signal.emit(self.format(record))