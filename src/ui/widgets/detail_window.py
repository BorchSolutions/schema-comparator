# ui/widgets/detail_window.py
import difflib
from PyQt5.QtWidgets import (QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
                           QLabel, QPushButton, QComboBox, QSplitter, QTextEdit)
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QFont, QIcon

class DetailWindow(QMainWindow):
    """Ventana separada para mostrar detalles de la comparación."""
    
    def __init__(self, tipo, objeto, detalle, estado, 
                esquema1, esquema2, text1, text2, parent=None):
        super().__init__(parent)
        self.tipo = tipo
        self.objeto = objeto
        self.detalle = detalle
        self.estado = estado
        self.esquema1 = esquema1
        self.esquema2 = esquema2
        self.text1 = text1
        self.text2 = text2
        
        self.init_ui()

    def init_ui(self):
        """Inicializa la interfaz de usuario."""
        self.setWindowTitle(f"{self.tipo}: {self.objeto} - Comparación Detallada")
        self.setGeometry(100, 100, 1200, 900)  # Mayor altura
        
        # Widget central
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        main_layout.setContentsMargins(5, 5, 5, 5)  # Reducir márgenes
        main_layout.setSpacing(5)  # Reducir espaciado
        
        # Encabezado con título y controles - HACERLO MÁS COMPACTO
        header = QWidget()
        header_layout = QHBoxLayout(header)
        header_layout.setContentsMargins(0, 0, 0, 5)  # Reducir márgenes
        
        # Título más compacto
        title_estado = QLabel(f"{self.tipo}: {self.objeto} - <span style='color: {'#e74c3c' if 'DIFERENTE' in self.estado else '#27ae60'};'>{self.estado}</span>")
        title_estado.setFont(QFont("Segoe UI", 10, QFont.Bold))  # Fuente más pequeña
        
        # Selector de modo
        mode_label = QLabel("Modo:")
        self.view_mode = QComboBox()
        self.view_mode.addItems(["Vista Clásica", "Vista Lado a Lado", "Solo Diferencias"])
        self.view_mode.currentIndexChanged.connect(self.change_view_mode)
        
        # Añadir widgets al layout de forma más compacta
        header_layout.addWidget(title_estado)
        header_layout.addStretch()
        header_layout.addWidget(mode_label)
        header_layout.addWidget(self.view_mode)
        
        # Detalle en la barra de estado en lugar de encabezado
        self.statusBar().showMessage(f"Detalle: {self.detalle}")
        self.statusBar().setStyleSheet("font-size: 10px;")
        # Añadir encabezado al layout principal
        main_layout.addWidget(header)
        
        # Contenedor principal para el contenido - DARLE MÁS ESPACIO
        self.content_widget = QWidget()
        content_layout = QVBoxLayout(self.content_widget)
        content_layout.setContentsMargins(0, 0, 0, 0)  # Eliminar márgenes
        
        # Panel de visualización - MAXIMIZAR ALTURA
        self.detail_view = QSplitter(Qt.Horizontal)
        
        # Panel izquierdo (Esquema 1)
        self.detail_left = QTextEdit()
        self.detail_left.setReadOnly(True)
        self.detail_left.setStyleSheet("""
            background-color: white;
            border: 1px solid #ddd;
            border-radius: 4px;
            padding: 10px;
            font-family: Consolas, monospace;
            font-size: 12px;
        """)
        
        # Panel derecho (Esquema 2)
        self.detail_right = QTextEdit()
        self.detail_right.setReadOnly(True)
        self.detail_right.setStyleSheet("""
            background-color: white;
            border: 1px solid #ddd;
            border-radius: 4px;
            padding: 10px;
            font-family: Consolas, monospace;
            font-size: 12px;
        """)
        
        # Configurar proporciones iniciales
        self.detail_view.addWidget(self.detail_left)
        self.detail_view.addWidget(self.detail_right)
        self.detail_view.setSizes([500, 500])
        
        # Añadir vista de detalles al layout
        content_layout.addWidget(self.detail_view)
        
        # Añadir contenido al layout principal - ASIGNAR MÁS ESPACIO VERTICAL
        main_layout.addWidget(self.content_widget, 1)  # El 1 es el factor de estiramiento
        
        # Mostrar detalles (por defecto en vista lado a lado)
        self.change_view_mode(1)  # Vista Lado a Lado    

    def showMaximized(self):
        """Muestra la ventana maximizada."""
        super().showMaximized()

    def normalize_text_format(self, text):  # Asegúrate de que tenga 'self' como primer parámetro
        """Normaliza el formato del texto para asegurar consistencia"""
        if text == "No existe":
            return text
            
        # 1. Convertir todos los saltos de línea a un formato estándar
        text = text.replace('\r\n', '\n').replace('\r', '\n')
        
        # 2. Eliminar líneas vacías consecutivas (más de 2)
        while '\n\n\n' in text:
            text = text.replace('\n\n\n', '\n\n')
        
        # 3. Eliminar espacios al final de cada línea
        lines = text.split('\n')
        lines = [line.rstrip() for line in lines]
        return '\n'.join(lines)
               
    def change_view_mode(self, index):
        """Cambia el modo de visualización de detalles."""
        # Normalizar textos para que tengan formato consistente
        text1 = self.normalize_text_format(self.text1)
        text2 = self.normalize_text_format(self.text2)
        if index == 0:  # Vista Clásica
            html_clasico = f"""
            <h3>{self.tipo}: {self.objeto}</h3>
            <p><b>Estado:</b> <span style="color:{'#e74c3c' if 'DIFERENTE' in self.estado else '#27ae60'};">
            {self.estado}</span></p>
            <p><b>Detalle:</b> {self.detalle}</p>
            <hr>
            <h4>Esquema 1:</h4>
            <pre style="background-color:#f9f9f9; padding:10px; border:1px solid #ddd; margin-bottom:15px; 
                    font-family: monospace; white-space: pre; overflow-x: auto;">
    {text1 if text1 != 'No existe' else '<span style="color:red;">No existe</span>'}
    </pre>
            <h4>Esquema 2:</h4>
            <pre style="background-color:#f9f9f9; padding:10px; border:1px solid #ddd; margin-bottom:15px; 
                    font-family: monospace; white-space: pre; overflow-x: auto;">
    {text2 if text2 != 'No existe' else '<span style="color:red;">No existe</span>'}
    </pre>
            """
            self.detail_left.setHtml(html_clasico)
            self.detail_right.setVisible(False)
            
        elif index == 1:  # Vista Lado a Lado
            # Contenido panel izquierdo (Esquema 1)
            html_izquierdo = f"""
            <h3>Esquema 1</h3>
            <p><b>Estado:</b> <span style="color:{'#e74c3c' if 'DIFERENTE' in self.estado else '#27ae60'};">
            {self.estado}</span></p>
            <p><b>Detalle:</b> {self.detalle}</p>
            <hr>
            <pre style="background-color:#f9f9f9; padding:10px; border:1px solid #ddd; 
                    font-family: monospace; white-space: pre; overflow-x: auto;">
    {text1 if text1 != 'No existe' else '<span style="color:red;">No existe</span>'}
    </pre>
            """
            
            # Contenido panel derecho (Esquema 2)
            html_derecho = f"""
            <h3>Esquema 2</h3>
            <p><b>Estado:</b> <span style="color:{'#e74c3c' if 'DIFERENTE' in self.estado else '#27ae60'};">
            {self.estado}</span></p>
            <p><b>Detalle:</b> {self.detalle}</p>
            <hr>
            <pre style="background-color:#f9f9f9; padding:10px; border:1px solid #ddd; 
                    font-family: monospace; white-space: pre; overflow-x: auto;">
    {text2 if text2 != 'No existe' else '<span style="color:red;">No existe</span>'}
    </pre>
            """
            
            # Actualizar paneles
            self.detail_left.setHtml(html_izquierdo)
            self.detail_right.setHtml(html_derecho)
            self.detail_right.setVisible(True)
            
        elif index == 2:  # Solo Diferencias
            # Crear HTML con diferencias resaltadas
            d = difflib.HtmlDiff()
            diff_html = d.make_file(
                self.text1.splitlines() if self.text1 != 'No existe' else [''],
                self.text2.splitlines() if self.text2 != 'No existe' else [''],
                fromdesc="Esquema 1",
                todesc="Esquema 2",
                context=True
            )
            
            # Agregar estilos y encabezado
            diff_html = f"""
            <style>
                .diff_header {{background-color: #f0f0f0;}}
                td.diff_header {{text-align:right;}}
                .diff_add {{background-color: #aaffaa;}}
                .diff_chg {{background-color: #ffff77;}}
                .diff_sub {{background-color: #ffaaaa;}}
            </style>
            {diff_html}
            """
            
            self.detail_left.setHtml(diff_html)
            self.detail_right.setVisible(False)