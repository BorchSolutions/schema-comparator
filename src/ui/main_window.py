# -*- coding: utf-8 -*-
"""
Ventana principal del Comparador de Esquemas PostgreSQL.
"""

import os
import logging
from datetime import datetime
import traceback
from PyQt5.QtWidgets import (QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
                            QGroupBox, QLabel, QLineEdit, QSpinBox, QCheckBox, 
                            QPushButton, QTabWidget, QTableWidget, QTableWidgetItem, 
                            QHeaderView, QComboBox, QFileDialog, QMessageBox, 
                            QProgressBar, QSplitter, QFrame, QTextEdit, QSlider, 
                            QFormLayout, QShortcut)  # Añade QShortcut aquí
from PyQt5.QtCore import Qt, QSize, QObject  # Quita QShortcut y QKeySequence de aquí
from PyQt5.QtGui import QIcon, QColor, QFont, QPalette, QTextCursor, QKeySequence  # Añade QKeySequence aquí
from PyQt5.QtWebEngineWidgets import QWebEngineView
from PyQt5.QtWidgets import QApplication
from core.schema_normalizer import SchemaNormalizer
from ui.styles import STYLE
from ui.widgets.diff_viewer import DiffViewer
from ui.widgets.log_widget import QTextEditLogger
from core.comparison_worker import ComparisonWorker
from utils.export_utils import (export_to_excel, export_to_csv, 
                                export_to_html, export_to_json)

# Obtener el logger
logger = logging.getLogger('SchemaComparator')

class SchemaComparatorApp(QMainWindow):
    """Ventana principal de la aplicación de comparación de esquemas PostgreSQL."""
    
    def __init__(self):
        super().__init__()
        self.results = []
        
        # Configurar logger para la interfaz
        self.log_handler = QTextEditLogger(self)
        self.log_handler.setLevel(logging.INFO)
        logger.addHandler(self.log_handler)
        
        self.init_ui()
    
    def init_ui(self):
        """Inicializa la interfaz de usuario."""
        self.setWindowTitle("Comparador de Esquemas PostgreSQL")
        self.setGeometry(100, 100, 1200, 800)
        self.setStyleSheet(f"background-color: {STYLE['BACKGROUND_COLOR']};")
        
        # Widget central
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        # Layout principal
        main_layout = QVBoxLayout(central_widget)
        
        # Título de la aplicación
        title_label = QLabel("Comparador de Esquemas PostgreSQL")
        title_label.setFont(QFont("Segoe UI", 16, QFont.Bold))
        title_label.setAlignment(Qt.AlignCenter)
        title_label.setStyleSheet(f"color: {STYLE['SECONDARY_COLOR']}; margin: 10px;")
        
        # Splitter principal (para dividir el área de conexión y el área de resultados)
        main_splitter = QSplitter(Qt.Vertical)
        main_splitter.setHandleWidth(8)  # Manijas más anchas para facilitar el redimensionamiento
        main_splitter.setStyleSheet("""
            QSplitter::handle {
                background-color: #ddd;
                border: 1px solid #ccc;
                border-radius: 2px;
            }
            QSplitter::handle:hover {
                background-color: #bbb;
            }
        """)

        # Área de conexión
        connection_widget = QWidget()
        connection_widget.setMaximumHeight(350)
        connection_layout = QVBoxLayout(connection_widget)
        self.connection_widget = connection_widget
        
        # Formulario de conexión
        form_layout = QHBoxLayout()
        
        # Conexión 1
        connection1_group = QGroupBox("Conexión 1")
        connection1_group.setStyleSheet(f"""
            QGroupBox {{
                font-weight: bold;
                border: 1px solid #ddd;
                border-radius: 6px;
                margin-top: 12px;
                background-color: #f5f5f5;
            }}
            QGroupBox::title {{
                subcontrol-origin: margin;
                left: 10px;
                padding: 0 5px;
                color: {STYLE['PRIMARY_COLOR']};
            }}
        """)
        connection1_form = QFormLayout(connection1_group)
        
        self.host1 = QLineEdit("localhost")
        self.port1 = QSpinBox()
        self.port1.setRange(1, 65535)
        self.port1.setValue(5432)
        self.dbname1 = QLineEdit()
        self.user1 = QLineEdit()
        self.password1 = QLineEdit()
        self.password1.setEchoMode(QLineEdit.Password)
        self.schema1 = QLineEdit()
        
        connection1_form.addRow("Host:", self.host1)
        connection1_form.addRow("Puerto:", self.port1)
        connection1_form.addRow("Base de Datos:", self.dbname1)
        connection1_form.addRow("Usuario:", self.user1)
        connection1_form.addRow("Contraseña:", self.password1)
        connection1_form.addRow("Esquema:", self.schema1)
        
        form_layout.addWidget(connection1_group)
        
        # Conexión 2
        connection2_group = QGroupBox("Conexión 2")
        connection2_group.setStyleSheet(f"""
            QGroupBox {{
                font-weight: bold;
                border: 1px solid #ddd;
                border-radius: 6px;
                margin-top: 12px;
                background-color: #f5f5f5;
            }}
            QGroupBox::title {{
                subcontrol-origin: margin;
                left: 10px;
                padding: 0 5px;
                color: {STYLE['PRIMARY_COLOR']};
            }}
        """)
        connection2_form = QFormLayout(connection2_group)
        
        self.host2 = QLineEdit("localhost")
        self.port2 = QSpinBox()
        self.port2.setRange(1, 65535)
        self.port2.setValue(5432)
        self.dbname2 = QLineEdit()
        self.user2 = QLineEdit()
        self.password2 = QLineEdit()
        self.password2.setEchoMode(QLineEdit.Password)
        self.schema2 = QLineEdit()
        
        connection2_form.addRow("Host:", self.host2)
        connection2_form.addRow("Puerto:", self.port2)
        connection2_form.addRow("Base de Datos:", self.dbname2)
        connection2_form.addRow("Usuario:", self.user2)
        connection2_form.addRow("Contraseña:", self.password2)
        connection2_form.addRow("Esquema:", self.schema2)
        
        form_layout.addWidget(connection2_group)
        
        # Agregar el layout de conexiones
        connection_layout.addLayout(form_layout)
        
        # Botones
        buttons_layout = QHBoxLayout()
        
        self.connect_btn = QPushButton("Comparar Esquemas")
        self.connect_btn.setFont(QFont("Segoe UI", 10, QFont.Bold))
        self.connect_btn.setStyleSheet(f"""
            QPushButton {{
                background-color: {STYLE['PRIMARY_COLOR']};
                color: white;
                border: none;
                padding: 10px 15px;
                border-radius: 4px;
                min-width: 200px;
            }}
            QPushButton:hover {{
                background-color: #2980b9;
            }}
            QPushButton:pressed {{
                background-color: #1c6ea4;
            }}
        """)
        self.connect_btn.clicked.connect(self.start_comparison)
        
        self.export_btn = QPushButton("Exportar Resultados")
        self.export_btn.setFont(QFont("Segoe UI", 10))
        self.export_btn.setStyleSheet(f"""
            QPushButton {{
                background-color: {STYLE['SUCCESS_COLOR']};
                color: white;
                border: none;
                padding: 10px 15px;
                border-radius: 4px;
                min-width: 200px;
            }}
            QPushButton:hover {{
                background-color: #219653;
            }}
            QPushButton:pressed {{
                background-color: #1e874b;
            }}
        """)
        self.export_btn.clicked.connect(self.export_results)
        self.export_btn.setEnabled(False)
        
        buttons_layout.addStretch()
        buttons_layout.addWidget(self.connect_btn)
        buttons_layout.addWidget(self.export_btn)
        buttons_layout.addStretch()
        
        connection_layout.addLayout(buttons_layout)
        
        # Barra de progreso
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.progress_bar.setTextVisible(True)
        self.progress_bar.setVisible(False)
        self.progress_bar.setStyleSheet(f"""
            QProgressBar {{
                border: 1px solid #ddd;
                border-radius: 4px;
                text-align: center;
                height: 20px;
            }}
            QProgressBar::chunk {{
                background-color: {STYLE['PRIMARY_COLOR']};
            }}
        """)
        
        connection_layout.addWidget(self.progress_bar)
        
        # Agregar widget de conexión al splitter
        main_splitter.addWidget(connection_widget)
        
        # Área de resultados y logs
        results_log_tabs = QTabWidget()
        
        # Tab para resultados
        results_widget = QWidget()
        results_layout = QVBoxLayout(results_widget)
        
        # Widget de resumen estadístico
        stats_frame = QFrame()
        stats_frame.setFrameShape(QFrame.StyledPanel)
        stats_frame.setStyleSheet("""
            QFrame {
                border: 1px solid #ddd;
                border-radius: 6px;
                background-color: #f5f5f5;
                padding: 10px;
                margin-top: 5px;
                margin-bottom: 5px;
            }
        """)
        stats_layout = QVBoxLayout(stats_frame)
        
        # Título del panel de estadísticas
        stats_title = QLabel("Resumen de la Comparación")
        stats_title.setFont(QFont("Segoe UI", 12, QFont.Bold))
        stats_title.setAlignment(Qt.AlignCenter)
        stats_title.setStyleSheet("color: #333; margin-bottom: 10px;")
        
        # MODIFICACIÓN 1: Añadir botón para colapsar/expandir el panel de estadísticas
        self.toggle_stats_btn = QPushButton()
        self.toggle_stats_btn.setIcon(QIcon.fromTheme("go-down"))
        self.toggle_stats_btn.setToolTip("Ocultar/Mostrar resumen")
        self.toggle_stats_btn.setFixedSize(24, 24)
        self.toggle_stats_btn.setStyleSheet("""
            QPushButton {
                border: 1px solid #ddd;
                border-radius: 3px;
                background-color: #f5f5f5;
            }
            QPushButton:hover {
                background-color: #e0e0e0;
            }
        """)
        self.toggle_stats_btn.clicked.connect(self.toggle_stats_panel)

        # Crear layout para el título y botón
        stats_header_layout = QHBoxLayout()
        stats_header_layout.addWidget(stats_title)
        stats_header_layout.addStretch()
        stats_header_layout.addWidget(self.toggle_stats_btn)

        # Reemplazar la adición directa del título
        stats_layout.addLayout(stats_header_layout)
        
        # Contenedor para los contadores con barra de progreso
        stats_counters_widget = QWidget()
        stats_counters_layout = QHBoxLayout(stats_counters_widget)
        stats_counters_layout.setSpacing(15)
        
        # Crear contadores para cada categoría
        self.total_counter_widget, self.total_counter, self.total_progress = self.create_counter(
            "Total Objetos", STYLE['SECONDARY_COLOR'], 
            "Número total de objetos comparados entre ambos esquemas")
        
        self.identical_counter_widget, self.identical_counter, self.identical_progress = self.create_counter(
            "Idénticos", STYLE['SUCCESS_COLOR'], 
            "Objetos que son idénticos en ambos esquemas (ignorando referencias a esquemas)")
        
        self.different_counter_widget, self.different_counter, self.different_progress = self.create_counter(
            "Con Diferencias", STYLE['WARNING_COLOR'], 
            "Objetos que existen en ambos esquemas pero tienen diferencias")
        
        self.only_schema1_counter_widget, self.only_schema1_counter, self.only_schema1_progress = self.create_counter(
            "Solo en Origen", STYLE['PRIMARY_COLOR'], 
            "Objetos que solo existen en el esquema origen")
        
        self.only_schema2_counter_widget, self.only_schema2_counter, self.only_schema2_progress = self.create_counter(
            "Solo en Destino", STYLE['DANGER_COLOR'], 
            "Objetos que solo existen en el esquema destino")
        
        # Añadir contadores al layout
        stats_counters_layout.addWidget(self.total_counter_widget)
        stats_counters_layout.addWidget(self.identical_counter_widget)
        stats_counters_layout.addWidget(self.different_counter_widget)
        stats_counters_layout.addWidget(self.only_schema1_counter_widget)
        stats_counters_layout.addWidget(self.only_schema2_counter_widget)
        
        # Añadir layout de contadores al panel de estadísticas
        stats_layout.addWidget(stats_counters_widget)
        
        # Guardar referencia para poder ocultar/mostrar
        self.stats_counters_widget = stats_counters_widget
        
        # Añadir panel de estadísticas al layout principal de resultados
        results_layout.addWidget(stats_frame)
        
        # Filtros
        filter_frame = QFrame()
        filter_frame.setFrameShape(QFrame.StyledPanel)
        filter_frame.setStyleSheet("""
            QFrame {
                border: 1px solid #ddd;
                border-radius: 6px;
                background-color: #f5f5f5;
                padding: 10px;
                margin-top: 10px;
            }
        """)
        
        # Crear un layout principal para los filtros con dos secciones
        filter_main_layout = QVBoxLayout(filter_frame)
        
        # MODIFICACIÓN 2: Añadir botón para colapsar/expandir el panel de filtros
        self.toggle_filters_btn = QPushButton()
        self.toggle_filters_btn.setIcon(QIcon.fromTheme("go-down"))
        self.toggle_filters_btn.setToolTip("Ocultar/Mostrar filtros")
        self.toggle_filters_btn.setFixedSize(24, 24)
        self.toggle_filters_btn.setStyleSheet("""
            QPushButton {
                border: 1px solid #ddd;
                border-radius: 3px;
                background-color: #f5f5f5;
            }
            QPushButton:hover {
                background-color: #e0e0e0;
            }
        """)
        self.toggle_filters_btn.clicked.connect(self.toggle_filters_panel)

        # Crear layout para título y botón
        filters_header = QWidget()
        filters_header_layout = QHBoxLayout(filters_header)
        filters_header_layout.setContentsMargins(5, 5, 5, 0)
        filters_header_layout.addWidget(QLabel("<b>Filtros</b>"))
        filters_header_layout.addStretch()
        filters_header_layout.addWidget(self.toggle_filters_btn)

        # Añadir encabezado al inicio del layout de filtros
        filter_main_layout.addWidget(filters_header)
        
        # Sección 1: Filtros por tipo de objeto
        type_filter_group = QGroupBox("Filtrar por Tipo de Objeto")
        type_filter_group.setStyleSheet("""
            QGroupBox {
                font-weight: bold;
                border: 1px solid #ccc;
                border-radius: 4px;
                margin-top: 8px;
                padding-top: 10px;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 10px;
                padding: 0 5px;
            }
        """)
        type_filter_layout = QHBoxLayout(type_filter_group)
        
        # Checkboxes para tipos de objeto
        self.filter_tables = QCheckBox("Tablas")
        self.filter_columns = QCheckBox("Columnas")
        self.filter_functions = QCheckBox("Funciones")
        self.filter_views = QCheckBox("Vistas")
        self.filter_constraints = QCheckBox("Constraints")
        self.filter_indexes = QCheckBox("Índices")
        self.filter_fks = QCheckBox("Foreign Keys")
        self.filter_params = QCheckBox("Parámetros")
        
        # Botones para seleccionar/deseleccionar todos
        select_all_types_btn = QPushButton("Seleccionar Todos")
        select_all_types_btn.setStyleSheet("padding: 4px 8px;")
        select_all_types_btn.clicked.connect(self.select_all_type_filters)
        
        clear_all_types_btn = QPushButton("Limpiar Todos")
        clear_all_types_btn.setStyleSheet("padding: 4px 8px;")
        clear_all_types_btn.clicked.connect(self.clear_all_type_filters)
        
        # Añadir widgets al layout de tipos
        type_filter_layout.addWidget(QLabel("Mostrar:"))
        type_filter_layout.addWidget(self.filter_tables)
        type_filter_layout.addWidget(self.filter_columns)
        type_filter_layout.addWidget(self.filter_functions)
        type_filter_layout.addWidget(self.filter_views)
        type_filter_layout.addWidget(self.filter_constraints)
        type_filter_layout.addWidget(self.filter_indexes)
        type_filter_layout.addWidget(self.filter_fks)
        type_filter_layout.addWidget(self.filter_params)
        type_filter_layout.addStretch()
        type_filter_layout.addWidget(select_all_types_btn)
        type_filter_layout.addWidget(clear_all_types_btn)
        
        # Sección 2: Filtros por estado de comparación
        status_filter_group = QGroupBox("Filtrar por Estado de Comparación")
        status_filter_group.setStyleSheet("""
            QGroupBox {
                font-weight: bold;
                border: 1px solid #ccc;
                border-radius: 4px;
                margin-top: 8px;
                padding-top: 10px;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 10px;
                padding: 0 5px;
            }
        """)
        status_filter_layout = QHBoxLayout(status_filter_group)
        
        # Checkboxes para estados
        self.filter_identical = QCheckBox("Idénticos")
        self.filter_identical.setToolTip("Objetos que son iguales en ambos esquemas")
        self.filter_identical.setStyleSheet(f"color: {STYLE['SUCCESS_COLOR']};")
        
        self.filter_different = QCheckBox("Con Diferencias")
        self.filter_different.setToolTip("Objetos que existen en ambos esquemas pero tienen diferencias")
        self.filter_different.setStyleSheet(f"color: {STYLE['WARNING_COLOR']};")
        
        self.filter_only_schema1 = QCheckBox("Solo en Origen")
        self.filter_only_schema1.setToolTip("Objetos que solo existen en el esquema de origen")
        self.filter_only_schema1.setStyleSheet(f"color: {STYLE['PRIMARY_COLOR']};")
        
        self.filter_only_schema2 = QCheckBox("Solo en Destino")
        self.filter_only_schema2.setToolTip("Objetos que solo existen en el esquema de destino")
        self.filter_only_schema2.setStyleSheet(f"color: {STYLE['DANGER_COLOR']};")
        
        # Botones para seleccionar/deseleccionar todos los estados
        select_all_states_btn = QPushButton("Seleccionar Todos")
        select_all_states_btn.setStyleSheet("padding: 4px 8px;")
        select_all_states_btn.clicked.connect(self.select_all_status_filters)
        
        clear_all_states_btn = QPushButton("Limpiar Todos")
        clear_all_states_btn.setStyleSheet("padding: 4px 8px;")
        clear_all_states_btn.clicked.connect(self.clear_all_status_filters)
        
        # Añadir widgets al layout de estados
        status_filter_layout.addWidget(QLabel("Estado:"))
        status_filter_layout.addWidget(self.filter_identical)
        status_filter_layout.addWidget(self.filter_different)
        status_filter_layout.addWidget(self.filter_only_schema1)
        status_filter_layout.addWidget(self.filter_only_schema2)
        status_filter_layout.addStretch()
        status_filter_layout.addWidget(select_all_states_btn)
        status_filter_layout.addWidget(clear_all_states_btn)
        
        # Guardar referencias a los grupos de filtros
        self.filter_type_group = type_filter_group
        self.filter_status_group = status_filter_group
        
        # Añadir los grupos de filtros al layout principal
        filter_main_layout.addWidget(type_filter_group)
        filter_main_layout.addWidget(status_filter_group)
        
        # Fila adicional para opciones avanzadas
        advanced_options_layout = QHBoxLayout()
        
        self.normalize_schemas = QCheckBox("Normalizar Referencias a Esquemas")
        self.normalize_schemas.setChecked(True)
        self.normalize_schemas.setToolTip("Ignora diferencias en nombres de esquemas al comparar objetos")
        
        self.show_details_btn = QPushButton("Mostrar Detalles")
        self.show_details_btn.setToolTip("Muestra información detallada del elemento seleccionado")
        self.show_details_btn.clicked.connect(self.show_details)
        self.show_details_btn.setEnabled(False)  # Habilitado cuando se selecciona un elemento
        # Añadir esto junto a la conexión del show_details_btn

        # Añadir widgets a las opciones avanzadas
        advanced_options_layout.addWidget(self.normalize_schemas)
        advanced_options_layout.addStretch()
        advanced_options_layout.addWidget(self.show_details_btn)
        
        # Añadir opciones avanzadas al layout principal
        filter_main_layout.addLayout(advanced_options_layout)
        
        # Activar todos los filtros por defecto
        self.activate_all_filters()
        
        # Conectar cambios en los filtros
        for cb in [self.filter_tables, self.filter_columns, self.filter_functions,
                self.filter_views, self.filter_constraints, self.filter_indexes,
                self.filter_fks, self.filter_params, self.filter_identical,
                self.filter_different, self.filter_only_schema1, self.filter_only_schema2]:
            cb.stateChanged.connect(self.apply_filters)
        
        # Añadir frame de filtros al layout principal
        results_layout.addWidget(filter_frame)
        
        # NUEVO DISEÑO: Panel dividido para resultados y detalles
        results_details_splitter = QSplitter(Qt.Vertical)
        results_details_splitter.setHandleWidth(8)
        results_details_splitter.setStyleSheet("""
            QSplitter::handle {
                background-color: #ddd;
                border: 1px solid #ccc;
                border-radius: 2px;
            }
            QSplitter::handle:hover {
                background-color: #bbb;
            }
        """)
        
        # Guardar referencia al splitter para poder ajustar sus tamaños
        self.results_details_splitter = results_details_splitter
        
        # 1. Tabla de resultados (parte superior)
        self.results_table = QTableWidget()
        self.results_table.setAlternatingRowColors(True)
        self.results_table.setSortingEnabled(True)
        self.results_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.results_table.setSelectionMode(QTableWidget.SingleSelection)
        self.results_table.verticalHeader().setDefaultSectionSize(30)
        self.results_table.itemSelectionChanged.connect(
            lambda: self.open_detail_window_btn.setEnabled(self.results_table.currentRow() >= 0)
        )        
        # Configurar columnas
        self.results_table.setColumnCount(6)
        self.results_table.setHorizontalHeaderLabels(["Tipo", "Objeto", "Detalle", "Esquema 1", "Esquema 2", "Estado"])
        
        # Ajustar anchos iniciales
        self.results_table.setColumnWidth(0, 120)  # Tipo
        self.results_table.setColumnWidth(1, 200)  # Objeto
        self.results_table.setColumnWidth(2, 300)  # Detalle
        self.results_table.setColumnWidth(3, 300)  # Esquema 1
        self.results_table.setColumnWidth(4, 300)  # Esquema 2
        self.results_table.setColumnWidth(5, 120)  # Estado
        
        # Permitir ajustar columnas
        header = self.results_table.horizontalHeader()
        for i in range(6):
            header.setSectionResizeMode(i, QHeaderView.Interactive)
        
        # Estilo para la tabla
        self.results_table.setStyleSheet("""
            QTableWidget {
                gridline-color: #ddd;
                selection-background-color: #e3f2fd;
                alternate-background-color: #fafafa;
            }
            QHeaderView::section {
                background-color: #f2f2f2;
                padding: 8px;
                border: 1px solid #ddd;
                font-weight: bold;
            }
            QTableWidget::item:selected {
                color: black;
            }
        """)
        
        # 2. Panel de detalles (parte inferior)
        detail_container = QWidget()
        detail_layout = QVBoxLayout(detail_container)
        detail_layout.setContentsMargins(0, 10, 0, 0)
        
        # Encabezado con título y controles
        detail_header = QWidget()
        detail_header_layout = QHBoxLayout(detail_header)
        detail_header_layout.setContentsMargins(0, 0, 0, 5)
        
        self.detail_object = QLabel("Seleccione un objeto para ver detalles")
        self.detail_object.setFont(QFont("Segoe UI", 12, QFont.Bold))
        
        # Opciones de visualización
        self.view_options = QComboBox()
        self.view_options.addItems(["Vista Clásica", "Vista Lado a Lado", "Solo Diferencias"])
        self.view_options.setToolTip("Cambiar modo de visualización de detalles")
        self.view_options.currentIndexChanged.connect(self.change_detail_view_mode)
        
        detail_header_layout.addWidget(self.detail_object)
        self.open_detail_window_btn = QPushButton("Abrir en Ventana")
        self.open_detail_window_btn.setToolTip("Abrir detalles en una ventana separada")
        self.open_detail_window_btn.setStyleSheet("""
            QPushButton {
                padding: 5px 10px;
                background-color: #f5f5f5;
                border: 1px solid #ddd;
                border-radius: 3px;
            }
            QPushButton:hover {
                background-color: #e0e0e0;
            }
        """)
        self.open_detail_window_btn.clicked.connect(self.open_detail_window)
        self.open_detail_window_btn.setEnabled(False)
        detail_header_layout.addWidget(self.open_detail_window_btn)        
        detail_header_layout.addStretch()
        detail_header_layout.addWidget(QLabel("Modo:"))
        detail_header_layout.addWidget(self.view_options)
        
        # MODIFICACIÓN 3: Añadir botón para maximizar detalles
        self.maximize_details_btn = QPushButton("Maximizar Vista")
        self.maximize_details_btn.setToolTip("Ocultar todas las secciones para maximizar espacio")
        self.maximize_details_btn.setStyleSheet("""
            QPushButton {
                padding: 5px 10px;
                background-color: #f5f5f5;
                border: 1px solid #ddd;
                border-radius: 3px;
            }
            QPushButton:hover {
                background-color: #e0e0e0;
            }
        """)
        self.maximize_details_btn.clicked.connect(self.maximize_details_view)
        detail_header_layout.addWidget(self.maximize_details_btn)
        
        # Agregar encabezado
        detail_layout.addWidget(detail_header)
        
        # Panel de visualización (puede cambiar según el modo seleccionado)
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
            font-size: 11px;  # Fuente ligeramente más pequeña
            line-height: 1.2;  # Menos espacio entre líneas
        """)
        
        # Panel derecho (Esquema 2)
        self.detail_right = QTextEdit()
        self.detail_right.setReadOnly(True)
        self.detail_right.setStyleSheet("""
            background-color: white;
            border: 1px solid #ddd;
            border-radius: 4px;
            padding: 10px;
        """)
        
        # Configurar proporciones iniciales
        self.detail_view.addWidget(self.detail_left)
        self.detail_view.addWidget(self.detail_right)
        self.detail_view.setSizes([500, 500])  # Distribución equitativa
        
        # Agregar vista de detalles al layout
        detail_layout.addWidget(self.detail_view)
        
        # Añadir tabla y detalles al splitter
        results_details_splitter.addWidget(self.results_table)
        results_details_splitter.addWidget(detail_container)
        results_details_splitter.setSizes([300, 500])  # Proporciones iniciales
        
        # Agregar splitter al layout de resultados
        results_layout.addWidget(results_details_splitter)
        
        # Conectar selección de tabla a mostrar detalles
        self.results_table.itemSelectionChanged.connect(self.show_details)
        
        # Agregar tab de resultados
        results_log_tabs.addTab(results_widget, "Resultados")
        
        # Tab para logs
        log_widget = QWidget()
        log_layout = QVBoxLayout(log_widget)
        log_layout.addWidget(self.log_handler.widget)
        self.log_handler.widget.setStyleSheet("""
            background-color: black;
            color: white;
            font-family: Consolas, monospace;
            padding: 5px;
        """)
        
        # Agregar tab de logs
        results_log_tabs.addTab(log_widget, "Logs")
        
        # Agregar tabs de resultados y logs al splitter
        main_splitter.addWidget(results_log_tabs)
        
        # Configurar proporción del splitter
        main_splitter.setSizes([300, 700])
        
        # Agregar botón para maximizar/restaurar el panel de resultados
        self.maximize_btn = QPushButton()
        self.maximize_btn.setIcon(QIcon.fromTheme("view-fullscreen"))
        self.maximize_btn.setToolTip("Maximizar/Restaurar panel de resultados")
        self.maximize_btn.setFixedSize(24, 24)
        self.maximize_btn.setStyleSheet("""
            QPushButton {
                border: 1px solid #ddd;
                border-radius: 3px;
                background-color: #f5f5f5;
            }
            QPushButton:hover {
                background-color: #e0e0e0;
            }
        """)
        self.maximize_btn.clicked.connect(self.toggle_maximize_results)
        
        # Añadir el botón a la barra de título o a un lugar visible
        title_layout = QHBoxLayout()
        title_layout.addWidget(title_label)
        title_layout.addWidget(self.maximize_btn)
        title_layout.setAlignment(self.maximize_btn, Qt.AlignRight)
        
        # Reemplazar la adición directa del título por este layout
        main_layout.addLayout(title_layout)
        
        # Agregar splitter al layout principal
        main_layout.addWidget(main_splitter)
        
        # Barra de estado
        self.statusBar().setStyleSheet(f"background-color: {STYLE['SECONDARY_COLOR']}; color: white;")
        self.statusBar().showMessage("Listo para comparar esquemas")
        
        # Mostrar la ventana
        self.show()
        
        # Atajo de teclado para maximizar (F11)
        maximize_shortcut = QShortcut(QKeySequence("F11"), self)
        maximize_shortcut.activated.connect(self.maximize_details_view)
        
        # Log inicial
        logger.info("Aplicación iniciada correctamente")

    def open_detail_window(self):
        """Abre una ventana separada con los detalles del elemento seleccionado."""
        # Verificar si hay una fila seleccionada
        row = self.results_table.currentRow()
        if row < 0:
            return
        
        tipo = self.results_table.item(row, 0).text()
        objeto = self.results_table.item(row, 1).text()
        detalle = self.results_table.item(row, 2).text()
        esquema1 = self.results_table.item(row, 3).text()
        esquema2 = self.results_table.item(row, 4).text()
        estado = self.results_table.item(row, 5).text()
        
        # Buscar definiciones completas y normalizadas
        esquema1_full = esquema1
        esquema2_full = esquema2
        text1 = esquema1
        text2 = esquema2
        
        for result in self.results:
            if (result['tipo'] == tipo and 
                result['objeto'] == objeto and 
                result['detalle'] == detalle):
                # Si hay definiciones completas disponibles, usarlas
                if 'esquema1_full' in result and 'esquema2_full' in result:
                    esquema1_full = result['esquema1_full']
                    esquema2_full = result['esquema2_full']
                
                # Si hay definiciones normalizadas disponibles, usarlas
                if 'esquema1_normalized' in result and 'esquema2_normalized' in result:
                    text1 = result['esquema1_normalized']
                    text2 = result['esquema2_normalized']
                
                break
        
        # Importar la clase DetailWindow
        from ui.widgets.detail_window import DetailWindow
        
        # Crear y mostrar la ventana de detalles
        self.detail_window = DetailWindow(
            tipo, objeto, detalle, estado, 
            esquema1, esquema2, text1, text2,
            parent=self
        )
        self.detail_window.showMaximized()
    
    def toggle_stats_panel(self):
        """Alternar la visibilidad del panel de estadísticas"""
        is_visible = self.stats_counters_widget.isVisible()
        self.stats_counters_widget.setVisible(not is_visible)
        
        # Cambiar icono según estado
        if is_visible:
            self.toggle_stats_btn.setIcon(QIcon.fromTheme("go-next"))
            self.toggle_stats_btn.setToolTip("Mostrar resumen")
        else:
            self.toggle_stats_btn.setIcon(QIcon.fromTheme("go-down"))
            self.toggle_stats_btn.setToolTip("Ocultar resumen")

    def toggle_filters_panel(self):
        """Alternar la visibilidad del panel de filtros"""
        is_visible = self.filter_type_group.isVisible() and self.filter_status_group.isVisible()
        self.filter_type_group.setVisible(not is_visible)
        self.filter_status_group.setVisible(not is_visible)
        
        # Cambiar icono según estado
        if is_visible:
            self.toggle_filters_btn.setIcon(QIcon.fromTheme("go-next"))
            self.toggle_filters_btn.setToolTip("Mostrar filtros")
        else:
            self.toggle_filters_btn.setIcon(QIcon.fromTheme("go-down"))
            self.toggle_filters_btn.setToolTip("Ocultar filtros")

    def maximize_details_view(self):
        """Maximizar la vista de detalles ocultando otras secciones"""
        # Obtener estado actual
        is_maximized = not self.stats_counters_widget.isVisible() and not self.filter_type_group.isVisible()
        
        if is_maximized:
            # Restaurar vista normal
            self.stats_counters_widget.setVisible(True)
            self.filter_type_group.setVisible(True)
            self.filter_status_group.setVisible(True)
            self.maximize_details_btn.setText("Maximizar Vista")
            
            # Restaurar tamaños de splitter
            self.results_details_splitter.setSizes([300, 500])
        else:
            # Maximizar vista
            self.stats_counters_widget.setVisible(False)
            self.filter_type_group.setVisible(False)
            self.filter_status_group.setVisible(False)
            self.maximize_details_btn.setText("Restaurar Vista")
            
            # Ajustar tamaños de splitter para dar más espacio a detalles
            self.results_details_splitter.setSizes([200, 600])
        
        # Actualizar estados de botones toggle
        self.toggle_stats_btn.setIcon(QIcon.fromTheme("go-next" if not self.stats_counters_widget.isVisible() else "go-down"))
        self.toggle_filters_btn.setIcon(QIcon.fromTheme("go-next" if not self.filter_type_group.isVisible() else "go-down"))

    # Método para actualizar los contadores con la información actual
    def update_statistics(self):
        """Actualiza los contadores y barras de progreso con la información actual"""
        # Contar objetos por categoría
        total_count = len(self.results)
        identical_count = sum(1 for r in self.results if r['estado'] == 'IDÉNTICO')
        only_schema1_count = sum(1 for r in self.results if r['esquema2'] == 'No existe')
        only_schema2_count = sum(1 for r in self.results if r['esquema1'] == 'No existe')
        different_count = total_count - identical_count - only_schema1_count - only_schema2_count
        
        # Actualizar contadores
        self.total_counter.setText(str(total_count))
        self.identical_counter.setText(str(identical_count))
        self.different_counter.setText(str(different_count))
        self.only_schema1_counter.setText(str(only_schema1_count))
        self.only_schema2_counter.setText(str(only_schema2_count))
        
        # Calcular porcentajes para las barras de progreso
        if total_count > 0:
            identical_pct = (identical_count / total_count) * 100
            different_pct = (different_count / total_count) * 100
            only_schema1_pct = (only_schema1_count / total_count) * 100
            only_schema2_pct = (only_schema2_count / total_count) * 100
        else:
            identical_pct = different_pct = only_schema1_pct = only_schema2_pct = 0
        
        # Actualizar barras de progreso
        self.total_progress.setValue(100)  # Siempre 100%
        self.identical_progress.setValue(int(identical_pct))
        self.different_progress.setValue(int(different_pct))
        self.only_schema1_progress.setValue(int(only_schema1_pct))
        self.only_schema2_progress.setValue(int(only_schema2_pct))

    # Métodos para cambiar la vista de diferencias
    def change_diff_view_mode(self, index):
        """Cambia el modo de visualización de diferencias"""
        # Si hay un elemento seleccionado, actualizar la vista
        if self.results_table.currentRow() >= 0:
            self.show_details()

    def change_diff_zoom(self, value):
        """Cambia el zoom de la vista de diferencias"""
        # Actualizar el zoom del visualizador web
        self.diff_view.setZoomFactor(value / 100)

    # Método para maximizar/restaurar el panel de resultados
    def toggle_maximize_results(self):
        """Alterna entre vista normal y maximizada del panel de resultados"""
        if self.connection_widget.isVisible():
            # Ocultar panel de conexión para maximizar resultados
            self.connection_widget.setVisible(False)
            self.maximize_btn.setIcon(QIcon.fromTheme("view-restore"))
            self.maximize_btn.setToolTip("Restaurar vista normal")
        else:
            # Mostrar panel de conexión para restaurar vista normal
            self.connection_widget.setVisible(True)
            self.maximize_btn.setIcon(QIcon.fromTheme("view-fullscreen"))
            self.maximize_btn.setToolTip("Maximizar panel de resultados")

    # Método de activación de todos los filtros
    def activate_all_filters(self):
        """Activa todos los filtros por defecto"""
        # Activar filtros de tipo
        for cb in [self.filter_tables, self.filter_columns, self.filter_functions,
                  self.filter_views, self.filter_constraints, self.filter_indexes,
                  self.filter_fks, self.filter_params]:
            cb.setChecked(True)
        
        # Activar filtros de estado
        for cb in [self.filter_identical, self.filter_different, 
                  self.filter_only_schema1, self.filter_only_schema2]:
            cb.setChecked(True)

    # Métodos para los botones de selección/deselección
    def select_all_type_filters(self):
        """Selecciona todos los filtros de tipo de objeto"""
        for cb in [self.filter_tables, self.filter_columns, self.filter_functions,
                  self.filter_views, self.filter_constraints, self.filter_indexes,
                  self.filter_fks, self.filter_params]:
            cb.setChecked(True)

    def clear_all_type_filters(self):
        """Deselecciona todos los filtros de tipo de objeto"""
        for cb in [self.filter_tables, self.filter_columns, self.filter_functions,
                  self.filter_views, self.filter_constraints, self.filter_indexes,
                  self.filter_fks, self.filter_params]:
            cb.setChecked(False)

    def select_all_status_filters(self):
        """Selecciona todos los filtros de estado"""
        for cb in [self.filter_identical, self.filter_different, 
                  self.filter_only_schema1, self.filter_only_schema2]:
            cb.setChecked(True)

    def clear_all_status_filters(self):
        """Deselecciona todos los filtros de estado"""
        for cb in [self.filter_identical, self.filter_different, 
                  self.filter_only_schema1, self.filter_only_schema2]:
            cb.setChecked(False)

    def show_details_for_selected(self):
        """Muestra detalles del elemento seleccionado"""
        selected_row = self.results_table.currentRow()
        if selected_row >= 0:  # Verificar que hay una fila seleccionada
            self.show_details()
            # Cambiar a la pestaña de detalles o diferencias según corresponda
            tipo = self.results_table.item(selected_row, 0).text()
            estado = self.results_table.item(selected_row, 5).text()
            
            # Si es un tipo que puede mostrar diferencias y tiene diferencias
            if (tipo in ['FUNCIÓN', 'VISTA', 'ÍNDICE'] and 
                'DIFERENTE' in estado and 
                self.results_table.item(selected_row, 3).text() != 'No existe' and 
                self.results_table.item(selected_row, 4).text() != 'No existe'):
                # Mostrar la pestaña de diferencias
                self.result_tabs.setCurrentIndex(2)  # Índice de la pestaña de diferencias
            else:
                # Mostrar la pestaña de detalles
                self.result_tabs.setCurrentIndex(1)  # Índice de la pestaña de detalles
        else:
            # No hay selección, mostrar mensaje
            QMessageBox.information(self, "Selección requerida", 
                                  "Por favor, seleccione un elemento de la tabla para ver sus detalles.")

    def change_detail_view_mode(self, index):
        """Cambia el modo de visualización de detalles"""
        logger.info(f"Cambiando modo de visualización a: {self.view_options.currentText()}")
        
        # Actualizar la configuración de los paneles según el modo seleccionado
        if index == 0:  # Vista Clásica
            # Ocultar panel derecho y expandir panel izquierdo
            self.detail_right.setVisible(False)
            self.detail_left.setVisible(True)
        elif index == 1:  # Vista Lado a Lado
            # Mostrar ambos paneles
            self.detail_left.setVisible(True)
            self.detail_right.setVisible(True)
            # Distribuir el espacio equitativamente
            self.detail_view.setSizes([500, 500])
        elif index == 2:  # Solo Diferencias
            # Modificar según visualización de diferencias
            self.detail_left.setVisible(True)
            self.detail_right.setVisible(False)
        
        # Forzar actualización de la vista
        self.detail_view.update()
        
        # Actualizar el contenido si hay un elemento seleccionado
        if self.results_table.currentRow() >= 0:
            self.show_details()

    def show_details(self):
        """Muestra los detalles del elemento seleccionado según el modo de visualización seleccionado"""
        selected_items = self.results_table.selectedItems()
        if not selected_items:
            logger.warning("No hay elementos seleccionados")
            return
        
        row = selected_items[0].row()
        logger.info(f"Fila seleccionada: {row}")
        
        tipo = self.results_table.item(row, 0).text()
        objeto = self.results_table.item(row, 1).text()
        detalle = self.results_table.item(row, 2).text()
        esquema1 = self.results_table.item(row, 3).text()
        esquema2 = self.results_table.item(row, 4).text()
        estado = self.results_table.item(row, 5).text()
        
        # Buscar definiciones completas y normalizadas en los resultados originales
        esquema1_full = esquema1
        esquema2_full = esquema2
        esquema1_normalized = None
        esquema2_normalized = None
        
        for result in self.results:
            if (result['tipo'] == tipo and 
                result['objeto'] == objeto and 
                result['detalle'] == detalle):
                # Si hay definiciones completas disponibles, usarlas
                if 'esquema1_full' in result and 'esquema2_full' in result:
                    esquema1_full = result['esquema1_full']
                    esquema2_full = result['esquema2_full']
                
                # Si hay definiciones normalizadas disponibles, usarlas
                if 'esquema1_normalized' in result and 'esquema2_normalized' in result:
                    esquema1_normalized = result['esquema1_normalized']
                    esquema2_normalized = result['esquema2_normalized']
                
                break
        
        # Actualizar título de detalles
        self.detail_object.setText(f"{tipo}: {objeto}")
        
        # Obtener el modo de visualización actual
        current_mode = self.view_options.currentIndex()
        
        try:
            # Normalizar definiciones si es necesario
            if tipo in ['FUNCIÓN', 'VISTA', 'ÍNDICE'] and esquema1 != 'No existe' and esquema2 != 'No existe':
                if esquema1_normalized and esquema2_normalized:
                    text1 = esquema1_normalized
                    text2 = esquema2_normalized
                else:
                    # Si no hay versiones normalizadas, intentar normalizar en el momento
                    normalizer = SchemaNormalizer(self.schema1.text(), self.schema2.text())
                    text1 = normalizer.normalize_definition(esquema1_full, self.schema1.text())
                    text2 = normalizer.normalize_definition(esquema2_full, self.schema2.text())
            else:
                text1 = esquema1
                text2 = esquema2
            
            # Construir HTML según el modo de visualización
            if current_mode == 0:  # Vista Clásica
                html_clasico = f"""
                <h3 style="color:{STYLE['SECONDARY_COLOR']};">{tipo}: {objeto}</h3>
                <p><b>Estado:</b> <span style="color:{STYLE['DANGER_COLOR'] if 'DIFERENTE' in estado else STYLE['SUCCESS_COLOR']};">
                {estado}</span></p>
                <p><b>Detalle:</b> {detalle}</p>
                <hr>
                <h4>Esquema 1:</h4>
                <div style="background-color:#f9f9f9; padding:10px; border:1px solid #ddd; margin-bottom:15px; 
                        font-family: monospace; white-space: pre-wrap;">
                    {text1 if text1 != 'No existe' else '<span style="color:red;">No existe</span>'}
                </div>
                <h4>Esquema 2:</h4>
                <div style="background-color:#f9f9f9; padding:10px; border:1px solid #ddd; margin-bottom:15px; 
                        font-family: monospace; white-space: pre-wrap;">
                    {text2 if text2 != 'No existe' else '<span style="color:red;">No existe</span>'}
                </div>
                """
                self.detail_left.setHtml(html_clasico)
                
            elif current_mode == 1:  # Vista Lado a Lado
                # Contenido panel izquierdo (Esquema 1)
                html_izquierdo = f"""
                <h3>Esquema 1</h3>
                <p><b>Estado:</b> <span style="color:{STYLE['DANGER_COLOR'] if 'DIFERENTE' in estado else STYLE['SUCCESS_COLOR']};">
                {estado}</span></p>
                <p><b>Detalle:</b> {detalle}</p>
                <hr>
                <div style="background-color:#f9f9f9; padding:10px; border:1px solid #ddd; 
                        font-family: monospace; white-space: pre-wrap;">
                    {text1 if text1 != 'No existe' else '<span style="color:red;">No existe</span>'}
                </div>
                """
                
                # Contenido panel derecho (Esquema 2)
                html_derecho = f"""
                <h3>Esquema 2</h3>
                <p><b>Estado:</b> <span style="color:{STYLE['DANGER_COLOR'] if 'DIFERENTE' in estado else STYLE['SUCCESS_COLOR']};">
                {estado}</span></p>
                <p><b>Detalle:</b> {detalle}</p>
                <hr>
                <div style="background-color:#f9f9f9; padding:10px; border:1px solid #ddd; 
                        font-family: monospace; white-space: pre-wrap;">
                    {text2 if text2 != 'No existe' else '<span style="color:red;">No existe</span>'}
                </div>
                """
                
                # Actualizar paneles
                self.detail_left.setHtml(html_izquierdo)
                self.detail_right.setHtml(html_derecho)
                
            elif current_mode == 2:  # Solo Diferencias
                # Para tipos que permiten comparación detallada
                if tipo in ['FUNCIÓN', 'VISTA', 'ÍNDICE'] and 'DIFERENTE' in estado:
                    # Resaltar las diferencias
                    import difflib
                    
                    # Crear HTML con diferencias resaltadas
                    d = difflib.HtmlDiff()
                    diff_html = d.make_file(
                        text1.splitlines() if text1 != 'No existe' else [''],
                        text2.splitlines() if text2 != 'No existe' else [''],
                        fromdesc="Esquema 1",
                        todesc="Esquema 2",
                        context=True
                    )
                    
                    # Agregar estilos y encabezado
                    diff_html = f"""
                    <h3 style="color:{STYLE['SECONDARY_COLOR']};">{tipo}: {objeto} - Diferencias</h3>
                    <p><b>Estado:</b> <span style="color:{STYLE['DANGER_COLOR']};">{estado}</span></p>
                    <p><b>Detalle:</b> {detalle}</p>
                    <hr>
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
                else:
                    # Para objetos sin diferencia o que no permiten comparación detallada
                    html_solo_diff = f"""
                    <h3 style="color:{STYLE['SECONDARY_COLOR']};">{tipo}: {objeto}</h3>
                    <p><b>Estado:</b> <span style="color:{STYLE['DANGER_COLOR'] if 'DIFERENTE' in estado else STYLE['SUCCESS_COLOR']};">
                    {estado}</span></p>
                    <p><b>Detalle:</b> {detalle}</p>
                    <hr>
                    """
                    
                    if estado == 'IDÉNTICO':
                        html_solo_diff += "<p>✅ <b>Objetos idénticos</b> - No hay diferencias para mostrar.</p>"
                    elif esquema1 == 'No existe':
                        html_solo_diff += "<p>⚠️ <b>Solo existe en Esquema 2</b></p>"
                    elif esquema2 == 'No existe':
                        html_solo_diff += "<p>⚠️ <b>Solo existe en Esquema 1</b></p>"
                    else:
                        html_solo_diff += f"""
                        <p>Diferencias detectadas pero no se pueden visualizar para este tipo de objeto.</p>
                        <h4>Esquema 1:</h4>
                        <div style="background-color:#f9f9f9; padding:10px; border:1px solid #ddd; margin-bottom:15px;">
                            {text1}
                        </div>
                        <h4>Esquema 2:</h4>
                        <div style="background-color:#f9f9f9; padding:10px; border:1px solid #ddd;">
                            {text2}
                        </div>
                        """
                    
                    self.detail_left.setHtml(html_solo_diff)
            
            logger.info(f"Detalles mostrados para {tipo}: {objeto} en modo: {self.view_options.currentText()}")
            
        except Exception as e:
            logger.error(f"Error al mostrar detalles: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
        # Añadir esta línea al final del método show_details
        self.open_detail_window_btn.setEnabled(True)
    def create_counter(self, title, color, tooltip=""):
        """Función auxiliar para crear un contador con barra de progreso"""
        counter_widget = QWidget()
        counter_layout = QVBoxLayout(counter_widget)
        counter_layout.setContentsMargins(5, 5, 5, 5)
        
        # Título
        counter_title = QLabel(title)
        counter_title.setAlignment(Qt.AlignCenter)
        counter_title.setStyleSheet(f"color: {color}; font-weight: bold;")
        counter_layout.addWidget(counter_title)
        
        # Contador
        counter_value = QLabel("0")
        counter_value.setAlignment(Qt.AlignCenter)
        counter_value.setFont(QFont("Segoe UI", 18, QFont.Bold))
        counter_value.setStyleSheet(f"color: {color};")
        counter_layout.addWidget(counter_value)
        
        # Barra de progreso
        progress_bar = QProgressBar()
        progress_bar.setRange(0, 100)
        progress_bar.setValue(0)
        progress_bar.setTextVisible(False)
        progress_bar.setFixedHeight(8)
        progress_bar.setStyleSheet(f"""
            QProgressBar {{
                border: none;
                border-radius: 4px;
                background-color: #e0e0e0;
            }}
            QProgressBar::chunk {{
                background-color: {color};
                border-radius: 4px;
            }}
        """)
        counter_layout.addWidget(progress_bar)
        
        # Añadir tooltip si se proporciona
        if tooltip:
            counter_widget.setToolTip(tooltip)
        
        return counter_widget, counter_value, progress_bar

    def export_results(self):
        if not self.results:
            QMessageBox.warning(self, "Sin Resultados", 
                               "No hay resultados para exportar.")
            return
        
        try:
            # Diálogo para seleccionar el formato
            format_combo = QComboBox()
            format_combo.addItems(["Excel (.xlsx)", "CSV (.csv)", "HTML (.html)", "JSON (.json)"])
            
            dialog = QMessageBox(self)
            dialog.setWindowTitle("Formato de Exportación")
            dialog.setText("Seleccione el formato para exportar los resultados:")
            dialog.setIcon(QMessageBox.Question)
            dialog.addButton(QPushButton("Cancelar"), QMessageBox.RejectRole)
            dialog.addButton(QPushButton("Exportar"), QMessageBox.AcceptRole)
            layout = dialog.layout()
            layout.addWidget(format_combo, 1, 1)
            
            if dialog.exec_() == QMessageBox.AcceptRole:
                selected_format = format_combo.currentText()
                
                # Filtrar resultados según los filtros aplicados
                filtered_results = []
                for i in range(self.results_table.rowCount()):
                    row_data = {
                        'tipo': self.results_table.item(i, 0).text(),
                        'objeto': self.results_table.item(i, 1).text(),
                        'detalle': self.results_table.item(i, 2).text(),
                        'esquema1': self.results_table.item(i, 3).text(),
                        'esquema2': self.results_table.item(i, 4).text(),
                        'estado': self.results_table.item(i, 5).text()
                    }
                    filtered_results.append(row_data)
                
                # Obtener la ruta para guardar el archivo
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                default_filename = f"comparacion_esquemas_{timestamp}"
                
                if "Excel" in selected_format:
                    file_path, _ = QFileDialog.getSaveFileName(
                        self, "Guardar como Excel", default_filename, "Excel Files (*.xlsx)")
                    if file_path:
                        self.export_to_excel(file_path, filtered_results)
                
                elif "CSV" in selected_format:
                    file_path, _ = QFileDialog.getSaveFileName(
                        self, "Guardar como CSV", default_filename, "CSV Files (*.csv)")
                    if file_path:
                        self.export_to_csv(file_path, filtered_results)
                
                elif "HTML" in selected_format:
                    file_path, _ = QFileDialog.getSaveFileName(
                        self, "Guardar como HTML", default_filename, "HTML Files (*.html)")
                    if file_path:
                        self.export_to_html(file_path, filtered_results)
                
                elif "JSON" in selected_format:
                    file_path, _ = QFileDialog.getSaveFileName(
                        self, "Guardar como JSON", default_filename, "JSON Files (*.json)")
                    if file_path:
                        self.export_to_json(file_path, filtered_results)
        
        except Exception as e:
            logger.error(f"Error en la exportación: {str(e)}")
            QMessageBox.critical(self, "Error de Exportación", 
                               f"Error al exportar: {str(e)}")

    def start_comparison(self):
        try:
            # Validar campos obligatorios
            for field, name in [(self.dbname1, "Base de Datos 1"), (self.user1, "Usuario 1"),
                                (self.dbname2, "Base de Datos 2"), (self.user2, "Usuario 2"),
                                (self.schema1, "Esquema 1"), (self.schema2, "Esquema 2")]:
                if not field.text():
                    QMessageBox.warning(self, "Campo Obligatorio", 
                                       f"El campo {name} es obligatorio.")
                    return
            
            # Recopilar datos de conexión
            conn_params1 = {
                'host': self.host1.text(),
                'port': self.port1.value(),
                'dbname': self.dbname1.text(),
                'user': self.user1.text(),
                'password': self.password1.text(),
                'schema': self.schema1.text(),
                'normalize_schemas': self.normalize_schemas.isChecked()  # Pasar el estado del checkbox
            }
            
            conn_params2 = {
                'host': self.host2.text(),
                'port': self.port2.value(),
                'dbname': self.dbname2.text(),
                'user': self.user2.text(),
                'password': self.password2.text(),
                'schema': self.schema2.text(),
                'normalize_schemas': self.normalize_schemas.isChecked()  # Pasar el estado del checkbox
            }
            
            # Limpiar tabla de resultados
            self.results_table.setRowCount(0)
            self.results = []
            self.export_btn.setEnabled(False)
            
            logger.info(f"Iniciando comparación entre {conn_params1['schema']} y {conn_params2['schema']}")
            
            # Iniciar el proceso de comparación en un hilo separado
            self.worker = ComparisonWorker(conn_params1, conn_params2)
            self.worker.progress_signal.connect(self.update_progress)
            self.worker.result_signal.connect(self.show_results)
            self.worker.error_signal.connect(self.show_error)
            self.worker.completed_signal.connect(self.comparison_completed)
            self.worker.log_signal.connect(self.handle_worker_log)
            
            # Mostrar la barra de progreso
            self.progress_bar.setValue(0)
            self.progress_bar.setVisible(True)
            self.statusBar().showMessage("Comparando esquemas...")
            
            # Deshabilitar botón de conexión durante la comparación
            self.connect_btn.setEnabled(False)
            
            # Iniciar hilo de comparación
            self.worker.start()
            
        except Exception as e:
            logger.error(f"Error al iniciar comparación: {str(e)}")
            QMessageBox.critical(self, "Error", f"Error al iniciar comparación: {str(e)}")
    
    def handle_worker_log(self, message, level):
        pass
    
    def update_progress(self, value):
        self.progress_bar.setValue(value)
        
    def show_results(self, results):
        self.results = results
        self.apply_filters()
        self.export_btn.setEnabled(True)
        
        # Actualizar estadísticas
        self.update_statistics()
    
    def show_error(self, error_msg):
        self.progress_bar.setVisible(False)
        self.connect_btn.setEnabled(True)
        QMessageBox.critical(self, "Error", f"Error en la comparación: {error_msg}")
        self.statusBar().showMessage("Error en la comparación")
    
    def comparison_completed(self):
        self.progress_bar.setVisible(False)
        self.connect_btn.setEnabled(True)
        self.statusBar().showMessage(f"Comparación completada. Se encontraron {len(self.results)} diferencias.")
    
    def apply_filters(self):
        """Aplicar filtros a los resultados y mostrarlos en la tabla"""
        try:
            # Limpiar tabla
            self.results_table.setRowCount(0)
            
            # Obtener estados de los checkboxes de tipo
            show_tables = self.filter_tables.isChecked()
            show_columns = self.filter_columns.isChecked()
            show_functions = self.filter_functions.isChecked()
            show_views = self.filter_views.isChecked()
            show_constraints = self.filter_constraints.isChecked()
            show_indexes = self.filter_indexes.isChecked()
            show_fks = self.filter_fks.isChecked()
            show_params = self.filter_params.isChecked()
            
            # Obtener estados de los checkboxes de estado
            show_identical = self.filter_identical.isChecked()
            show_different = self.filter_different.isChecked()
            show_only_schema1 = self.filter_only_schema1.isChecked()
            show_only_schema2 = self.filter_only_schema2.isChecked()
            
            # Crear un mapa de tipos a mostrar
            show_types = {
                'TABLA': show_tables,
                'COLUMNA': show_columns,
                'FUNCIÓN': show_functions,
                'VISTA': show_views,
                'CONSTRAINT': show_constraints,
                'ÍNDICE': show_indexes,
                'FOREIGN KEY': show_fks,
                'PARÁMETRO': show_params
            }
            
            # Filtrar resultados
            filtered_results = []
            identical_count = 0
            different_count = 0
            only_schema1_count = 0
            only_schema2_count = 0
            
            for result in self.results:
                # Verificar si el tipo debe mostrarse
                if not show_types.get(result['tipo'], True):
                    continue
                
                # Clasificar el objeto según su estado
                only_in_schema1 = result['esquema2'] == 'No existe'
                only_in_schema2 = result['esquema1'] == 'No existe'
                identical = result['estado'] == 'IDÉNTICO'
                
                different = (not only_in_schema1 and 
                             not only_in_schema2 and 
                             not identical)
                
                # Determinar si el objeto debe mostrarse según los filtros de estado
                show_this_result = False
                
                if identical and show_identical:
                    show_this_result = True
                    identical_count += 1
                elif different and show_different:
                    show_this_result = True
                    different_count += 1
                elif only_in_schema1 and show_only_schema1:
                    show_this_result = True
                    only_schema1_count += 1
                elif only_in_schema2 and show_only_schema2:
                    show_this_result = True
                    only_schema2_count += 1
                
                if not show_this_result:
                    continue
                
                filtered_results.append(result)

            self.results_table.setRowCount(len(filtered_results))
            
            # Definir colores para los diferentes estados
            identical_color = QColor('#e8f5e9')  # Verde claro
            different_color = QColor('#fff8e1')  # Amarillo claro
            only_schema1_color = QColor('#e3f2fd')  # Azul claro
            only_schema2_color = QColor('#ffebee')  # Rojo claro
            
            for i, result in enumerate(filtered_results):
                # Determinar el color según el estado
                only_in_schema1 = result['esquema2'] == 'No existe'
                only_in_schema2 = result['esquema1'] == 'No existe'
                identical = result['estado'] == 'IDÉNTICO'
                
                if identical:
                    row_color = identical_color
                elif only_in_schema1:
                    row_color = only_schema1_color
                elif only_in_schema2:
                    row_color = only_schema2_color
                else:  # different
                    row_color = different_color
                
                # Tipo
                item = QTableWidgetItem(result['tipo'])
                item.setFont(STYLE['NORMAL_FONT'])
                item.setBackground(row_color)
                self.results_table.setItem(i, 0, item)
                
                # Objeto
                item = QTableWidgetItem(result['objeto'])
                item.setFont(STYLE['NORMAL_FONT'])
                item.setBackground(row_color)
                self.results_table.setItem(i, 1, item)
                
                # Detalle
                item = QTableWidgetItem(result['detalle'])
                item.setFont(STYLE['NORMAL_FONT'])
                item.setBackground(row_color)
                self.results_table.setItem(i, 2, item)
                
                # Esquema 1
                item = QTableWidgetItem(result['esquema1'])
                item.setFont(STYLE['NORMAL_FONT'])
                item.setBackground(row_color)
                self.results_table.setItem(i, 3, item)
                
                # Esquema 2
                item = QTableWidgetItem(result['esquema2'])
                item.setFont(STYLE['NORMAL_FONT'])
                item.setBackground(row_color)
                self.results_table.setItem(i, 4, item)
                
                # Estado
                item = QTableWidgetItem(result['estado'])
                item.setFont(STYLE['NORMAL_FONT'])
                item.setBackground(row_color)
                self.results_table.setItem(i, 5, item)
            
            # Habilitar/deshabilitar botón de detalles
            self.show_details_btn.setEnabled(self.results_table.currentRow() >= 0)
            
            # Actualizar barra de estado con conteo
            total_results = len(self.results)
            filtered_count = len(filtered_results)
            
            status_msg = (f"Se muestran {filtered_count} de {total_results} resultados. "
                         f"Idénticos: {identical_count}, Con diferencias: {different_count}, "
                         f"Solo en origen: {only_schema1_count}, Solo en destino: {only_schema2_count}")
            
            self.statusBar().showMessage(status_msg)
            
            logger.info(f"Filtros aplicados: {status_msg}")
            
            # AGREGAR ESTAS LÍNEAS JUSTO AQUÍ ↓
            # Forzar actualización de la tabla
            self.results_table.update()
            self.results_table.resizeColumnsToContents()
            
            # Seleccionar automáticamente la primera fila si hay resultados
            if filtered_results and self.results_table.rowCount() > 0:
                self.results_table.selectRow(0)
                self.show_details_btn.setEnabled(True)
                # Mostrar detalles de la primera fila automáticamente
                self.show_details()

            # Al final de apply_filters()
            if self.results_table.rowCount() > 0:
                # Seleccionar primera fila y forzar mostrar detalles
                self.results_table.selectRow(0)
                row_data = {
                    'tipo': self.results_table.item(0, 0).text(),
                    'objeto': self.results_table.item(0, 1).text()
                }
                logger.info(f"Seleccionando automáticamente primera fila: {row_data['tipo']}: {row_data['objeto']}")
                # Forzar actualización de UI
                QApplication.processEvents()
                # Intentar mostrar detalles explícitamente
                self.show_details()

        except Exception as e:
            logger.error(f"Error al aplicar filtros: {str(e)}")
            traceback_str = traceback.format_exc()
            logger.debug(f"Detalles del error:\n{traceback_str}")
            # Mostrar mensaje al usuario
            QMessageBox.warning(self, "Error", 
                               f"Error al mostrar resultados: {str(e)}\n\nLos resultados están disponibles pero puede haber problemas de visualización.")
        
        # Conectar selección de tabla a habilitar botón de detalles
        try:
            self.results_table.itemSelectionChanged.disconnect()
        except Exception:
            pass  # Si no hay conexiones previas, ignorar el error
        
        # Reconectar el evento de selección
        self.results_table.itemSelectionChanged.connect(
            lambda: self.show_details_btn.setEnabled(self.results_table.currentRow() >= 0)
        )
        # Conectar también para mostrar detalles automáticamente al seleccionar
        self.results_table.itemSelectionChanged.connect(self.show_details)
