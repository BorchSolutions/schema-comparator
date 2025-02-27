import sys
from cx_Freeze import setup, Executable
import os

# Ruta base del proyecto
base_path = os.path.abspath(os.path.dirname(__file__))

# Dependencias a incluir
build_exe_options = {
    "packages": [
        # Módulos estándar de Python
        "os", 
        "sys", 
        "re",
        "logging",
        "datetime",
        "traceback",
        "difflib",
        
        # Módulos de interfaz y gráficos
        "PyQt5.QtWidgets",
        "PyQt5.QtCore",
        "PyQt5.QtGui",
        "PyQt5.QtWebEngineWidgets",
        
        # Módulos personalizados de tu proyecto
        "core",
        "ui",
        "utils",

        # Dependencias adicionales basadas en el log de módulos faltantes
        "pandas.io",
        "pandas.core",
        "pandas.compat",
        "numpy",
        "pygments.lexers",
        "pygments.formatters",
        "openpyxl",
    ],
    "include_files": [
        # Incluir recursos adicionales si es necesario
        # (os.path.join(base_path, 'recursos'), 'recursos'),
    ],
    "excludes": [
        # Módulos a excluir
        "tkinter",
        "unittest",
        "test",
        "matplotlib",
        "scipy",
        "numba",
    ],
    "include_msvcr": True,  # Para Windows
    "zip_include_packages": ["*"],
    "zip_exclude_packages": [
        "PyQt5",
        "numpy",
        "pandas",
        "matplotlib",
        "scipy"
    ]
}

# Configuración base para Windows
base = None
if sys.platform == "win32":
    base = "Win32GUI"  # Oculta la consola en Windows para GUI

# Dependencias adicionales 
additional_dependencies = [
    "PyQt5",
    "numpy",
    "pandas",
    "pygments",
    "openpyxl"
]

setup(
    name="SchemaComparator",
    version="0.1",
    description="Comparador de Esquemas",
    options={"build_exe": build_exe_options},
    executables=[
        Executable(
            "main.py",  # Tu script principal
            base=base,
            target_name="schema_comparator",  # Nombre del ejecutable
            icon=None  # Puedes añadir un icono aquí si lo deseas
        )
    ],
    requires=additional_dependencies
)