# Comparador de Esquemas PostgreSQL

Una aplicación de escritorio que permite comparar la estructura de dos esquemas de PostgreSQL, identificando diferencias en:
- Tablas y columnas
- Funciones y parámetros
- Vistas
- Constraints e índices
- Foreign Keys

## Características

- Conexión a bases de datos PostgreSQL locales o remotas
- Interfaz gráfica intuitiva con PyQt5
- Visualización de diferencias en código SQL
- Filtros por tipo de objeto y estado de comparación
- Exportación de resultados a Excel, CSV, HTML y JSON
- Normalización de referencias a esquemas para evitar falsos positivos

## Requisitos

- Python 3.6 o superior
- PyQt5 y PyQtWebEngine
- Otras dependencias listadas en requirements.txt

## Instalación

1. Clone el repositorio:
```bash
git clone https://github.com/borchsolutions/schema-comparator.git
cd schema-comparator