# -*- coding: utf-8 -*-
"""
Normalizador de definiciones de esquemas PostgreSQL.
"""

import re

class SchemaNormalizer:
    """Clase para normalizar definiciones de objetos eliminando referencias a esquemas."""
    
    def __init__(self, schema1, schema2):
        """Inicializar con los nombres de los esquemas a normalizar."""
        self.schema1 = schema1
        self.schema2 = schema2
        
        # Detectar automáticamente otros esquemas relacionados
        self.related_schemas = set()
        
        # Extraer prefijos comunes de los esquemas conocidos
        if self.schema1 and len(self.schema1) > 3:
            prefix1 = ''.join([c for c in self.schema1 if not c.isdigit()])
            self.related_schemas.add(prefix1.lower())
        
        if self.schema2 and len(self.schema2) > 3:
            prefix2 = ''.join([c for c in self.schema2 if not c.isdigit()])
            self.related_schemas.add(prefix2.lower())
        
        # Lista de palabras clave SQL
        self.sql_keywords = ['SELECT', 'FROM', 'WHERE', 'JOIN', 'GROUP', 'ORDER', 'HAVING', 
                            'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER', 'DROP',
                            'TABLE', 'VIEW', 'FUNCTION', 'TRIGGER', 'INDEX', 'CONSTRAINT',
                            'PRIMARY', 'FOREIGN', 'KEY', 'REFERENCES', 'NOT', 'NULL',
                            'DEFAULT', 'UNIQUE', 'CHECK', 'RETURNS']
    
    def normalize_definition(self, definition, source_schema):
        """Normaliza una definición SQL eliminando referencias a esquemas específicos"""
        if definition == "No existe":
            return definition
        
        # Normalizar delimitadores de funciones
        normalized = self._normalize_all_dollar_signs(definition)
        
        # Buscar todos los posibles esquemas mencionados en la definición
        mentioned_schemas = self._find_schema_references(normalized)
        
        # Normalizar cada referencia a esquema encontrada
        for schema_name in mentioned_schemas:
            normalized = self._normalize_schema_reference(normalized, schema_name)
        
        # Normalizar explícitamente los esquemas conocidos
        normalized = self._normalize_schema_reference(normalized, self.schema1)
        normalized = self._normalize_schema_reference(normalized, self.schema2)
        
        # Buscar y normalizar otras construcciones específicas de PostgreSQL
        normalized = self._normalize_search_path(normalized)
        normalized = self._normalize_comments(normalized)
        
        return normalized

    def _find_schema_references(self, text):
        """Encuentra todos los posibles nombres de esquema en el texto SQL"""
        import re
        
        # Patrones comunes para nombres de esquema en SQL
        # 1. Patrones como "schema.object" en sentencias SQL
        schema_patterns = [
            # Patrón para SELECT ... FROM schema.table
            r'FROM\s+([a-zA-Z0-9_]+)\.', 
            # Patrón para INSERT INTO schema.table
            r'INTO\s+([a-zA-Z0-9_]+)\.', 
            # Patrón para UPDATE schema.table
            r'UPDATE\s+([a-zA-Z0-9_]+)\.', 
            # Patrón para DELETE FROM schema.table
            r'DELETE\s+FROM\s+([a-zA-Z0-9_]+)\.',
            # Patrón para referencias en JOIN
            r'JOIN\s+([a-zA-Z0-9_]+)\.',
            # Patrón para referencias genéricas schema.object
            r'([a-zA-Z0-9_]+)\.[a-zA-Z0-9_]+'
        ]
        
        schemas = set()
        
        # Buscar todos los patrones y extraer posibles nombres de esquema
        for pattern in schema_patterns:
            matches = re.finditer(pattern, text, re.IGNORECASE)
            for match in matches:
                schema_name = match.group(1)
                # Solo agregar si no es una palabra clave SQL
                if schema_name.upper() not in self.sql_keywords:
                    # Reconocimiento de patrón para esquemas de cliente ERP (como EMP0044PRO, EMP0045PRO)
                    if (schema_name.upper().startswith('EMP') or 
                        any(schema_name.lower().startswith(prefix) for prefix in self.related_schemas)):
                        schemas.add(schema_name)
        
        return schemas

    def _normalize_schema_reference(self, text, schema_name):
        """Normaliza todas las referencias a un esquema específico en el texto SQL"""
        import re
        
        if not schema_name or len(schema_name) < 2:
            return text
        
        # Patrones de reemplazo más específicos para comandos SQL comunes
        patterns = [
            # FROM schema.table
            (r'(FROM\s+)' + re.escape(schema_name) + r'\.', r'\1NORMALIZED_SCHEMA.'),
            # INSERT INTO schema.table
            (r'(INTO\s+)' + re.escape(schema_name) + r'\.', r'\1NORMALIZED_SCHEMA.'),
            # UPDATE schema.table
            (r'(UPDATE\s+)' + re.escape(schema_name) + r'\.', r'\1NORMALIZED_SCHEMA.'),
            # DELETE FROM schema.table
            (r'(DELETE\s+FROM\s+)' + re.escape(schema_name) + r'\.', r'\1NORMALIZED_SCHEMA.'),
            # JOIN schema.table
            (r'(JOIN\s+)' + re.escape(schema_name) + r'\.', r'\1NORMALIZED_SCHEMA.'),
            # Referencias generales schema.object
            (r'([^a-zA-Z0-9_])' + re.escape(schema_name) + r'\.', r'\1NORMALIZED_SCHEMA.'),
            # Al inicio del texto
            (r'^' + re.escape(schema_name) + r'\.', r'NORMALIZED_SCHEMA.')
        ]
        
        # Aplicar cada patrón de reemplazo
        result = text
        for pattern, replacement in patterns:
            result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
        
        return result

    def _normalize_all_dollar_signs(self, text):
        """Normaliza todos los símbolos de dólar en funciones PostgreSQL"""
        import re
        
        # 1. Primero, manejar los delimitadores de funciones completos (AS $tag$ ... $tag$)
        function_body_pattern = r'(AS|LANGUAGE\s+[a-zA-Z_]+)\s+(\$[^\$]*\$)(.*?)(\$[^\$]*\$)'
        
        def replace_delimiters(match):
            prefix = match.group(1)
            body = match.group(3)
            return f"{prefix} $$" + body + "$$"
        
        text = re.sub(function_body_pattern, replace_delimiters, text, flags=re.DOTALL | re.IGNORECASE)
        
        # 2. Eliminar símbolos $ sueltos al inicio de la definición de función
        standalone_dollar_pattern = r'^(\s*|\n*)\$+'
        text = re.sub(standalone_dollar_pattern, '', text)
        
        # 3. Normalizar delimitadores de dólar que aparecen solos al final
        standalone_end_dollar_pattern = r'\$+(\s*|\n*)$'
        text = re.sub(standalone_end_dollar_pattern, '', text)
        
        # 4. Normalizar otras combinaciones de delimitadores con etiquetas
        tagged_dollar_pattern = r'\$([a-zA-Z0-9_]*)\$(.*?)\$\1\$'
        
        def replace_tagged_dollars(match):
            body = match.group(2)
            return "$$" + body + "$$"
        
        return re.sub(tagged_dollar_pattern, replace_tagged_dollars, text, flags=re.DOTALL)

    def _normalize_function_delimiters(self, text):
        """Normaliza los delimitadores de funciones PostgreSQL ($tag$) a una forma estándar ($$)"""
        import re
        
        # Patrón para identificar los delimitadores de funciones PostgreSQL
        # Captura tanto el inicio como el final de los delimitadores
        function_body_pattern = r'(AS|LANGUAGE\s+[a-zA-Z_]+)\s+(\$[^\$]*\$)(.*?)(\$[^\$]*\$)'
        
        def replace_delimiters(match):
            # Mantener la parte inicial (AS o LANGUAGE)
            prefix = match.group(1)
            # Mantener el cuerpo de la función
            body = match.group(3)
            # Usar delimitador estándar
            return f"{prefix} $$" + body + "$$"
        
        # Reemplazar usando expresión regular
        return re.sub(function_body_pattern, replace_delimiters, text, flags=re.DOTALL | re.IGNORECASE)
    
    def _replace_schema_references(self, text, schema_name):
        """Reemplazar referencias a un esquema específico en el texto SQL"""
        import re
        
        # Patrón para identificar referencias a esquemas que sean palabras completas
        # y evitar reemplazar subcadenas de otros identificadores
        pattern = r'\b' + re.escape(schema_name) + r'\.'
        
        # Reemplazar con un marcador genérico consistente
        return re.sub(pattern, 'NORMALIZED_SCHEMA.', text)
    
    def _normalize_search_path(self, text):
        """Normalizar declaraciones de search_path"""
        import re
        
        # Normalizar SET search_path
        pattern = r'SET\s+search_path\s*=\s*[^;,]+[;,]'
        return re.sub(pattern, 'SET search_path = NORMALIZED_SCHEMA;', text, flags=re.IGNORECASE)
    
    def _normalize_comments(self, text):
        """Normalizar comentarios que pueden contener nombres de esquemas"""
        import re
        
        # Tratar de preservar comentarios pero normalizar esquemas dentro de ellos
        # Esto es más complejo y quizás requiera un parser más sofisticado
        
        # Por ahora, una aproximación simple:
        # Buscar comentarios de línea (--) y de bloque (/* */)
        line_comment_pattern = r'--.*?$'
        block_comment_pattern = r'/\*.*?\*/'
        
        # Función para reemplazar esquemas en comentarios
        def normalize_comment(comment):
            result = comment
            result = self._normalize_schema_reference(result, self.schema1)
            result = self._normalize_schema_reference(result, self.schema2)
            return result
        
        # Normalizar comentarios de línea
        lines = text.split('\n')
        for i, line in enumerate(lines):
            # Buscar comentarios de línea
            comment_start = line.find('--')
            if comment_start >= 0:
                # Separar código y comentario
                code = line[:comment_start]
                comment = line[comment_start:]
                # Normalizar el comentario y reconstruir la línea
                lines[i] = code + normalize_comment(comment)
        
        # Reconstruir el texto con comentarios de línea normalizados
        text = '\n'.join(lines)
        
        # Normalizar comentarios de bloque (esto es más complejo y puede requerir
        # un parser real para hacerlo correctamente, esta es una aproximación)
        def replace_block_comment(match):
            return normalize_comment(match.group(0))
        
        return re.sub(block_comment_pattern, replace_block_comment, text, flags=re.DOTALL)
