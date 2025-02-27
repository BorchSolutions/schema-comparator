from pygments import highlight
from pygments.lexers import SqlLexer
from pygments.formatters import HtmlFormatter
import difflib

class DiffViewer:
    """Clase para generar y mostrar diferencias entre textos"""
    
    @staticmethod
    def generate_diff_html(text1, text2, context_lines=3, title_suffix="", mode="unified"):
        """Genera HTML mostrando las diferencias entre dos textos con opciones de visualización
        
        Args:
            text1: Texto del primer esquema
            text2: Texto del segundo esquema
            context_lines: Número de líneas de contexto para el modo unificado
            title_suffix: Sufijo para el título de la página
            mode: Modo de visualización ('unified', 'side-by-side', 'diff-only')
        """
        if text1 == "No existe" or text2 == "No existe":
            return None  # No podemos comparar si uno no existe
        
        # Aplicar resaltado de sintaxis SQL a los textos
        highlighted1 = highlight(text1, SqlLexer(), HtmlFormatter(noclasses=True))
        highlighted2 = highlight(text2, SqlLexer(), HtmlFormatter(noclasses=True))
        
        # Dividir en líneas
        lines1 = text1.splitlines()
        lines2 = text2.splitlines()
        
        # Generar diferencias
        diff = list(difflib.unified_diff(
            lines1, lines2, 
            fromfile="Esquema 1", 
            tofile="Esquema 2",
            lineterm="",
            n=context_lines
        ))
        
        # Opciones de visualización para el modo seleccionado
        if mode == "diff-only":
            # Mostrar solo las líneas que tienen cambios
            diff_lines = []
            for line in diff:
                if line.startswith('+') or line.startswith('-'):
                    diff_lines.append(line)
            diff = diff_lines
            context_lines = 0
        
        # Convertir a HTML
        html = ["<html><head><style>",
                "body { font-family: Consolas, monospace; }",
                ".diff { white-space: pre-wrap; }",
                ".diff-header { background-color: #f8f8f8; padding: 5px; border-bottom: 1px solid #ddd; }",
                ".diff-add { background-color: #e6ffed; color: #22863a; }",
                ".diff-remove { background-color: #ffeef0; color: #cb2431; }",
                ".diff-line { margin: 0; padding: 1px 5px; }",
                ".side-by-side { display: flex; }",
                ".side-by-side div { flex: 1; padding: 10px; border: 1px solid #ddd; margin: 5px; }",
                ".side-by-side pre { margin: 0; white-space: pre-wrap; }",
                ".tabs { display: flex; border-bottom: 1px solid #ddd; }",
                ".tab { padding: 8px 16px; cursor: pointer; }",
                ".tab.active { border: 1px solid #ddd; border-bottom: none; background-color: white; }",
                ".tab-content { display: none; padding: 15px; }",
                ".tab-content.active { display: block; }",
                ".line-number { color: #999; user-select: none; }",
                ".highlight-line { background-color: #fffbdd; }",
                "</style>",
                "<script>",
                "function switchTab(evt, tabName) {",
                "  const tabs = document.querySelectorAll('.tab-content');",
                "  tabs.forEach(tab => tab.classList.remove('active'));",
                "  const tabButtons = document.querySelectorAll('.tab');",
                "  tabButtons.forEach(button => button.classList.remove('active'));",
                "  document.getElementById(tabName).classList.add('active');",
                "  evt.currentTarget.classList.add('active');",
                "}",
                "</script>",
                "</head><body>",
                "<div class='tabs'>",
                f"<div class='tab active' onclick='switchTab(event, \"unified\")'>Vista Unificada</div>",
                f"<div class='tab' onclick='switchTab(event, \"side-by-side\")'>Vista Lado a Lado</div>",
                "</div>",
                "<div id='unified' class='tab-content active'>",
                f"<h3>Diferencias Detectadas{title_suffix}</h3>",
                "<div class='diff'>"]
        
        for line in diff:
            if line.startswith('---') or line.startswith('+++'):
                html.append(f"<div class='diff-header'>{line}</div>")
            elif line.startswith('+'):
                html.append(f"<div class='diff-line diff-add'>{line}</div>")
            elif line.startswith('-'):
                html.append(f"<div class='diff-line diff-remove'>{line}</div>")
            elif line.startswith('@@'):
                html.append(f"<div class='diff-line diff-info'>{line}</div>")
            else:
                html.append(f"<div class='diff-line'>{line}</div>")
        
        html.append("</div></div>")
        
        # Agregar vista lado a lado con numeración de líneas
        html.append("<div id='side-by-side' class='tab-content'>")
        html.append("<div class='side-by-side'>")
        
        # Columna izquierda (Esquema 1)
        html.append("<div>")
        html.append("<h3>Esquema 1</h3>")
        html.append("<div style='display: flex;'>")
        
        # Números de línea
        html.append("<div class='line-number' style='text-align: right; padding-right: 10px;'>")
        for i in range(1, len(lines1) + 1):
            html.append(f"<div>{i}</div>")
        html.append("</div>")
        
        # Código con resaltado
        html.append("<div style='flex: 1;'>")
        html.append(f"<pre>{highlighted1}</pre>")
        html.append("</div>")
        
        html.append("</div>")
        html.append("</div>")
        
        # Columna derecha (Esquema 2)
        html.append("<div>")
        html.append("<h3>Esquema 2</h3>")
        html.append("<div style='display: flex;'>")
        
        # Números de línea
        html.append("<div class='line-number' style='text-align: right; padding-right: 10px;'>")
        for i in range(1, len(lines2) + 1):
            html.append(f"<div>{i}</div>")
        html.append("</div>")
        
        # Código con resaltado
        html.append("<div style='flex: 1;'>")
        html.append(f"<pre>{highlighted2}</pre>")
        html.append("</div>")
        
        html.append("</div>")
        html.append("</div>")
        
        html.append("</div></div>")
        
        html.append("</body></html>")
        
        return ''.join(html)
    
    @staticmethod
    def get_cleaned_definition(text):
        """Extrae la definición limpia de un objeto SQL"""
        if text == "No existe":
            return text
        
        # Intentar extraer solo la parte de la definición que necesitamos
        # Por ejemplo, de una definición de función completa, extraer solo el cuerpo
        if "CREATE FUNCTION" in text or "CREATE OR REPLACE FUNCTION" in text:
            # Tratar de extraer el cuerpo de la función
            try:
                # Encontrar donde comienza el cuerpo (después de AS o LANGUAGE)
                body_start = text.find("AS $$")
                if body_start == -1:
                    body_start = text.find("as $$")
                
                if body_start != -1:
                    # Extraer desde $$ hasta el final de la definición
                    body_start += 4  # Saltar el "AS $$"
                    body_end = text.rfind("$$")
                    if body_end > body_start:
                        return text[body_start:body_end].strip()
            except:
                pass  # Si hay algún error, devolvemos el texto completo
        
        # Para otras definiciones, devolver el texto tal como está
        return text
