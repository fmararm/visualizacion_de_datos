import pandas as pd
import numpy as np
import plotnine as p9
import re, requests, subprocess, os
from dagster import asset, Output, MetadataValue

# --- Helpers para IA ---
def get_ia_template(context, description, df):
    # Obtener una muestra del DataFrame para que la IA entienda los datos
    sample_data = df.head(3).to_markdown() if hasattr(df, 'to_markdown') else str(df.head(3))
    df_columns = list(df.columns)

    template_tecnico = """
def generar_plot(df):
    # 1. Preprocesamiento necesario (filtrar, agrupar)
    # 2. El gráfico: plot = (p9.ggplot(df, p9.aes(...)) + ...)
    # 3. Importante: asegurar tipos de datos correctos para plotnine
    return plot
"""
    system_content = (
      "Eres un experto en Data Science con Plotnine/Python. Genera código SIMPLE y ROBUSTO.\n"
      f"Template: {template_tecnico}\n"
      "REGLAS:\n"
      "- Devuelve EXCLUSIVAMENTE el bloque de código Python.\n"
      "- Usa 'p9' para plotnine.\n"
      "- NO incluyas explicaciones.\n"
      "- Prefiere gráficos directos sin transformaciones complejas en la IA."
    )
    user_content = (
      f"Tarea: {description}\n"
      f"Columnas disponibles: {', '.join(df_columns)}\n"
      f"Muestra de datos:\n{sample_data}"
    )
    
    return {
        "model": "ollama/llama3.1:8b",
        "messages": [
            {"role": "system", "content": system_content},
            {"role": "user", "content": user_content}
        ],
        "temperature": 0.1,
        "stream": False
    }

def get_ia_code(context, template):
    url = "http://gpu1.esit.ull.es:4000/v1/chat/completions"
    headers = {"Authorization": "Bearer sk-1234"}
    try:
        response = requests.post(url, json=template, headers=headers, timeout=60)
        response.raise_for_status()
        codigo_raw = response.json()['choices'][0]['message']['content']
        match = re.search(r"```python\s+(.*?)\s+```", codigo_raw, re.DOTALL)
        codigo = match.group(1).strip() if match else codigo_raw.strip()
        return codigo
    except Exception as e:
        context.log.error(f"Error AI: {e}")
        raise e

def render_ia_viz(context, code, df, filename):
    if not code or "def generar_plot" not in code:
        match = re.search(r"def generar_plot.*return plot", code, re.DOTALL)
        if match:
            code = match.group(0)
        else:
            raise ValueError("El modelo de IA no generó la función 'generar_plot'.")

    env = {'p9': p9, 'pd': pd, 'np': np}
    env.update({k: v for k, v in p9.__dict__.items() if not k.startswith('_')})
    
    try:
        exec(code, env)
        plot = env['generar_plot'](df)
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        plot.save(filename, width=10, height=6, dpi=100)
        return filename
    except Exception as e:
        context.log.error(f"Error Render en {filename}: {e}\nCódigo:\n{code}")
        raise e

# --- Pipeline de Datos ---

@asset
def renta_load():
    return pd.read_csv("data/distribucion-renta-canarias.csv")

@asset
def renta_cleaning(renta_load):
    renta = renta_load.copy().drop_duplicates()
    renta = renta.rename(columns={
        "TERRITORIO#es": "region",
        "TIME_PERIOD#es": "year",
        "MEDIDAS#es": "measure",
        "OBS_VALUE": "value"
    })
    renta['year'] = pd.to_numeric(renta['year'].astype(str).str.extract(r'^(\d{4})')[0], errors='coerce')
    renta = renta.dropna(subset=['region', 'year', 'measure', 'value'])
    renta['year'] = renta['year'].astype(int)
    return renta

@asset
def nivel_estudios_load():
    return pd.read_excel("data/nivelestudios.xlsx")

@asset(deps=[nivel_estudios_load])
def nivel_estudios_cleaning(nivel_estudios_load):
    df = nivel_estudios_load.copy()
    df = df.rename(columns={
        "Municipios de 500 habitantes o más": "municipality_raw",
        "Sexo": "sex",
        "Nacionalidad": "nationality",
        "Nivel de estudios en curso": "education_level",
        "Periodo": "year",
        "Total": "total"
    })
    df['municipality_code'] = df['municipality_raw'].astype(str).str.extract(r'^(\d{5})')
    df['municipality'] = df['municipality_raw'].astype(str).str.extract(r'^\d{5}\s+(.*)')
    
    def map_island(code):
        if not isinstance(code, str): return "Canarias"
        if code.startswith("35"):
            code_int = int(code)
            if code_int >= 35500 and code_int <= 35580: return "Lanzarote"
            if code_int >= 35600 and code_int <= 35660: return "Fuerteventura"
            return "Gran Canaria"
        if code.startswith("38"):
            code_int = int(code)
            if code_int >= 38700 and code_int <= 38799: return "La Palma"
            if code_int >= 38800 and code_int <= 38892: return "La Gomera"
            if code_int >= 38900 and code_int <= 38911: return "El Hierro"
            return "Tenerife"
        return "Canarias"

    df['island'] = df['municipality_code'].apply(map_island)
    df['year'] = pd.to_numeric(df['year'].astype(str).str.extract(r'^(\d{4})')[0], errors='coerce')
    df = df.dropna(subset=['municipality_code', 'year', 'total', 'education_level'])
    df['year'] = df['year'].astype(int)
    return df

# --- Visualizaciones Simples ---

# 1. Boxplot de Renta
@asset
def prompt_income_distribution_boxplot(renta_cleaning):
    desc = """
    - Dataset: renta_cleaning
    - Preprocesamiento: Filtrar los datos para el año 2023.
    - Estéticas: 
        * Variable 'measure' mapeada al eje X.
        * Variable 'value' mapeada al eje Y.
        * Variable 'measure' mapeada al color (fill).
    - Geometría: Boxplot (geom_boxplot).
    - Etiquetas: 
        * Título: 'Distribución de Renta por Medida (2023)'.
        * Eje X: 'Medida'.
        * Eje Y: 'Valor'.
    - Principio Gestalt: 
        * Usar colores distintos para cada medida para facilitar la comparación.
    """
    return get_ia_template(None, desc, renta_cleaning)

@asset
def income_distribution_boxplot(context, prompt_income_distribution_boxplot, renta_cleaning):
    code = get_ia_code(context, prompt_income_distribution_boxplot)
    path = render_ia_viz(context, code, renta_cleaning, "plots/income/income_distribution_boxplot.png")
    return Output(path, metadata={"code": MetadataValue.md(f"```python\n{code}\n```")})

# 2. Tendencia de Desempleo (Líneas)
@asset
def prompt_unemployment_trend_by_region(renta_cleaning):
    desc = """
    - Dataset: renta_cleaning
    - Preprocesamiento: Filtrar por 'measure' igual a 'Prestaciones por desempleo' y filtrar por 'region' que pertenezcan exactamente a estas islas: ['Tenerife', 'Gran Canaria', 'Lanzarote', 'Fuerteventura', 'La Palma', 'La Gomera', 'El Hierro']. (Excluir 'Canarias' y municipios).
    - Estéticas: 
        * Variable 'year' mapeada al eje X (como continua o factor).
        * Variable 'value' mapeada al eje Y.
        * Variable 'region' mapeada al color (color).
        * Variable 'region' agrupada (group).
    - Geometría: Línea (geom_line).
    - Etiquetas: 
        * Título: 'Evolución de Prestaciones por Desempleo por Isla'.
        * Eje X: 'Año'.
        * Eje Y: 'Prestaciones por Desempleo'.
    - Principio Gestalt (Continuidad): 
        * Cada isla tiene su propia línea conectada a lo largo de los años.
    """
    return get_ia_template(None, desc, renta_cleaning)

@asset
def unemployment_trend_by_region(context, prompt_unemployment_trend_by_region, renta_cleaning):
    code = get_ia_code(context, prompt_unemployment_trend_by_region)
    path = render_ia_viz(context, code, renta_cleaning, "plots/income/unemployment_trend_by_region.png")
    return Output(path, metadata={"code": MetadataValue.md(f"```python\n{code}\n```")})

# 4. Educación Superior por Isla (Barras)
@asset
def prompt_higher_ed_by_island_bar(nivel_estudios_cleaning):
    desc = """
    - Dataset: nivel_estudios_cleaning
    - Preprocesamiento: Filtrar el año 2023 y 'education_level' que contenga 'Educación superior'.
    - Estéticas: 
        * Variable 'island' mapeada al eje X.
        * Variable 'total' mapeada al eje Y.
        * Variable 'island' mapeada al color (fill).
    - Geometría: Barra (geom_col o geom_bar con stat='identity').
    - Etiquetas: 
        * Título: 'Total de Estudiantes de Educación Superior por Isla (2023)'.
        * Eje X: 'Isla'.
        * Eje Y: 'Total de Estudiantes'.
    - Principio Gestalt: 
        * Usar un color distinto para cada isla o colorear por isla para separarlas visualmente.
    """
    return get_ia_template(None, desc, nivel_estudios_cleaning)

@asset
def higher_ed_by_island_bar(context, prompt_higher_ed_by_island_bar, nivel_estudios_cleaning):
    code = get_ia_code(context, prompt_higher_ed_by_island_bar)
    path = render_ia_viz(context, code, nivel_estudios_cleaning, "plots/education/higher_ed_by_island_bar.png")
    return Output(path, metadata={"code": MetadataValue.md(f"```python\n{code}\n```")})
# 6. Comparativa Educación Superior entre Tenerife y Gran Canaria
@asset
def data_higher_ed_tf_gc(nivel_estudios_cleaning):
    df = nivel_estudios_cleaning.copy()
    # Filtramos ambos para Educación Superior, Total de sexos, en todos sus años
    df = df[(df['sex'] == 'Total') & (df['education_level'] == 'Educación superior') & (df['island'].isin(['Tenerife', 'Gran Canaria']))]
    
    # Sumar por año e isla (ya que cada isla está dividida en los municipios que extrajimos)
    df_grouped = df.groupby(['year', 'island'], as_index=False)['total'].sum()
    return df_grouped

@asset
def prompt_higher_ed_tf_gc_point(data_higher_ed_tf_gc):
    desc = """
    - Dataset: data_higher_ed_tf_gc
    - Preprocesamiento: Ninguno. Usar el DataFrame tal cual está.
    - Estéticas:
        * Variable 'year' mapeada al eje X.
        * Variable 'total' mapeada al eje Y.
        * Variable 'island' mapeada al color (color).
    - Geometría: Gráfico de puntos y líneas (geom_point() + geom_line(aes(group=island))).
    - Título y Ejes: Usa ggtitle("Evolución de Estudiantes en Educación Superior: GC vs TF") y labs(y="Número de estudiantes", x="Año").
    - Tema: theme_minimal().
    """
    return get_ia_template(None, desc, data_higher_ed_tf_gc)

@asset
def higher_ed_tf_gc_point(context, prompt_higher_ed_tf_gc_point, data_higher_ed_tf_gc):
    code = get_ia_code(context, prompt_higher_ed_tf_gc_point)
    path = render_ia_viz(context, code, data_higher_ed_tf_gc, "plots/education/higher_ed_tf_gc_point.png")
    return Output(path, metadata={"code": MetadataValue.md(f"```python\n{code}\n```")})
