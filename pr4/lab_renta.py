import pandas as pd
import numpy as np
import plotnine as p9
import re, requests, subprocess, os
from dagster import asset, Output, MetadataValue

# --- Helpers para IA ---
def get_ia_template(context, description, df_columns):
    template_tecnico = """
def generar_plot(df):
    # Estructura: plot = (p9.ggplot(df, p9.aes(...)) + ...)
    # return plot
"""
    system_content = (
        "Eres un experto en Plotnine. Traduce la descripción a código.\n"
        f"Template: {template_tecnico}\n"
        "Devuelve exclusivamente el código Python. Usa 'p9' para plotnine."
    )
    user_content = f"Basándote en: {description}\nColumnas: {', '.join(df_columns)}"
    
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
    env = globals().copy()
    env.update({'p9': p9, 'pd': pd})
    env.update({k: v for k, v in p9.__dict__.items() if not k.startswith('_')})
    try:
        exec(code, env)
        plot = env['generar_plot'](df)
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        plot.save(filename, width=10, height=6, dpi=100)
        return filename
    except Exception as e:
        context.log.error(f"Error Render: {e}")
        raise e
# -----------------------

def renta_load():
    renta = pd.read_csv("data/distribucion-renta-canarias.csv")
    return renta

@asset(deps=[renta_load])
def renta_cleaning(renta_load):
    # Start data cleaning
    renta = renta_load.copy()
    renta = renta.drop_duplicates()
    
    # Drop empty columns and redundant code columns
    columns_to_drop = [
        "ESTADO_OBSERVACION#es", 
        "CONFIDENCIALIDAD_OBSERVACION#es",
        "TIME_PERIOD_CODE",
        "MEDIDAS_CODE"
    ]
    renta = renta.drop(columns=columns_to_drop, errors='ignore') # errors='ignore' in case they are already gone or names differ slightly
    
    # Rename columns for better readability
    renta = renta.rename(columns={
        "TERRITORIO#es": "region",
        "TIME_PERIOD#es": "year",
        "MEDIDAS#es": "measure",
        "OBS_VALUE": "value"
    })
    
    # Ensure year is strictly numeric (YYYY)
    # Handle potential "YYYY-MM-DD" strings or other formats
    renta['year'] = pd.to_numeric(renta['year'].astype(str).str.extract(r'^(\d{4})')[0], errors='coerce')
    
    # Drop rows with missing values in important columns
    renta = renta.dropna(subset=['region', 'year', 'measure', 'value'])
    renta['year'] = renta['year'].astype(int)
    
    return renta

@asset
def prompt_income_composition_stacked_bar(renta_cleaning):
    desc = """Gráfico de barras apiladas al 100% que muestre la composición de la renta por isla para el año 2023.
    - Filtrar: islas principales ('Lanzarote', 'Fuerteventura', 'Gran Canaria', 'Tenerife', 'La Gomera', 'La Palma', 'El Hierro') y año 2023.
    - Ejes: x='region', y='value', fill='measure'.
    - Formato: geom_bar(position='fill'), etiquetas de eje Y en porcentaje."""
    return get_ia_template(None, desc, renta_cleaning.columns)

@asset
def income_composition_stacked_bar(context, prompt_income_composition_stacked_bar, renta_cleaning):
    code = get_ia_code(context, prompt_income_composition_stacked_bar)
    path = render_ia_viz(context, code, renta_cleaning, "plots/income/income_composition_stacked_bar.png")
    return Output(path, metadata={"code": MetadataValue.md(f"```python\n{code}\n```")})

@asset
def prompt_wage_deviation_from_avg(renta_cleaning):
    desc = """Gráfico de barras horizontales mostrando la desviación del promedio regional de 'Sueldos y salarios' en 2023.
    - Filtrar: año 2023, medida 'Sueldos y salarios', excluir 'Canarias', 'Las Palmas' y 'Santa Cruz de Tenerife'.
    - Lógica: Calcular media de 'value', restar media a cada municipio para obtener 'deviation'.
    - Mostrar: Top 10 y Bottom 10 municipios por desviación.
    - Ejes: x='reorder(region, deviation)', y='deviation', fill='deviation > 0' (verde/rojo).
    - Formato: coord_flip()."""
    return get_ia_template(None, desc, renta_cleaning.columns)

@asset
def wage_deviation_from_avg(context, prompt_wage_deviation_from_avg, renta_cleaning):
    code = get_ia_code(context, prompt_wage_deviation_from_avg)
    path = render_ia_viz(context, code, renta_cleaning, "plots/income/wage_deviation_chart.png")
    return Output(path, metadata={"code": MetadataValue.md(f"```python\n{code}\n```")})

@asset
def prompt_income_distribution_boxplot(renta_cleaning):
    desc = "Boxplot de la variable 'value' para cada 'measure' en el año 2023."
    return get_ia_template(None, desc, renta_cleaning.columns)

@asset
def income_distribution_boxplot(context, prompt_income_distribution_boxplot, renta_cleaning):
    code = get_ia_code(context, prompt_income_distribution_boxplot)
    path = render_ia_viz(context, code, renta_cleaning, "plots/income/income_distribution_boxplot.png")
    return Output(path, metadata={"code": MetadataValue.md(f"```python\n{code}\n```")})

@asset
def prompt_unemployment_trend_by_region(renta_cleaning):
    desc = """Gráfico de líneas de la evolución de las 'Prestaciones por desempleo'.
    - Filtrar: medida 'Prestaciones por desempleo', regiones ('Canarias', 'Lanzarote', 'Fuerteventura', 'Gran Canaria', 'Tenerife', 'La Gomera', 'La Palma', 'El Hierro').
    - Ejes: x='year', y='value', color='region'.
    - Resaltar: La línea de 'Canarias' debe ser más gruesa y opaca (usar size y alpha mapeados a 'region == Canarias')."""
    return get_ia_template(None, desc, renta_cleaning.columns)

@asset
def unemployment_trend_by_region(context, prompt_unemployment_trend_by_region, renta_cleaning):
    code = get_ia_code(context, prompt_unemployment_trend_by_region)
    path = render_ia_viz(context, code, renta_cleaning, "plots/income/unemployment_trend_by_region.png")
    return Output(path, metadata={"code": MetadataValue.md(f"```python\n{code}\n```")})

@asset
def prompt_income_composition_heatmap(renta_cleaning):
    desc = """Mapa de calor (geom_tile) de la medida 'Pensiones' a lo largo de los años.
    - Filtrar: medida 'Pensiones', islas principales.
    - Ejes: x='year', y='region', fill='value'."""
    return get_ia_template(None, desc, renta_cleaning.columns)

@asset
def income_composition_heatmap(context, prompt_income_composition_heatmap, renta_cleaning):
    code = get_ia_code(context, prompt_income_composition_heatmap)
    path = render_ia_viz(context, code, renta_cleaning, "plots/income/income_composition_heatmap.png")
    return Output(path, metadata={"code": MetadataValue.md(f"```python\n{code}\n```")})

@asset
def prompt_pension_growth_ranking(renta_cleaning):
    desc = """Gráfico Lollipop de los 15 municipios con mayor crecimiento en 'Pensiones' entre 2015 y 2023.
    - Lógica: Pivotar datos para años 2015 y 2023, calcular diferencia. Filtrar solo municipios.
    - Ejes: x='reorder(region, change)', y='change'.
    - Formato: geom_segment + geom_point, coord_flip()."""
    return get_ia_template(None, desc, renta_cleaning.columns)

@asset
def pension_growth_ranking(context, prompt_pension_growth_ranking, renta_cleaning):
    code = get_ia_code(context, prompt_pension_growth_ranking)
    path = render_ia_viz(context, code, renta_cleaning, "plots/income/pension_growth_ranking.png")
    return Output(path, metadata={"code": MetadataValue.md(f"```python\n{code}\n```")})

# Nivel Estudios Pipeline
@asset
def nivel_estudios_load():
    # Read the excel file
    return pd.read_excel("data/nivelestudios.xlsx")

@asset(deps=[nivel_estudios_load])
def nivel_estudios_cleaning(nivel_estudios_load):
    df = nivel_estudios_load.copy()
    
    # Rename columns
    df = df.rename(columns={
        "Municipios de 500 habitantes o más": "municipality_raw",
        "Sexo": "sex",
        "Nacionalidad": "nationality",
        "Nivel de estudios en curso": "education_level",
        "Periodo": "year",
        "Total": "total"
    })
    
    # Split municipality_raw into code and name
    # Expecting format "35001 Agaete"
    # match 5 digits at the start
    df['municipality_code'] = df['municipality_raw'].astype(str).str.extract(r'^(\d{5})')
    df['municipality'] = df['municipality_raw'].astype(str).str.extract(r'^\d{5}\s+(.*)')
    
    # Ensure year is strictly numeric (YYYY)
    df['year'] = pd.to_numeric(df['year'].astype(str).str.extract(r'^(\d{4})')[0], errors='coerce')
    
    # Let's filter out rows where municipality_code is NaN, assuming we only want specific municipality data
    df = df.dropna(subset=['municipality_code'])
    
    # Filter out rows with missing essential data
    df = df.dropna(subset=['year', 'total', 'education_level'])
    df['year'] = df['year'].astype(int)
    
    return df

@asset
def prompt_top_foreign_students_municipalities_bar(nivel_estudios_cleaning):
    desc = """Gráfico de barras horizontales de los 20 municipios con más estudiantes extranjeros en el año más reciente.
    - Filtrar: año máximo, sexo 'Total', nacionalidad 'Extranjera', niveles educativos que no sean 'Total' ni 'No cursa estudios'.
    - Lógica: Agrupar por 'municipality' y sumar 'total'. Seleccionar Top 20.
    - Ejes: x='reorder(municipality, total)', y='total'.
    - Formato: coord_flip()."""
    return get_ia_template(None, desc, nivel_estudios_cleaning.columns)

@asset
def top_foreign_students_municipalities_bar(context, prompt_top_foreign_students_municipalities_bar, nivel_estudios_cleaning):
    code = get_ia_code(context, prompt_top_foreign_students_municipalities_bar)
    path = render_ia_viz(context, code, nivel_estudios_cleaning, "plots/education/top_foreign_students_municipalities_bar.png")
    return Output(path, metadata={"code": MetadataValue.md(f"```python\n{code}\n```")})

@asset
def prompt_higher_ed_gender_gap_diverging_bar(nivel_estudios_cleaning):
    desc = """Gráfico de barras divergentes que muestre el desequilibrio de género en Educación Superior para el año más reciente.
    - Filtrar: año máximo, nivel 'Educación superior', sexo ('Hombres', 'Mujeres').
    - Lógica: Calcular % de mujeres regional (promedio). Para cada municipio (Top 20 por total alumnos), calcular desviación de su % de mujeres respecto al promedio regional.
    - Ejes: x='reorder(municipality, deviation)', y='deviation', fill='deviation > 0'.
    - Formato: coord_flip(), colores distintos para 'Más Mujeres' vs 'Más Hombres'."""
    return get_ia_template(None, desc, nivel_estudios_cleaning.columns)

@asset
def higher_ed_gender_gap_diverging_bar(context, prompt_higher_ed_gender_gap_diverging_bar, nivel_estudios_cleaning):
    code = get_ia_code(context, prompt_higher_ed_gender_gap_diverging_bar)
    path = render_ia_viz(context, code, nivel_estudios_cleaning, "plots/education/higher_ed_gender_gap_diverging_bar.png")
    return Output(path, metadata={"code": MetadataValue.md(f"```python\n{code}\n```")})

@asset
def prompt_nationality_proportion_bar(nivel_estudios_cleaning):
    desc = """Gráfico de barras apiladas que muestre la evolución de la proporción de estudiantes locales vs extranjeros por año.
    - Filtrar: sexo 'Total', niveles educativos válidos (no 'Total' ni 'No cursa').
    - Lógica: Calcular porcentaje de cada nacionalidad ('Española'/'Extranjera' si existen) por año.
    - Ejes: x='factor(year)', y='percentage', fill='nationality'.
    - Formato: geom_bar(stat='identity')."""
    return get_ia_template(None, desc, nivel_estudios_cleaning.columns)

@asset
def nationality_proportion_bar(context, prompt_nationality_proportion_bar, nivel_estudios_cleaning):
    code = get_ia_code(context, prompt_nationality_proportion_bar)
    path = render_ia_viz(context, code, nivel_estudios_cleaning, "plots/education/nationality_proportion_bar.png")
    return Output(path, metadata={"code": MetadataValue.md(f"```python\n{code}\n```")})

@asset
def prompt_education_level_proportion_bar(nivel_estudios_cleaning):
    desc = """Gráfico de una sola barra horizontal (apilada) mostrando la proporción de niveles educativos en el año más reciente.
    - Filtrar: año máximo, sexo 'Total', excluir niveles 'Total' y 'No cursa'.
    - Lógica: Calcular porcentajes de cada nivel.
    - Ejes: x=0, y='percentage', fill='education_level'.
    - Formato: coord_flip(), simplificar etiquetas si son muy largas."""
    return get_ia_template(None, desc, nivel_estudios_cleaning.columns)

@asset
def education_level_proportion_bar(context, prompt_education_level_proportion_bar, nivel_estudios_cleaning):
    code = get_ia_code(context, prompt_education_level_proportion_bar)
    path = render_ia_viz(context, code, nivel_estudios_cleaning, "plots/education/education_level_proportion_bar.png")
    return Output(path, metadata={"code": MetadataValue.md(f"```python\n{code}\n```")})

@asset
def prompt_higher_ed_by_island_bar(nivel_estudios_cleaning):
    desc = """Gráfico de barra horizontal apilada mostrando la proporción de estudiantes de Educación Superior por isla.
    - Filtrar: año máximo, nivel 'Educación superior', sexo 'Total'.
    - Lógica: Mapear códigos de municipio a islas (Gran Canaria, Tenerife, etc.) y sumar totales.
    - Ejes: x=0, y='percentage', fill='island'.
    - Formato: coord_flip()."""
    return get_ia_template(None, desc, nivel_estudios_cleaning.columns)

@asset
def higher_ed_by_island_bar(context, prompt_higher_ed_by_island_bar, nivel_estudios_cleaning):
    code = get_ia_code(context, prompt_higher_ed_by_island_bar)
    path = render_ia_viz(context, code, nivel_estudios_cleaning, "plots/education/higher_ed_by_island_bar.png")
    return Output(path, metadata={"code": MetadataValue.md(f"```python\n{code}\n```")})

@asset
def prompt_income_vs_higher_ed_scatter(renta_cleaning, nivel_estudios_cleaning):
    desc = """Gráfico de dispersión que relacione la proporción de Renta de 'Sueldos y salarios' con el % de población con Educación Superior por municipio.
    - Lógica: Unir ambos DataFrames por municipio. Calcular % Educación Superior sobre el total de estudiantes por municipio.
    - Ejes: x='value' (renta), y='% Educación Superior'.
    - Formato: geom_point() + geom_smooth(method='lm')."""
    return get_ia_template(None, desc, list(renta_cleaning.columns) + list(nivel_estudios_cleaning.columns))

@asset
def income_vs_higher_ed_scatter(context, prompt_income_vs_higher_ed_scatter, renta_cleaning, nivel_estudios_cleaning):
    code = get_ia_code(context, prompt_income_vs_higher_ed_scatter)
    # Combinamos para que la IA tenga acceso a ambos si es necesario, 
    # pero renderize sobre el merge que ella misma debe hacer en el código generado
    path = render_ia_viz(context, code, pd.DataFrame(), "plots/combination/income_vs_higher_ed_scatter.png") 
    return Output(path, metadata={"code": MetadataValue.md(f"```python\n{code}\n```")})

@asset
def prompt_higher_ed_mun_wage_comparison(renta_cleaning, nivel_estudios_cleaning):
    desc = """Gráfico comparativo de Educación Superior en municipios con mayor vs menor cuota salarial.
    - Lógica: Unir datos, seleccionar Top 5 y Bottom 5 municipios por renta de 'Sueldos y salarios'. Calcular % Educación Superior regional promedio.
    - Ejes: x='reorder(municipality, pct_higher_ed)', y='pct_higher_ed', fill='Category' (Mayor/Menor).
    - Formato: facet_wrap('~Category'), geom_hline para el promedio regional, coord_flip()."""
    return get_ia_template(None, desc, list(renta_cleaning.columns) + list(nivel_estudios_cleaning.columns))

@asset
def higher_ed_mun_wage_comparison(context, prompt_higher_ed_mun_wage_comparison, renta_cleaning, nivel_estudios_cleaning):
    code = get_ia_code(context, prompt_higher_ed_mun_wage_comparison)
    path = render_ia_viz(context, code, pd.DataFrame(), "plots/combination/higher_ed_mun_wage_comparison.png")
    return Output(path, metadata={"code": MetadataValue.md(f"```python\n{code}\n```")})