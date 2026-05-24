import pandas as pd
import numpy as np
import plotnine as p9
import re, os, shutil, hashlib, json, subprocess
from dagster import asset, Output, MetadataValue

CLAUDE_BIN = "/home/francisco/.vscode-server/extensions/anthropic.claude-code-2.1.145-linux-x64/resources/native-binary/claude"

SOURCES_DIR = "scripts"


# ---------------------------------------------------------------------------
# Helpers IA
# ---------------------------------------------------------------------------

def _strip_date_prefix(gc):
    m = re.match(r'\d{8}_(.*)', str(gc))
    return m.group(1) if m else str(gc)


def get_ai_template(context, description, df):
    sample_data = df.head(3).to_markdown() if hasattr(df, 'to_markdown') else str(df.head(3))
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
        f"Columnas disponibles: {', '.join(df.columns)}\n"
        f"Muestra de datos:\n{sample_data}"
    )
    return {
        "messages": [
            {"role": "system", "content": system_content},
            {"role": "user",   "content": user_content}
        ]
    }


def get_ai_code(context, template):
    os.makedirs(SOURCES_DIR, exist_ok=True)

    asset_name  = context.asset_key.path[-1]
    code_path   = os.path.join(SOURCES_DIR, f"{asset_name}.py")
    hash_path   = os.path.join(SOURCES_DIR, f"{asset_name}.hash")

    prompt_hash = hashlib.md5(
        json.dumps(template["messages"], sort_keys=True).encode()
    ).hexdigest()

    # Reutilizar código cacheado si el prompt no ha cambiado
    if os.path.exists(code_path) and os.path.exists(hash_path):
        with open(hash_path) as f:
            if f.read().strip() == prompt_hash:
                context.log.info(f"Cache hit — reutilizando {code_path}")
                with open(code_path) as f:
                    return f.read()

    # Llamada a Claude Code CLI (prompt completo via stdin para evitar límites de args)
    system_msg = next(m["content"] for m in template["messages"] if m["role"] == "system")
    user_msg   = next(m["content"] for m in template["messages"] if m["role"] == "user")
    full_prompt = f"{system_msg}\n\n{user_msg}"

    env = os.environ.copy()
    env.setdefault("HOME", os.path.expanduser("~"))
    env.pop("ANTHROPIC_API_KEY", None)  # forzar sesión OAuth (Claude Pro), no la API key

    result = subprocess.run(
        [CLAUDE_BIN, "-p", full_prompt,
         "--output-format", "text", "--model", "claude-sonnet-4-6",
         "--dangerously-skip-permissions"],
        capture_output=True, text=True, env=env
    )
    if result.returncode != 0:
        context.log.error(f"Claude CLI stdout: {result.stdout!r}")
        context.log.error(f"Claude CLI stderr: {result.stderr!r}")
        raise RuntimeError(
            f"Claude CLI falló (código {result.returncode}):\n"
            f"stdout: {result.stdout.strip()}\nstderr: {result.stderr.strip()}"
        )
    codigo_raw = result.stdout
    match = re.search(r"```python\s+(.*?)\s+```", codigo_raw, re.DOTALL)
    codigo = match.group(1).strip() if match else codigo_raw.strip()

    # Guardar en caché
    with open(code_path, "w") as f:
        f.write(codigo)
    with open(hash_path, "w") as f:
        f.write(prompt_hash)

    context.log.info(f"Código generado por Claude y guardado en {code_path}")
    return codigo


def render_ia_viz(context, code, df, filename, width=10, height=6):
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
        plot.save(filename, width=width, height=height, dpi=100)

        docs_img_path = os.path.join("docs", "images", os.path.basename(filename))
        os.makedirs(os.path.dirname(docs_img_path), exist_ok=True)
        shutil.copy(filename, docs_img_path)

        return filename
    except Exception as e:
        context.log.error(f"Error Render en {filename}: {e}\nCódigo:\n{code}")
        raise e


# ---------------------------------------------------------------------------
# Loads
# ---------------------------------------------------------------------------

@asset
def rentamedia_load():
    return pd.read_csv("data/rentamedia-sc-3.csv", encoding='utf-8-sig')

@asset
def actividad_load():
    return pd.read_csv("data/actividad-sc-3.csv", encoding='utf-8-sig')

@asset
def ocupacion_load():
    return pd.read_csv("data/ocupacion-sc-3.csv", encoding='utf-8-sig')

@asset
def ingresos_load():
    return pd.read_csv("data/distribucion-renta-ingresos.csv", encoding='utf-8-sig')


# ---------------------------------------------------------------------------
# Cleans
# ---------------------------------------------------------------------------

@asset
def rentamedia_clean(rentamedia_load):
    df = rentamedia_load.copy().drop_duplicates()
    df['municipio'] = df['municipio'].str.strip()
    df['OBS_VALUE'] = pd.to_numeric(df['OBS_VALUE'], errors='coerce')
    df = df.dropna(subset=['OBS_VALUE', 'TERRITORIO_CODE', 'año'])
    df['año'] = df['año'].astype(int)
    df['geo_key'] = df['TERRITORIO_CODE'].apply(_strip_date_prefix)
    return df

@asset
def actividad_clean(actividad_load):
    df = actividad_load.copy().drop_duplicates()
    df['municipio'] = df['municipio'].str.strip()
    df['num_casos'] = pd.to_numeric(df['num_casos'], errors='coerce').fillna(0)
    df['Periodo'] = df['Periodo'].astype(int)
    df['geo_key'] = df['geocode'].apply(_strip_date_prefix)
    return df

@asset
def ocupacion_clean(ocupacion_load):
    df = ocupacion_load.copy().drop_duplicates()
    df['municipio'] = df['municipio'].str.strip()
    df['num_casos'] = pd.to_numeric(df['num_casos'], errors='coerce').fillna(0)
    df['año'] = df['año'].astype(int)
    df['geo_key'] = df['geocode'].apply(_strip_date_prefix)
    return df

@asset
def ingresos_clean(ingresos_load):
    df = ingresos_load.copy().drop_duplicates()
    df['municipio'] = df['municipio'].str.strip()
    df['OBS_VALUE'] = (
        df['OBS_VALUE'].astype(str)
        .str.strip()
        .str.replace(',', '.', regex=False)
    )
    df['OBS_VALUE'] = pd.to_numeric(df['OBS_VALUE'], errors='coerce')
    df = df.dropna(subset=['OBS_VALUE', 'TERRITORIO_CODE', 'año'])
    df['año'] = df['año'].astype(int)
    df['geo_key'] = df['TERRITORIO_CODE'].apply(_strip_date_prefix)
    return df


# ---------------------------------------------------------------------------
# Datasets analíticos — narrativa "Dos Tenerifes"
# ---------------------------------------------------------------------------

@asset
def data_renta_municipio(rentamedia_clean):
    """Renta neta media por hogar, por municipio (2023). Para lollipop."""
    df = rentamedia_clean[
        (rentamedia_clean['año'] == 2023) &
        (rentamedia_clean['MEDIDAS_CODE'] == 'RENTA_NETA_MEDIA_HOGAR')
    ]
    return (
        df.groupby('municipio', as_index=False)['OBS_VALUE']
        .mean()
        .rename(columns={'OBS_VALUE': 'renta_media'})
        .sort_values('renta_media', ascending=False)
        .reset_index(drop=True)
    )


@asset
def data_hist_renta(rentamedia_clean):
    """Distribución de renta neta por sección censal (2023). Para histograma."""
    return (
        rentamedia_clean[
            (rentamedia_clean['año'] == 2023) &
            (rentamedia_clean['MEDIDAS_CODE'] == 'RENTA_NETA_MEDIA_HOGAR')
        ][['municipio', 'OBS_VALUE']]
        .rename(columns={'OBS_VALUE': 'renta_neta'})
        .reset_index(drop=True)
    )




@asset
def data_fuentes_ingresos(ingresos_clean):
    """
    % medio de cada fuente de ingresos en Tenerife (2023), con posiciones para waterfall.
    Columnas extra: inicio, fin (acumulados para geom_rect).
    """
    df = (
        ingresos_clean[ingresos_clean['año'] == 2023]
        .groupby('MEDIDAS#es', as_index=False)['OBS_VALUE']
        .mean()
        .rename(columns={'MEDIDAS#es': 'fuente', 'OBS_VALUE': 'pct'})
    )
    df = df.sort_values('pct', ascending=False).reset_index(drop=True)
    df['inicio'] = df['pct'].cumsum().shift(1).fillna(0)
    df['fin']    = df['pct'].cumsum()
    df['idx']    = range(len(df))
    return df


@asset
def data_piramide_ocupacion(ocupacion_clean):
    """
    Desviación porcentual respecto a la paridad (50/50) por categoría de ocupación, 2023.
    desviacion = pct_mujeres - 50  (positivo → más mujeres, negativo → más hombres)
    """
    df = (
        ocupacion_clean[
            (ocupacion_clean['año'] == 2023) &
            (ocupacion_clean['ocupacion'] != 'No consta')
        ]
        .groupby(['ocupacion', 'sexo'], as_index=False)['num_casos'].sum()
    )
    pivot = df.pivot(index='ocupacion', columns='sexo', values='num_casos').reset_index()
    pivot['total'] = pivot['Hombres'] + pivot['Mujeres']
    pivot['desviacion'] = (pivot['Mujeres'] / pivot['total'] * 100) - 50
    pivot['mayoria'] = pivot['desviacion'].apply(
        lambda v: 'Más mujeres' if v > 0 else 'Más hombres'
    )
    return pivot[['ocupacion', 'desviacion', 'mayoria', 'total']]




# ---------------------------------------------------------------------------
# Prompts + Visualizaciones
# ---------------------------------------------------------------------------

# 1. Lollipop — ranking de municipios (Dos Tenerifes)
@asset
def prompt_ranking_municipios(data_renta_municipio):
    desc = """
    - Dataset: data_renta_municipio
    - Columnas: municipio (str), renta_media (float, euros)
    - Preprocesamiento: tomar top 15 y bottom 15 por renta_media.
      Añadir columna 'grupo' = 'Top 15' o 'Bottom 15'. Concatenar ambos.
    - Geometría: lollipop horizontal. El segmento y el punto usan el mismo color por grupo.
      geom_segment(aes(x=0, xend='renta_media', y='reorder(municipio,renta_media)',
                       yend='reorder(municipio,renta_media)', color='grupo'), size=0.8) +
      geom_point(aes(x='renta_media', y='reorder(municipio,renta_media)', color='grupo'), size=3).
    - Colores: scale_color_manual(values={'Bottom 15': '#b03a2e', 'Top 15': '#2166ac'}).
    - Título: 'Los municipios más ricos y más pobres de Tenerife (2023)'
    - Eje X: 'Renta neta media por hogar (€)'. Eje Y: ''.
    - Tema: theme_minimal(). Ocultar leyenda de color.
    """
    return get_ai_template(None, desc, data_renta_municipio)

@asset
def ranking_municipios(context, prompt_ranking_municipios, data_renta_municipio):
    code = get_ai_code(context, prompt_ranking_municipios)
    path = render_ia_viz(context, code, data_renta_municipio, "plots/renta/lollipop_ranking_municipios.png")
    return Output(path, metadata={"code": MetadataValue.md(f"```python\n{code}\n```")})


# 2. Histogram — distribución de renta por sección
@asset
def prompt_hist_renta(data_hist_renta):
    desc = """
    - Dataset: data_hist_renta
    - Columnas: municipio (str), renta_neta (float, euros)
    - Geometría: geom_histogram(bins=40, fill='#2166ac', color='white', alpha=0.85).
    - Añadir geom_vline con la mediana:
        mediana = df['renta_neta'].median()
        geom_vline(xintercept=mediana, color='#d73027', linetype='dashed', size=0.9)
    - Añadir annotate('text', x=mediana, y=..., label=f'Mediana: {mediana:,.0f} €', color='#d73027', ha='left').
      Calcular y dinámicamente como el 90% del máximo del histograma contando con bins=40.
    - Título: 'Distribución de la renta por sección censal en Tenerife (2023)'
    - Eje X: 'Renta neta media por hogar (€)'. Eje Y: 'Número de secciones'.
    - Tema: theme_minimal().
    """
    return get_ai_template(None, desc, data_hist_renta)

@asset
def hist_renta(context, prompt_hist_renta, data_hist_renta):
    code = get_ai_code(context, prompt_hist_renta)
    path = render_ia_viz(context, code, data_hist_renta, "plots/renta/histograma_hist_renta.png")
    return Output(path, metadata={"code": MetadataValue.md(f"```python\n{code}\n```")})


# 3. Waterfall — composición de fuentes de ingresos (ángulo B)


# 5. Waterfall — composición de fuentes de ingresos (ángulo B)
@asset
def prompt_fuentes_ingresos(data_fuentes_ingresos):
    desc = """
    - Dataset: data_fuentes_ingresos
    - Columnas: fuente (str), pct (float), inicio (float), fin (float), idx (int)
    - Geometría: waterfall con geom_rect.
      geom_rect(aes(xmin='idx - 0.4', xmax='idx + 0.4', ymin='inicio', ymax='fin', fill='fuente')) +
      geom_segment(aes(x='idx + 0.4', xend='idx + 0.6', y='fin', yend='fin'),
                   color='gray', linetype='dashed', size=0.5)  # conectores entre barras
    - Añadir geom_text con el valor de pct formateado ('{:.1f}%') centrado en cada barra:
        aes(x='idx', y='(inicio+fin)/2', label='fuente_label')
        Calcular 'fuente_label' = df['pct'].apply(lambda x: f'{x:.1f}%')
    - Usar scale_x_continuous con breaks=df['idx'] y labels=df['fuente'].tolist().
    - Rotar etiquetas del eje X 20° (theme(axis_text_x=element_text(angle=20, ha='right'))).
    - Título: 'De dónde viene la renta: composición de ingresos por sección en Tenerife (2023)'
    - Eje X: ''. Eje Y: '% acumulado del total de ingresos'.
    - Ocultar leyenda de fill. Tema: theme_minimal().
    """
    return get_ai_template(None, desc, data_fuentes_ingresos)

@asset
def fuentes_ingresos(context, prompt_fuentes_ingresos, data_fuentes_ingresos):
    code = get_ai_code(context, prompt_fuentes_ingresos)
    path = render_ia_viz(context, code, data_fuentes_ingresos, "plots/renta/waterfall_fuentes_ingresos.png")
    return Output(path, metadata={"code": MetadataValue.md(f"```python\n{code}\n```")})


# 6. Population Pyramid — ocupación por sexo
@asset
def prompt_piramide_ocupacion(data_piramide_ocupacion):
    desc = """
    - Dataset: data_piramide_ocupacion
    - Columnas: ocupacion (str, 3 categorías), desviacion (float, % mujeres − 50%),
      mayoria ('Más mujeres' / 'Más hombres'), total (float)
    - Geometría: diverging bar horizontal.
        aes(x='ocupacion', y='desviacion', fill='mayoria')
        + geom_col(width=0.6)
        + coord_flip()
        + geom_hline(yintercept=0, color='black', size=0.7)
    - Ordenar categorías por desviacion ascendente con pd.Categorical antes del plot.
    - scale_fill_manual: '#d73027' para 'Más mujeres', '#4575b4' para 'Más hombres'.
    - scale_y_continuous con labels con signo y un decimal:
        labels=lambda lst: [f'{v:+.1f}%' for v in lst]
    - Título: 'Brecha de género por categoría de ocupación en Tenerife (2023)'
    - Eje Y: 'Desviación respecto a la paridad (% mujeres − 50%)'. Eje X: ''. Leyenda: ''.
    - theme_minimal() + theme(figure_size=(10, 5), axis_text_y=element_text(size=8))
    """
    return get_ai_template(None, desc, data_piramide_ocupacion)

@asset
def piramide_ocupacion(context, prompt_piramide_ocupacion, data_piramide_ocupacion):
    code = get_ai_code(context, prompt_piramide_ocupacion)
    path = render_ia_viz(context, code, data_piramide_ocupacion, "plots/genero/piramide_ocupacion.png")
    return Output(path, metadata={"code": MetadataValue.md(f"```python\n{code}\n```")})




# 8. Waterfall — distribución de trabajadores por sector CNAE
@asset
def data_waterfall_actividades(actividad_clean):
    """
    % de trabajadores por sector CNAE en Tenerife (2023), con posiciones para waterfall.
    Excluye 'No consta'. Ordenado de mayor a menor %.
    """
    df = (
        actividad_clean[
            (actividad_clean['Periodo'] == 2023) &
            (actividad_clean['Actividad económica'] != 'No consta')
        ]
        .groupby('Actividad económica', as_index=False)['num_casos'].sum()
        .rename(columns={'Actividad económica': 'sector', 'num_casos': 'total'})
    )
    total_global = df['total'].sum()
    df['pct']    = df['total'] / total_global * 100
    df = df.sort_values('pct', ascending=False).reset_index(drop=True)
    df['inicio'] = df['pct'].cumsum().shift(1).fillna(0)
    df['fin']    = df['pct'].cumsum()
    df['idx']    = range(len(df))
    return df


@asset
def prompt_waterfall_actividades(data_waterfall_actividades):
    desc = """
    - Dataset: data_waterfall_actividades
    - Columnas: sector (str), total (int), pct (float), inicio (float), fin (float), idx (int)
    - Geometría: waterfall con geom_rect.
      geom_rect(aes(xmin='idx - 0.4', xmax='idx + 0.4', ymin='inicio', ymax='fin', fill='bar_color'))
    - Colores de barra: BARRA_COLS = ['#2196F3', '#FF9800', '#4CAF50', '#F44336']
      asignados por posición (idx 0 → primer color).
    - Colores de texto interior contrastantes (distintos entre sí):
      TEXTO_COLS = ['#FFF9C4', '#1A237E', '#FCE4EC', '#E0F7FA']
    - Conectores entre barras:
      geom_segment(aes(x='idx+0.4', xend='idx+0.6', y='fin', yend='fin'), color='gray', linetype='dashed')
    - Texto interior con pct formateado '{:.1f}%', size=11, fontweight='bold'.
      Usar scale_color_identity() (NO guide=False ni guide='none').
    - Etiquetas del eje X: geom_text(aes(x='idx', y=-8, label='sector', color='bar_color'),
      angle=20, ha='right', va='top', size=9).
    - Añadir expand_limits(y=-22) para que no se corten las etiquetas.
    - scale_x_continuous(breaks=[], labels=[]).
    - scale_fill_identity() (sin guide=).
    - NUNCA usar guide=False ni guide='none'. Para ocultar leyendas usar theme(legend_position='none').
    - Título: 'Distribución de trabajadores por sector económico en Tenerife (2023)'
    - Eje X: ''. Eje Y: '% acumulado de trabajadores'.
    - Tema: theme_minimal() + theme(legend_position='none', axis_text_x=element_blank(), axis_ticks_x=element_blank()).
    """
    return get_ai_template(None, desc, data_waterfall_actividades)

@asset
def waterfall_actividades(context, prompt_waterfall_actividades, data_waterfall_actividades):
    code = get_ai_code(context, prompt_waterfall_actividades)
    path = render_ia_viz(context, code, data_waterfall_actividades, "plots/actividad/waterfall_actividades.png")
    return Output(path, metadata={"code": MetadataValue.md(f"```python\n{code}\n```")})


# ============================================================
# Gráficos de correlación — narrativa desigualdad
# ============================================================

# 9. Slope chart — evolución renta municipal 2021→2023
@asset
def data_slope_brecha(rentamedia_clean):
    """Renta media por municipio en 2021 y 2023 con dirección del cambio. Para slope chart."""
    df = (
        rentamedia_clean[
            (rentamedia_clean['año'].isin([2021, 2023])) &
            (rentamedia_clean['MEDIDAS_CODE'] == 'RENTA_NETA_MEDIA_HOGAR')
        ]
        .groupby(['municipio', 'año'], as_index=False)['OBS_VALUE'].mean()
        .rename(columns={'OBS_VALUE': 'renta_media'})
    )
    pivot = df.pivot(index='municipio', columns='año', values='renta_media').dropna().reset_index()
    pivot.columns.name = None
    pivot = pivot.rename(columns={2021: 'renta_2021', 2023: 'renta_2023'})
    pivot['cambio'] = (pivot['renta_2023'] >= pivot['renta_2021']).map({True: 'Sube', False: 'Baja'})
    pivot['variacion_pct'] = (pivot['renta_2023'] - pivot['renta_2021']) / pivot['renta_2021'] * 100
    return pivot.sort_values('renta_2023', ascending=False).reset_index(drop=True)

@asset
def prompt_slope_brecha(data_slope_brecha):
    desc = """
    - Dataset: data_slope_brecha
    - Columnas: municipio (str), renta_2021 (float, €), renta_2023 (float, €),
      cambio ('Sube'/'Baja'), variacion_pct (float, %)
    - Geometría: slope chart (gráfico de pendientes).
      geom_segment(aes(x=0, xend=1, y='renta_2021', yend='renta_2023', color='cambio'),
                   size=0.7, alpha=0.6)
      geom_point(aes(x=0, y='renta_2021', color='cambio'), size=2)
      geom_point(aes(x=1, y='renta_2023', color='cambio'), size=2)
    - scale_color_manual(values={'Sube': '#1a9850', 'Baja': '#d73027'})
    - scale_x_continuous(breaks=[0, 1], labels=['2021', '2023'], limits=[-0.15, 1.15])
    - NUNCA usar guide=False ni guide='none'. Para ocultar leyendas usar theme(legend_position='none').
    - Título: '¿Quién mejoró y quién empeoró? Evolución de la renta por municipio (2021–2023)'
    - Eje X: ''. Eje Y: 'Renta neta media por hogar (€)'. Color legend: 'Tendencia'.
    - Tema: theme_minimal().
    """
    return get_ai_template(None, desc, data_slope_brecha)

@asset
def slope_brecha(context, prompt_slope_brecha, data_slope_brecha):
    code = get_ai_code(context, prompt_slope_brecha)
    path = render_ia_viz(context, code, data_slope_brecha, "plots/renta/slope_brecha_temporal.png")
    return Output(path, metadata={"code": MetadataValue.md(f"```python\n{code}\n```")})


# 10. Box plot — desigualdad intra-municipal
@asset
def data_boxplot_intramunicipal(rentamedia_clean):
    """Renta por sección censal agrupada por isla (2023). Para box plot comparativo."""
    ISLA_MAP = {
        38002: 'La Gomera', 38003: 'La Gomera', 38021: 'La Gomera',
        38036: 'La Gomera', 38049: 'La Gomera', 38050: 'La Gomera',
        38007: 'La Palma',  38008: 'La Palma',  38009: 'La Palma',
        38014: 'La Palma',  38016: 'La Palma',  38024: 'La Palma',
        38027: 'La Palma',  38029: 'La Palma',  38030: 'La Palma',
        38033: 'La Palma',  38037: 'La Palma',  38045: 'La Palma',
        38047: 'La Palma',  38053: 'La Palma',
        38013: 'El Hierro', 38048: 'El Hierro', 38901: 'El Hierro',
    }
    df = (
        rentamedia_clean[
            (rentamedia_clean['año'] == 2023) &
            (rentamedia_clean['MEDIDAS_CODE'] == 'RENTA_NETA_MEDIA_HOGAR')
        ][['geo_key', 'municipio', 'OBS_VALUE']]
        .rename(columns={'OBS_VALUE': 'renta_neta'})
        .dropna()
        .reset_index(drop=True)
    )
    df['cod_municipio'] = df['geo_key'].str[:5].astype(int)
    df['isla'] = df['cod_municipio'].map(ISLA_MAP).fillna('Tenerife')
    return df[['isla', 'municipio', 'renta_neta']]

@asset
def boxplot_intramunicipal(context, data_boxplot_intramunicipal):
    """Genera un PNG por isla con boxplot de renta por municipio."""
    script_path = os.path.join(SOURCES_DIR, "boxplot_intramunicipal.py")
    with open(script_path) as f:
        code = f.read()

    islas = [
        ('Tenerife',  'tenerife'),
        ('La Palma',  'la_palma'),
        ('La Gomera', 'la_gomera'),
        ('El Hierro', 'el_hierro'),
    ]
    paths = []
    for isla, slug in islas:
        df_isla = data_boxplot_intramunicipal[
            data_boxplot_intramunicipal['isla'] == isla
        ].copy()
        n_mun = df_isla['municipio'].nunique()
        height = max(4, n_mun * 0.35)
        path = f"plots/renta/boxplot_desigualdad_intramunicipal_{slug}.png"
        render_ia_viz(context, code, df_isla, path, width=10, height=height)
        paths.append(path)

    return Output(paths[0], metadata={"paths": MetadataValue.text(str(paths))})


# 11. Scatter — ocupaciones elementales vs renta
@asset
def data_scatter_elementales(ocupacion_clean, rentamedia_clean):
    """% trabajadores en ocupaciones elementales vs renta neta por sección (2023). Para scatter."""
    ocu = ocupacion_clean[ocupacion_clean['año'] == 2023]
    total = ocu.groupby('geo_key')['num_casos'].sum().rename('total')
    elem  = (
        ocu[ocu['ocupacion'] == 'Ocupaciones elementales']
        .groupby('geo_key')['num_casos'].sum().rename('elementales')
    )
    pct = (
        pd.concat([total, elem], axis=1)
        .assign(pct_elementales=lambda x: x['elementales'] / x['total'].replace(0, np.nan) * 100)
        .dropna()
        .reset_index()
    )
    renta = rentamedia_clean[
        (rentamedia_clean['año'] == 2023) &
        (rentamedia_clean['MEDIDAS_CODE'] == 'RENTA_NETA_MEDIA_HOGAR')
    ][['geo_key', 'OBS_VALUE', 'municipio']].rename(columns={'OBS_VALUE': 'renta_neta'})
    return pct.merge(renta, on='geo_key', how='inner')

@asset
def prompt_scatter_elementales(data_scatter_elementales):
    desc = """
    - Dataset: data_scatter_elementales
    - Columnas: geo_key (str), total (float, trabajadores totales), elementales (float),
      pct_elementales (float, 0-100), municipio (str), renta_neta (float, €)
    - Geometría: bubble scatter.
      geom_point(aes(x='pct_elementales', y='renta_neta', size='total'),
                 color='#4575b4', alpha=0.35)
      scale_size_continuous(range=(1, 10), name='Trabajadores')
      geom_smooth(aes(x='pct_elementales', y='renta_neta'), method='lm',
                  color='#d73027', size=1)
    - NUNCA usar guide=False ni guide='none'. Para ocultar leyendas usar theme(legend_position='none').
    - Título: 'A más trabajo elemental, menos renta: correlación por sección censal (2023)'
    - Eje X: '% trabajadores en ocupaciones elementales'. Eje Y: 'Renta neta media por hogar (€)'.
    - Tema: theme_minimal().
    """
    return get_ai_template(None, desc, data_scatter_elementales)

@asset
def scatter_elementales(context, prompt_scatter_elementales, data_scatter_elementales):
    code = get_ai_code(context, prompt_scatter_elementales)
    path = render_ia_viz(context, code, data_scatter_elementales,
                         "plots/renta/scatter_elementales_renta.png")
    return Output(path, metadata={"code": MetadataValue.md(f"```python\n{code}\n```")})


# 12. Grouped bar — actividad económica por quintil de renta
@asset
def data_ingresos_quintiles(actividad_clean, rentamedia_clean):
    """% de trabajadores por sector de actividad y quintil de renta (2023). Para grouped bar."""
    renta_sec = (
        rentamedia_clean[
            (rentamedia_clean['año'] == 2023) &
            (rentamedia_clean['MEDIDAS_CODE'] == 'RENTA_NETA_MEDIA_HOGAR')
        ][['geo_key', 'OBS_VALUE']]
        .drop_duplicates('geo_key')
        .copy()
    )
    renta_sec['quintil'] = pd.qcut(
        renta_sec['OBS_VALUE'], q=5,
        labels=['Q1 (más pobre)', 'Q2', 'Q3', 'Q4', 'Q5 (más rico)']
    )
    act = actividad_clean[
        (actividad_clean['Periodo'] == 2023) &
        (actividad_clean['Actividad económica'] != 'No consta')
    ]
    merged = act.merge(renta_sec[['geo_key', 'quintil']], on='geo_key', how='inner')
    result = (
        merged.groupby(['quintil', 'Actividad económica'], as_index=False)
        ['num_casos'].sum()
        .rename(columns={'Actividad económica': 'actividad'})
    )
    result['pct'] = result.groupby('quintil')['num_casos'].transform(
        lambda x: x / x.sum() * 100
    )
    return result[['quintil', 'actividad', 'pct']]

@asset
def prompt_ingresos_quintiles(data_ingresos_quintiles):
    desc = """
    - Dataset: data_ingresos_quintiles
    - Columnas: quintil (str, 5 niveles Q1 más pobre → Q5 más rico), actividad (str, 4 sectores), pct (float, %)
    - Geometría: grouped bar.
      geom_col(aes(x='quintil', y='pct', fill='actividad'), position='dodge')
    - Rotar etiquetas eje X 15°: theme(axis_text_x=element_text(angle=15, ha='right'))
    - NUNCA usar guide=False ni guide='none'. Para ocultar leyendas usar theme(legend_position='none').
    - Título: '¿En qué trabajan los más ricos y los más pobres? Actividad por quintil de renta (2023)'
    - Eje X: 'Quintil de renta'. Eje Y: '% de trabajadores'. Fill legend: 'Sector'.
    - Tema: theme_minimal().
    """
    return get_ai_template(None, desc, data_ingresos_quintiles)

@asset
def grouped_bar_quintiles(context, prompt_ingresos_quintiles, data_ingresos_quintiles):
    code = get_ai_code(context, prompt_ingresos_quintiles)
    path = render_ia_viz(context, code, data_ingresos_quintiles,
                         "plots/renta/grouped_bar_ingresos_quintiles.png", width=12)
    return Output(path, metadata={"code": MetadataValue.md(f"```python\n{code}\n```")})



# 13. Grouped bar — ocupación por quintil de renta
@asset
def data_ocupacion_quintiles(ocupacion_clean, rentamedia_clean):
    """% de trabajadores por categoría de ocupación y quintil de renta (2023). Para grouped bar."""
    renta_sec = (
        rentamedia_clean[
            (rentamedia_clean['año'] == 2023) &
            (rentamedia_clean['MEDIDAS_CODE'] == 'RENTA_NETA_MEDIA_HOGAR')
        ][['geo_key', 'OBS_VALUE']]
        .drop_duplicates('geo_key')
        .copy()
    )
    renta_sec['quintil'] = pd.qcut(
        renta_sec['OBS_VALUE'], q=5,
        labels=['Q1 (más pobre)', 'Q2', 'Q3', 'Q4', 'Q5 (más rico)']
    )
    ocu = ocupacion_clean[
        (ocupacion_clean['año'] == 2023) &
        (ocupacion_clean['ocupacion'] != 'No consta')
    ]
    merged = ocu.merge(renta_sec[['geo_key', 'quintil']], on='geo_key', how='inner')
    result = (
        merged.groupby(['quintil', 'ocupacion'], as_index=False)['num_casos'].sum()
    )
    result['pct'] = result.groupby('quintil')['num_casos'].transform(
        lambda x: x / x.sum() * 100
    )
    return result[['quintil', 'ocupacion', 'pct']]

@asset
def prompt_ocupacion_quintiles(data_ocupacion_quintiles):
    desc = """
    - Dataset: data_ocupacion_quintiles
    - Columnas: quintil (str, 5 niveles Q1 más pobre → Q5 más rico), ocupacion (str, 3 categorías), pct (float, %)
    - Geometría: grouped bar.
      geom_col(aes(x='quintil', y='pct', fill='ocupacion'), position='dodge')
    - Rotar etiquetas eje X 15°: theme(axis_text_x=element_text(angle=15, ha='right'))
    - NUNCA usar guide=False ni guide='none'. Para ocultar leyendas usar theme(legend_position='none').
    - Título: '¿Qué ocupación tienen los más ricos y los más pobres? Por quintil de renta (2023)'
    - Eje X: 'Quintil de renta'. Eje Y: '% de trabajadores'. Fill legend: 'Ocupación'.
    - Tema: theme_minimal().
    """
    return get_ai_template(None, desc, data_ocupacion_quintiles)

@asset
def grouped_bar_ocupacion(context, prompt_ocupacion_quintiles, data_ocupacion_quintiles):
    code = get_ai_code(context, prompt_ocupacion_quintiles)
    path = render_ia_viz(context, code, data_ocupacion_quintiles,
                         "plots/renta/grouped_bar_ocupacion_quintiles.png", width=12)
    return Output(path, metadata={"code": MetadataValue.md(f"```python\n{code}\n```")})


# 14. Heatmap — renta por municipio y año
@asset
def data_heatmap_renta(rentamedia_clean):
    """Renta media por municipio y año (2021-2023), municipios con los 3 años. Para heatmap."""
    df = (
        rentamedia_clean[rentamedia_clean['MEDIDAS_CODE'] == 'RENTA_NETA_MEDIA_HOGAR']
        .groupby(['municipio', 'año'], as_index=False)['OBS_VALUE'].mean()
        .rename(columns={'OBS_VALUE': 'renta_media'})
    )
    mun_completos = df.groupby('municipio')['año'].count()
    mun_completos = mun_completos[mun_completos == 3].index
    df = df[df['municipio'].isin(mun_completos)].copy()
    orden = df[df['año'] == 2023].sort_values('renta_media')['municipio'].tolist()
    df['municipio'] = pd.Categorical(df['municipio'], categories=orden, ordered=True)
    return df.sort_values(['municipio', 'año']).reset_index(drop=True)

@asset
def prompt_heatmap_renta(data_heatmap_renta):
    desc = """
    - Dataset: data_heatmap_renta
    - Columnas: municipio (str, categorical ordenado por renta 2023 asc), año (int), renta_media (float, €)
    - Preprocesamiento: convertir año a str para que el eje X sea discreto.
    - Geometría: heatmap de tiles.
      geom_tile(aes(x='año', y='municipio', fill='renta_media'), color='white', size=0.3)
    - scale_fill_gradient(low='#d73027', high='#1a9850', name='Renta (€)')
    - NUNCA usar guide=False ni guide='none'. Para ocultar leyendas usar theme(legend_position='none').
    - Título: 'Evolución de la renta por municipio: ¿se amplía la brecha? (2021–2023)'
    - Eje X: ''. Eje Y: ''.
    - Tema: theme_minimal() + theme(axis_text_y=element_text(size=7)).
    """
    return get_ai_template(None, desc, data_heatmap_renta)

@asset
def heatmap_renta(context, prompt_heatmap_renta, data_heatmap_renta):
    code = get_ai_code(context, prompt_heatmap_renta)
    path = render_ia_viz(context, code, data_heatmap_renta,
                         "plots/renta/heatmap_renta_municipio.png", height=10)
    return Output(path, metadata={"code": MetadataValue.md(f"```python\n{code}\n```")})
