import pandas as pd
import numpy as np
import plotnine as p9
import re, os, shutil, hashlib, json, subprocess
from dagster import asset, Output, MetadataValue

CLAUDE_BIN = "/home/francisco/.vscode-server/extensions/anthropic.claude-code-2.1.145-linux-x64/resources/native-binary/claude"

SOURCES_DIR = "sources"


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
def data_servicios_vs_renta(actividad_clean, rentamedia_clean):
    """Por sección (2023): % trabajadores en Servicios vs renta neta. Para bubble chart."""
    act = actividad_clean[actividad_clean['Periodo'] == 2023]
    total = act.groupby('geo_key')['num_casos'].sum().rename('total')
    svc = (
        act[act['Actividad económica'] == 'Servicios']
        .groupby('geo_key')['num_casos'].sum()
        .rename('servicios')
    )
    pct = (
        pd.concat([total, svc], axis=1)
        .assign(pct_servicios=lambda x: x['servicios'] / x['total'].replace(0, np.nan) * 100)
        .dropna(subset=['pct_servicios'])
        .reset_index()
    )
    renta = rentamedia_clean[
        (rentamedia_clean['año'] == 2023) &
        (rentamedia_clean['MEDIDAS_CODE'] == 'RENTA_NETA_MEDIA_HOGAR')
    ][['geo_key', 'OBS_VALUE', 'municipio']].rename(columns={'OBS_VALUE': 'renta_neta'})
    return pct.merge(renta, on='geo_key', how='inner')


@asset
def data_brecha_genero(ocupacion_clean):
    """
    Por municipio (2023): desviación de paridad (% mujeres − 50) en ocupaciones elementales.
    Para diverging bar.
    """
    df = ocupacion_clean[ocupacion_clean['año'] == 2023]
    cat_elem = 'Ocupaciones elementales'

    rows = []
    for municipio, grp in df.groupby('municipio'):
        sub = grp[grp['ocupacion'] == cat_elem]
        total = sub['num_casos'].sum()
        if total == 0:
            continue
        pct = sub.loc[sub['sexo'] == 'Mujeres', 'num_casos'].sum() / total * 100
        rows.append({
            'municipio':  municipio,
            'pct_mujeres': round(pct, 1),
            'desviacion':  round(pct - 50, 1),
        })

    return (
        pd.DataFrame(rows)
        .dropna()
        .sort_values('desviacion')
        .reset_index(drop=True)
    )


@asset
def data_fuentes_ingresos(ingresos_clean):
    """
    % medio de cada fuente de ingresos en Tenerife (2023), con posiciones para waterfall.
    Columnas extra: inicio, fin (acumulados para geom_rect).
    """
    ORDEN = ['Sueldos y salarios', 'Pensiones', 'Prestaciones por desempleo',
             'Otras prestaciones', 'Otros ingresos']
    df = (
        ingresos_clean[ingresos_clean['año'] == 2023]
        .groupby('MEDIDAS#es', as_index=False)['OBS_VALUE']
        .mean()
        .rename(columns={'MEDIDAS#es': 'fuente', 'OBS_VALUE': 'pct'})
    )
    df['fuente'] = pd.Categorical(df['fuente'], categories=ORDEN, ordered=True)
    df = df.sort_values('fuente').reset_index(drop=True)
    df['inicio'] = df['pct'].cumsum().shift(1).fillna(0)
    df['fin']    = df['pct'].cumsum()
    df['idx']    = range(len(df))
    return df


@asset
def data_piramide_ocupacion(ocupacion_clean):
    """
    Trabajadores por municipio y sexo en Tenerife (2023), top 25 municipios por total.
    Hombres con valor negativo para pirámide horizontal.
    """
    df = (
        ocupacion_clean[ocupacion_clean['año'] == 2023]
        .groupby(['municipio', 'sexo'], as_index=False)['num_casos'].sum()
    )
    top25 = (
        df.groupby('municipio')['num_casos'].sum()
        .nlargest(25).index
    )
    df = df[df['municipio'].isin(top25)].copy()
    df['num_casos_dir'] = df.apply(
        lambda r: -r['num_casos'] if r['sexo'] == 'Hombres' else r['num_casos'], axis=1
    )
    return df


@asset
def data_marimekko_sectores(actividad_clean, rentamedia_clean):
    """
    Composición sectorial del empleo según grupo de renta (Baja/Media/Alta), 2023.
    Columnas xmin/xmax/ymin/ymax pre-calculadas para geom_rect (Marimekko).
    """
    SECTORES = ['Servicios', 'Construcción', 'Industria', 'Agricultura, ganadería y pesca']
    ORDEN_G  = ['Baja renta', 'Media renta', 'Alta renta']

    # Secciones con grupo de renta asignado
    renta_sec = (
        rentamedia_clean[
            (rentamedia_clean['año'] == 2023) &
            (rentamedia_clean['MEDIDAS_CODE'] == 'RENTA_NETA_MEDIA_HOGAR')
        ][['geo_key', 'OBS_VALUE']]
        .drop_duplicates('geo_key')
        .copy()
    )
    renta_sec['grupo_renta'] = pd.qcut(
        renta_sec['OBS_VALUE'], q=3, labels=ORDEN_G
    )

    # Trabajadores por (grupo_renta, sector)
    act = actividad_clean[
        (actividad_clean['Periodo'] == 2023) &
        (actividad_clean['Actividad económica'].isin(SECTORES))
    ].merge(renta_sec[['geo_key', 'grupo_renta']], on='geo_key', how='inner')

    mat = (
        act.groupby(['grupo_renta', 'Actividad económica'], as_index=False)['num_casos'].sum()
        .rename(columns={'Actividad económica': 'sector'})
    )

    # Anchos de columna (proporcional al total de trabajadores por grupo)
    g_totals = mat.groupby('grupo_renta')['num_casos'].sum().reset_index(name='total_grupo')
    g_totals['grupo_renta'] = pd.Categorical(g_totals['grupo_renta'], categories=ORDEN_G, ordered=True)
    g_totals = g_totals.sort_values('grupo_renta')
    g_totals['width'] = g_totals['total_grupo'] / g_totals['total_grupo'].sum()
    g_totals['xmax']  = g_totals['width'].cumsum()
    g_totals['xmin']  = g_totals['xmax'] - g_totals['width']
    g_totals['xcenter'] = (g_totals['xmin'] + g_totals['xmax']) / 2

    # Alturas de cada sector dentro de cada columna
    mat = mat.merge(g_totals[['grupo_renta', 'total_grupo', 'xmin', 'xmax', 'xcenter']], on='grupo_renta')
    mat['pct'] = mat['num_casos'] / mat['total_grupo']
    mat['sector'] = pd.Categorical(mat['sector'], categories=SECTORES, ordered=True)
    mat = mat.sort_values(['grupo_renta', 'sector'])
    mat['ymax']    = mat.groupby('grupo_renta')['pct'].cumsum()
    mat['ymin']    = mat['ymax'] - mat['pct']
    mat['ycenter'] = (mat['ymin'] + mat['ymax']) / 2

    return mat.reset_index(drop=True)


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
    - Geometría: lollipop horizontal.
      geom_segment(aes(x=0, xend='renta_media', y='reorder(municipio,renta_media)',
                       yend='reorder(municipio,renta_media)'), color='gray', size=0.6) +
      geom_point(aes(x='renta_media', y='reorder(municipio,renta_media)', color='grupo'), size=3).
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


# 3. Bubble — paradoja del turismo (ángulo C)
@asset
def prompt_servicios_vs_renta(data_servicios_vs_renta):
    desc = """
    - Dataset: data_servicios_vs_renta
    - Columnas: pct_servicios (float, 0-100), renta_neta (float, €), total (int, trabajadores), municipio (str)
    - Geometría: bubble chart.
      geom_point(aes(x='pct_servicios', y='renta_neta', size='total'),
                 color='#4575b4', alpha=0.35) +
      scale_size_continuous(range=(1, 12), name='Trabajadores').
    - Título: 'La paradoja del turismo: más servicios, ¿menos renta? (2023)'
    - Eje X: '% trabajadores en Servicios'. Eje Y: 'Renta neta por hogar (€)'.
    - Tema: theme_minimal().
    """
    return get_ai_template(None, desc, data_servicios_vs_renta)

@asset
def servicios_vs_renta(context, prompt_servicios_vs_renta, data_servicios_vs_renta):
    code = get_ai_code(context, prompt_servicios_vs_renta)
    path = render_ia_viz(context, code, data_servicios_vs_renta, "plots/actividad/bubble_servicios_vs_renta.png")
    return Output(path, metadata={"code": MetadataValue.md(f"```python\n{code}\n```")})


# 4. Diverging Bar — brecha de género (ángulo D)
@asset
def prompt_brecha_genero(data_brecha_genero):
    desc = """
    - Dataset: data_brecha_genero
    - Columnas: municipio (str), pct_mujeres (float, %), desviacion (float, pct_mujeres - 50)
    - Geometría: diverging bar horizontal.
      Crear columna 'color_grupo' = 'Mayoría mujeres' si desviacion >= 0, 'Mayoría hombres' si < 0.
      geom_col(aes(x='desviacion', y='reorder(municipio, desviacion)', fill='color_grupo')) +
      geom_vline(xintercept=0, color='black', size=0.6).
    - Usar scale_fill_manual con dos colores distintos (ej. '#d73027' y '#4575b4').
    - Título: '¿Dónde trabajan más mujeres en empleos elementales? (2023)'
    - Eje X: 'Desviación respecto a paridad (puntos porcentuales)'. Eje Y: ''.
    - Leyenda título: 'Predominio'. Tema: theme_minimal().
    """
    return get_ai_template(None, desc, data_brecha_genero)

@asset
def brecha_genero(context, prompt_brecha_genero, data_brecha_genero):
    code = get_ai_code(context, prompt_brecha_genero)
    path = render_ia_viz(context, code, data_brecha_genero, "plots/genero/diverging_bar_brecha_genero.png")
    return Output(path, metadata={"code": MetadataValue.md(f"```python\n{code}\n```")})


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
    - Columnas: municipio (str, 25 municipios), sexo ('Hombres'/'Mujeres'),
      num_casos (int), num_casos_dir (int, negativo para Hombres)
    - Geometría: pirámide de población horizontal.
      geom_col(aes(x='num_casos_dir', y='reorder(municipio, num_casos)', fill='sexo'),
               width=0.7, position='identity') +
      geom_vline(xintercept=0, color='black', size=0.5).
    - CRÍTICO: usar position='identity' en geom_col para que las barras no se apilen.
    - Usar scale_x_continuous con labels que muestren valor absoluto:
        labels=lambda lst: [f'{abs(int(v)):,}' for v in lst]
    - Usar scale_fill_manual con '#4575b4' para Hombres y '#d73027' para Mujeres.
    - Título: 'Distribución de trabajadores por sexo y municipio en Tenerife (2023)'
    - Eje X: 'Número de trabajadores'. Eje Y: ''.
    - Leyenda título: 'Sexo'. Tema: theme_minimal().
    """
    return get_ai_template(None, desc, data_piramide_ocupacion)

@asset
def piramide_ocupacion(context, prompt_piramide_ocupacion, data_piramide_ocupacion):
    code = get_ai_code(context, prompt_piramide_ocupacion)
    path = render_ia_viz(context, code, data_piramide_ocupacion, "plots/genero/piramide_ocupacion.png")
    return Output(path, metadata={"code": MetadataValue.md(f"```python\n{code}\n```")})


# 7. Marimekko — composición sectorial por grupo de renta
@asset
def prompt_marimekko_sectores(data_marimekko_sectores):
    desc = """
    - Dataset: data_marimekko_sectores
    - Columnas: grupo_renta (str), sector (str), pct (float), xmin, xmax, ymin, ymax (float),
      xcenter, ycenter (float), total_grupo (int)
    - Geometría: Marimekko con geom_rect.
      geom_rect(aes(xmin='xmin', xmax='xmax', ymin='ymin', ymax='ymax', fill='sector'),
                color='white', size=0.5).
    - Añadir etiquetas de grupo_renta en el eje X:
        geom_text(data=df.drop_duplicates('grupo_renta'),
                  aes(x='xcenter', y=-0.05, label='grupo_renta'), size=8, va='top').
    - Añadir etiquetas de porcentaje dentro de cada celda (solo si pct > 0.07):
        geom_text(data=df[df['pct'] > 0.07],
                  aes(x='xcenter', y='ycenter', label='pct_label'), size=7, color='white').
        Calcular pct_label = df['pct'].apply(lambda x: f'{x*100:.0f}%').
    - Usar scale_fill_brewer(type='qual', palette='Set2').
    - scale_y_continuous(labels=lambda l: [f'{v*100:.0f}%' for v in l]).
    - Título: 'Composición del empleo por sector según nivel de renta (2023)'
    - Eje X: 'Grupo de renta (ancho proporcional a trabajadores)'. Eje Y: '% de trabajadores por sector'.
    - Tema: theme_minimal() + theme(axis_text_x=element_blank(), axis_ticks_x=element_blank()).
    """
    return get_ai_template(None, desc, data_marimekko_sectores)

@asset
def marimekko_sectores(context, prompt_marimekko_sectores, data_marimekko_sectores):
    code = get_ai_code(context, prompt_marimekko_sectores)
    path = render_ia_viz(context, code, data_marimekko_sectores, "plots/actividad/marimekko_sectores.png")
    return Output(path, metadata={"code": MetadataValue.md(f"```python\n{code}\n```")})
