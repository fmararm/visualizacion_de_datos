import streamlit as st
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuración de página
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Dos Tenerifes",
    page_icon="🏝️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# CSS personalizado
# ---------------------------------------------------------------------------
st.markdown("""
<style>
    .block-container { padding-top: 2rem; padding-bottom: 2rem; }
    h1 { color: #1a1a2e; }
    h2 { color: #16213e; border-bottom: 2px solid #e94560; padding-bottom: 0.3rem; }
    h3 { color: #0f3460; }
    .caption-box {
        background: #f8f9fa;
        border-left: 4px solid #4575b4;
        padding: 0.8rem 1rem;
        border-radius: 0 6px 6px 0;
        margin: 0.5rem 0 1.5rem 0;
        font-size: 0.92rem;
        color: #444;
        line-height: 1.6;
    }
    .insight-box {
        background: #fff8e1;
        border-left: 4px solid #f9a825;
        padding: 0.8rem 1rem;
        border-radius: 0 6px 6px 0;
        margin: 0.5rem 0 1.5rem 0;
        font-size: 0.92rem;
        color: #444;
        line-height: 1.6;
    }
    .section-intro {
        font-size: 1.05rem;
        color: #555;
        line-height: 1.7;
        margin-bottom: 1.5rem;
    }
    .stTabs [data-baseweb="tab"] { font-size: 0.9rem; }
</style>
""", unsafe_allow_html=True)


def plot(path: str, caption: str = "", insight: str = "", width: int = None):
    p = Path(path)
    if not p.exists():
        st.warning(f"Imagen no encontrada: {path}")
        return
    st.image(str(p), use_column_width=(width is None), width=width)
    if caption:
        st.markdown(f'<div class="caption-box">{caption}</div>', unsafe_allow_html=True)
    if insight:
        st.markdown(f'<div class="insight-box">💡 {insight}</div>', unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Sidebar — navegación
# ---------------------------------------------------------------------------
SECCIONES = [
    "🏠 Introducción",
    "📊 La fractura de la renta",
    "📉 Desigualdad interna",
    "💼 Actividad y ocupación",
    "⚖️ Brecha de género",
    "🗺️ Mapa interactivo",
]
seccion = st.sidebar.radio("Navegar a", SECCIONES)
st.sidebar.markdown("---")
st.sidebar.markdown(
    "**Proyecto final** · Visualización de Datos 2025–2026  \n"
    "Fuente: INE · Atlas de Distribución de Renta de los Hogares"
)

# ===========================================================================
# 1. INTRODUCCIÓN
# ===========================================================================
if seccion == "🏠 Introducción":
    st.title("🏝️ Dos Tenerifes")
    st.markdown(
        '<p class="section-intro">'
        "Tenerife es una isla de contrastes. Bajo una imagen turística homogénea "
        "conviven realidades económicas muy distintas: municipios con renta media "
        "superior a 50 000 € por hogar y otros que no alcanzan los 25 000 €; "
        "secciones censales donde casi la mitad de los ingresos provienen de "
        "prestaciones, junto a otras donde el trabajo cualificado domina. "
        "Este dashboard explora esa fractura a través de los datos del "
        "<b>Atlas de Distribución de Renta de los Hogares del INE (2021–2023)</b>."
        "</p>",
        unsafe_allow_html=True,
    )

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Municipios analizados", "54")
    col2.metric("Años cubiertos", "2021 – 2023")
    col3.metric("Secciones censales", "≈ 681")
    col4.metric("Ratio renta máx/mín", "≈ 3×")

    st.markdown("---")
    st.markdown("### ¿Qué encontrarás aquí?")
    st.markdown("""
| Sección | Qué muestra |
|---|---|
| 📊 La fractura de la renta | Ranking de municipios, distribución y evolución temporal |
| 📉 Desigualdad interna | Desigualdad dentro de cada municipio e isla, correlación con ocupación |
| 💼 Actividad y ocupación | Composición del empleo y cómo varía según el nivel de renta |
| ⚖️ Brecha de género | Diferencias por sexo en las categorías de ocupación |
| 🗺️ Mapa interactivo | Exploración espacial de las 4 variables clave |
""")

# ===========================================================================
# 2. LA FRACTURA DE LA RENTA
# ===========================================================================
elif seccion == "📊 La fractura de la renta":
    st.header("📊 La fractura de la renta")
    st.markdown(
        '<p class="section-intro">'
        "Los datos revelan una isla partida en dos: un grupo reducido de municipios "
        "concentra la renta alta mientras la mayoría queda muy por debajo de la media. "
        "El histograma evidencia que la distribución está claramente sesgada a la derecha, "
        "con una cola de secciones muy ricas que elevan la media sin representar a la mayoría."
        "</p>",
        unsafe_allow_html=True,
    )

    st.subheader("Ranking de municipios por renta")
    plot(
        "plots/renta/lollipop_ranking_municipios.png",
        caption=(
            "Top 15 y bottom 15 municipios de Tenerife ordenados por renta neta media por hogar (2023). "
            "La brecha entre el municipio más rico y el más pobre supera el factor 2×."
        ),
        insight=(
            "Los municipios del sur turístico (Adeje, Arona) aparecen en posiciones intermedias, "
            "lo que sugiere que el turismo genera empleo pero no necesariamente renta elevada para sus residentes."
        ),
    )

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Distribución de la renta")
        plot(
            "plots/renta/histograma_hist_renta.png",
            caption=(
                "Histograma de la renta neta media por sección censal (2023). "
                "La distribución es asimétrica positiva: la mayoría de secciones concentra rentas "
                "entre 25 000 € y 45 000 €, pero existe una larga cola de secciones muy ricas."
            ),
        )
    with col2:
        st.subheader("Evolución 2021–2023")
        plot(
            "plots/renta/heatmap_renta_municipio.png",
            caption=(
                "Heatmap de renta media por municipio y año. "
                "El color verde indica mayor renta. "
                "Se puede observar si la brecha entre municipios se amplía o estrecha con el tiempo."
            ),
            insight=(
                "La mayoría de municipios mejoran su posición relativa de forma homogénea, "
                "pero los extremos (los más ricos y los más pobres) tienden a mantenerse estables."
            ),
        )

    st.subheader("¿Quién mejoró y quién empeoró? (2021–2023)")
    plot(
        "plots/renta/slope_brecha_temporal.png",
        caption=(
            "Slope chart que muestra la evolución de la renta por municipio entre 2021 y 2023. "
            "Las líneas verdes indican municipios que mejoran su renta; las rojas, los que empeoran."
        ),
        insight="La mayor parte de municipios sube, pero los que ya eran pobres crecen proporcionalmente menos.",
    )

# ===========================================================================
# 3. DESIGUALDAD INTERNA
# ===========================================================================
elif seccion == "📉 Desigualdad interna":
    st.header("📉 Desigualdad interna")
    st.markdown(
        '<p class="section-intro">'
        "Más allá de la comparación entre municipios, la desigualdad también existe "
        "<em>dentro</em> de cada municipio. Las secciones censales de Santa Cruz de Tenerife "
        "o San Cristóbal de La Laguna muestran una dispersión interna enorme, "
        "con barrios muy ricos y muy pobres separados por pocos kilómetros. "
        "Además, existe una correlación clara entre el tipo de trabajo disponible "
        "y el nivel de renta de la sección."
        "</p>",
        unsafe_allow_html=True,
    )

    st.subheader("Desigualdad intramunicipal por isla")
    st.markdown(
        "Cada caja muestra la distribución de renta de las secciones censales "
        "de un municipio. Cuanto más ancha la caja y más largos los bigotes, "
        "mayor desigualdad interna."
    )
    tabs = st.tabs(["🟦 Tenerife", "🟩 La Palma", "🟨 La Gomera", "🟥 El Hierro"])
    configs = [
        ("plots/renta/boxplot_desigualdad_intramunicipal_tenerife.png",
         "Tenerife concentra la mayor dispersión interna. "
         "Santa Cruz y La Laguna tienen los rangos más amplios, con outliers extremos "
         "que corresponden a las urbanizaciones de mayor renta.",
         "La amplitud del bigote derecho en Santa Cruz de Tenerife evidencia "
         "que conviven barrios con rentas >80 000 € junto a secciones por debajo de 25 000 €."),
        ("plots/renta/boxplot_desigualdad_intramunicipal_la_palma.png",
         "La Palma muestra municipios con distribuciones más compactas. "
         "Santa Cruz de La Palma destaca como el municipio con mayor dispersión de la isla.",
         None),
        ("plots/renta/boxplot_desigualdad_intramunicipal_la_gomera.png",
         "La Gomera tiene pocos municipios y pocas secciones. "
         "San Sebastián de La Gomera presenta la renta más alta y mayor dispersión.",
         None),
        ("plots/renta/boxplot_desigualdad_intramunicipal_el_hierro.png",
         "El Hierro, con solo 3 municipios, muestra las rentas más bajas del archipiélago "
         "y poca variabilidad interna.",
         "El Hierro es la isla con menor renta media pero también la que tiene "
         "menor desigualdad interna."),
    ]
    for tab, (img, cap, ins) in zip(tabs, configs):
        with tab:
            plot(img, caption=cap, insight=ins)

    st.subheader("Correlación: ocupaciones elementales y renta")
    plot(
        "plots/renta/scatter_elementales_renta.png",
        caption=(
            "Scatter plot de secciones censales: eje X = % de trabajadores en ocupaciones elementales, "
            "eje Y = renta neta media, tamaño de la burbuja = número total de trabajadores (2023). "
            "La línea roja muestra la tendencia lineal."
        ),
        insight=(
            "La correlación negativa es significativa: las secciones con más trabajadores en ocupaciones "
            "elementales tienen sistemáticamente menos renta. Es la huella estadística de la segregación "
            "laboral y residencial."
        ),
    )

# ===========================================================================
# 4. ACTIVIDAD Y OCUPACIÓN
# ===========================================================================
elif seccion == "💼 Actividad y ocupación":
    st.header("💼 Actividad y ocupación")
    st.markdown(
        '<p class="section-intro">'
        "La estructura productiva de Tenerife está dominada por los Servicios, "
        "pero la distribución del empleo varía significativamente según el nivel de renta "
        "de la sección censal. Analizar qué tipo de trabajo realizan los residentes "
        "de cada quintil revela los mecanismos de reproducción de la desigualdad."
        "</p>",
        unsafe_allow_html=True,
    )

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Fuentes de ingresos de los hogares")
        plot(
            "plots/renta/waterfall_fuentes_ingresos.png",
            caption=(
                "Waterfall de la composición media de ingresos por sección censal en Tenerife (2023). "
                "Cada barra representa el peso porcentual de cada fuente sobre el total."
            ),
            insight=(
                "Los sueldos y salarios dominan, pero las pensiones tienen un peso "
                "sorprendentemente alto, lo que refleja el envejecimiento de la población residente."
            ),
        )
    with col2:
        st.subheader("Trabajadores por sector económico")
        plot(
            "plots/actividad/waterfall_actividades.png",
            caption=(
                "Waterfall del % de trabajadores por sector CNAE en Tenerife (2023). "
                "Los Servicios concentran más del 85% del empleo, seguidos de Construcción e Industria."
            ),
            insight=(
                "La hiperconcentración en Servicios hace a la economía de Tenerife "
                "especialmente dependiente del turismo y vulnerable a sus ciclos."
            ),
        )

    st.subheader("¿En qué trabajan los más ricos y los más pobres?")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Por sector de actividad**")
        plot(
            "plots/renta/grouped_bar_ingresos_quintiles.png",
            caption=(
                "% de trabajadores en cada sector de actividad según quintil de renta (2023). "
                "Q1 = 20% más pobre, Q5 = 20% más rico."
            ),
            insight=(
                "Los Servicios dominan todos los quintiles, pero en Q5 (más rico) "
                "su peso es mayor (~90%) y Construcción y Agricultura casi desaparecen."
            ),
        )
    with col2:
        st.markdown("**Por categoría de ocupación**")
        plot(
            "plots/renta/grouped_bar_ocupacion_quintiles.png",
            caption=(
                "% de trabajadores en cada categoría de ocupación según quintil de renta (2023). "
                "La composición por tipo de trabajo cambia drásticamente entre quintiles."
            ),
            insight=(
                "En Q1 el 61% son trabajadores cualificados/operarios; en Q5 ese porcentaje cae al 42% "
                "mientras los Directores/técnicos suben del 20% al 51%. "
                "Las Ocupaciones elementales pasan del 19% al 7%."
            ),
        )

# ===========================================================================
# 5. BRECHA DE GÉNERO
# ===========================================================================
elif seccion == "⚖️ Brecha de género":
    st.header("⚖️ Brecha de género")
    st.markdown(
        '<p class="section-intro">'
        "La segregación ocupacional por género es otro eje de desigualdad. "
        "Aunque globalmente hay paridad numérica entre hombres y mujeres en el empleo, "
        "la distribución entre categorías de ocupación no es simétrica. "
        "Esto tiene implicaciones directas sobre la brecha salarial."
        "</p>",
        unsafe_allow_html=True,
    )

    st.subheader("Brecha de género por categoría de ocupación")
    plot(
        "plots/genero/piramide_ocupacion.png",
        caption=(
            "Diverging bar chart que muestra la desviación porcentual respecto a la paridad (50/50) "
            "para cada categoría de ocupación en Tenerife (2023). "
            "Barras hacia la derecha (rojo) = más mujeres; hacia la izquierda (azul) = más hombres."
        ),
        insight=(
            "Los 'Trabajadores cualificados/operarios' son el único sector con predominio masculino "
            "significativo (~55% hombres). 'Directores/técnicos' y 'Ocupaciones elementales' "
            "tienen leve mayoría femenina, pero en estos últimos la renta es más baja, "
            "lo que contribuye a la brecha salarial de género."
        ),
    )

# ===========================================================================
# 6. MAPA INTERACTIVO
# ===========================================================================
elif seccion == "🗺️ Mapa interactivo":
    st.header("🗺️ Mapa interactivo de secciones censales")
    st.markdown(
        '<p class="section-intro">'
        "El mapa permite explorar espacialmente cuatro variables clave a nivel de "
        "sección censal: renta neta media, peso de las prestaciones por desempleo, "
        "porcentaje de trabajadores en Servicios y brecha de género en ocupaciones elementales. "
        "Usa el selector de capas para cambiar entre variables."
        "</p>",
        unsafe_allow_html=True,
    )

    map_path = Path("plots/secciones_map.html")

    if map_path.exists():
        with open(map_path, "r", encoding="utf-8") as f:
            html = f.read()
        st.components.v1.html(html, height=600, scrolling=False)
    else:
        st.info(
            "El mapa no se ha generado todavía. "
            "Materializa el asset `interactive_secciones_map` en Dagster para generarlo."
        )

    st.markdown("---")
    st.markdown(
        "También disponible en GitHub Pages: "
        "[Ver mapa completo](https://fmararm.github.io/master/index_map.html) · "
        "O en local en `http://127.0.0.1:8050/secciones_map.html` "
        "(ejecutar `interactive_secciones_map` en Dagster)."
    )
