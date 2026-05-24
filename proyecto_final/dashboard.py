import streamlit as st
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuración de página
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Dos Tenerifes",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# CSS dramático
# ---------------------------------------------------------------------------
st.markdown("""
<style>
    /* Fondo oscuro en toda la aplicación */
    .stApp {
        background-color: #111111;
    }
    [data-testid="stAppViewContainer"] {
        background-color: #111111;
    }
    [data-testid="stMain"] {
        background-color: #111111;
    }
    .block-container {
        background-color: #111111;
        padding-top: 2rem;
        padding-bottom: 2rem;
    }

    [data-testid="stSidebar"] {
        background-color: #0d0d0d;
    }
    [data-testid="stSidebar"] .stRadio label,
    [data-testid="stSidebar"] p,
    [data-testid="stSidebar"] span,
    [data-testid="stSidebar"] div {
        color: #cccccc !important;
    }
    [data-testid="stSidebar"] hr {
        border-color: #333333;
    }
    [data-testid="stSidebar"] strong {
        color: #ffffff !important;
    }

    h1 {
        color: #ffffff !important;
        font-size: 2.8rem !important;
        font-weight: 900 !important;
        letter-spacing: -0.03em;
        border-bottom: 5px solid #b03a2e;
        padding-bottom: 0.5rem;
        margin-bottom: 1.2rem;
    }
    h2 {
        color: #ffffff !important;
        font-weight: 800 !important;
        letter-spacing: -0.02em;
        border-bottom: 3px solid #b03a2e;
        padding-bottom: 0.3rem;
        margin-top: 1.8rem;
    }
    h3 {
        color: #eeeeee !important;
        font-weight: 700 !important;
        letter-spacing: -0.01em;
    }
    p, li, td, th {
        color: #dddddd !important;
    }

    .caption-box {
        background: #111111;
        border-left: 4px solid #555555;
        padding: 0.9rem 1.1rem;
        border-radius: 0 4px 4px 0;
        margin: 0.4rem 0 1rem 0;
        font-size: 0.88rem;
        color: #aaaaaa !important;
        line-height: 1.7;
        font-style: italic;
    }
    .insight-box {
        background: #7b241c;
        border-left: 6px solid #b03a2e;
        padding: 1rem 1.2rem;
        border-radius: 0 4px 4px 0;
        margin: 0.4rem 0 1.8rem 0;
        font-size: 0.95rem;
        color: #ffffff !important;
        line-height: 1.7;
        font-weight: 600;
    }
    .section-intro {
        font-size: 1.08rem;
        color: #dddddd !important;
        line-height: 1.85;
        margin-bottom: 1.8rem;
        border-left: 5px solid #b03a2e;
        padding-left: 1.2rem;
    }

    div[data-testid="stMetric"] {
        background: #0d0d0d;
        border-radius: 4px;
        padding: 1rem 1.2rem;
    }
    div[data-testid="stMetricLabel"] p {
        color: #888888 !important;
        text-transform: uppercase;
        font-size: 0.72rem !important;
        letter-spacing: 0.12em;
    }
    div[data-testid="stMetricValue"] {
        color: #b03a2e !important;
        font-size: 2.2rem !important;
        font-weight: 900 !important;
    }

    .stTabs [data-baseweb="tab"] {
        font-size: 0.9rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.04em;
    }
    .stTabs [aria-selected="true"] {
        color: #b03a2e !important;
        border-bottom-color: #b03a2e !important;
    }
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
        st.markdown(f'<div class="insight-box">{insight}</div>', unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
SECCIONES = [
    "Introducción",
    "La fractura de la renta",
    "Desigualdad interna",
    "Actividad y ocupación",
    "Brecha de género",
    "Mapa interactivo",
]
seccion = st.sidebar.radio("Navegar a", SECCIONES)
st.sidebar.markdown("---")
st.sidebar.markdown(
    "**Proyecto final**  \n"
    "Visualización de Datos 2025-2026  \n"
    "Fuente: INE, Atlas de Distribución de Renta de los Hogares"
)

# ===========================================================================
# 1. INTRODUCCIÓN
# ===========================================================================
if seccion == "Introducción":
    st.title("Dos Tenerifes")
    st.markdown(
        '<p class="section-intro">'
        "Tenerife no es una isla. Son dos. Una acumula renta, empleo cualificado y "
        "oportunidades y la otra sostiene la economía de servicio. Los datos del "
        "<strong>Atlas de Distribución de Renta de los Hogares del INE (2021-2023)</strong> "
        "permiten ver con precisión dónde está la línea que las separa, y confirmar "
        "que esa línea no se ha movido."
        "</p>",
        unsafe_allow_html=True,
    )

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Municipios analizados", "54")
    col2.metric("Años cubiertos", "2021 - 2023")
    col3.metric("Secciones censales", "681")
    col4.metric("Ratio renta max/min", "3x")

    st.markdown("---")
    st.markdown("### Qué encontrarás aquí")
    st.markdown("""
| Sección | Qué muestra |
|---|---|
| La fractura de la renta | Ranking de municipios, distribución y evolución temporal |
| Desigualdad interna | Desigualdad dentro de cada municipio e isla, correlación con ocupación |
| Actividad y ocupación | Composición del empleo y cómo varía según el nivel de renta |
| Brecha de género | Diferencias por sexo en las categorías de ocupación |
| Mapa interactivo | Exploración espacial de las 4 variables clave |
""")

# ===========================================================================
# 2. LA FRACTURA DE LA RENTA
# ===========================================================================
elif seccion == "La fractura de la renta":
    st.header("La fractura de la renta")
    st.markdown(
        '<p class="section-intro">'
        "Hay municipios en Tenerife que triplican la renta de otros situados a menos "
        "de 50 kilómetros. No es una anomalía estadística ni un efecto puntual de la "
        "pandemia: es una estructura que los datos de tres años consecutivos reproducen "
        "con precisión. La isla que vende igualdad de sol y playa es, en términos "
        "económicos, profundamente asimétrica."
        "</p>",
        unsafe_allow_html=True,
    )

    st.subheader("Ranking de municipios por renta")
    plot(
        "plots/renta/lollipop_ranking_municipios.png",
        caption=(
            "Primeros y últimos 15 municipios de Tenerife ordenados por renta neta media "
            "por hogar (2023). La distancia entre el primero y el último supera el factor 2."
        ),
        insight=(
            "Los municipios del sur turístico aparecen en posiciones intermedias. "
            "El turismo de masas genera empleo pero no redistribuye renta: "
            "quienes sirven las mesas y limpian las habitaciones viven en secciones "
            "censales que no salen de la mitad baja del ranking."
        ),
    )

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Distribución de la renta")
        plot(
            "plots/renta/histograma_hist_renta.png",
            caption=(
                "Histograma de la renta neta media por sección censal (2023). "
                "La distribución es asimétrica positiva: la mayoría de secciones "
                "se concentra entre 25.000 y 45.000 euros, pero una cola larga "
                "de secciones muy ricas eleva artificialmente la media."
            ),
            insight=(
                "La media estadística de la isla miente sobre la experiencia "
                "de la mayoría. Gran parte de las secciones censales vive "
                "por debajo de ella. Los outliers de la cola derecha no son "
                "la norma: son la excepción que distorsiona el promedio."
            ),
        )
    with col2:
        st.subheader("Evolución 2021-2023")
        plot(
            "plots/renta/heatmap_renta_municipio.png",
            caption=(
                "Heatmap de renta media por municipio y año (2021-2023). "
                "Verde intenso indica renta alta; amarillo, renta baja. "
                "El orden de los municipios es constante en los tres años."
            ),
            insight=(
                "Tres años de datos cuentan la misma historia: los municipios "
                "ricos permanecen verdes y los pobres no salen del amarillo. "
                "La pandemia no igualó. El boom turístico no igualó. "
                "La jerarquía económica de la isla es estructural."
            ),
        )

    st.subheader("Quién mejoró y quién quedó atrás (2021-2023)")
    plot(
        "plots/renta/slope_brecha_temporal.png",
        caption=(
            "Slope chart de la renta por municipio entre 2021 y 2023. "
            "Líneas rojas: municipios que pierden posición relativa. "
            "Líneas verdes: municipios que mejoran."
        ),
        insight=(
            "El crecimiento existe, pero tiene dueños claros. "
            "Los municipios que ya partían de una posición baja crecen "
            "proporcionalmente menos que los que ya eran ricos. "
            "La fractura no se cierra: se consolida."
        ),
    )

# ===========================================================================
# 3. DESIGUALDAD INTERNA
# ===========================================================================
elif seccion == "Desigualdad interna":
    st.header("Desigualdad interna")
    st.markdown(
        '<p class="section-intro">'
        "La comparación entre municipios subestima la magnitud del problema. "
        "Dentro de Santa Cruz de Tenerife o de San Cristóbal de La Laguna "
        "coexisten secciones censales separadas por menos de cinco kilómetros "
        "y más de 60.000 euros de diferencia en renta media. "
        "La desigualdad no es solo geográfica: es de calle a calle, de bloque a bloque."
        "</p>",
        unsafe_allow_html=True,
    )

    st.subheader("Desigualdad intramunicipal por isla")
    st.markdown(
        "Cada caja muestra la distribución de renta de las secciones censales "
        "de un municipio. Cuanto más ancha la caja y más largos los bigotes, "
        "mayor desigualdad interna."
    )
    tabs = st.tabs(["Tenerife", "La Palma", "La Gomera", "El Hierro"])
    configs = [
        (
            "plots/renta/boxplot_desigualdad_intramunicipal_tenerife.png",
            "Tenerife. Cada caja representa un municipio; los puntos son secciones atípicas. "
            "El rango intercuartílico de Santa Cruz y La Laguna es el más amplio de toda la provincia.",
            "Santa Cruz de Tenerife tiene secciones censales con rentas superiores a 80.000 euros "
            "por hogar y otras que no alcanzan los 20.000 euros. La misma ciudad, "
            "dos realidades económicas que no se tocan."
        ),
        (
            "plots/renta/boxplot_desigualdad_intramunicipal_la_palma.png",
            "La Palma. Distribuciones más compactas que Tenerife, "
            "pero Santa Cruz de La Palma concentra la mayor dispersión de la isla.",
            None
        ),
        (
            "plots/renta/boxplot_desigualdad_intramunicipal_la_gomera.png",
            "La Gomera. Pocos municipios y pocas secciones. "
            "San Sebastián de La Gomera presenta la renta más alta y la mayor dispersión.",
            None
        ),
        (
            "plots/renta/boxplot_desigualdad_intramunicipal_el_hierro.png",
            "El Hierro. Solo 3 municipios. Rentas sistemáticamente más bajas que el resto "
            "del archipiélago, con poca variabilidad interna.",
            "El Hierro es la isla con menor renta media y también la que tiene menor "
            "desigualdad interna. Cuando todos son pobres por igual, la caja se estrecha."
        ),
    ]
    for tab, (img, cap, ins) in zip(tabs, configs):
        with tab:
            plot(img, caption=cap, insight=ins)

    st.subheader("Correlación: ocupaciones elementales y renta")
    plot(
        "plots/renta/scatter_elementales_renta.png",
        caption=(
            "Scatter de secciones censales: eje X = porcentaje de trabajadores en ocupaciones "
            "elementales, eje Y = renta neta media, tamaño de burbuja = número total de trabajadores "
            "(2023). Línea de tendencia en rojo."
        ),
        insight=(
            "La pendiente no engaña. Cuanto mayor el peso de las ocupaciones elementales, "
            "menor la renta de toda la sección. No es una correlación débil ni ruidosa: "
            "es una caída sistemática que dibuja el mapa de la segregación laboral y "
            "residencial de Tenerife con una claridad que incomoda."
        ),
    )

# ===========================================================================
# 4. ACTIVIDAD Y OCUPACIÓN
# ===========================================================================
elif seccion == "Actividad y ocupación":
    st.header("Actividad y ocupación")
    st.markdown(
        '<p class="section-intro">'
        "Todos trabajan en Servicios. Pero no en el mismo Servicios. "
        "La etiqueta sectorial es la misma para quien dirige un hotel de lujo "
        "y para quien limpia sus habitaciones. Lo que los separa (la ocupación, "
        "la renta, el barrio donde viven) queda oculto bajo esa misma categoría. "
        "Los datos de quintiles lo hacen visible."
        "</p>",
        unsafe_allow_html=True,
    )

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Fuentes de ingresos de los hogares")
        plot(
            "plots/renta/waterfall_fuentes_ingresos.png",
            caption=(
                "Composición media de ingresos por sección censal en Tenerife (2023). "
                "Cada barra representa el peso porcentual de cada fuente sobre el total."
            ),
            insight=(
                "Las pensiones tienen un peso desproporcionado. Tenerife envejece, "
                "y una fracción significativa de su renta disponible procede no del "
                "trabajo activo sino de prestaciones acumuladas. "
                "Cuando ese colchón demográfico se erosione, la fractura se profundizará."
            ),
        )
    with col2:
        st.subheader("Trabajadores por sector económico")
        plot(
            "plots/actividad/waterfall_actividades.png",
            caption=(
                "Porcentaje de trabajadores por sector CNAE en Tenerife (2023). "
                "Los Servicios concentran más del 85% del empleo."
            ),
            insight=(
                "Una sola crisis en el turismo golpea a casi toda la fuerza de trabajo activa. "
                "No es diversificación económica: es dependencia total. "
                "Tenerife conoce bien ese riesgo, y sigue sin resolverlo."
            ),
        )

    st.subheader("En qué trabajan los más ricos y los más pobres")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Por sector de actividad**")
        plot(
            "plots/renta/grouped_bar_ingresos_quintiles.png",
            caption=(
                "Porcentaje de trabajadores en cada sector de actividad según quintil de renta (2023). "
                "Q1 = 20% más pobre, Q5 = 20% más rico."
            ),
            insight=(
                "El Q5 trabaja en Servicios financieros, de gestión y consultoría. "
                "El Q1 trabaja en hostelería, limpieza y comercio minorista. "
                "La etiqueta sectorial es idéntica; la realidad económica, opuesta."
            ),
        )
    with col2:
        st.markdown("**Por categoría de ocupación**")
        plot(
            "plots/renta/grouped_bar_ocupacion_quintiles.png",
            caption=(
                "Porcentaje de trabajadores en cada categoría de ocupación según quintil de renta (2023). "
                "La composición cambia drásticamente del Q1 al Q5."
            ),
            insight=(
                "Del Q1 al Q5, la categoría 'Directores y técnicos' pasa del 20% al 51%. "
                "Las ocupaciones elementales (las peor remuneradas) concentran en el quintil "
                "más pobre casi el doble de trabajadores que en el más rico. "
                "No es talento distribuido al azar: es el resultado acumulado del barrio "
                "en que se nació, la educación recibida y el capital social heredado."
            ),
        )

# ===========================================================================
# 5. BRECHA DE GÉNERO
# ===========================================================================
elif seccion == "Brecha de género":
    st.header("Brecha de género")
    st.markdown(
        '<p class="section-intro">'
        "La paridad numérica en el empleo total oculta una segregación funcional "
        "que los datos de ocupación hacen aflorar. Las mujeres no están ausentes "
        "del mercado laboral de Tenerife: están presentes en las categorías "
        "que menos cobran. Eso no es igualdad. Es la apariencia de ella."
        "</p>",
        unsafe_allow_html=True,
    )

    st.subheader("Brecha de género por categoría de ocupación")
    plot(
        "plots/genero/piramide_ocupacion.png",
        caption=(
            "Desviación porcentual respecto a la paridad (50/50) para cada categoría "
            "de ocupación en Tenerife (2023). "
            "Barras a la derecha = más mujeres; a la izquierda = más hombres."
        ),
        insight=(
            "Las mujeres están sobrerrepresentadas en las ocupaciones elementales, "
            "las de menor retribución. Los hombres dominan el trabajo cualificado "
            "de operario y técnico, mejor remunerado. "
            "La brecha salarial de género no es una abstracción estadística: "
            "es la suma de estas asimetrías repetidas en cada sección censal de la isla."
        ),
    )

# ===========================================================================
# 6. MAPA INTERACTIVO
# ===========================================================================
elif seccion == "Mapa interactivo":
    st.header("Mapa interactivo de secciones censales")
    st.markdown(
        '<p class="section-intro">'
        "La fractura que los gráficos describen tiene coordenadas precisas. "
        "El mapa muestra tres capas a nivel de sección censal: "
        "renta neta media por hogar, porcentaje de ingresos por desempleo "
        "y porcentaje de trabajadores en Servicios. "
        "Cambia de capa para ver cómo se superponen las distintas caras de la desigualdad "
        "sobre el mismo territorio."
        "</p>",
        unsafe_allow_html=True,
    )

    map_path = Path("plots/secciones_map.html")

    if map_path.exists():
        with open(map_path, "r", encoding="utf-8") as f:
            html = f.read()
        st.components.v1.html(html, height=620, scrolling=False)
    else:
        st.info(
            "El mapa no se ha generado todavía. "
            "Materializa el asset interactive_secciones_map en Dagster para generarlo."
        )

    st.markdown("---")
    st.markdown(
        "También disponible en GitHub Pages: "
        "[Ver mapa completo](https://fmararm.github.io/visualizacion_de_datos/index_map.html). "
        "En local: http://127.0.0.1:8050/secciones_map.html "
        "(requiere materializar el asset en Dagster)."
    )
