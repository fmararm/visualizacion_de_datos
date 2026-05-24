# Dos Tenerifes — Visualización de Datos 2025–2026

Proyecto final de la asignatura de Visualización de Datos. Cuenta una historia sobre la **desigualdad territorial en Tenerife** a través de datos socioeconómicos a nivel de sección censal (2021–2023).

La narrativa articula cuatro ángulos complementarios:

| Ángulo | Pregunta |
|--------|----------|
| Dos Tenerifes | ¿Qué municipios concentran la riqueza y cuáles la pobreza? |
| Paradoja del turismo | ¿Trabajar en Servicios implica ganar menos? |
| Precariedad estructural | ¿Dónde viven quienes dependen de prestaciones por desempleo? |
| Brecha de género | ¿Se reparte igual el trabajo precario entre hombres y mujeres? |
| Desigualdad intramunicipal | ¿Cuánta desigualdad hay dentro de un mismo municipio? |

---

## Estructura del proyecto

```
proyecto_final/
├── data/                        # Datos (ver data/README.md)
│   ├── cartografia-secciones/   # GeoJSON secciones censales Tenerife (2021-2024)
│   ├── rentamedia-sc-3.csv
│   ├── actividad-sc-3.csv
│   ├── ocupacion-sc-3.csv
│   ├── distribucion-renta-ingresos.csv
│   └── codislas.csv
├── plots/                       # Gráficos generados (creados al ejecutar)
│   ├── renta/
│   ├── actividad/
│   └── genero/
├── docs/                        # GitHub Pages
│   ├── images/                  # Copias de los gráficos
│   └── index_map.html           # Mapa interactivo
├── definitions.py               # Registro Dagster (assets, jobs, sensor)
├── lab_renta.py                 # Pipeline principal: carga, limpieza, análisis y plots
├── interactive_map.py           # Mapa Folium de secciones censales
├── lab_renta_checks.py          # Asset checks de calidad y coherencia narrativa
└── requirements.txt
```

---

## Requisitos

- Python 3.10+
- Las dependencias están en `requirements.txt`:

```
dagster
dagster-webserver
pandas
numpy
plotnine
requests
folium
branca
```

---

## Instalación

```bash
# 1. Crear y activar entorno virtual
python3 -m venv .venv
source .venv/bin/activate        # Linux / macOS
# .venv\Scripts\activate         # Windows

# 2. Instalar dependencias
pip install -r requirements.txt
```

---

## Ejecución

### Opción A — Interfaz web de Dagster (recomendada)

```bash
dagster dev -f definitions.py
```

Se abre en `http://localhost:3000`. Desde ahí se puede:

- Materializar todos los assets con **Materialize All**
- Ver el grafo de dependencias en la vista **Asset Graph**
- Consultar los resultados de cada **Asset Check**
- Revisar los logs y el código generado por la IA en los metadatos de cada asset

### Opción B — Materializar por línea de comandos

```bash
# Todos los assets de una vez
dagster asset materialize -f definitions.py --select "*"

# Solo el pipeline de datos (sin mapa ni plots)
dagster asset materialize -f definitions.py \
  --select "rentamedia_load,actividad_load,ocupacion_load,ingresos_load,rentamedia_clean,actividad_clean,ocupacion_clean,ingresos_clean"

# Solo los datasets analíticos
dagster asset materialize -f definitions.py \
  --select "data_renta_municipio,data_desigualdad_intramunicipal,data_servicios_vs_renta,data_brecha_genero,data_prestaciones_vs_renta"

# Solo los gráficos
dagster asset materialize -f definitions.py \
  --select "ranking_municipios,desigualdad_intramunicipal,servicios_vs_renta,brecha_genero,prestaciones_vs_renta"

# Solo el mapa interactivo
dagster asset materialize -f definitions.py --select "interactive_secciones_map"
```

### Sensor automático

`definitions.py` incluye un sensor (`watch_folder_sensor`) que vigila cambios en `data/` y en los archivos `.py` de la raíz. Cuando detecta un cambio, lanza el job completo automáticamente. Se activa al arrancar `dagster dev`.

---

## Mapa interactivo de secciones

El mapa coroplético de Tenerife se genera con Folium al materializar el asset `interactive_secciones_map`.

### Ver en GitHub Pages (recomendado)

El mapa publicado está en:

**[https://fmararm.github.io/visualizacion_de_datos/index_map.html](https://fmararm.github.io/visualizacion_de_datos/index_map.html)**

### Ver en local tras materializar

Al materializar el asset `interactive_secciones_map`, Dagster levanta automáticamente un servidor HTTP en el puerto 8050. El mapa queda accesible en:

**[http://127.0.0.1:8050/secciones_map.html](http://127.0.0.1:8050/secciones_map.html)**

También puedes abrir directamente el archivo generado sin servidor:

```bash
# Desde la raíz del repositorio
open docs/index_map.html          # macOS
xdg-open docs/index_map.html      # Linux
start docs/index_map.html         # Windows
```

---

## Dashboard interactivo

El dashboard muestra todos los gráficos generados con texto explicativo y permite navegar por las secciones del análisis.

### Requisito previo

Asegúrate de que los assets de Dagster están materializados (los archivos PNG deben existir en `plots/`). Si no lo están, ejecuta primero:

```bash
dagster asset materialize -f definitions.py --select "*"
```

### Arrancar el dashboard

```bash
source .venv/bin/activate   # si no está ya activado
streamlit run dashboard.py --server.port 8501
```

Se abre en **[http://localhost:8501](http://localhost:8501)**.

El menú lateral permite navegar entre seis secciones:

| Sección | Contenido |
|---------|-----------|
| Mapa interactivo | Mapa coroplético de secciones censales coloreado por renta |
| Distribución de la renta | Lollipop de ranking de municipios e histograma |
| Composición económica | Waterfall de fuentes de ingresos y sectores de actividad |
| Desigualdad y brecha de género | Slope chart 2021–2023 y diverging bar de género |
| Desigualdad intramunicipal | Box plots por isla |
| Educación y quintiles | Scatter de nivel formativo y grouped bars por quintil |

Para parar el servidor pulsa `Ctrl+C` en la terminal.

---

## Pipeline de assets

```
rentamedia_load ──► rentamedia_clean ──┬──► data_renta_municipio ──────────► ranking_municipios
                                       ├──► data_desigualdad_intramunicipal ► desigualdad_intramunicipal
                                       ├──► data_servicios_vs_renta ─────────► servicios_vs_renta
                                       └──► data_prestaciones_vs_renta ──────► prestaciones_vs_renta

actividad_load ──► actividad_clean ────┬──► data_servicios_vs_renta
                                       └──► interactive_secciones_map

ocupacion_load ──► ocupacion_clean ────────► data_brecha_genero ────────────► brecha_genero

ingresos_load ──► ingresos_clean ──────┬──► data_prestaciones_vs_renta
                                       └──► interactive_secciones_map

rentamedia_clean ──────────────────────────► interactive_secciones_map
```

Cada visualización pasa por un asset de **prompt** (construye la instrucción para la IA) y un asset de **render** (ejecuta el código generado con plotnine). El mapa interactivo (`interactive_secciones_map`) se genera directamente con Folium, se sirve en `http://localhost:8050/secciones_map.html` y se copia a `docs/index_map.html`.

---

## Generación de gráficos con IA

Los plots de plotnine se generan mediante Claude o alternativamente un modelo de lenguaje local (`llama3.1:8b`) servido en `http://gpu1.esit.ull.es:4000`. El flujo es:

1. El asset `prompt_*` construye un dict con la descripción del gráfico y una muestra de los datos.
2. El asset de render llama a la API, extrae el bloque `def generar_plot(df)` de la respuesta y lo ejecuta.
3. El PNG resultante se guarda en `plots/` y se copia a `docs/images/`.

Si la API no está disponible, los assets de render fallarán con un error de conexión. Los assets de carga, limpieza y análisis no dependen de ella.

