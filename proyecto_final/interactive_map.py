from dagster import asset, Output, MetadataValue
import folium
import json
import os, shutil, re
import pandas as pd
import branca.colormap as cm
from branca.element import Element

GEOJSON_PATH = "data/cartografia-secciones/secciones_20240101_tenerife.json"
OUTPUT_HTML  = "plots/secciones_map.html"
DOCS_HTML    = "docs/index_map.html"


def _strip_prefix(code):
    """Elimina el prefijo de fecha YYYYMMDD_ del geocode para normalizar claves."""
    m = re.match(r'\d{8}_(.*)', str(code))
    return m.group(1) if m else str(code)


def _build_layer(m, geo, name, data_map, colors, tooltip_alias):
    vals = [v for v in data_map.values() if pd.notna(v)]
    if not vals:
        return None
    vmin, vmax = min(vals), max(vals)
    cmap = cm.LinearColormap(colors, vmin=vmin, vmax=vmax, caption=name)

    fg = folium.FeatureGroup(name=name, overlay=False)
    folium.GeoJson(
        geo,
        style_function=lambda feat, d=data_map, c=cmap: {
            "fillColor": c(d[feat["properties"]["_key"]])
                         if feat["properties"]["_key"] in d and pd.notna(d[feat["properties"]["_key"]])
                         else "#cccccc",
            "color": "#444444",
            "weight": 0.4,
            "fillOpacity": 0.78,
        },
        tooltip=folium.GeoJsonTooltip(
            fields=["etiqueta", "_valor"],
            aliases=["Sección:", tooltip_alias],
            localize=True,
            sticky=True,
        ),
    ).add_to(fg)
    fg.add_to(m)
    return cmap


@asset
def interactive_secciones_map(context):
    """
    Mapa interactivo de secciones censales de Tenerife con cuatro capas temáticas
    que ilustran los ángulos narrativos de 'Dos Tenerifes':
      1. Renta neta media por hogar (2023)
      2. % ingresos procedentes de prestaciones por desempleo (2023)
      3. % trabajadores en el sector Servicios (2023)
      4. % mujeres en ocupaciones elementales (2023)
    """
    # ------------------------------------------------------------------
    # 1. Cartografía
    # ------------------------------------------------------------------
    with open(GEOJSON_PATH, encoding="utf-8") as f:
        geo = json.load(f)

    for feat in geo["features"]:
        feat["properties"]["_key"] = _strip_prefix(feat["properties"]["geocode"])

    # ------------------------------------------------------------------
    # 2. Datos socioeconómicos
    # ------------------------------------------------------------------

    # Renta neta media por hogar, 2023
    renta_df = pd.read_csv("data/rentamedia-sc-3.csv", encoding="utf-8-sig")
    renta_df = renta_df[
        (renta_df["año"] == 2023) &
        (renta_df["MEDIDAS_CODE"] == "RENTA_NETA_MEDIA_HOGAR")
    ].copy()
    renta_df["_key"] = renta_df["TERRITORIO_CODE"].apply(_strip_prefix)
    renta_map = dict(zip(renta_df["_key"], renta_df["OBS_VALUE"]))

    # % ingresos por prestaciones de desempleo, 2023
    ing_df = pd.read_csv("data/distribucion-renta-ingresos.csv", encoding="utf-8-sig")
    ing_df["OBS_VALUE"] = pd.to_numeric(
        ing_df["OBS_VALUE"].astype(str).str.replace(",", "."), errors="coerce"
    )
    desemp_df = ing_df[
        (ing_df["año"] == 2023) &
        (ing_df["MEDIDAS_CODE"] == "PRESTACIONES_DESEMPLEO")
    ].copy()
    desemp_df["_key"] = desemp_df["TERRITORIO_CODE"].apply(_strip_prefix)
    desemp_map = dict(zip(desemp_df["_key"], desemp_df["OBS_VALUE"]))

    # % trabajadores en Servicios sobre total, 2023
    act_df = pd.read_csv("data/actividad-sc-3.csv", encoding="utf-8-sig")
    act_df["_key"] = act_df["geocode"].apply(_strip_prefix)
    act_2023    = act_df[act_df["Periodo"] == 2023]
    total_act   = act_2023.groupby("_key")["num_casos"].sum()
    serv_act    = act_2023[act_2023["Actividad económica"] == "Servicios"].groupby("_key")["num_casos"].sum()
    servicios_map = ((serv_act / total_act * 100).fillna(0)).to_dict()

    # % mujeres en ocupaciones elementales, 2023
    ocu_df = pd.read_csv("data/ocupacion-sc-3.csv", encoding="utf-8-sig")
    ocu_df["_key"] = ocu_df["geocode"].apply(_strip_prefix)
    elem         = ocu_df[(ocu_df["año"] == 2023) & (ocu_df["ocupacion"] == "Ocupaciones elementales")]
    total_elem   = elem.groupby("_key")["num_casos"].sum()
    mujeres_elem = elem[elem["sexo"] == "Mujeres"].groupby("_key")["num_casos"].sum()
    genero_map   = ((mujeres_elem / total_elem * 100).fillna(0)).to_dict()

    # ------------------------------------------------------------------
    # 3. Construir mapa
    # ------------------------------------------------------------------
    m = folium.Map(location=[28.25, -16.55], zoom_start=9, tiles=None)
    folium.TileLayer("cartodbpositron", name="Mapa base", overlay=False).add_to(m)

    layers = [
        ("Renta neta media por hogar (€)",       renta_map,     ["#d73027", "#fee08b", "#1a9850"], "Renta (€):",       ".0f"),
        ("% Ingresos por desempleo",              desemp_map,    ["#edf8e9", "#74c476", "#005a32"], "% Desempleo:",     ".1f"),
        ("% Trabajadores en Servicios",           servicios_map, ["#fff5eb", "#fd8d3c", "#7f2704"], "% Servicios:",     ".1f"),
        ("% Mujeres en ocupaciones elementales",  genero_map,    ["#f7f4f9", "#9e9ac8", "#3f007d"], "% Mujeres elem.:", ".1f"),
    ]

    cmaps = []
    layer_names = []
    for name, data_map, colors, alias, fmt in layers:
        # Inyectar valor formateado para el tooltip de cada capa
        for feat in geo["features"]:
            key = feat["properties"]["_key"]
            val = data_map.get(key)
            feat["properties"]["_valor"] = (
                f"{val:{fmt}}" if val is not None and pd.notna(val) else "—"
            )
        cmap = _build_layer(m, geo, name, data_map, colors, alias)
        if cmap:
            cmaps.append(cmap)
            layer_names.append(name)

    folium.LayerControl(collapsed=False).add_to(m)
    for cmap in cmaps:
        m.add_child(cmap)

    # ------------------------------------------------------------------
    # 4. Leyendas dinámicas: mostrar solo la del layer activo
    # ------------------------------------------------------------------
    m.get_root().header.add_child(Element("""
    <style>
        .legend { display:none !important; position:fixed !important;
                  bottom:30px !important; right:10px !important;
                  z-index:1000 !important; background:rgba(255,255,255,.9);
                  border:1px solid #888; padding:8px; border-radius:5px;
                  box-shadow:0 0 8px rgba(0,0,0,.2); }
        .legend.legend-visible { display:block !important; }
    </style>
    """))

    first_layer = layer_names[0] if layer_names else ""
    m.get_root().html.add_child(Element(f"""
    <script>
    (function() {{
        var FIRST = {json.dumps(first_layer)};
        function findLegend(caption) {{
            return Array.from(document.querySelectorAll('.legend'))
                        .find(el => el.innerText && el.innerText.includes(caption));
        }}
        function showOnly(name) {{
            document.querySelectorAll('.legend')
                    .forEach(el => el.classList.remove('legend-visible'));
            var el = findLegend(name);
            if (el) el.classList.add('legend-visible');
        }}
        function init() {{
            var map = Object.values(window).find(v => v instanceof L.Map);
            if (!map) return;
            map.on('baselayerchange', e => showOnly(e.name));
            setTimeout(() => showOnly(FIRST), 700);
        }}
        document.readyState === 'loading'
            ? document.addEventListener('DOMContentLoaded', init)
            : setTimeout(init, 100);
    }})();
    </script>
    """))

    # ------------------------------------------------------------------
    # 5. Guardar y servir
    # ------------------------------------------------------------------
    os.makedirs(os.path.dirname(OUTPUT_HTML), exist_ok=True)
    m.save(OUTPUT_HTML)
    shutil.copy(OUTPUT_HTML, DOCS_HTML)

    import subprocess, time, webbrowser
    port = 8050
    plot_dir = os.path.abspath(os.path.dirname(OUTPUT_HTML))
    subprocess.run(["fuser", "-k", f"{port}/tcp"],
                   stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
    subprocess.Popen(
        ["python3", "-m", "http.server", str(port)],
        cwd=plot_dir, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    time.sleep(1)
    url = f"http://127.0.0.1:{port}/secciones_map.html"
    webbrowser.open(url)

    return Output(
        value=OUTPUT_HTML,
        metadata={
            "url":       MetadataValue.url(url),
            "secciones": MetadataValue.int(len(geo["features"])),
        },
    )
