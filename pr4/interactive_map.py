from dagster import asset, Output, MetadataValue
import folium
import json
import os, shutil
import webbrowser
from branca.element import Element

def map_island(code):
    if not isinstance(code, str): return "Canarias"
    if code.startswith("35"):
        code_int = int(code)
        FUERTEVENTURA = {35003, 35007, 35014, 35015, 35017, 35023}
        LANZAROTE = {35004, 35010, 35019, 35022, 35024, 35028, 35029}
        if code_int in FUERTEVENTURA: return "Fuerteventura"
        if code_int in LANZAROTE: return "Lanzarote"
        return "Gran Canaria"
    if code.startswith("38"):
        code_int = int(code)
        LA_GOMERA = {38004, 38019, 38021, 38026, 38041}
        EL_HIERRO = {38002, 38043, 38048}
        LA_PALMA  = {38001, 38003, 38006, 38009, 38015, 38024,
                     38027, 38030, 38033, 38036, 38037}
        if code_int in EL_HIERRO: return "El Hierro"
        if code_int in LA_PALMA: return "La Palma"
        if code_int in LA_GOMERA: return "La Gomera"
        return "Tenerife"
    return "Canarias"

@asset
def interactive_municipality_map(context):
    """
    Asset de Dagster que lee el GeoJSON de Municipios-2024.json,
    genera un mapa interactivo usando Folium y lo guarda en HTML.
    Finalmente, abre el mapa en el navegador por defecto del sistema.
    """
    # Ruta al archivo GeoJSON
    geojson_path = "data/Municipios-2024.json"
    
    # Comprobar si el archivo existe
    if not os.path.exists(geojson_path):
        raise FileNotFoundError(f"No se encontró el archivo: {geojson_path}")
        
    # Crear un mapa base
    m = folium.Map(location=[28.2915, -16.6291], zoom_start=8, tiles=None)
    
    # Añadir el mapa base plano
    folium.TileLayer('cartodbpositron', name="Mapa Base", overlay=False).add_to(m)

    # Cargar los datos GeoJSON
    with open(geojson_path, 'r', encoding='utf-8') as f:
        geo_data = json.load(f)

    import branca.colormap as cm
    
    # helper para extraer valores ignorando nulos/vacíos
    def get_values(geo_data, prop):
        vals = []
        for f in geo_data['features']:
            p = f['properties']
            if prop in p and p[prop] is not None:
                try: vals.append(float(p[prop]))
                except ValueError: pass
        return vals

    # 1. Población Activa
    renta_vals = get_values(geo_data, 'pact_t')
    if renta_vals:
        cmap_renta = cm.LinearColormap(colors=['#ffeda0', '#feb24c', '#f03b20'], vmin=min(renta_vals), vmax=max(renta_vals))
        
        fg_renta = folium.FeatureGroup(name="Población Activa", overlay=False)
        folium.GeoJson(
            geo_data,
            style_function=lambda feature: {
                'fillColor': cmap_renta(float(feature['properties']['pact_t'])) if feature['properties'].get('pact_t') else '#808080',
                'color': 'black',
                'weight': 1,
                'fillOpacity': 0.7,
            },
            tooltip=folium.GeoJsonTooltip(
                fields=['label', 'pact_t'],
                aliases=['Municipio:', 'Población Activa:'],
                localize=True
            )
        ).add_to(fg_renta)
        fg_renta.add_to(m)

    # 2. Población Ocupada
    pob_vals = get_values(geo_data, 'pocu_t')
    if pob_vals:
        cmap_pob = cm.LinearColormap(colors=['#edf8b1', '#7fcdbb', '#2c7fb8'], vmin=min(pob_vals), vmax=max(pob_vals))
        
        fg_pob = folium.FeatureGroup(name="Población Ocupada", overlay=False)
        folium.GeoJson(
            geo_data,
            style_function=lambda feature: {
                'fillColor': cmap_pob(float(feature['properties']['pocu_t'])) if feature['properties'].get('pocu_t') else '#808080',
                'color': 'black',
                'weight': 1,
                'fillOpacity': 0.7,
            },
            tooltip=folium.GeoJsonTooltip(
                fields=['label', 'pocu_t'],
                aliases=['Municipio:', 'Población Ocupada:'],
                localize=True
            )
        ).add_to(fg_pob)
        fg_pob.add_to(m)
        # Notas: Folium no gestiona bien colormaps dinámicos atados al feature group en la visibilidad por defecto.
        # Solo mostraremos la de Población Activa por defecto.

    # 3. Tasa de Paro
    paro_vals = get_values(geo_data, 'tpar_t')
    if paro_vals:
        cmap_paro = cm.LinearColormap(colors=['#e0ecf4', '#9ebcda', '#8856a7'], vmin=min(paro_vals), vmax=max(paro_vals))
        
        fg_paro = folium.FeatureGroup(name="Tasa de Paro (%)", overlay=False)
        folium.GeoJson(
            geo_data,
            style_function=lambda feature: {
                'fillColor': cmap_paro(float(feature['properties']['tpar_t'])) if feature['properties'].get('tpar_t') else '#808080',
                'color': 'black',
                'weight': 1,
                'fillOpacity': 0.7,
            },
            tooltip=folium.GeoJsonTooltip(
                fields=['label', 'tpar_t'],
                aliases=['Municipio:', 'Tasa Paro (%):'],
                localize=True
            )
        ).add_to(fg_paro)
        fg_paro.add_to(m)

    # Añadir control de capas (Dropdown arriba a la derecha)
    folium.LayerControl(collapsed=False).add_to(m)

    # 4. Lógica de Leyenda Dinámica (JS)
    # --- Gestión de Leyendas ---
    cmap_renta.caption = 'Población Activa'
    cmap_pob.caption = 'Población Ocupada'
    cmap_paro.caption = 'Tasa de Paro (%)'
    
    # CSS: Forzamos a todas las leyendas a ocupar el mismo espacio arriba a la derecha.
    # El uso de 'display: none' asegura que no se apilen verticalmente.
    m.get_root().header.add_child(Element("""
    <style>
        .legend { 
            display: none; 
            position: fixed !important; 
            top: 10px !important; 
            right: 10px !important; 
            z-index: 1000 !important;
            background: rgba(255,255,255,0.85);
            border: 1px solid #777;
            padding: 8px;
            border-radius: 5px;
            box-shadow: 0 0 10px rgba(0,0,0,0.2);
        }
    </style>"""))
    
    m.add_child(cmap_renta)
    m.add_child(cmap_pob)
    m.add_child(cmap_paro)

    # Script para alternar leyendas basado en el nombre de la capa activa (Población Activa, etc.)
    js_legend_toggle = """
    <script>
    function toggleLegends() {
        var mapInstance = null;
        // Buscamos la instancia del mapa de Folium/Leaflet
        for (var key in window) {
            if (key.startsWith('map_') && window[key] instanceof L.Map) {
                mapInstance = window[key];
                break;
            }
        }
        
        if (mapInstance) {
            function updateLegends(targetName) {
                var legends = document.getElementsByClassName('legend');
                for (var i = 0; i < legends.length; i++) {
                    // El texto de la leyenda (caption) está dentro del SVG o div
                    var legendContent = legends[i].innerHTML || "";
                    if (legendContent.includes(targetName)) {
                        legends[i].style.display = 'block';
                    } else {
                        legends[i].style.display = 'none';
                    }
                }
            }

            // Escuchar el cambio de capa base
            mapInstance.on('baselayerchange', function(e) {
                updateLegends(e.name);
            });
            
            // Inicializar con la primera capa tras un breve retardo para que el DOM esté listo
            setTimeout(function() {
                updateLegends("Población Activa");
            }, 300);
        }
    }
    window.onload = toggleLegends;
    </script>
    """
    m.get_root().html.add_child(Element(js_legend_toggle))
    
    # Ruta de salida para el HTML
    output_html_path = "plots/municipios_map.html"
    os.makedirs(os.path.dirname(output_html_path), exist_ok=True)
    
    # Guardar el mapa
    m.save(output_html_path)
    
    # --- Copia automática para GitHub Pages ---
    docs_map_path = os.path.join("docs", "index_map.html")
    os.makedirs(os.path.dirname(docs_map_path), exist_ok=True)
    shutil.copy(output_html_path, docs_map_path)
    
    # Ejecutar un pequeño servidor de Python en esa carpeta, en segundo plano (puerto 8050)
    import subprocess
    import time
    
    port = 8050
    plot_dir = os.path.abspath(os.path.dirname(output_html_path))
    
    # Intentamos matar si había otro servidor en este puerto antes de lanzarlo (para no bloquearlo de nuevo)
    # Suprimimos la salida de error por si falla (es decir, por si no había nada corriendo)
    subprocess.run(["fuser", "-k", f"{port}/tcp"], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
    
    # Levantamos el servidor en background
    subprocess.Popen(
        ["python3", "-m", "http.server", str(port)], 
        cwd=plot_dir, 
        stdout=subprocess.DEVNULL, 
        stderr=subprocess.DEVNULL
    )
    
    # Esperamos 1 segundo para asegurarnos de que el servidor está levantado
    time.sleep(1)
    
    # Abrir en el navegador apuntando a localhost
    url = f"http://127.0.0.1:{port}/municipios_map.html"
    webbrowser.open(url)
    
    return Output(
        value=output_html_path,
        metadata={
            "url_servidor": url,
            "elementos_dibujados": len(geo_data.get('features', [])),
            "mensaje": f"Mapa interactivo sirviéndose en {url}"
        }
    )


