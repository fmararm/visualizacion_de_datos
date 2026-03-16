
import pandas as pd
import numpy as np
import plotnine as p9
import re, requests, os, sys

# Mock context
class MockContext:
    def __init__(self):
        self.log = self
    def error(self, msg):
        print(f"ERROR: {msg}")
    def info(self, msg):
        print(f"INFO: {msg}")

def get_ia_template(description, df_columns):
    template_tecnico = "def generar_plot(df):\n    # plot = (p9.ggplot(df, p9.aes(...)) + ...)\n    # return plot"
    system_content = f"Eres un experto en Plotnine. Traduce la descripción a código.\nTemplate: {template_tecnico}\nDevuelve exclusivamente el código Python. Usa 'p9' para plotnine."
    user_content = f"Basándote en: {description}\nColumnas: {', '.join(df_columns)}"
    return {"model": "ollama/llama3.1:8b", "messages": [{"role": "system", "content": system_content}, {"role": "user", "content": user_content}], "temperature": 0.1, "stream": False}

def get_ia_code(template):
    url = "http://gpu1.esit.ull.es:4000/v1/chat/completions"
    headers = {"Authorization": "Bearer sk-1234"}
    response = requests.post(url, json=template, headers=headers, timeout=60)
    response.raise_for_status()
    codigo_raw = response.json()['choices'][0]['message']['content']
    match = re.search(r"```python\s+(.*?)\s+```", codigo_raw, re.DOTALL)
    return match.group(1).strip() if match else codigo_raw.strip()

def test_asset(name, desc, df):
    print(f"\n--- Testing {name} ---")
    template = get_ia_template(desc, df.columns)
    try:
        code = get_ia_code(template)
        print("Generated Code:")
        print(code)
        # Attempt to exec
        env = globals().copy()
        env.update({'p9': p9, 'pd': pd})
        env.update({k: v for k, v in p9.__dict__.items() if not k.startswith('_')})
        exec(code, env)
        plot = env['generar_plot'](df)
        print("Success: Plot generated.")
    except Exception as e:
        print(f"FAILURE: {e}")

if __name__ == "__main__":
    renta = pd.read_csv("data/distribucion-renta-canarias.csv")
    # Minimal cleaning for simulation
    renta = renta.rename(columns={"TERRITORIO#es": "region", "TIME_PERIOD#es": "year", "MEDIDAS#es": "measure", "OBS_VALUE": "value"})
    renta['year'] = pd.to_numeric(renta['year'].astype(str).str.extract(r'^(\d{4})')[0], errors='coerce').fillna(0).astype(int)
    
    # Test one that worked
    test_asset("income_composition_heatmap", "Mapa de calor (geom_tile) de la medida 'Pensiones' a lo largo de los años. Filtrar: medida 'Pensiones', islas principales. Ejes: x='year', y='region', fill='value'.", renta)
    
    # Test one that failed
    test_asset("income_composition_stacked_bar", "Gráfico de barras apiladas al 100% que muestre la composición de la renta por isla para el año 2023. Filtrar: islas principales y año 2023. Ejes: x='region', y='value', fill='measure'. Formato: geom_bar(position='fill'), etiquetas de eje Y en porcentaje.", renta)
