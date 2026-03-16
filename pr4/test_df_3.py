import pandas as pd
df = pd.read_excel("data/nivelestudios.xlsx")
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
        if code_int in range(35003, 35022) and code_int not in [35002, 35010]: return "Fuerteventura"
        if code_int in [35002, 35010, 35016, 35022, 35024, 35028, 35029]: return "Lanzarote"
        return "Gran Canaria"
    if code.startswith("38"):
        code_int = int(code)
        if code_int in [38002, 38043, 38048]: return "El Hierro"
        if code_int in [38001, 38003, 38006, 38009, 38015, 38024, 38027, 38030, 38033, 38036, 38037]: return "La Palma"
        if code_int in [38004, 38019, 38021, 38026, 38041]: return "La Gomera"
        return "Tenerife"
    return "Canarias"

df['island'] = df['municipality_code'].apply(map_island)
df['year'] = pd.to_numeric(df['year'].astype(str).str.extract(r'^(\d{4})')[0], errors='coerce')
df = df.dropna(subset=['municipality_code', 'year', 'total', 'education_level'])
df['year'] = df['year'].astype(int)

# Filtros que se aplican en la descripcion
df_filtrado = df[(df['year'] == 2023) & (df['sex'] == 'Total')]
df_filtrado = df_filtrado[~df_filtrado['education_level'].isin(['Total', 'No cursa estudios'])]

print(df_filtrado.head(10).to_string())
print("\n" + "="*50 + "\n")
print(df_filtrado.groupby('education_level')['total'].sum())
