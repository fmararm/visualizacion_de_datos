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
print(df['education_level'].unique())
