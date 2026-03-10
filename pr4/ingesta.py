import pandas as pd
from dagster import asset

@asset
def raw_sociodemo_canarias():
    # En clase: dales el CSV ya descargado en la carpeta /data
    df = pd.read_csv("./data/distribucion-renta-canarias.csv")
    return df
@asset
def raw_codislas():
    # En clase: dales el CSV ya descargado en la carpeta /data
    df = pd.read_csv("./data/codislas.csv", sep =";")
    return df

@asset(deps=[raw_sociodemo_canarias, raw_codislas])
def limpia_poblacion(raw_sociodemo_canarias, raw_codislas):
    df_renta = raw_sociodemo_canarias.copy()
    df_maestro = raw_codislas.copy()
    df_maestro['TERRITORIO_CODE'] = (
        df_maestro['CPRO'].astype(str).str.zfill(2) + 
        df_maestro['CMUN'].astype(str).str.zfill(3)
    )
    df_final = pd.merge(df_renta, df_maestro, on='TERRITORIO_CODE', how='left')
    df_final = df_final.rename(columns={
        'TIME_PERIOD#es': 'Anio',
        'TERRITORIO#es': 'Municipio',
        'MEDIDAS#es': 'Medida'
    })
    df_final.to_csv("./data/poblacion_limpia.csv", index=False)
    return df_maestro