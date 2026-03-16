import pandas as pd
renta = pd.read_csv("data/distribucion-renta-canarias.csv")
renta = renta.rename(columns={
    "TERRITORIO#es": "region",
    "TIME_PERIOD#es": "year",
    "MEDIDAS#es": "measure",
    "OBS_VALUE": "value"
})
print(renta['region'].unique())
