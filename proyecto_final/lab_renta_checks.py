import os
import pandas as pd
from dagster import asset_check, AssetCheckResult, MetadataValue
from lab_renta import (
    rentamedia_load, rentamedia_clean,
    actividad_load, actividad_clean,
    ocupacion_load, ocupacion_clean,
    ingresos_load, ingresos_clean,
    data_renta_municipio, data_hist_renta,
    data_fuentes_ingresos, data_piramide_ocupacion,
    data_waterfall_actividades,
    data_slope_brecha, data_boxplot_intramunicipal,
    data_scatter_elementales, data_ingresos_quintiles, data_ocupacion_quintiles, data_heatmap_renta,
    ranking_municipios, hist_renta,
    fuentes_ingresos, piramide_ocupacion,
    waterfall_actividades,
    slope_brecha, boxplot_intramunicipal,
    scatter_elementales, grouped_bar_quintiles, grouped_bar_ocupacion, heatmap_renta,
)


def _check_file(path, description, gestalt, min_size=5000):
    exists = os.path.exists(path)
    size   = os.path.getsize(path) if exists else 0
    return AssetCheckResult(
        passed=bool(exists and size > min_size),
        metadata={
            "path":        MetadataValue.path(path),
            "size_kb":     MetadataValue.float(size / 1024),
            "description": description,
            "gestalt":     gestalt,
        }
    )


# ---------------------------------------------------------------------------
# Checks de carga (integridad básica)
# ---------------------------------------------------------------------------

@asset_check(asset=rentamedia_load)
def test_rentamedia_no_nulos(rentamedia_load):
    null_pct = rentamedia_load['OBS_VALUE'].isna().mean()
    return AssetCheckResult(
        passed=bool(null_pct < 0.05),
        metadata={
            "porcentaje_nulos": MetadataValue.float(float(null_pct * 100)),
            "description": "Tolerancia del 5% de nulos en OBS_VALUE.",
        }
    )

@asset_check(asset=actividad_load)
def test_actividad_sectores_completos(actividad_load):
    esperados = {'Agricultura, ganadería y pesca', 'Construcción', 'Industria', 'Servicios', 'No consta'}
    presentes = set(actividad_load['Actividad económica'].unique())
    faltan    = esperados - presentes
    return AssetCheckResult(
        passed=len(faltan) == 0,
        metadata={
            "sectores_faltantes": MetadataValue.text(str(faltan)),
            "description": "Verifica que los 5 sectores de actividad estén presentes.",
        }
    )

@asset_check(asset=ingresos_load)
def test_ingresos_fuentes_completas(ingresos_load):
    esperadas = {'SUELDOS_SALARIOS', 'PENSIONES', 'PRESTACIONES_DESEMPLEO',
                 'OTRAS_PRESTACIONES', 'OTROS_INGRESOS'}
    presentes = set(ingresos_load['MEDIDAS_CODE'].unique())
    faltan    = esperadas - presentes
    return AssetCheckResult(
        passed=len(faltan) == 0,
        metadata={
            "fuentes_faltantes": MetadataValue.text(str(faltan)),
            "description": "Verifica que las 5 fuentes de ingresos estén presentes.",
        }
    )


# ---------------------------------------------------------------------------
# Checks de limpieza
# ---------------------------------------------------------------------------

@asset_check(asset=rentamedia_clean)
def test_rentamedia_rango_valores(rentamedia_clean):
    vmin = rentamedia_clean['OBS_VALUE'].min()
    vmax = rentamedia_clean['OBS_VALUE'].max()
    passed = bool(vmin > 3_000 and vmax < 200_000)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "min_euros": MetadataValue.float(float(vmin)),
            "max_euros": MetadataValue.float(float(vmax)),
            "description": "Rango razonable de renta neta: [3 000 €, 200 000 €].",
        }
    )

@asset_check(asset=rentamedia_clean)
def test_rentamedia_cobertura_temporal(rentamedia_clean):
    años = sorted(rentamedia_clean['año'].unique())
    passed = bool(set(años) >= {2021, 2022, 2023})
    return AssetCheckResult(
        passed=passed,
        metadata={
            "años_presentes": MetadataValue.text(str(años)),
            "description": "Los tres años del estudio (2021-2023) deben estar presentes.",
            "gestalt": "Continuidad",
        }
    )

@asset_check(asset=ingresos_clean)
def test_ingresos_conversion_decimal(ingresos_clean):
    fuera_rango = ((ingresos_clean['OBS_VALUE'] < 0) | (ingresos_clean['OBS_VALUE'] > 100)).sum()
    return AssetCheckResult(
        passed=bool(fuera_rango == 0),
        metadata={
            "valores_fuera_rango": MetadataValue.int(int(fuera_rango)),
            "description": "OBS_VALUE debe estar en [0, 100] tras conversión de porcentajes.",
        }
    )

@asset_check(asset=ocupacion_clean)
def test_ocupacion_categorias(ocupacion_clean):
    n = ocupacion_clean['ocupacion'].nunique()
    return AssetCheckResult(
        passed=bool(n <= 10),
        metadata={
            "n_categorias": MetadataValue.int(n),
            "description": "Límite cognitivo: ≤ 10 categorías de ocupación para visualización.",
            "gestalt": "Carga Cognitiva",
        }
    )


# ---------------------------------------------------------------------------
# Checks analíticos — validan los supuestos narrativos
# ---------------------------------------------------------------------------

@asset_check(asset=data_renta_municipio)
def test_cobertura_municipios(data_renta_municipio):
    n = len(data_renta_municipio)
    return AssetCheckResult(
        passed=bool(n >= 40),
        metadata={
            "n_municipios": MetadataValue.int(n),
            "description": "Al menos 40 de los 54 municipios de Tenerife deben tener datos de renta.",
        }
    )

@asset_check(asset=data_renta_municipio)
def test_punto_focal_renta(data_renta_municipio):
    ratio = data_renta_municipio['renta_media'].max() / data_renta_municipio['renta_media'].min()
    return AssetCheckResult(
        passed=bool(ratio >= 2.0),
        metadata={
            "ratio_max_min": MetadataValue.float(float(ratio)),
            "description": "La renta máxima debe ser ≥ 2× la mínima para sostener la narrativa.",
            "gestalt": "Punto Focal",
        }
    )

@asset_check(asset=data_hist_renta)
def test_hist_dispersion(data_hist_renta):
    cv = data_hist_renta['renta_neta'].std() / data_hist_renta['renta_neta'].mean()
    return AssetCheckResult(
        passed=bool(cv > 0.15),
        metadata={
            "coef_variacion": MetadataValue.float(float(cv)),
            "description": "El histograma necesita suficiente dispersión (CV > 0.15) para ser informativo.",
            "gestalt": "Figura-Fondo",
        }
    )

@asset_check(asset=data_fuentes_ingresos)
def test_waterfall_suma_100(data_fuentes_ingresos):
    total = data_fuentes_ingresos['pct'].sum()
    passed = bool(abs(total - 100) < 5)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "suma_pct": MetadataValue.float(float(total)),
            "description": "Las 5 fuentes de ingresos deben sumar ~100%.",
        }
    )

@asset_check(asset=data_piramide_ocupacion)
def test_piramide_categorias(data_piramide_ocupacion):
    n = len(data_piramide_ocupacion)
    tiene_desviacion = 'desviacion' in data_piramide_ocupacion.columns
    return AssetCheckResult(
        passed=bool(n >= 2 and tiene_desviacion),
        metadata={
            "n_categorias": MetadataValue.int(n),
            "description": "Diverging bar necesita ≥2 categorías y columna desviacion.",
            "gestalt": "Destino Común",
        }
    )



# ---------------------------------------------------------------------------
# Checks de integridad de archivos generados
# ---------------------------------------------------------------------------

@asset_check(asset=ranking_municipios)
def test_archivo_ranking(ranking_municipios):
    return _check_file(ranking_municipios, "Lollipop de ranking generado.", "Figura-Fondo")

@asset_check(asset=hist_renta)
def test_archivo_hist(hist_renta):
    return _check_file(hist_renta, "Histograma de distribución generado.", "Proximidad")

@asset_check(asset=fuentes_ingresos)
def test_archivo_fuentes(fuentes_ingresos):
    return _check_file(fuentes_ingresos, "Waterfall de fuentes de ingresos generado.", "Continuidad")

@asset_check(asset=piramide_ocupacion)
def test_archivo_piramide(piramide_ocupacion):
    return _check_file(piramide_ocupacion, "Pirámide de ocupación generada.", "Simetría")


@asset_check(asset=data_waterfall_actividades)
def test_waterfall_actividades_suma_100(data_waterfall_actividades):
    total = data_waterfall_actividades['pct'].sum()
    return AssetCheckResult(
        passed=bool(abs(total - 100) < 1),
        metadata={
            "suma_pct": MetadataValue.float(float(total)),
            "description": "Los 4 sectores CNAE deben sumar ~100%.",
        }
    )

@asset_check(asset=waterfall_actividades)
def test_archivo_waterfall_actividades(waterfall_actividades):
    return _check_file(waterfall_actividades, "Waterfall de sectores de actividad generado.", "Parte-Todo")


# ---------------------------------------------------------------------------
# Checks — gráficos de correlación (desigualdad)
# ---------------------------------------------------------------------------

@asset_check(asset=data_slope_brecha)
def test_slope_dos_anios(data_slope_brecha):
    tiene_2021 = 'renta_2021' in data_slope_brecha.columns
    tiene_2023 = 'renta_2023' in data_slope_brecha.columns
    n = len(data_slope_brecha)
    return AssetCheckResult(
        passed=bool(tiene_2021 and tiene_2023 and n >= 40),
        metadata={
            "n_municipios": MetadataValue.int(n),
            "description": "Slope chart necesita renta_2021, renta_2023 y ≥40 municipios.",
        }
    )

@asset_check(asset=data_boxplot_intramunicipal)
def test_boxplot_cobertura(data_boxplot_intramunicipal):
    n_mun = data_boxplot_intramunicipal['municipio'].nunique()
    n_sec = len(data_boxplot_intramunicipal)
    return AssetCheckResult(
        passed=bool(n_mun >= 40 and n_sec >= 200),
        metadata={
            "n_municipios": MetadataValue.int(n_mun),
            "n_secciones":  MetadataValue.int(n_sec),
            "description":  "Box plot necesita ≥40 municipios y ≥200 secciones censales.",
        }
    )

@asset_check(asset=data_scatter_elementales)
def test_scatter_correlacion_negativa(data_scatter_elementales):
    corr = data_scatter_elementales[['pct_elementales', 'renta_neta']].corr().iloc[0, 1]
    return AssetCheckResult(
        passed=bool(corr < -0.1),
        metadata={
            "correlacion": MetadataValue.float(float(corr)),
            "description": "Se espera correlación negativa entre % elementales y renta.",
            "gestalt": "Destino Común",
        }
    )

@asset_check(asset=data_ingresos_quintiles)
def test_quintiles_completos(data_ingresos_quintiles):
    n_quintiles  = data_ingresos_quintiles['quintil'].nunique()
    n_actividades = data_ingresos_quintiles['actividad'].nunique()
    return AssetCheckResult(
        passed=bool(n_quintiles == 5 and n_actividades >= 3),
        metadata={
            "n_quintiles":   MetadataValue.int(n_quintiles),
            "n_actividades": MetadataValue.int(n_actividades),
            "description":   "Grouped bar necesita 5 quintiles y ≥3 sectores de actividad.",
        }
    )

@asset_check(asset=data_ocupacion_quintiles)
def test_ocupacion_quintiles_completos(data_ocupacion_quintiles):
    n_quintiles  = data_ocupacion_quintiles['quintil'].nunique()
    n_ocupaciones = data_ocupacion_quintiles['ocupacion'].nunique()
    return AssetCheckResult(
        passed=bool(n_quintiles == 5 and n_ocupaciones >= 2),
        metadata={
            "n_quintiles":   MetadataValue.int(n_quintiles),
            "n_ocupaciones": MetadataValue.int(n_ocupaciones),
            "description":   "Grouped bar ocupación necesita 5 quintiles y ≥2 categorías.",
        }
    )

@asset_check(asset=grouped_bar_ocupacion)
def test_archivo_grouped_bar_ocupacion(grouped_bar_ocupacion):
    return _check_file(grouped_bar_ocupacion, "Grouped bar ocupación por quintil generado.", "Magnitud")

@asset_check(asset=data_heatmap_renta)
def test_heatmap_tres_anios(data_heatmap_renta):
    años = sorted(data_heatmap_renta['año'].unique())
    n_mun = data_heatmap_renta['municipio'].nunique()
    return AssetCheckResult(
        passed=bool(set(años) >= {2021, 2022, 2023} and n_mun >= 40),
        metadata={
            "años":        MetadataValue.text(str(años)),
            "n_municipios": MetadataValue.int(n_mun),
            "description": "Heatmap necesita los 3 años y ≥40 municipios.",
        }
    )

@asset_check(asset=slope_brecha)
def test_archivo_slope(slope_brecha):
    return _check_file(slope_brecha, "Slope chart de evolución de renta generado.", "Cambio Temporal")

@asset_check(asset=boxplot_intramunicipal)
def test_archivo_boxplot(boxplot_intramunicipal):
    return _check_file(boxplot_intramunicipal, "Box plot de desigualdad intramunicipal generado.", "Distribución")

@asset_check(asset=scatter_elementales)
def test_archivo_scatter(scatter_elementales):
    return _check_file(scatter_elementales, "Scatter elementales vs renta generado.", "Correlación")

@asset_check(asset=grouped_bar_quintiles)
def test_archivo_grouped_bar(grouped_bar_quintiles):
    return _check_file(grouped_bar_quintiles, "Grouped bar fuentes por quintil generado.", "Magnitud")

@asset_check(asset=heatmap_renta)
def test_archivo_heatmap(heatmap_renta):
    return _check_file(heatmap_renta, "Heatmap de renta por municipio y año generado.", "Cambio Temporal")
