import os
import pandas as pd
from dagster import asset_check, AssetCheckResult, MetadataValue
from lab_renta import (
    rentamedia_load, rentamedia_clean,
    actividad_load, actividad_clean,
    ocupacion_load, ocupacion_clean,
    ingresos_load, ingresos_clean,
    data_renta_municipio, data_hist_renta,
    data_servicios_vs_renta, data_brecha_genero,
    data_fuentes_ingresos, data_piramide_ocupacion, data_marimekko_sectores,
    ranking_municipios, hist_renta,
    servicios_vs_renta, brecha_genero,
    fuentes_ingresos, piramide_ocupacion, marimekko_sectores,
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

@asset_check(asset=data_servicios_vs_renta)
def test_join_servicios_renta(data_servicios_vs_renta):
    n = len(data_servicios_vs_renta)
    return AssetCheckResult(
        passed=bool(n >= 400),
        metadata={
            "secciones_cruzadas": MetadataValue.int(n),
            "description": "El join secciones-renta debe producir al menos 400 filas.",
            "gestalt": "Continuidad",
        }
    )

@asset_check(asset=data_brecha_genero)
def test_brecha_existe(data_brecha_genero):
    medias_negativas = (data_brecha_genero['desviacion'] < 0).sum()
    medias_positivas = (data_brecha_genero['desviacion'] > 0).sum()
    passed = bool(medias_negativas > 0 and medias_positivas > 0)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "municipios_mayoria_hombres": MetadataValue.int(int(medias_negativas)),
            "municipios_mayoria_mujeres": MetadataValue.int(int(medias_positivas)),
            "description": "El diverging bar necesita valores a ambos lados de la paridad.",
            "gestalt": "Destino Común",
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
def test_piramide_simetria(data_piramide_ocupacion):
    total_h = data_piramide_ocupacion.loc[data_piramide_ocupacion['sexo'] == 'Hombres', 'num_casos'].sum()
    total_m = data_piramide_ocupacion.loc[data_piramide_ocupacion['sexo'] == 'Mujeres', 'num_casos'].sum()
    ratio   = max(total_h, total_m) / min(total_h, total_m)
    passed  = bool(ratio < 2.0)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "ratio_h_m": MetadataValue.float(float(ratio)),
            "description": "La pirámide no debe estar desequilibrada > 2× entre sexos.",
            "gestalt": "Simetría",
        }
    )

@asset_check(asset=data_marimekko_sectores)
def test_marimekko_grupos(data_marimekko_sectores):
    n_grupos = data_marimekko_sectores['grupo_renta'].nunique()
    return AssetCheckResult(
        passed=bool(n_grupos == 3),
        metadata={
            "n_grupos": MetadataValue.int(n_grupos),
            "description": "El Marimekko debe tener exactamente 3 grupos de renta.",
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

@asset_check(asset=servicios_vs_renta)
def test_archivo_servicios(servicios_vs_renta):
    return _check_file(servicios_vs_renta, "Bubble chart servicios vs renta generado.", "Continuidad")

@asset_check(asset=brecha_genero)
def test_archivo_brecha(brecha_genero):
    return _check_file(brecha_genero, "Diverging bar de brecha de género generado.", "Semejanza")

@asset_check(asset=fuentes_ingresos)
def test_archivo_fuentes(fuentes_ingresos):
    return _check_file(fuentes_ingresos, "Waterfall de fuentes de ingresos generado.", "Continuidad")

@asset_check(asset=piramide_ocupacion)
def test_archivo_piramide(piramide_ocupacion):
    return _check_file(piramide_ocupacion, "Pirámide de ocupación generada.", "Simetría")

@asset_check(asset=marimekko_sectores)
def test_archivo_marimekko(marimekko_sectores):
    return _check_file(marimekko_sectores, "Marimekko de sectores generado.", "Proximidad")
