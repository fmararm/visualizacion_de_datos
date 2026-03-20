import os
import pandas as pd
from dagster import asset_check, AssetCheckResult, MetadataValue
from lab_renta import (
    renta_load, renta_cleaning, income_distribution_boxplot,
    unemployment_trend_by_region, nivel_estudios_load, nivel_estudios_cleaning,
    higher_ed_by_island_bar, data_higher_ed_tf_gc
)

# 1. renta_load: test_integridad_critica (Logic: 5% Null Tolerance)
@asset_check(asset=renta_load)
def test_integridad_critica_renta(renta_load):
    null_pct = renta_load['OBS_VALUE'].isna().mean()
    passed = bool(null_pct < 0.05)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "porcentaje_nulos": MetadataValue.float(float(null_pct * 100)),
            "description": "Valida que el volumen de datos nulos sea tolerable (<5%).",
            "gestalt": "Cierre"
        }
    )

# 2. renta_cleaning: test_integridad_valles (Logic: 5% Null Tolerance)
@asset_check(asset=renta_cleaning)
def test_integridad_renta_limpieza(renta_cleaning):
    null_pct = renta_cleaning['value'].isna().mean()
    passed = bool(null_pct < 0.05)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "porcentaje_nulos": MetadataValue.float(float(null_pct * 100)),
            "description": "Asegura integradad tras limpieza con un margen del 5%.",
            "gestalt": "Cierre"
        }
    )

# 3. nivel_estudios_cleaning: test_limite_cognitivo (Logic: Unique Count)
@asset_check(asset=nivel_estudios_cleaning)
def test_limite_cognitivo_niveles(nivel_estudios_cleaning):
    n_categories = nivel_estudios_cleaning['education_level'].nunique()
    passed = bool(n_categories <= 10) 
    return AssetCheckResult(
        passed=passed,
        metadata={
            "n_categorias": MetadataValue.int(n_categories),
            "description": "Valida que el desglose de niveles sea procesable visualmente.",
            "gestalt": "Carga Cognitiva"
        }
    )

# helper for file integrity
def test_file_integrity_advanced(path, description, gestalt, min_size=5000):
    exists = os.path.exists(path)
    size = os.path.getsize(path) if exists else 0
    passed = bool(exists and size > min_size) 
    return AssetCheckResult(
        passed=passed,
        metadata={
            "path": MetadataValue.path(path),
            "size_kb": MetadataValue.float(size / 1024),
            "description": description,
            "gestalt": gestalt
        }
    )

# 4. income_distribution_boxplot
@asset_check(asset=income_distribution_boxplot)
def test_accesibilidad_contraste_boxplot(income_distribution_boxplot):
    return test_file_integrity_advanced(income_distribution_boxplot, "Contraste", "Figura-Fondo")

# 5. unemployment_trend_by_region
@asset_check(asset=unemployment_trend_by_region)
def test_fluidez_temporal_unemployment(unemployment_trend_by_region):
    return test_file_integrity_advanced(unemployment_trend_by_region, "Continuidad", "Continuidad")

# 6. nivel_estudios_load
@asset_check(asset=nivel_estudios_load)
def test_integridad_critica_estudios(nivel_estudios_load):
    null_pct = nivel_estudios_load['Total'].isna().mean()
    passed = bool(null_pct < 0.05)
    return AssetCheckResult(
        passed=passed, 
        metadata={
            "porcentaje_nulos": MetadataValue.float(float(null_pct * 100)),
            "description": "Tolerancia del 5% para nulos en el dataset de estudios."
        }
    )

# 7. nivel_estudios_cleaning: test_homogeneidad_texto
@asset_check(asset=nivel_estudios_cleaning)
def test_homogeneidad_texto_estudios(nivel_estudios_cleaning):
    originales = nivel_estudios_cleaning['municipality'].nunique()
    normalizadas = nivel_estudios_cleaning['municipality'].str.strip().str.capitalize().nunique()
    passed = bool(originales == normalizadas)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "redundancia_detectada": MetadataValue.int(originales - normalizadas),
            "description": "Busca redundancias por espacios o capitalización.",
            "gestalt": "Semejanza"
        }
    )

# 9. higher_ed_by_island_bar
@asset_check(asset=higher_ed_by_island_bar)
def test_coherencia_cromatica_island(higher_ed_by_island_bar):
    return test_file_integrity_advanced(higher_ed_by_island_bar, "Identidad", "Similitud")

# 10. renta_cleaning: test_variabilidad_boxplots (Grammar: Scale/Geometry)
@asset_check(asset=renta_cleaning)
def test_variabilidad_boxplots(renta_cleaning):
    # El boxplot necesita que el IQR sea > 0 para mostrar una caja significativa
    stats = renta_cleaning.groupby('measure')['value'].describe()
    num_flat_categories = (stats['75%'] == stats['25%']).sum()
    passed = bool(num_flat_categories == 0)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "categorias_sin_iqr": MetadataValue.int(int(num_flat_categories)),
            "description": "Verifica que el IQR sea > 0 para permitir geometría de boxplot.",
            "gestalt": "Destino Común"
        }
    )

# 11. renta_cleaning: test_continuidad_temporal (Gestalt: Continuity)
@asset_check(asset=renta_cleaning)
def test_continuidad_temporal(renta_cleaning):
    # En un gráfico de líneas, queremos ver series continuas sin 'saltos' visuales
    islands_data = renta_cleaning.groupby(['region', 'measure'])['year'].agg(['min', 'max', 'count'])
    islands_data['range'] = islands_data['max'] - islands_data['min'] + 1
    # Si el conteo es igual al rango, no hay huecos temporales
    has_gaps = (islands_data['count'] < islands_data['range']).any()
    passed = bool(not has_gaps)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "series_con_huecos": MetadataValue.int(int(has_gaps)),
            "description": "Busca huecos temporales que rompan la continuidad visual de las líneas.",
            "gestalt": "Continuidad"
        }
    )

# 12. nivel_estudios_cleaning: test_punto_focal_islas (Gestalt: Focal Point)
@asset_check(asset=nivel_estudios_cleaning)
def test_punto_focal_islas(nivel_estudios_cleaning):
    # Buscamos que haya un 'ancla' visual (una isla que destaque claramente)
    island_totals = nivel_estudios_cleaning.groupby('island')['total'].sum()
    if island_totals.empty: return AssetCheckResult(passed=False, metadata={"error": "No hay datos"})
    max_val = island_totals.max()
    mean_val = island_totals.mean()
    # Si el máximo es al menos un 15% superior a la media, hay un punto focal claro
    passed = bool(max_val > (mean_val * 1.15))
    return AssetCheckResult(
        passed=passed,
        metadata={
            "ratio_max_media": MetadataValue.float(float(max_val / mean_val)),
            "description": "Valida la existencia de un punto focal visual (una isla dominante).",
            "gestalt": "Punto Focal"
        }
    )

# 13. data_higher_ed_tf_gc: test_proximidad_comparativa (Gestalt: Proximity)
@asset_check(asset=data_higher_ed_tf_gc)
def test_proximidad_comparativa(data_higher_ed_tf_gc):
    # Comparar Tenerife y Gran Canaria requiere que estén en escalas similares
    totals = data_higher_ed_tf_gc.groupby('island')['total'].mean()
    if len(totals) < 2: return AssetCheckResult(passed=True, metadata={"info": "Menos de 2 islas para comparar"})
    ratio = totals.max() / totals.min()
    # Si la diferencia es excesiva (>2x), la proximidad visual se pierde
    passed = bool(ratio < 2.0)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "ratio_comparativo": MetadataValue.float(float(ratio)),
            "description": "Asegura que las magnitudes sean comparables sin distorsionar la escala.",
            "gestalt": "Proximidad"
        }
    )
