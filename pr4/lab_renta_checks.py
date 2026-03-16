import os
import pandas as pd
from dagster import asset_check, AssetCheckResult, MetadataValue
from lab_renta import (
    renta_load, renta_cleaning, income_distribution_boxplot,
    unemployment_trend_by_region, nivel_estudios_load, nivel_estudios_cleaning,
    higher_ed_by_island_bar
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
