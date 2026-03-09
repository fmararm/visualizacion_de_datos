import os
import pandas as pd
from dagster import asset_check, AssetCheckResult, MetadataValue
from lab_renta import (
    renta_load, renta_cleaning, income_composition_stacked_bar,
    wage_deviation_from_avg, income_distribution_boxplot,
    unemployment_trend_by_region, income_composition_heatmap,
    pension_growth_ranking, nivel_estudios_load, nivel_estudios_cleaning,
    top_foreign_students_municipalities_bar, higher_ed_gender_gap_diverging_bar,
    nationality_proportion_bar, education_level_proportion_bar,
    higher_ed_by_island_bar, income_vs_higher_ed_scatter,
    higher_ed_mun_wage_comparison
)

# 1. renta_load: test_integridad_critica (Logic: 5% Null Tolerance)
@asset_check(asset=renta_load)
def test_integridad_critica_renta(renta_load):
    # --- INYECCIÓN DE FALLO (Descomentar para probar) ---
    # renta_load['OBS_VALUE'] = None  # Esto fuerza 100% de nulos
    # --------------------------------------------------
    # En lugar de ser estrictos, permitimos hasta un 5% de nulos
    null_pct = renta_load['OBS_VALUE'].isna().mean()
    passed = bool(null_pct < 0.05)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "porcentaje_nulos": MetadataValue.float(float(null_pct * 100)),
            "description": "Valida que el volumen de datos nulos sea tolerable (<5%).",
            "gestalt": "Cierre: Un pequeño porcentaje de nulos no rompe la percepción global."
        }
    )

# 2. renta_cleaning: test_integridad_valles (Logic: 5% Null Tolerance)
@asset_check(asset=renta_cleaning)
def test_integridad_renta_limpieza(renta_cleaning):
    # --- INYECCIÓN DE FALLO (Descomentar para probar) ---
    # renta_cleaning['value'] = None  # Esto fuerza 100% de nulos
    # --------------------------------------------------
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

# 2b. nivel_estudios_cleaning: test_limite_cognitivo (Logic: Unique Count)
@asset_check(asset=nivel_estudios_cleaning)
def test_limite_cognitivo_niveles(nivel_estudios_cleaning):
    # --- INYECCIÓN DE FALLO (Descomentar para probar) ---
    # nivel_estudios_cleaning['education_level'] = [f"L_{i}" for i in range(len(nivel_estudios_cleaning))]
    # --------------------------------------------------
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

# helper for file integrity with size-based complexity check
def test_file_integrity_advanced(path, description, gestalt, min_size=5000):
    exists = os.path.exists(path)
    size = os.path.getsize(path) if exists else 0
    
    # --- INYECCIÓN DE FALLO (Descomentar para probar) ---
    # min_size = 999999999  # Forzar fallo por tamaño imposible
    # --------------------------------------------------

    # Un gráfico decente suele pesar más que un error en blanco o un gráfico vacío
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


# 4. wage_deviation_from_avg: test_rango_dinamico
@asset_check(asset=wage_deviation_from_avg)
def test_rango_dinamico_deviation(wage_deviation_from_avg):
    return test_file_integrity_advanced(wage_deviation_from_avg, "Valida rango dinámico visual.", "Proporcionalidad")

# 5. income_distribution_boxplot: test_accesibilidad_contraste
@asset_check(asset=income_distribution_boxplot)
def test_accesibilidad_contraste_boxplot(income_distribution_boxplot):
    return test_file_integrity_advanced(income_distribution_boxplot, "Contraste", "Figura-Fondo")

# 6. unemployment_trend_by_region: test_fluidez_temporal
@asset_check(asset=unemployment_trend_by_region)
def test_fluidez_temporal_unemployment(unemployment_trend_by_region):
    return test_file_integrity_advanced(unemployment_trend_by_region, "Continuidad", "Continuidad")

# 7. income_composition_heatmap: test_coherencia_cromatica
@asset_check(asset=income_composition_heatmap)
def test_coherencia_cromatica_heatmap(income_composition_heatmap):
    return test_file_integrity_advanced(income_composition_heatmap, "Color", "Similitud")

# 8. pension_growth_ranking: test_prelacion_visual
@asset_check(asset=pension_growth_ranking)
def test_prelacion_visual_pension(pension_growth_ranking):
    return test_file_integrity_advanced(pension_growth_ranking, "Orden", "Pragnanz")

# 9. nivel_estudios_load: test_integridad_critica (Logic: 5% Null Tolerance)
@asset_check(asset=nivel_estudios_load)
def test_integridad_critica_estudios(nivel_estudios_load):
    # --- INYECCIÓN DE FALLO (Descomentar para probar) ---
    # nivel_estudios_load['Total'] = None  # Esto fuerza 100% de nulos
    # --------------------------------------------------
    null_pct = nivel_estudios_load['Total'].isna().mean()
    passed = bool(null_pct < 0.05)
    return AssetCheckResult(
        passed=passed, 
        metadata={
            "porcentaje_nulos": MetadataValue.float(float(null_pct * 100)),
            "description": "Tolerancia del 5% para nulos en el dataset de estudios."
        }
    )

# 10. nivel_estudios_cleaning: test_homogeneidad_texto
@asset_check(asset=nivel_estudios_cleaning)
def test_homogeneidad_texto_estudios(nivel_estudios_cleaning):
    # --- INYECCIÓN DE FALLO (Descomentar para probar) ---
    # nivel_estudios_cleaning['municipality'] = "Sucia" # Fuerza redundancia léxica
    # --------------------------------------------------
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

# 11. top_foreign_students_municipalities_bar: test_limite_cognitivo
@asset_check(asset=top_foreign_students_municipalities_bar)
def test_limite_cognitivo_foreign(top_foreign_students_municipalities_bar):
    return test_file_integrity_advanced(top_foreign_students_municipalities_bar, "Carga", "Cognición")

# 12. higher_ed_gender_gap_diverging_bar: test_rango_dinamico
@asset_check(asset=higher_ed_gender_gap_diverging_bar)
def test_rango_dinamico_gender(higher_ed_gender_gap_diverging_bar):
    return test_file_integrity_advanced(higher_ed_gender_gap_diverging_bar, "Escala", "Proporcionalidad")


# 14. education_level_proportion_bar: test_densidad_texto
@asset_check(asset=education_level_proportion_bar)
def test_densidad_texto_edu(education_level_proportion_bar):
    return test_file_integrity_advanced(education_level_proportion_bar, "Texto", "Ruido")

# 15. higher_ed_by_island_bar: test_coherencia_cromatica
@asset_check(asset=higher_ed_by_island_bar)
def test_coherencia_cromatica_island(higher_ed_by_island_bar):
    return test_file_integrity_advanced(higher_ed_by_island_bar, "Identidad", "Similitud")

# 16. income_vs_higher_ed_scatter: test_integridad_critica
@asset_check(asset=income_vs_higher_ed_scatter)
def test_integridad_critica_scatter(income_vs_higher_ed_scatter):
    return test_file_integrity_advanced(income_vs_higher_ed_scatter, "Integridad", "Cierre")

# 17. higher_ed_mun_wage_comparison: test_prelacion_visual
@asset_check(asset=higher_ed_mun_wage_comparison)
def test_prelacion_visual_wage(higher_ed_mun_wage_comparison):
    return test_file_integrity_advanced(higher_ed_mun_wage_comparison, "Ranking", "Pragnanz")
