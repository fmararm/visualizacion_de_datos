import pandas as pd
import plotnine as p9
import os
from lab_renta import (
    renta_load, renta_cleaning, evolution_stacked_area_plot, 
    income_composition_stacked_bar, wage_deviation_from_avg, income_distribution_violin, 
    top_wage_municipalities_bar, unemployment_trend_faceted, income_composition_heatmap, 
    wages_vs_pensions_correlation, pension_growth_ranking, 
    nivel_estudios_load, nivel_estudios_cleaning, education_ranking_dotplot, 
    education_evolution_top_municipalities
)

def run_verification():
    print("Loading data...")
    renta = renta_load()
    print("Cleaning data...")
    clean_renta = renta_cleaning(renta)
    
    print("Columns in clean data:", clean_renta.columns)
    
    # Create plots directory
    if not os.path.exists("plots"):
        os.makedirs("plots")
        
    print("Generating evolution_stacked_area_plot...")
    res = evolution_stacked_area_plot(clean_renta)
    print(f"Result: {res}")
    
    print("Generating income_composition_stacked_bar...")
    res = income_composition_stacked_bar(clean_renta)
    print(f"Result: {res}")

    print("Generating wage_deviation_from_avg...")
    res = wage_deviation_from_avg(clean_renta)
    print(f"Result: {res}")
    
    print("Generating income_distribution_violin...")
    res = income_distribution_violin(clean_renta)
    print(f"Result: {res}")
    
    print("Generating top_wage_municipalities_bar...")
    res = top_wage_municipalities_bar(clean_renta)
    print(f"Result: {res}")
    
    print("Generating unemployment_trend_faceted...")
    res = unemployment_trend_faceted(clean_renta)
    print(f"Result: {res}")
    
    print("Generating income_composition_heatmap...")
    res = income_composition_heatmap(clean_renta)
    print(f"Result: {res}")

    print("Generating wages_vs_pensions_correlation...")
    res = wages_vs_pensions_correlation(clean_renta)
    print(f"Result: {res}")

    print("Generating pension_growth_ranking...")
    res = pension_growth_ranking(clean_renta)
    print(f"Result: {res}")
    
    # Nivel Estudios Verification
    print("Loading nivel estudios...")
    ne_load = nivel_estudios_load()
    print("Cleaning nivel estudios...")
    ne_clean = nivel_estudios_cleaning(ne_load)
    
    print("Generating education_ranking_dotplot...")
    res = education_ranking_dotplot(ne_clean)
    print(f"Result: {res}")
    
    print("Generating education_evolution_top_municipalities...")
    res = education_evolution_top_municipalities(ne_clean)
    print(f"Result: {res}")
    
    print("All plots generated successfully (saved directly by assets to plots/ directory).")

if __name__ == "__main__":
    run_verification()
