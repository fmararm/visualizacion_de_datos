import pandas as pd
import plotnine as p9
import os
from lab_renta import (
    renta_load, renta_cleaning, 
    income_composition_stacked_bar, wage_deviation_from_avg, income_distribution_boxplot, 
    unemployment_trend_by_region, income_composition_heatmap, 
    nivel_estudios_load, nivel_estudios_cleaning,
    higher_ed_gender_gap_diverging_bar,
    gender_proportion_bar, nationality_proportion_bar,
    education_level_proportion_bar, higher_ed_by_island_bar,
    top_foreign_students_municipalities_bar,
    income_vs_higher_ed_scatter, higher_ed_mun_wage_comparison
)

def run_verification():
    print("Loading data...")
    renta = renta_load()
    print("Cleaning data...")
    clean_renta = renta_cleaning(renta)
    
    print("Columns in clean data:", clean_renta.columns)
    
    # Create plots directory
    for d in ["plots", "plots/income", "plots/education", "plots/combination"]:
        if not os.path.exists(d):
            os.makedirs(d)
        

    
    print("Generating income_composition_stacked_bar...")
    res = income_composition_stacked_bar(clean_renta)
    print(f"Result: {res}")

    print("Generating wage_deviation_from_avg...")
    res = wage_deviation_from_avg(clean_renta)
    print(f"Result: {res}")
    
    print("Generating income_distribution_boxplot...")
    res = income_distribution_boxplot(clean_renta)
    print(f"Result: {res}")
    
    print("Generating unemployment_trend_by_region...")
    res = unemployment_trend_by_region(clean_renta)
    print(f"Result: {res}")
    
    print("Generating income_composition_heatmap...")
    res = income_composition_heatmap(clean_renta)
    print(f"Result: {res}")
    
    print("Unique Renta Measures:")
    print(clean_renta['measure'].unique())
    

    
    # Nivel Estudios Verification
    print("Loading nivel estudios...")
    ne_load = nivel_estudios_load()
    print("Cleaning nivel estudios...")
    ne_clean = nivel_estudios_cleaning(ne_load)
    
    print("Unique Municipalities in Data:")
    print(sorted(ne_clean['municipality'].unique()))
    
    print("Unique Education Levels:")
    print(ne_clean['education_level'].unique())
    
    print("Generating top_foreign_students_municipalities_bar...")
    res = top_foreign_students_municipalities_bar(ne_clean)
    print(f"Result: {res}")





    

    print("Generating higher_ed_gender_gap_diverging_bar...")
    res = higher_ed_gender_gap_diverging_bar(ne_clean)
    print(f"Result: {res}")
    

    print("Generating gender_proportion_bar...")
    res = gender_proportion_bar(ne_clean)
    print(f"Result: {res}")
    
    print("Generating nationality_proportion_bar...")
    res = nationality_proportion_bar(ne_clean)
    print(f"Result: {res}")
    
    print("Generating education_level_proportion_bar...")
    res = education_level_proportion_bar(ne_clean)
    print(f"Result: {res}")
    
    print("Generating higher_ed_by_island_bar...")
    res = higher_ed_by_island_bar(ne_clean)
    print(f"Result: {res}")
    
    print("Generating income_vs_higher_ed_scatter...")
    res = income_vs_higher_ed_scatter(clean_renta, ne_clean)
    print(f"Result: {res}")
    
    print("Generating higher_ed_mun_wage_comparison...")
    res = higher_ed_mun_wage_comparison(clean_renta, ne_clean)
    print(f"Result: {res}")

    print("All plots generated successfully (saved directly by assets to plots/ directory).")

if __name__ == "__main__":
    run_verification()
