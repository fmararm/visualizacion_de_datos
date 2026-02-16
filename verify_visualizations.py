import pandas as pd
import plotnine as p9
import os
from lab_renta import renta_load, renta_cleaning, evolution_stacked_area_plot, income_variance_boxplot, top_wage_municipalities_bar, unemployment_trend_faceted, income_composition_heatmap, wages_vs_pensions_correlation, pension_growth_ranking

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
    p1 = evolution_stacked_area_plot(clean_renta)
    p1.save("plots/evolution_stacked_area_plot.png")
    
    print("Generating income_variance_boxplot...")
    p2 = income_variance_boxplot(clean_renta)
    p2.save("plots/income_variance_boxplot.png")
    
    print("Generating top_wage_municipalities_bar...")
    p3 = top_wage_municipalities_bar(clean_renta)
    p3.save("plots/top_wage_municipalities_bar.png")
    
    print("Generating unemployment_trend_faceted...")
    p4 = unemployment_trend_faceted(clean_renta)
    p4.save("plots/unemployment_trend_faceted.png")
    
    print("Generating income_composition_heatmap...")
    p5 = income_composition_heatmap(clean_renta)
    p5.save("plots/income_composition_heatmap.png")

    print("Generating wages_vs_pensions_correlation...")
    p6 = wages_vs_pensions_correlation(clean_renta)
    p6.save("plots/wages_vs_pensions_correlation.png")

    print("Generating pension_growth_ranking...")
    p7 = pension_growth_ranking(clean_renta)
    p7.save("plots/pension_growth_ranking.png")
    
    print("All plots generated successfully.")

if __name__ == "__main__":
    run_verification()
