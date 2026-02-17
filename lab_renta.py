import pandas as pd
import numpy as np
import plotnine as p9
from dagster import asset

@asset
def renta_load():
    renta = pd.read_csv("data/distribucion-renta-canarias.csv")
    return renta

@asset(deps=[renta_load])
def renta_cleaning(renta_load):
    # Start data cleaning
    renta = renta_load.copy()
    renta = renta.drop_duplicates()
    
    # Drop empty columns and redundant code columns
    columns_to_drop = [
        "ESTADO_OBSERVACION#es", 
        "CONFIDENCIALIDAD_OBSERVACION#es",
        "TIME_PERIOD_CODE",
        "MEDIDAS_CODE"
    ]
    renta = renta.drop(columns=columns_to_drop, errors='ignore') # errors='ignore' in case they are already gone or names differ slightly
    
    # Rename columns for better readability
    renta = renta.rename(columns={
        "TERRITORIO#es": "region",
        "TIME_PERIOD#es": "year",
        "MEDIDAS#es": "measure",
        "OBS_VALUE": "value"
    })
    
    # Drop rows with missing values in important columns
    renta = renta.dropna(subset=['region', 'year', 'measure', 'value'])
    
    return renta

@asset(deps=[renta_cleaning])
def evolution_stacked_area_plot(renta_cleaning):
    # Filter for TERRITORIO#es == 'Canarias'
    df = renta_cleaning[renta_cleaning['region'] == 'Canarias']
    
    # Create a stacked area chart showing the evolution of all income categories
    plot = (
        p9.ggplot(df, p9.aes(x='year', y='value', fill='measure'))
        + p9.geom_area()
        + p9.labs(title='Evolution of Income Categories in Canarias', y='Value', x='Year', fill='Measure')
        + p9.theme_minimal()
    )
    plot.save("plots/evolution_stacked_area_plot.png")
    plot.save("plots/evolution_stacked_area_plot.png")
    return "plots/evolution_stacked_area_plot.png"

@asset(deps=[renta_cleaning])
def income_composition_stacked_bar(renta_cleaning):
    # Filter for the 7 main islands and the most recent year
    main_islands = ['Lanzarote', 'Fuerteventura', 'Gran Canaria', 'Tenerife', 'La Gomera', 'La Palma', 'El Hierro']
    df = renta_cleaning[(renta_cleaning['year'] == 2023) & (renta_cleaning['region'].isin(main_islands))]
    
    plot = (
        p9.ggplot(df, p9.aes(x='region', y='value', fill='measure'))
        + p9.geom_bar(stat='identity', position='fill') # 'fill' makes it 100% stacked
        + p9.scale_y_continuous(labels=lambda l: [f"{int(x*100)}%" for x in l])
        + p9.labs(title='Income Composition by Island (2023)', x='', y='Percentage Share', fill='Source')
        + p9.theme_minimal()
        + p9.theme(axis_text_x=p9.element_text(rotation=45, hjust=1))
    )
    plot.save("plots/income_composition_stacked_bar.png")
    return "plots/income_composition_stacked_bar.png"

@asset(deps=[renta_cleaning])
def wage_deviation_from_avg(renta_cleaning):
    # 1. Get regional average for 2023
    avg_val = 61.2 
    
    # 2. Filter for municipalities in 2023 for Wages
    df = renta_cleaning[(renta_cleaning['year'] == 2023) & 
                        (renta_cleaning['measure'] == 'Sueldos y salarios') &
                        (~renta_cleaning['region'].isin(['Canarias', 'Las Palmas', 'Santa Cruz de Tenerife']))]
    
    # 3. Calculate deviation
    df['deviation'] = df['value'] - avg_val
    df = df.sort_values('deviation').iloc[np.r_[0:10, -10:0]] # Top 10 and Bottom 10
    
    plot = (
        p9.ggplot(df, p9.aes(x='reorder(region, deviation)', y='deviation', fill='deviation > 0'))
        + p9.geom_col()
        + p9.coord_flip()
        + p9.scale_fill_manual(values={True: "#2ecc71", False: "#e74c3c"})
        + p9.labs(title='Wage Share: Deviation from Canarias Average (61.2%)', 
                  subtitle='Top and Bottom 10 Municipalities', x='', y='Percentage Points +/- Avg')
        + p9.theme_minimal()
        + p9.theme(legend_position='none')
    )
    plot.save("plots/wage_deviation_chart.png")
    return "plots/wage_deviation_chart.png"

@asset(deps=[renta_cleaning])
def income_distribution_violin(renta_cleaning):
    df = renta_cleaning[renta_cleaning['year'] == 2023]
    
    plot = (
        p9.ggplot(df, p9.aes(x='measure', y='value', fill='measure'))
        + p9.geom_violin(alpha=0.3) # Shows density/shape
        + p9.geom_boxplot(width=0.1, outlier_size=0.5) # Keeps the summary stats
        + p9.labs(title='Density of Income Distribution by Category (2023)', x='', y='Value (%)')
        + p9.theme_minimal()
        + p9.theme(legend_position='none', axis_text_x=p9.element_text(rotation=45, hjust=1))
    )
    plot.save("plots/income_distribution_violin.png")
    return "plots/income_distribution_violin.png"

@asset(deps=[renta_cleaning])
def top_wage_municipalities_bar(renta_cleaning):
    # Filter for the most recent year and category 'Sueldos y salarios'
    max_year = renta_cleaning['year'].max()
    df = renta_cleaning[(renta_cleaning['year'] == max_year) & (renta_cleaning['measure'] == 'Sueldos y salarios')]
    
    # Filter for municipalities (exclude codes starting with 'ES')
    # Using str.startswith('ES') to identify regions/islands and inverting it
    # Ensure TERRITORIO_CODE is string
    df['TERRITORIO_CODE'] = df['TERRITORIO_CODE'].astype(str)
    municipalities = df[~df['TERRITORIO_CODE'].str.startswith('ES')]
    
    # Sort and take top 10
    top_10 = municipalities.sort_values(by='value', ascending=False).head(10)
    
    # Create a horizontal bar chart
    plot = (
        p9.ggplot(top_10, p9.aes(x='reorder(region, value)', y='value'))
        + p9.geom_col(fill='steelblue')
        + p9.coord_flip()
        + p9.labs(title=f'Top 10 Municipalities by Wage Share ({max_year})', y='Value', x='Municipality')
        + p9.theme_minimal()
    )
    plot.save("plots/top_wage_municipalities_bar.png")
    return "plots/top_wage_municipalities_bar.png"

@asset(deps=[renta_cleaning])
def unemployment_trend_faceted(renta_cleaning):
    # Filter for category 'Prestaciones por desempleo'
    target_regions = ['Canarias', 'Lanzarote', 'La Palma', 'Tenerife', 'Gran Canaria']
    df = renta_cleaning[
        (renta_cleaning['measure'] == 'Prestaciones por desempleo') &
        (renta_cleaning['region'].isin(target_regions))
    ]
    
    # Create a faceted line chart
    plot = (
        p9.ggplot(df, p9.aes(x='year', y='value', color='region'))
        + p9.geom_line()
        + p9.facet_wrap('~region')
        + p9.labs(title='Unemployment Benefits Trend by Region', y='Value', x='Year')
        + p9.theme_minimal()
        + p9.theme(legend_position='none') # Hide legend since facet shows name
    )
    plot.save("plots/unemployment_trend_faceted.png")
    return "plots/unemployment_trend_faceted.png"

# GOOD
@asset(deps=[renta_cleaning])
def income_composition_heatmap(renta_cleaning):
    # Filter for 'Pensiones'
    # Use a subset of territories to avoid overcrowding
    # Let's pick the 7 main islands and Canarias
    target_subset = [
        'Canarias', 'Lanzarote', 'Fuerteventura', 'Gran Canaria', 
        'Tenerife', 'La Gomera', 'La Palma', 'El Hierro'
    ]
    df = renta_cleaning[
        (renta_cleaning['measure'] == 'Pensiones') & 
        (renta_cleaning['region'].isin(target_subset))
    ]
    
    # Create a heatmap
    plot = (
        p9.ggplot(df, p9.aes(x='factor(year)', y='region', fill='value'))
        + p9.geom_tile()
        + p9.labs(title='Pension Income Composition Heatmap', y='Region', x='Year', fill='Value')
        + p9.theme_minimal()
        + p9.scale_fill_gradient(low="white", high="red")
    )
    plot.save("plots/income_composition_heatmap.png")
    return "plots/income_composition_heatmap.png"

@asset(deps=[renta_cleaning])
def wages_vs_pensions_correlation(renta_cleaning):
    # Filter for year 2023
    df = renta_cleaning[renta_cleaning['year'] == 2023]
    
    # Filter for municipalities (exclude codes starting with 'ES')
    df['TERRITORIO_CODE'] = df['TERRITORIO_CODE'].astype(str)
    municipalities = df[~df['TERRITORIO_CODE'].str.startswith('ES')]
    
    # Pivot to get measures as columns
    df_pivot = municipalities.pivot_table(
        index='region', 
        columns='measure', 
        values='value', 
        aggfunc='sum'
    ).reset_index()
    
    # Create scatter plot with regression line
    plot = (
        p9.ggplot(df_pivot, p9.aes(x='Sueldos y salarios', y='Pensiones'))
        + p9.geom_point()
        + p9.geom_smooth(method='lm', color='blue')
        + p9.labs(
            title='Correlation between Wages and Pensions (2023)', 
            x='Wages and Salaries (%)', 
            y='Pensions (%)'
        )
        + p9.theme_minimal()
    )
    plot.save("plots/wages_vs_pensions_correlation.png")
    return "plots/wages_vs_pensions_correlation.png"

@asset(deps=[renta_cleaning])
def pension_growth_ranking(renta_cleaning):
    # Filter for 'Pensiones' and years 2015 and 2023
    df = renta_cleaning[
        (renta_cleaning['measure'] == 'Pensiones') & 
        (renta_cleaning['year'].isin([2015, 2023]))
    ]
    
    # Filter for municipalities
    df['TERRITORIO_CODE'] = df['TERRITORIO_CODE'].astype(str)
    municipalities = df[~df['TERRITORIO_CODE'].str.startswith('ES')]
    
    # Pivot to get years as columns
    df_pivot = municipalities.pivot_table(
        index='region', 
        columns='year', 
        values='value'
    ).reset_index()
    
    # Calculate change
    df_pivot['change'] = df_pivot[2023] - df_pivot[2015]
    
    # Get top 15 municipalities by growth
    top_15 = df_pivot.sort_values(by='change', ascending=False).head(15)
    
    # Create lollipop chart
    plot = (
        p9.ggplot(top_15, p9.aes(x='reorder(region, change)', y='change'))
        + p9.geom_segment(p9.aes(xend='region', yend=0), color='gray')
        + p9.geom_point(size=3, color='orange')
        + p9.coord_flip()
        + p9.labs(
            title='Top 15 Municipalities by Pension Share Growth (2015-2023)', 
            x='Municipality', 
            y='Change in Percentage Points'
        )
        + p9.theme_minimal()
    )
    plot.save("plots/pension_growth_ranking.png")
    return "plots/pension_growth_ranking.png"

# Nivel Estudios Pipeline

@asset
def nivel_estudios_load():
    # Read the excel file
    # Note: openpyxl is required and was installed
    return pd.read_excel("data/nivelestudios.xlsx")

@asset(deps=[nivel_estudios_load])
def nivel_estudios_cleaning(nivel_estudios_load):
    df = nivel_estudios_load.copy()
    
    # Rename columns
    df = df.rename(columns={
        "Municipios de 500 habitantes o más": "municipality_raw",
        "Sexo": "sex",
        "Nacionalidad": "nationality",
        "Nivel de estudios en curso": "education_level",
        "Periodo": "year",
        "Total": "total"
    })
    
    # Split municipality_raw into code and name
    # Expecting format "35001 Agaete"
    # match 5 digits at the start
    df['municipality_code'] = df['municipality_raw'].astype(str).str.extract(r'^(\d{5})')
    df['municipality'] = df['municipality_raw'].astype(str).str.extract(r'^\d{5}\s+(.*)')
    
    # If regex fails (e.g. for totals or other formats), we might get NaNs. 
    # Let's filter out rows where municipality_code is NaN, assuming we only want specific municipality data
    df = df.dropna(subset=['municipality_code'])
    
    # Filter out rows with missing essential data
    df = df.dropna(subset=['year', 'total', 'education_level'])
    
    return df

@asset(deps=[nivel_estudios_cleaning])
def education_ranking_dotplot(nivel_estudios_cleaning):
    max_year = nivel_estudios_cleaning['year'].max()
    df_agg = nivel_estudios_cleaning[nivel_estudios_cleaning['year'] == max_year].groupby('education_level', as_index=False)['total'].sum()
    
    plot = (
        p9.ggplot(df_agg, p9.aes(x='reorder(education_level, total)', y='total'))
        + p9.geom_segment(p9.aes(xend='reorder(education_level, total)', yend=0), color='lightgrey') # The "stem"
        + p9.geom_point(size=4, color='purple')
        + p9.coord_flip()
        + p9.labs(title=f'Ranking of Education Levels ({max_year})', x='', y='Total Students')
        + p9.theme_minimal()
    )
    plot.save("plots/education_dotplot.png")
    return "plots/education_dotplot.png"

@asset(deps=[nivel_estudios_cleaning])
def education_evolution_top_municipalities(nivel_estudios_cleaning):
    # Select top 5 municipalities by total students in most recent year
    max_year = nivel_estudios_cleaning['year'].max()
    
    top_munis = (
        nivel_estudios_cleaning[nivel_estudios_cleaning['year'] == max_year]
        .groupby('municipality')['total']
        .sum()
        .sort_values(ascending=False)
        .head(5)
        .index.tolist()
    )
    
    # Filter data for these municipalities
    df = nivel_estudios_cleaning[nivel_estudios_cleaning['municipality'].isin(top_munis)]
    
    # Aggregate total students per municipality per year
    df_agg = df.groupby(['year', 'municipality'], as_index=False)['total'].sum()
    
    # Line chart
    plot = (
        p9.ggplot(df_agg, p9.aes(x='year', y='total', color='municipality'))
        + p9.geom_line(size=1.2)
        + p9.labs(
            title='Evolution of Students in Top 5 Municipalities', 
            x='Year', 
            y='Total Students',
            color='Municipality'
        )
        + p9.theme_minimal()
    )
    plot.save("plots/education_evolution_top_municipalities.png")
    return "plots/education_evolution_top_municipalities.png"