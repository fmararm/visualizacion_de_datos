import pandas as pd
import plotnine as p9
from dagster import asset

@asset
def renta_load():
    renta = pd.read_csv("distribucion-renta-canarias.csv")
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
def renta_visualizations(renta_cleaning):
    # Create a simple line plot
    plot = (
        p9.ggplot(renta_cleaning, p9.aes(x='year', y='value', color='region'))
        + p9.geom_line()
        + p9.labs(title='Renta distribution over time by region', y='Value', x='Year')
        + p9.theme_minimal()
    )
    # Save the plot (optional, but good for verification)
    plot.save("renta_plot_region.png")
    
    return plot

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
    plot.save("evolution_stacked_area_plot.png")
    return plot

@asset(deps=[renta_cleaning])
def income_variance_boxplot(renta_cleaning):
    # Filter for the most recent year
    max_year = renta_cleaning['year'].max()
    df = renta_cleaning[renta_cleaning['year'] == max_year]
    
    # Create a boxplot showing the distribution of OBS_VALUE across all territories, grouped by MEDIDAS#es
    plot = (
        p9.ggplot(df, p9.aes(x='measure', y='value', fill='measure'))
        + p9.geom_boxplot()
        + p9.labs(title=f'Income Variance Boxplot ({max_year})', y='Value', x='Measure')
        + p9.theme_minimal()
        + p9.theme(axis_text_x=p9.element_text(rotation=45, hjust=1))
    )
    plot.save("income_variance_boxplot.png")
    return plot

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
    plot.save("top_wage_municipalities_bar.png")
    return plot

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
    plot.save("unemployment_trend_faceted.png")
    return plot

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
    plot.save("income_composition_heatmap.png")
    return plot

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
    plot.save("wages_vs_pensions_correlation.png")
    return plot

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
    plot.save("pension_growth_ranking.png")
    return plot

# Nivel Estudios Pipeline

@asset
def nivel_estudios_load():
    # Read the excel file
    # Note: openpyxl is required and was installed
    return pd.read_excel("nivelestudios.xlsx")

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
def education_level_distribution(nivel_estudios_cleaning):
    # Filter for most recent year
    max_year = nivel_estudios_cleaning['year'].max()
    df = nivel_estudios_cleaning[nivel_estudios_cleaning['year'] == max_year]
    
    # Group by education_level and sum total
    df_agg = df.groupby('education_level', as_index=False)['total'].sum()
    
    # Bar chart
    plot = (
        p9.ggplot(df_agg, p9.aes(x='reorder(education_level, total)', y='total'))
        + p9.geom_col(fill='purple')
        + p9.coord_flip()
        + p9.labs(
            title=f'Students by Education Level ({max_year})', 
            x='Education Level', 
            y='Total Students'
        )
        + p9.theme_minimal()
    )
    plot.save("education_level_distribution.png")
    return plot

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
    plot.save("education_evolution_top_municipalities.png")
    return plot