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
    plot.save("plots/income/income_composition_stacked_bar.png")
    return "plots/income/income_composition_stacked_bar.png"

@asset(deps=[renta_cleaning])
def wage_deviation_from_avg(renta_cleaning):
    # 1. Get regional average for 2023
    # Change it so that it actually calculates the average
    # 2. Filter for municipalities in 2023 for Wages
    df = renta_cleaning[(renta_cleaning['year'] == 2023) & 
                        (renta_cleaning['measure'] == 'Sueldos y salarios') &
                        (~renta_cleaning['region'].isin(['Canarias', 'Las Palmas', 'Santa Cruz de Tenerife']))]
    
    avg_val = df['value'].mean()

    # 3. Calculate deviation
    df['deviation'] = df['value'] - avg_val
    df = df.sort_values('deviation').iloc[np.r_[0:10, -10:0]] # Top 10 and Bottom 10
    
    plot = (
        p9.ggplot(df, p9.aes(x='reorder(region, deviation)', y='deviation', fill='deviation > 0'))
        + p9.geom_col()
        + p9.coord_flip()
        + p9.scale_fill_manual(values={True: "#2ecc71", False: "#e74c3c"})
        + p9.labs(title=f'Wage Share: Deviation from Canarias Average ({avg_val})', 
                  subtitle='Top and Bottom 10 Municipalities', x='', y='Percentage Points +/- Avg')
        + p9.theme_minimal()
        + p9.theme(legend_position='none')
    )
    plot.save("plots/income/wage_deviation_chart.png")
    return "plots/income/wage_deviation_chart.png"

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
    plot.save("plots/income/income_distribution_violin.png")
    return "plots/income/income_distribution_violin.png"

@asset(deps=[renta_cleaning])
def unemployment_trend_by_region(renta_cleaning):
    # Filter for category 'Prestaciones por desempleo'
    target_regions = [
        'Canarias', 'Lanzarote', 'Fuerteventura', 'Gran Canaria', 
        'Tenerife', 'La Gomera', 'La Palma', 'El Hierro'
    ]
    df = renta_cleaning[
        (renta_cleaning['measure'] == 'Prestaciones por desempleo') &
        (renta_cleaning['region'].isin(target_regions))
    ]
    
    # Create a line chart matching the request (single graph, multiple lines)
    plot = (
        p9.ggplot(df, p9.aes(x='year', y='value', color='region'))
        + p9.geom_line(size=1)
        + p9.labs(title='Unemployment Benefits Trend by Region (Islands & Canarias)', y='Value', x='Year', color='Region')
        + p9.theme_minimal()
        # Ensure the legend is visible now that we don't have facets
        + p9.theme(legend_position='right') 
    )
    plot.save("plots/income/unemployment_trend_by_region.png")
    return "plots/income/unemployment_trend_by_region.png"


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
        + p9.scale_fill_gradient(low="white", high="red", limits=(df['value'].min(), df['value'].max()))
    )
    plot.save("plots/income/income_composition_heatmap.png")
    return "plots/income/income_composition_heatmap.png"

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
    plot.save("plots/income/pension_growth_ranking.png")
    return "plots/income/pension_growth_ranking.png"

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
    plot.save("plots/education/education_dotplot.png")
    return "plots/education/education_dotplot.png"

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
    plot.save("plots/education/education_evolution_top_municipalities.png")
    return "plots/education/education_evolution_top_municipalities.png"

@asset(deps=[nivel_estudios_cleaning])
def nationality_impact_diverging_bar(nivel_estudios_cleaning):
    max_year = nivel_estudios_cleaning['year'].max()
    df = nivel_estudios_cleaning[
        (nivel_estudios_cleaning['year'] == max_year) &
        (nivel_estudios_cleaning['education_level'] == 'Educación superior') &
        (nivel_estudios_cleaning['sex'] == 'Total')
    ]
    
    # Top 15 municipalities by total students in Higher Ed
    # Need to group by municipality to get total (Española + Extranjera)
    # But wait, does 'Total' sex include both nationalities? Yes.
    # But does DataFrame have rows for 'Española' and 'Extranjera' separately?
    # Yes, column 'nationality'.
    
    # Total students per municipality
    total_per_muni = df.groupby('municipality')['total'].sum()
    top_15_munis = total_per_muni.sort_values(ascending=False).head(15).index.tolist()
    
    df_filtered = df[df['municipality'].isin(top_15_munis)]
    
    # Calculate % Foreign (Extranjera) per municipality
    # Pivot to get cols 'Española', 'Extranjera'
    df_pivot = df_filtered.pivot_table(index='municipality', columns='nationality', values='total', aggfunc='sum').fillna(0)
    
    if 'Extranjera' in df_pivot.columns and 'Española' in df_pivot.columns:
        df_pivot['total'] = df_pivot['Española'] + df_pivot['Extranjera']
        df_pivot['pct_foreign'] = (df_pivot['Extranjera'] / df_pivot['total']) * 100
    else:
        # Fallback if categories are different
        df_pivot['pct_foreign'] = 0
        
    # Calculate Regional Avg % Foreign (from the filtered set or whole set? Requirement says "average regional percentage")
    # Let's calculate from the whole dataset for that level/year
    all_higher_ed = df.groupby('nationality')['total'].sum()
    # Assuming nationalities are Extranjera and Española
    total_foreign = all_higher_ed.get('Extranjera', 0)
    total_all = all_higher_ed.sum()
    avg_foreign_pct = (total_foreign / total_all) * 100 if total_all > 0 else 0
    
    df_pivot['deviation'] = df_pivot['pct_foreign'] - avg_foreign_pct
    df_pivot = df_pivot.reset_index()
    
    plot = (
        p9.ggplot(df_pivot, p9.aes(x='reorder(municipality, deviation)', y='deviation', fill='deviation > 0'))
        + p9.geom_col()
        + p9.coord_flip()
        + p9.scale_fill_manual(values={True: "#2ecc71", False: "#e74c3c"})
        + p9.geom_hline(yintercept=0, color="gray", linetype="dashed")
        + p9.labs(
            title=f'Deviation in Foreign Student % (Higher Ed, {max_year})',
            subtitle=f'Regional Avg: {avg_foreign_pct:.1f}%',
            x='Municipality', 
            y='Deviation form Average (pp)'
        )
        + p9.theme_minimal()
        + p9.theme(legend_position='none')
    )
    plot.save("plots/education/nationality_impact_diverging_bar.png")
    return "plots/education/nationality_impact_diverging_bar.png"

@asset(deps=[nivel_estudios_cleaning])
def education_intensity_heatmap(nivel_estudios_cleaning):
    # Year vs Education Level
    df = nivel_estudios_cleaning[
        (nivel_estudios_cleaning['sex'] == 'Total') &
        (~nivel_estudios_cleaning['education_level'].isin(['Total', 'No cursa estudios']))
    ]
    
    # Aggregate total students
    df_agg = df.groupby(['year', 'education_level'], as_index=False)['total'].sum()
    
    plot = (
        p9.ggplot(df_agg, p9.aes(x='factor(year)', y='education_level', fill='total'))
        + p9.geom_tile()
        + p9.scale_fill_gradient(low="yellow", high="#4a148c") # Yellow to Dark Purple
        + p9.labs(
            title='Education Intensity: Total Students Scale', 
            x='Year', 
            y='Education Level', 
            fill='Total Students'
        )
        + p9.theme_minimal()
        + p9.theme(axis_text_x=p9.element_text(rotation=45, hjust=1))
    )
    plot.save("plots/education/education_intensity_heatmap.png")
    return "plots/education/education_intensity_heatmap.png"

@asset(deps=[nivel_estudios_cleaning])
def higher_ed_gender_gap_lollipop(nivel_estudios_cleaning):
    max_year = nivel_estudios_cleaning['year'].max()
    df = nivel_estudios_cleaning[
        (nivel_estudios_cleaning['year'] == max_year) &
        (nivel_estudios_cleaning['education_level'] == 'Educación superior') &
        (nivel_estudios_cleaning['sex'].isin(['Hombres', 'Mujeres']))
    ]
    
    # Identify top 20 municipalities (by total students H+M)
    total_per_muni = df.groupby('municipality')['total'].sum()
    top_20 = total_per_muni.sort_values(ascending=False).head(20).index.tolist()
    
    df_filtered = df[df['municipality'].isin(top_20)]
    
    # Calculate Gender Gap
    # Pivot
    df_pivot = df_filtered.pivot_table(index='municipality', columns='sex', values='total', aggfunc='sum').fillna(0)
    
    df_pivot['total'] = df_pivot['Hombres'] + df_pivot['Mujeres']
    # If total is 0, avoid div by zero
    df_pivot = df_pivot[df_pivot['total'] > 0]
    
    df_pivot['pct_women'] = (df_pivot['Mujeres'] / df_pivot['total']) * 100
    df_pivot['pct_men'] = (df_pivot['Hombres'] / df_pivot['total']) * 100
    
    df_pivot['gender_gap'] = df_pivot['pct_women'] - df_pivot['pct_men'] # Higher positive means more women relative to men
    df_pivot = df_pivot.reset_index()
    
    plot = (
        p9.ggplot(df_pivot, p9.aes(x='reorder(municipality, gender_gap)', y='gender_gap'))
        + p9.geom_segment(p9.aes(xend='municipality', yend=0), color='gray')
        + p9.geom_point(size=3, color='purple')
        + p9.coord_flip()
        + p9.labs(
            title=f'Gender Gap in Higher Education ({max_year})', 
            subtitle='(% Women - % Men) Top 20 Municipalities',
            x='Municipality', 
            y='Gap (Percentage Points)'
        )
        + p9.theme_minimal()
    )
    plot.save("plots/education/higher_ed_gender_gap_lollipop.png")
    return "plots/education/higher_ed_gender_gap_lollipop.png"

@asset(deps=[nivel_estudios_cleaning])
def education_level_ridge_plot(nivel_estudios_cleaning):
    # Violin plot
    max_year = nivel_estudios_cleaning['year'].max()
    df = nivel_estudios_cleaning[
        (nivel_estudios_cleaning['year'] == max_year) &
        (nivel_estudios_cleaning['sex'] == 'Total') &
        (~nivel_estudios_cleaning['education_level'].isin(['Total']))
    ]
    
    # We want distribution of "Total Students" across municipalities for each level
    # Currently df is melted by nationality. We should sum by municipality + level first.
    df_agg = df.groupby(['municipality', 'education_level'], as_index=False)['total'].sum()
    
    plot = (
        p9.ggplot(df_agg, p9.aes(x='education_level', y='total', fill='education_level'))
        + p9.geom_violin(alpha=0.6, trim=False)
        # Using a log scale might help visualization if skew is high, but standard is requested
        + p9.coord_flip()
        + p9.labs(
            title=f'Distribution of Student Totals by Level ({max_year})', 
            x='', 
            y='Total Students (per Municipality)'
        )
        + p9.theme_minimal()
        + p9.theme(legend_position='none')
    )
    plot.save("plots/education/education_level_ridge_plot.png")
    return "plots/education/education_level_ridge_plot.png"