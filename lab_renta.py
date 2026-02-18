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
def income_distribution_boxplot(renta_cleaning):
    df = renta_cleaning[renta_cleaning['year'] == 2023]
    
    plot = (
        p9.ggplot(df, p9.aes(x='measure', y='value', fill='measure'))
        + p9.geom_boxplot() # Just a boxplot
        + p9.labs(title='Income Distribution by Category (2023)', x='', y='Value (%)')
        + p9.theme_minimal()
        + p9.theme(legend_position='none', axis_text_x=p9.element_text(rotation=45, hjust=1))
    )
    plot.save("plots/income/income_distribution_boxplot.png")
    return "plots/income/income_distribution_boxplot.png"

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
def top_foreign_students_municipalities_bar(nivel_estudios_cleaning):
    max_year = nivel_estudios_cleaning['year'].max()
    
    # Filter for Foreign students, Total Sex, All levels (summed)
    # Check nationality column existence
    if 'nationality' not in nivel_estudios_cleaning.columns:
        return "Skipped: Nationality column missing"

    # Assume we sum all education levels (excluding Total if present to avoid double counting)
    # Based on other assets, we exclude 'Total' and 'No cursa estudios'
    df = nivel_estudios_cleaning[
        (nivel_estudios_cleaning['year'] == max_year) &
        (nivel_estudios_cleaning['sex'] == 'Total') &
        (nivel_estudios_cleaning['nationality'] == 'Extranjera') & 
        (~nivel_estudios_cleaning['education_level'].isin(['Total', 'No cursa estudios']))
    ]
    
    # Aggregate by Municipality
    df_agg = df.groupby('municipality', as_index=False)['total'].sum()
    
    # Top 20
    top_20 = df_agg.nlargest(20, 'total')
    
    plot = (
        p9.ggplot(top_20, p9.aes(x='reorder(municipality, total)', y='total'))
        + p9.geom_col(fill='steelblue') # Simple bar
        + p9.coord_flip()
        + p9.labs(
            title=f'Municipalities with Most Foreign Students ({max_year})', 
            x='Municipality', 
            y='Total Foreign Students'
        )
        + p9.theme_minimal()
    )
    plot.save("plots/education/top_foreign_students_municipalities_bar.png")
    return "plots/education/top_foreign_students_municipalities_bar.png"

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
def higher_ed_gender_gap_diverging_bar(nivel_estudios_cleaning):
    max_year = nivel_estudios_cleaning['year'].max()
    df = nivel_estudios_cleaning[
        (nivel_estudios_cleaning['year'] == max_year) &
        (nivel_estudios_cleaning['education_level'] == 'Educación superior') &
        (nivel_estudios_cleaning['sex'].isin(['Hombres', 'Mujeres']))
    ]
    
    # Identify top 20 municipalities (by total students H+M)
    total_per_muni = df.groupby('municipality')['total'].sum()
    top_20 = total_per_muni.sort_values(ascending=False).head(20).index.tolist()
    
    # Calculate Regional Reference (Average Proportion of Women)
    # Using the whole dataset for this level/year
    all_totals = df.groupby('sex')['total'].sum()
    total_students = all_totals.sum()
    total_women = all_totals.get('Mujeres', 0)
    avg_pct_women = (total_women / total_students) * 100 if total_students > 0 else 0
    
    df_filtered = df[df['municipality'].isin(top_20)]
    
    # Calculate % Women per municipality
    df_pivot = df_filtered.pivot_table(index='municipality', columns='sex', values='total', aggfunc='sum').fillna(0)
    
    df_pivot['total'] = df_pivot['Hombres'] + df_pivot['Mujeres']
    df_pivot = df_pivot[df_pivot['total'] > 0]
    
    df_pivot['pct_women'] = (df_pivot['Mujeres'] / df_pivot['total']) * 100
    
    # Calculate deviation from reference
    df_pivot['deviation'] = df_pivot['pct_women'] - avg_pct_women
    df_pivot = df_pivot.reset_index()
    
    # Create diverging bar chart
    plot = (
        p9.ggplot(df_pivot, p9.aes(x='reorder(municipality, deviation)', y='deviation', fill='deviation > 0'))
        + p9.geom_col()
        + p9.coord_flip()
        + p9.geom_hline(yintercept=0, color="gray", linetype="dashed")
        + p9.scale_fill_manual(values={True: "#9b59b6", False: "#e67e22"}, labels={True: "More Women", False: "More Men"}) # Purple vs Orange
        + p9.labs(
            title=f'Gender Imbalance in Higher Education ({max_year})', 
            subtitle=f'Deviation from Regional Avg Women Share ({avg_pct_women:.1f}%)',
            x='Municipality', 
            y='Deviation (Percentage Points)',
            fill='Direction'
        )
        + p9.theme_minimal()
        # + p9.theme(legend_position='bottom')
    )
    plot.save("plots/education/higher_ed_gender_gap_diverging_bar.png")
    return "plots/education/higher_ed_gender_gap_diverging_bar.png"

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

@asset(deps=[nivel_estudios_cleaning])
def gender_composition_stacked_bar(nivel_estudios_cleaning):
    max_year = nivel_estudios_cleaning['year'].max()
    df = nivel_estudios_cleaning[
        (nivel_estudios_cleaning['year'] == max_year) &
        (nivel_estudios_cleaning['sex'].isin(['Hombres', 'Mujeres'])) &
        (~nivel_estudios_cleaning['education_level'].isin(['Total', 'No cursa estudios']))
    ]
    
    plot = (
        p9.ggplot(df, p9.aes(x='education_level', y='total', fill='sex'))
        + p9.geom_bar(stat='identity', position='fill')
        + p9.scale_y_continuous(labels=lambda l: [f"{int(x*100)}%" for x in l])
        + p9.coord_flip() # Readable labels
        + p9.labs(
            title=f'Gender Composition by Education Level ({max_year})', 
            x='', 
            y='Percentage', 
            fill='Sex'
        )
        + p9.theme_minimal()
    )
    plot.save("plots/education/gender_composition_stacked_bar.png")
    return "plots/education/gender_composition_stacked_bar.png"

@asset(deps=[nivel_estudios_cleaning])
def gender_proportion_bar(nivel_estudios_cleaning):
    max_year = nivel_estudios_cleaning['year'].max()
    df = nivel_estudios_cleaning[
        (nivel_estudios_cleaning['year'] == max_year) &
        (nivel_estudios_cleaning['sex'].isin(['Hombres', 'Mujeres'])) &
        (~nivel_estudios_cleaning['education_level'].isin(['Total', 'No cursa estudios']))
    ]
    
    # Aggregate by Sex
    df_agg = df.groupby('sex', as_index=False)['total'].sum()
    df_agg['percentage'] = df_agg['total'] / df_agg['total'].sum() * 100
    
    # Stacked Bar Chart
    plot = (
        p9.ggplot(df_agg, p9.aes(x=0, y='percentage', fill='sex'))
        + p9.geom_bar(stat='identity', width=0.5)
        + p9.coord_flip()
        + p9.geom_text(p9.aes(label='round(percentage, 1)'), position=p9.position_stack(vjust=0.5), size=10)
        + p9.labs(
            title=f'Proportion of Men vs Women ({max_year})', 
            fill='Sex',
            x='', y='Percentage'
        )
        + p9.theme_minimal()
        + p9.theme(
            axis_text_y=p9.element_blank(), 
            axis_ticks=p9.element_blank(),
            panel_grid=p9.element_blank()
        )
    )
    plot.save("plots/education/gender_proportion_bar.png")
    return "plots/education/gender_proportion_bar.png"

@asset(deps=[nivel_estudios_cleaning])
def nationality_proportion_bar(nivel_estudios_cleaning):
    max_year = nivel_estudios_cleaning['year'].max()
    df = nivel_estudios_cleaning[
        (nivel_estudios_cleaning['year'] == max_year) &
        (nivel_estudios_cleaning['sex'] == 'Total') &
        (~nivel_estudios_cleaning['education_level'].isin(['Total', 'No cursa estudios']))
    ]
    
    if 'nationality' not in df.columns:
         return "Skipped: Nationality column missing"
         
    df_agg = df.groupby('nationality', as_index=False)['total'].sum()
    df_agg['percentage'] = df_agg['total'] / df_agg['total'].sum() * 100
    
    plot = (
        p9.ggplot(df_agg, p9.aes(x=0, y='percentage', fill='nationality'))
        + p9.geom_bar(stat='identity', width=0.5)
        + p9.coord_flip()
        + p9.geom_text(p9.aes(label='round(percentage, 1)'), position=p9.position_stack(vjust=0.5), size=10)
        + p9.labs(
            title=f'Proportion of Locals vs Foreigners ({max_year})', 
            fill='Nationality',
            x='', y='Percentage'
        )
        + p9.theme_minimal()
        + p9.theme(
            axis_text_y=p9.element_blank(), 
            axis_ticks=p9.element_blank(),
            panel_grid=p9.element_blank()
        )
    )
    plot.save("plots/education/nationality_proportion_bar.png")
    return "plots/education/nationality_proportion_bar.png"

@asset(deps=[nivel_estudios_cleaning])
def education_level_proportion_bar(nivel_estudios_cleaning):
    max_year = nivel_estudios_cleaning['year'].max()
    df = nivel_estudios_cleaning[
        (nivel_estudios_cleaning['year'] == max_year) &
        (nivel_estudios_cleaning['sex'] == 'Total') &
        (~nivel_estudios_cleaning['education_level'].isin(['Total', 'No cursa estudios']))
    ]
    
    df_agg = df.groupby('education_level', as_index=False)['total'].sum()
    df_agg['percentage'] = df_agg['total'] / df_agg['total'].sum() * 100
    
    plot = (
        p9.ggplot(df_agg, p9.aes(x=0, y='percentage', fill='education_level'))
        + p9.geom_bar(stat='identity', width=0.5)
        + p9.coord_flip()
        # Text might be crowded for many levels, disable only if needed or keep small
        # + p9.geom_text(p9.aes(label='round(percentage, 1)'), position=p9.position_stack(vjust=0.5), size=8)
        + p9.labs(
            title=f'Proportion of Education Levels ({max_year})', 
            fill='Level',
            x='', y='Percentage'
        )
        + p9.theme_minimal()
        + p9.theme(
            axis_text_y=p9.element_blank(), 
            axis_ticks=p9.element_blank(),
            panel_grid=p9.element_blank()
        )
    )
    plot.save("plots/education/education_level_proportion_bar.png")
    return "plots/education/education_level_proportion_bar.png"

@asset(deps=[nivel_estudios_cleaning])
def higher_ed_by_island_bar(nivel_estudios_cleaning):
    max_year = nivel_estudios_cleaning['year'].max()
    df = nivel_estudios_cleaning[
        (nivel_estudios_cleaning['year'] == max_year) &
        (nivel_estudios_cleaning['education_level'] == 'Educación superior') &
        (nivel_estudios_cleaning['sex'] == 'Total')
    ]
    
    # Island Mapping
    def get_island(code_str):
        if not isinstance(code_str, str):
            return "Unknown"
        try:
            code = int(code_str)
        except:
            return "Unknown"
            
        # Santa Cruz de Tenerife Province (38)
        if 38000 <= code < 39000:
            if code in [38013, 38048, 38054]: return "El Hierro"
            if code in [38002, 38003, 38021, 38036, 38049, 38050]: return "La Gomera"
            if code in [38007, 38008, 38009, 38014, 38016, 38024, 38027, 38029, 38030, 38033, 38037, 38045, 38047, 38053]: return "La Palma"
            return "Tenerife" 
            
        # Las Palmas Province (35)
        if 35000 <= code < 36000:
            if code in [35004, 35010, 35018, 35024, 35028, 35029, 35034]: return "Lanzarote"
            if code in [35003, 35007, 35014, 35015, 35017, 35030]: return "Fuerteventura"
            return "Gran Canaria" 
            
        return "Unknown"

    df['island'] = df['municipality_code'].apply(get_island)
    
    df_agg = df.groupby('island', as_index=False)['total'].sum()
    df_agg = df_agg[df_agg['island'] != "Unknown"] 
    
    df_agg['percentage'] = df_agg['total'] / df_agg['total'].sum() * 100
    
    plot = (
        p9.ggplot(df_agg, p9.aes(x=0, y='percentage', fill='island'))
        + p9.geom_bar(stat='identity', width=0.5)
        + p9.coord_flip()

        + p9.labs(
            title=f'Higher Education Students by Island ({max_year})', 
            fill='Island',
            x='', y='Percentage'
        )
        + p9.theme_minimal()
        + p9.theme(
            axis_text_y=p9.element_blank(), 
            axis_ticks=p9.element_blank(),
            panel_grid=p9.element_blank()
        )
    )
    return "plots/education/higher_ed_by_island_bar.png"

@asset(deps=[renta_cleaning, nivel_estudios_cleaning])
def income_vs_higher_ed_scatter(renta_cleaning, nivel_estudios_cleaning):
    # Prepare Income Data
    max_year_renta = renta_cleaning['year'].max()
    income_df = renta_cleaning[
        (renta_cleaning['year'] == max_year_renta) &
        (renta_cleaning['measure'] == 'Sueldos y salarios')
    ].copy()
    
    # Prepare Education Data (Higher Ed %)
    max_year_ne = nivel_estudios_cleaning['year'].max()
    ne_df = nivel_estudios_cleaning[
        (nivel_estudios_cleaning['year'] == max_year_ne) &
        (nivel_estudios_cleaning['sex'] == 'Total')
    ].copy()
    
    # Calculate % Higher Ed per municipality
    # Total students per muni
    idx_cols = ['municipality']
    total_students = ne_df[~ne_df['education_level'].isin(['Total', 'No cursa estudios'])].groupby(idx_cols)['total'].sum().reset_index(name='total_students')
    
    higher_ed = ne_df[ne_df['education_level'] == 'Educación superior'].groupby(idx_cols)['total'].sum().reset_index(name='higher_ed_students')
    
    edu_df = pd.merge(total_students, higher_ed, on='municipality', how='left').fillna(0)
    edu_df['pct_higher_ed'] = (edu_df['higher_ed_students'] / edu_df['total_students']) * 100
    
    # Merge Income and Education
    merged = pd.merge(income_df, edu_df, left_on='region', right_on='municipality', how='inner')
    
    plot = (
        p9.ggplot(merged, p9.aes(x='value', y='pct_higher_ed'))
        + p9.geom_point(alpha=0.7, color='blue')
        + p9.geom_smooth(method='lm', color='red', se=False)
        + p9.labs(
            title=f'Income vs Higher Education ({max_year_renta})', 
            x='Average Income (Sueldos y salarios)', 
            y='% Population with Higher Education'
        )
        + p9.theme_minimal()
    )
    plot.save("plots/combination/income_vs_higher_ed_scatter.png")
    return "plots/combination/income_vs_higher_ed_scatter.png"

@asset(deps=[renta_cleaning, nivel_estudios_cleaning])
def income_vs_foreign_pop_scatter(renta_cleaning, nivel_estudios_cleaning):
    # Prepare Income
    max_year_renta = renta_cleaning['year'].max()
    income_df = renta_cleaning[
        (renta_cleaning['year'] == max_year_renta) &
        (renta_cleaning['measure'] == 'Sueldos y salarios')
    ].copy()
    
    # Prepare Foreign %
    max_year_ne = nivel_estudios_cleaning['year'].max()
    # Need nationality breakdown. Assuming 'nationality' exists and we can filter.
    if 'nationality' not in nivel_estudios_cleaning.columns:
         return "Skipped: Nationality column missing"

    ne_df = nivel_estudios_cleaning[
        (nivel_estudios_cleaning['year'] == max_year_ne) &
        (nivel_estudios_cleaning['sex'] == 'Total') &
        (~nivel_estudios_cleaning['education_level'].isin(['Total', 'No cursa estudios']))
    ].copy()
    
    # Total students per muni
    total_students = ne_df.groupby('municipality')['total'].sum().reset_index(name='total_students')
    
    # Foreign students
    foreign_students = ne_df[ne_df['nationality'] == 'Extranjera'].groupby('municipality')['total'].sum().reset_index(name='foreign_students')
    
    edu_df = pd.merge(total_students, foreign_students, on='municipality', how='left').fillna(0)
    edu_df['pct_foreign'] = (edu_df['foreign_students'] / edu_df['total_students']) * 100
    
    # Merge
    merged = pd.merge(income_df, edu_df, left_on='region', right_on='municipality', how='inner')
    
    plot = (
        p9.ggplot(merged, p9.aes(x='value', y='pct_foreign'))
        + p9.geom_point(alpha=0.7, color='green')
        + p9.geom_smooth(method='lm', color='orange', se=False)
        + p9.labs(
            title=f'Income vs Foreign Student % ({max_year_renta})', 
            x='Average Income (Sueldos y salarios)', 
            y='% Foreign Students'
        )
        + p9.theme_minimal()
    )
    plot.save("plots/combination/income_vs_foreign_pop_scatter.png")
    return "plots/combination/income_vs_foreign_pop_scatter.png"

@asset(deps=[renta_cleaning, nivel_estudios_cleaning])
def rich_vs_poor_higher_ed_bar(renta_cleaning, nivel_estudios_cleaning):
    # Same merge logic as first asset
    # Prepare Income
    max_year_renta = renta_cleaning['year'].max()
    income_df = renta_cleaning[
        (renta_cleaning['year'] == max_year_renta) &
        (renta_cleaning['measure'] == 'Sueldos y salarios')
    ].copy()
    
    # Prepare Higher Ed %
    max_year_ne = nivel_estudios_cleaning['year'].max()
    ne_df = nivel_estudios_cleaning[
        (nivel_estudios_cleaning['year'] == max_year_ne) &
        (nivel_estudios_cleaning['sex'] == 'Total')
    ].copy()
    
    idx_cols = ['municipality']
    total_students = ne_df[~ne_df['education_level'].isin(['Total', 'No cursa estudios'])].groupby(idx_cols)['total'].sum().reset_index(name='total_students')
    higher_ed = ne_df[ne_df['education_level'] == 'Educación superior'].groupby(idx_cols)['total'].sum().reset_index(name='higher_ed_students')
    
    edu_df = pd.merge(total_students, higher_ed, on='municipality', how='left').fillna(0)
    edu_df['pct_higher_ed'] = (edu_df['higher_ed_students'] / edu_df['total_students']) * 100
    
    merged = pd.merge(income_df, edu_df, left_on='region', right_on='municipality', how='inner')
    
    # Sort by Income
    merged = merged.sort_values('value', ascending=False)
    
    top_5 = merged.head(5).copy()
    top_5['Category'] = 'Top 5 Richest'
    
    bottom_5 = merged.tail(5).copy()
    bottom_5['Category'] = 'Bottom 5 Poorest'
    
    combined = pd.concat([top_5, bottom_5])
    
    plot = (
        p9.ggplot(combined, p9.aes(x='reorder(municipality, pct_higher_ed)', y='pct_higher_ed', fill='Category'))
        + p9.geom_col()
        + p9.coord_flip()
        + p9.labs(
            title=f'Higher Education in Richest vs Poorest Municipalities ({max_year_renta})', 
            x='Municipality', 
            y='% Higher Education Students',
            fill='Income Grp'
        )
        + p9.theme_minimal()
    )
    plot.save("plots/combination/rich_vs_poor_higher_ed_bar.png")
    return "plots/combination/rich_vs_poor_higher_ed_bar.png"