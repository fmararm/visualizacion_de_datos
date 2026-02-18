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
    
    # Ensure year is strictly numeric (YYYY)
    # Handle potential "YYYY-MM-DD" strings or other formats
    renta['year'] = pd.to_numeric(renta['year'].astype(str).str.extract(r'^(\d{4})')[0], errors='coerce')
    
    # Drop rows with missing values in important columns
    renta = renta.dropna(subset=['region', 'year', 'measure', 'value'])
    renta['year'] = renta['year'].astype(int)
    
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
        + p9.labs(title='Composición de la Renta por Isla (2023)', x='', y='Porcentaje', fill='Fuente')
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
        + p9.labs(title=f'Cuota Salarial: Desviación del Promedio de Canarias ({avg_val:.1f})', 
                  subtitle='Top 10 y Bottom 10 Municipios', x='', y='Puntos Porcentuales +/- Promedio')
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
        + p9.labs(title='Distribución de la Renta por Categoría (2023)', x='', y='Valor (%)')
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
    ].copy()
    
    # Highlight Canarias
    df['is_canarias'] = df['region'] == 'Canarias'
    years = sorted(df['year'].unique())
    
    # Create a line chart matching the request (single graph, multiple lines)
    plot = (
        p9.ggplot(df, p9.aes(x='year', y='value', color='region', size='is_canarias', alpha='is_canarias'))
        + p9.geom_line()
        + p9.scale_size_manual(values={True: 2.0, False: 0.8}) # Thicker for Canarias
        + p9.scale_alpha_manual(values={True: 1.0, False: 0.7}) # More opaque for Canarias
        + p9.scale_x_continuous(breaks=years)
        + p9.labs(
            title='Evolución de Prestaciones por Desempleo por Región', 
            y='Valor', 
            x='Año', 
            color='Región'
        )
        + p9.theme_minimal()
        # Ensure the legend is visible now that we don't have facets
        + p9.theme(legend_position='right') 
        + p9.guides(size=False, alpha=False)
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
        + p9.labs(title='Mapa de Calor de Pensiones (Principales Regiones)', y='Región', x='Año', fill='Valor')
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
            title='Top 15 Municipios con Mayor Crecimiento en Pensiones (2015-2023)', 
            x='Municipio', 
            y='Cambio en Puntos Porcentuales'
        )
        + p9.theme_minimal()
        + p9.theme(figure_size=(12, 8))
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
    
    # Ensure year is strictly numeric (YYYY)
    df['year'] = pd.to_numeric(df['year'].astype(str).str.extract(r'^(\d{4})')[0], errors='coerce')
    
    # If regex fails (e.g. for totals or other formats), we might get NaNs. 
    # Let's filter out rows where municipality_code is NaN, assuming we only want specific municipality data
    df = df.dropna(subset=['municipality_code'])
    
    # Filter out rows with missing essential data
    df = df.dropna(subset=['year', 'total', 'education_level'])
    df['year'] = df['year'].astype(int)
    
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
            title=f'Municipios con Más Estudiantes Extranjeros ({max_year})', 
            x='Municipio', 
            y='Total Estudiantes Extranjeros'
        )
        + p9.theme_minimal()
    )
    plot.save("plots/education/top_foreign_students_municipalities_bar.png")
    return "plots/education/top_foreign_students_municipalities_bar.png"



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
        + p9.scale_fill_manual(values={True: "#9b59b6", False: "#e67e22"}, labels={True: "Más Mujeres", False: "Más Hombres"}) # Purple vs Orange
        + p9.labs(
            title=f'Desequilibrio de Género en Educación Superior ({max_year})', 
            subtitle=f'Desviación del Promedio Regional de Mujeres ({avg_pct_women:.1f}%)',
            x='Municipio', 
            y='Desviación (Puntos Porcentuales)',
            fill='Dirección'
        )
        + p9.theme_minimal()
        + p9.theme(figure_size=(12, 8))
        # + p9.theme(legend_position='bottom')
    )
    plot.save("plots/education/higher_ed_gender_gap_diverging_bar.png")
    return "plots/education/higher_ed_gender_gap_diverging_bar.png"





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
            title=f'Proporción de Hombres vs Mujeres ({max_year})', 
            fill='Sexo',
            x='', y='Porcentaje'
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
    # Evolution over all years
    df = nivel_estudios_cleaning[
        (nivel_estudios_cleaning['sex'] == 'Total') &
        (~nivel_estudios_cleaning['education_level'].isin(['Total', 'No cursa estudios']))
    ]
    
    if 'nationality' not in df.columns:
         return "Skipped: Nationality column missing"
         
    df_agg = df.groupby(['year', 'nationality'], as_index=False)['total'].sum()
    
    # Calculate percentage per year
    # Join with total per year
    year_totals = df_agg.groupby('year')['total'].transform('sum')
    df_agg['percentage'] = df_agg['total'] / year_totals * 100
    
    plot = (
        p9.ggplot(df_agg, p9.aes(x='factor(year)', y='percentage', fill='nationality'))
        + p9.geom_bar(stat='identity', width=0.7)
        # Vertical bars for time series
        + p9.geom_text(p9.aes(label='round(percentage, 1)'), position=p9.position_stack(vjust=0.5), size=8)
        + p9.labs(
            title='Proporción de Locales vs Extranjeros por Año', 
            fill='Nacionalidad',
            x='Año', y='Porcentaje'
        )
        + p9.theme_minimal()
        + p9.theme(
            panel_grid_major_x=p9.element_blank(),
            figure_size=(12, 6)
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
    
    # Shorten labels for legend (Spanish)
    label_map = {
        'Educación primaria e inferior': 'Primaria e Inferior',
        'Cursa estudios pero no hay información sobre los mismos': 'Desconocido (Cursando)',
        'Primera etapa de Educación Secundaria y similar': 'Secundaria (1ª Etapa)',
        'Educación superior': 'Educación Superior',
        'Segunda etapa de Educación Secundaria, con orientación profesional (con y sin continuidad en la educación superior); Educación postsecundaria no superior': 'FP / Post-Secundaria',
        'Segunda etapa de educación secundaria, con orientación general': 'Secundaria (General)'
    }
    df_agg['education_level'] = df_agg['education_level'].replace(label_map)
    
    df_agg['percentage'] = df_agg['total'] / df_agg['total'].sum() * 100
    
    plot = (
        p9.ggplot(df_agg, p9.aes(x=0, y='percentage', fill='education_level'))
        + p9.geom_bar(stat='identity', width=0.5)
        + p9.coord_flip()
        # Text might be crowded for many levels, disable only if needed or keep small
        # + p9.geom_text(p9.aes(label='round(percentage, 1)'), position=p9.position_stack(vjust=0.5), size=8)
        + p9.labs(
            title=f'Proporción de Niveles Educativos ({max_year})', 
            fill='Nivel Educativo',
            x='', y='Porcentaje'
        )
        + p9.theme_minimal()
        + p9.theme(
            axis_text_y=p9.element_blank(), 
            axis_ticks_y=p9.element_blank(),
            axis_text_x=p9.element_text(color='black', size=10),
            panel_grid_major_y=p9.element_blank(),
            figure_size=(12, 6),
            plot_title=p9.element_text(size=14, ha='center')
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
            title=f'Estudiantes de Educación Superior por Isla ({max_year})', 
            fill='Isla',
            x='', y='Porcentaje'
        )
        + p9.theme_minimal()
        + p9.theme(
            axis_text_y=p9.element_blank(), 
            axis_ticks=p9.element_blank(),
            panel_grid=p9.element_blank()
        )
    )
    plot.save("plots/education/higher_ed_by_island_bar.png")
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
            title=f'Renta vs Educación Superior ({max_year_renta})', 
            x='Renta Media (Sueldos y salarios)', 
            y='% Población con Educación Superior'
        )
        + p9.theme_minimal()
    )
    plot.save("plots/combination/income_vs_higher_ed_scatter.png")
    return "plots/combination/income_vs_higher_ed_scatter.png"



@asset(deps=[renta_cleaning, nivel_estudios_cleaning])
def higher_ed_mun_wage_comparison(renta_cleaning, nivel_estudios_cleaning):
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
    
    # Calculate regional average (weighted)
    total_higher = edu_df['higher_ed_students'].sum()
    total_pop = edu_df['total_students'].sum()
    avg_higher_ed = (total_higher / total_pop) * 100 if total_pop > 0 else 0
    
    top_5 = merged.head(5).copy()
    top_5['Category'] = 'Mayor Cuota Salarial'
    
    bottom_5 = merged.tail(5).copy()
    bottom_5['Category'] = 'Menor Cuota Salarial'
    
    combined = pd.concat([top_5, bottom_5])
    
    plot = (
        p9.ggplot(combined, p9.aes(x='reorder(municipality, pct_higher_ed)', y='pct_higher_ed', fill='Category'))
        + p9.geom_col()
        + p9.coord_flip()
        + p9.facet_wrap('~Category', scales='free_y', ncol=1)
        + p9.geom_hline(yintercept=avg_higher_ed, linetype='dashed', color='black')
        + p9.labs(
            title=f'Educación Superior en Municipios con Mayor Cuota Salarial ({max_year_renta})', 
            subtitle=f'Promedio Regional: {avg_higher_ed:.1f}%',
            x='Municipio', 
            y='% Estudiantes Educación Superior',
            fill='Grupo Salarial'
        )
        + p9.theme_minimal()
        + p9.theme(legend_position='none')
    )
    plot.save("plots/combination/higher_ed_mun_wage_comparison.png")
    return "plots/combination/higher_ed_mun_wage_comparison.png"