def generar_plot(df):
    import pandas as pd
    import plotnine as p9

    df = df.copy()
    orden = (
        df.groupby('municipio')['renta_neta']
        .median()
        .sort_values()
        .index.tolist()
    )
    df['municipio'] = pd.Categorical(df['municipio'], categories=orden, ordered=True)
    isla = df['isla'].iloc[0]

    plot = (
        p9.ggplot(df, p9.aes(x='municipio', y='renta_neta'))
        + p9.geom_boxplot(fill='#4575b4', alpha=0.7, outlier_size=0.8, color='#2c5282')
        + p9.coord_flip()
        + p9.labs(
            title=f'Desigualdad intramunicipal en {isla}: renta por sección censal (2023)',
            x='',
            y='Renta neta media por hogar (€)'
        )
        + p9.theme_minimal()
        + p9.theme(
            legend_position='none',
            axis_text_y=p9.element_text(size=8)
        )
    )
    return plot
