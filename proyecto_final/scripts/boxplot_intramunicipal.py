def generar_plot(df):
    import pandas as pd
    import plotnine as p9

    orden_islas = (
        df.groupby('isla')['renta_neta']
        .median()
        .sort_values()
        .index.tolist()
    )
    df = df.copy()
    df['isla'] = pd.Categorical(df['isla'], categories=orden_islas, ordered=True)

    plot = (
        p9.ggplot(df, p9.aes(x='isla', y='renta_neta'))
        + p9.geom_boxplot(fill='#4575b4', alpha=0.7, outlier_size=1.2, color='#2c5282')
        + p9.coord_flip()
        + p9.labs(
            title='Distribución de renta por sección censal según isla (2023)',
            x='',
            y='Renta neta media por hogar (€)'
        )
        + p9.theme_minimal()
        + p9.theme(legend_position='none', figure_size=(10, 5))
    )
    return plot