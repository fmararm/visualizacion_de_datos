def generar_plot(df):
    import pandas as pd
    import plotnine as p9

    df = df.copy()
    df['año'] = df['año'].astype(str)
    isla = df['isla'].iloc[0]
    orden = (
        df[df['año'] == '2023']
        .sort_values('renta_media')['municipio']
        .tolist()
    )
    df['municipio'] = pd.Categorical(df['municipio'], categories=orden, ordered=True)

    plot = (
        p9.ggplot(df, p9.aes(x='año', y='municipio', fill='renta_media'))
        + p9.geom_tile(color='white', size=0.3)
        + p9.scale_fill_gradient(low='#d73027', high='#1a9850', name='Renta (€)')
        + p9.labs(
            title=f'Evolución de la renta en {isla} (2021-2023)',
            x='',
            y=''
        )
        + p9.theme_minimal()
        + p9.theme(axis_text_y=p9.element_text(size=8))
    )
    return plot
