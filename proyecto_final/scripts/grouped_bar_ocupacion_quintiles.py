def generar_plot(df):
    import plotnine as p9

    ETIQUETAS = {
        'Directores/gerentes y profesionales/técnicos de nivel medio o alto': 'Directores/técnicos',
        'Trabajadores cualificados y oficiales/operarios de nivel bajo':       'Trabajadores cualificados',
        'Ocupaciones elementales':                                             'Ocupaciones elementales',
    }
    COLORES = {
        'Directores/técnicos':         '#1a9850',
        'Trabajadores cualificados':   '#4575b4',
        'Ocupaciones elementales':     '#d73027',
    }

    df = df.copy()
    df['quintil']  = df['quintil'].astype(str)
    df['ocupacion'] = df['ocupacion'].map(ETIQUETAS).fillna(df['ocupacion'])
    df['pct']      = df['pct'].astype(float)

    plot = (
        p9.ggplot(df, p9.aes(x='quintil', y='pct', fill='ocupacion'))
        + p9.geom_col(position='dodge')
        + p9.scale_fill_manual(values=COLORES, name='Ocupación')
        + p9.scale_y_continuous(labels=lambda lst: [f'{v:.0f}%' for v in lst])
        + p9.labs(
            title='¿Qué ocupación tienen los más ricos y los más pobres? Por quintil de renta (2023)',
            x='Quintil de renta',
            y='% de trabajadores'
        )
        + p9.theme_minimal()
        + p9.theme(axis_text_x=p9.element_text(angle=15, ha='right'))
    )
    return plot
