def generar_plot(df):
    df['quintil'] = df['quintil'].astype(str)
    df['ocupacion'] = df['ocupacion'].astype(str)
    df['pct'] = df['pct'].astype(float)

    plot = (
        p9.ggplot(df, p9.aes(x='quintil', y='pct', fill='ocupacion'))
        + p9.geom_col(position='dodge')
        + p9.scale_fill_brewer(type='qual', palette='Set2')
        + p9.labs(
            title='¿Qué ocupación tienen los más ricos y los más pobres? Por quintil de renta (2023)',
            x='Quintil de renta',
            y='% de trabajadores',
            fill='Ocupación'
        )
        + p9.theme_minimal()
        + p9.theme(axis_text_x=p9.element_text(angle=15, ha='right'))
    )
    return plot