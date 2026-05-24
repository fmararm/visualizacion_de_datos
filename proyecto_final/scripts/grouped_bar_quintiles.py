def generar_plot(df):
    df['quintil'] = df['quintil'].astype(str)
    df['actividad'] = df['actividad'].astype(str)
    df['pct'] = df['pct'].astype(float)

    plot = (
        p9.ggplot(df, p9.aes(x='quintil', y='pct', fill='actividad'))
        + p9.geom_col(position='dodge')
        + p9.labs(
            title='¿En qué trabajan los más ricos y los más pobres? Actividad por quintil de renta (2023)',
            x='Quintil de renta',
            y='% de trabajadores',
            fill='Sector'
        )
        + p9.theme_minimal()
        + p9.theme(axis_text_x=p9.element_text(angle=15, ha='right'))
    )
    return plot