def generar_plot(df):
    plot = (
        p9.ggplot(df, p9.aes(x='quintil', y='pct', fill='fuente'))
        + p9.geom_col(position='dodge')
        + p9.scale_fill_discrete(name='Fuente')
        + p9.labs(
            title='¿De qué viven los más ricos y los más pobres? Fuentes de ingreso por quintil (2023)',
            x='Quintil de renta',
            y='% medio de ingresos'
        )
        + p9.theme_minimal()
        + p9.theme(axis_text_x=p9.element_text(angle=15, ha='right'))
    )
    return plot