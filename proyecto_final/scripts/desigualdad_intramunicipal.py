def generar_plot(df):
    plot = (
        p9.ggplot(df, p9.aes(x='reorder(municipio, renta_neta)', y='renta_neta'))
        + p9.geom_boxplot(fill='#2166ac', alpha=0.7)
        + p9.labs(
            title='Un mismo municipio, mundos distintos: dispersión de renta por sección (2023)',
            x='',
            y='Renta neta por hogar (€)'
        )
        + p9.theme_minimal()
        + p9.theme(
            axis_text_x=p9.element_text(angle=45, ha='right'),
            legend_position='none'
        )
    )
    return plot