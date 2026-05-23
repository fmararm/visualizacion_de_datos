def generar_plot(df):
    plot = (
        p9.ggplot(df, p9.aes(x='pct_servicios', y='renta_neta'))
        + p9.geom_point(color='steelblue', alpha=0.4, size=1.5)
        + p9.geom_smooth(method='lm', color='tomato')
        + p9.labs(
            title='La paradoja del turismo: más trabajadores en Servicios, ¿menos renta? (2023)',
            x='% trabajadores en Servicios',
            y='Renta neta por hogar (€)'
        )
        + p9.theme_minimal()
    )
    return plot