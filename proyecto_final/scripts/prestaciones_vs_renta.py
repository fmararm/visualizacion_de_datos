def generar_plot(df):
    plot = (
        p9.ggplot(df, p9.aes(x='pct_desempleo', y='renta_neta'))
        + p9.geom_point(color='#4575b4', alpha=0.4, size=1.5)
        + p9.geom_smooth(method='lm', color='#d73027')
        + p9.labs(
            title='Donde hay más desempleo, hay menos renta: precariedad estructural (2023)',
            x='% ingresos por prestaciones de desempleo',
            y='Renta neta por hogar (€)'
        )
        + p9.theme_minimal()
    )
    return plot