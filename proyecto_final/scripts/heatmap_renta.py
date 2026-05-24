def generar_plot(df):
    df = df.copy()
    df['año'] = df['año'].astype(str)

    plot = (
        p9.ggplot(df, p9.aes(x='año', y='municipio', fill='renta_media'))
        + p9.geom_tile(color='white', size=0.3)
        + p9.scale_fill_gradient(low='#d73027', high='#1a9850', name='Renta (€)')
        + p9.labs(
            title='Evolución de la renta por municipio: ¿se amplía la brecha? (2021–2023)',
            x='',
            y=''
        )
        + p9.theme_minimal()
        + p9.theme(axis_text_y=p9.element_text(size=7))
    )
    return plot