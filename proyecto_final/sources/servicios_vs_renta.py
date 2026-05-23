def generar_plot(df):
    data = df.groupby('municipio', as_index=False).agg(
        pct_servicios=('pct_servicios', 'mean'),
        renta_neta=('renta_neta', 'mean'),
        total=('total', 'sum')
    )

    plot = (
        p9.ggplot(data, p9.aes(x='pct_servicios', y='renta_neta', size='total')) +
        p9.geom_point(color='#4575b4', alpha=0.35) +
        p9.scale_size_continuous(range=(1, 12), name='Trabajadores') +
        p9.labs(
            title='La paradoja del turismo: más servicios, ¿menos renta? (2023)',
            x='% trabajadores en Servicios',
            y='Renta neta por hogar (€)'
        ) +
        p9.theme_minimal()
    )
    return plot