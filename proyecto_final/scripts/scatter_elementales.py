def generar_plot(df):
    df = df.copy()
    df['total'] = df['total'].astype(float)
    df['pct_elementales'] = df['pct_elementales'].astype(float)
    df['renta_neta'] = df['renta_neta'].astype(float)

    plot = (
        p9.ggplot(df)
        + p9.geom_point(
            p9.aes(x='pct_elementales', y='renta_neta', size='total'),
            color='#4575b4', alpha=0.35
        )
        + p9.geom_smooth(
            p9.aes(x='pct_elementales', y='renta_neta'),
            method='lm', color='#d73027', size=1
        )
        + p9.scale_size_continuous(range=(1, 10), name='Trabajadores')
        + p9.labs(
            title='A más trabajo elemental, menos renta: correlación por sección censal (2023)',
            x='% trabajadores en ocupaciones elementales',
            y='Renta neta media por hogar (€)'
        )
        + p9.theme_minimal()
    )
    return plot