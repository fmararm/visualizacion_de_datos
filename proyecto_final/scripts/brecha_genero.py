def generar_plot(df):
    df = df.copy()
    df['categoria'] = df['categoria'].astype(str)
    df['pct_mujeres'] = df['pct_mujeres'].astype(float)

    plot = (
        p9.ggplot(df, p9.aes(x='categoria', y='pct_mujeres'))
        + p9.geom_line(p9.aes(group='municipio'), alpha=0.25, color='gray')
        + p9.geom_point(p9.aes(color='categoria'), size=2, alpha=0.7)
        + p9.geom_hline(yintercept=50, linetype='dashed', color='red', alpha=0.5)
        + p9.labs(
            title='Brecha de género por tipo de ocupación en municipios de Tenerife (2023)',
            x='Categoría',
            y='% Mujeres',
            color='Ocupación'
        )
        + p9.theme_minimal()
    )
    return plot