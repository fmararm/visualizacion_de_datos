def generar_plot(df):
    df = df.copy()
    df['municipio'] = df['municipio'].astype(str)
    df['cambio'] = df['cambio'].astype(str)
    df['renta_2021'] = df['renta_2021'].astype(float)
    df['renta_2023'] = df['renta_2023'].astype(float)

    plot = (
        p9.ggplot(df)
        + p9.geom_segment(
            p9.aes(x=0, xend=1, y='renta_2021', yend='renta_2023', color='cambio'),
            size=0.7, alpha=0.6
        )
        + p9.geom_point(p9.aes(x=0, y='renta_2021', color='cambio'), size=2)
        + p9.geom_point(p9.aes(x=1, y='renta_2023', color='cambio'), size=2)
        + p9.scale_color_manual(values={'Sube': '#1a9850', 'Baja': '#d73027'})
        + p9.scale_x_continuous(breaks=[0, 1], labels=['2021', '2023'], limits=[-0.15, 1.15])
        + p9.labs(
            title='¿Quién mejoró y quién empeoró? Evolución de la renta por municipio (2021–2023)',
            x='',
            y='Renta neta media por hogar (€)',
            color='Tendencia'
        )
        + p9.theme_minimal()
    )

    return plot