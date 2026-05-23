def generar_plot(df):
    df = df.copy()
    df['municipio'] = df['municipio'].astype(str)
    df['sexo'] = df['sexo'].astype(str)
    df['num_casos_dir'] = df['num_casos_dir'].astype(int)
    df['num_casos'] = df['num_casos'].astype(int)

    plot = (
        p9.ggplot(df, p9.aes(x='num_casos_dir', y='reorder(municipio, num_casos)', fill='sexo'))
        + p9.geom_col(width=0.7, position='identity')
        + p9.geom_vline(xintercept=0, color='black', size=0.5)
        + p9.scale_x_continuous(labels=lambda lst: [f'{abs(int(v)):,}' for v in lst])
        + p9.scale_fill_manual(values={'Hombres': '#4575b4', 'Mujeres': '#d73027'})
        + p9.labs(
            title='Distribución de trabajadores por sexo y municipio en Tenerife (2023)',
            x='Número de trabajadores',
            y='',
            fill='Sexo'
        )
        + p9.theme_minimal()
    )
    return plot