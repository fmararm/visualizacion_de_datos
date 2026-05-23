def generar_plot(df):
    df = df.copy()
    df['color_grupo'] = df['desviacion'].apply(lambda x: 'Mayoría mujeres' if x >= 0 else 'Mayoría hombres')

    plot = (
        p9.ggplot(df, p9.aes(x='desviacion', y='reorder(municipio, desviacion)', fill='color_grupo')) +
        p9.geom_col() +
        p9.geom_vline(xintercept=0, color='black', size=0.6) +
        p9.scale_fill_manual(values={'Mayoría mujeres': '#d73027', 'Mayoría hombres': '#4575b4'}) +
        p9.labs(
            title='¿Dónde trabajan más mujeres en empleos elementales? (2023)',
            x='Desviación respecto a paridad (puntos porcentuales)',
            y='',
            fill='Predominio'
        ) +
        p9.theme_minimal()
    )

    return plot