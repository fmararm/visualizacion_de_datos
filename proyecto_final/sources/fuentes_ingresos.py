def generar_plot(df):
    df = df.copy()
    df['fuente_label'] = df['pct'].apply(lambda x: f'{x:.1f}%')

    plot = (
        p9.ggplot(df)
        + p9.geom_rect(p9.aes(xmin='idx - 0.4', xmax='idx + 0.4', ymin='inicio', ymax='fin', fill='fuente'))
        + p9.geom_segment(p9.aes(x='idx + 0.4', xend='idx + 0.6', y='fin', yend='fin'),
                          color='gray', linetype='dashed', size=0.5)
        + p9.geom_text(p9.aes(x='idx', y='(inicio+fin)/2', label='fuente_label'), size=8)
        + p9.scale_x_continuous(breaks=df['idx'].tolist(), labels=df['fuente'].tolist())
        + p9.labs(
            title='De dónde viene la renta: composición de ingresos por sección en Tenerife (2023)',
            x='',
            y='% acumulado del total de ingresos'
        )
        + p9.theme_minimal()
        + p9.theme(
            axis_text_x=p9.element_text(angle=20, ha='right'),
            legend_position='none'
        )
    )
    return plot