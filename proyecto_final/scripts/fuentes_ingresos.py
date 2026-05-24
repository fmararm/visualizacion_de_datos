def generar_plot(df):
    df = df.copy()
    df['fuente_label'] = df['pct'].apply(lambda x: f'{x:.1f}%')

    BARRA_COLS = ['#4E79A7', '#F28E2B', '#59A14F', '#E15759', '#B07AA1']
    TEXTO_COLS = ['#FFE66D', '#1B2A4A', '#FFD6E7', '#E0FFFF', '#CCFFCC']

    fuentes = df.sort_values('idx')['fuente'].tolist()
    barra_map = {f: BARRA_COLS[i] for i, f in enumerate(fuentes)}
    texto_map = {f: TEXTO_COLS[i] for i, f in enumerate(fuentes)}

    df['bar_color']  = df['fuente'].map(barra_map)
    df['text_color'] = df['fuente'].map(texto_map)

    df_grande  = df[df['pct'] >= 5].copy()
    df_pequeño = df[df['pct'] < 5].copy()

    plot = (
        p9.ggplot(df)
        + p9.geom_rect(p9.aes(xmin='idx - 0.4', xmax='idx + 0.4',
                               ymin='inicio', ymax='fin', fill='bar_color'))
        + p9.scale_fill_identity()
        + p9.geom_segment(p9.aes(x='idx + 0.4', xend='idx + 0.6', y='fin', yend='fin'),
                          color='gray', linetype='dashed', size=0.5)
        + p9.geom_text(p9.aes(x='idx', y='(inicio + fin) / 2', label='fuente_label',
                               color='text_color'),
                       data=df_grande, size=11, fontweight='bold')
        + p9.geom_text(p9.aes(x='idx', y='(inicio + fin) / 2', label='fuente_label',
                               color='text_color'),
                       data=df_pequeño, size=7, fontweight='bold')
        + p9.scale_color_identity()
        + p9.geom_text(p9.aes(x='idx', y=-8, label='fuente', color='bar_color'),
                       angle=20, ha='right', va='top', size=9)
        + p9.expand_limits(y=-22)
        + p9.scale_x_continuous(breaks=[], labels=[])
        + p9.labs(
            title='Composición de ingresos por sección en Tenerife (2023)',
            x='',
            y='% acumulado del total de ingresos'
        )
        + p9.theme_minimal()
        + p9.theme(
            axis_text_x=p9.element_blank(),
            axis_ticks_x=p9.element_blank(),
            legend_position='none',
        )
    )
    return plot
