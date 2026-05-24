def generar_plot(df):
    BARRA_COLS = ['#2196F3', '#FF9800', '#4CAF50', '#F44336']
    TEXTO_COLS = ['#FFF9C4', '#1A237E', '#FCE4EC', '#E0F7FA']

    df = df.copy()
    df['bar_color'] = df['idx'].apply(lambda i: BARRA_COLS[i % len(BARRA_COLS)])
    df['text_color'] = df['idx'].apply(lambda i: TEXTO_COLS[i % len(TEXTO_COLS)])
    df['label'] = df['pct'].apply(lambda v: f'{v:.1f}%')
    df['y_mid'] = (df['inicio'] + df['fin']) / 2

    df_seg    = df[df['idx'] < df['idx'].max()].copy()
    df_grande = df[df['pct'] >= 5].copy()
    df_pequeño = df[df['pct'] < 5].copy()

    plot = (
        p9.ggplot(df)
        + p9.geom_rect(
            p9.aes(xmin='idx - 0.4', xmax='idx + 0.4', ymin='inicio', ymax='fin', fill='bar_color')
        )
        + p9.geom_segment(
            p9.aes(x='idx+0.4', xend='idx+0.6', y='fin', yend='fin'),
            data=df_seg,
            color='gray',
            linetype='dashed'
        )
        + p9.geom_text(
            p9.aes(x='idx', y='y_mid', label='label', color='text_color'),
            data=df_grande, size=11, fontweight='bold'
        )
        + p9.geom_text(
            p9.aes(x='idx', y='y_mid', label='label', color='text_color'),
            data=df_pequeño, size=7, fontweight='bold'
        )
        + p9.geom_text(
            p9.aes(x='idx', y=-8, label='sector', color='bar_color'),
            angle=20,
            ha='right',
            va='top',
            size=9
        )
        + p9.scale_fill_identity()
        + p9.scale_color_identity()
        + p9.scale_x_continuous(breaks=[], labels=[])
        + p9.scale_y_continuous(breaks=[0, 25, 50, 75, 100])
        + p9.expand_limits(y=-22)
        + p9.labs(
            title='Distribución de trabajadores por sector económico en Tenerife (2023)',
            x='',
            y='% acumulado de trabajadores'
        )
        + p9.theme_minimal()
        + p9.theme(
            legend_position='none',
            axis_text_x=p9.element_blank(),
            axis_ticks_x=p9.element_blank()
        )
    )
    return plot