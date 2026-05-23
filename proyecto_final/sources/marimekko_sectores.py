def generar_plot(df):
    df = df.copy()
    df['pct_label'] = df['pct'].apply(lambda x: f'{x*100:.0f}%')

    plot = (
        p9.ggplot(df)
        + p9.geom_rect(
            p9.aes(xmin='xmin', xmax='xmax', ymin='ymin', ymax='ymax', fill='sector'),
            color='white', size=0.5
        )
        + p9.geom_text(
            data=df.drop_duplicates('grupo_renta'),
            mapping=p9.aes(x='xcenter', y=-0.05, label='grupo_renta'),
            size=8, va='top'
        )
        + p9.geom_text(
            data=df[df['pct'] > 0.07],
            mapping=p9.aes(x='xcenter', y='ycenter', label='pct_label'),
            size=7, color='white'
        )
        + p9.scale_fill_brewer(type='qual', palette='Set2')
        + p9.scale_y_continuous(labels=lambda l: [f'{v*100:.0f}%' for v in l])
        + p9.scale_x_continuous(expand=(0, 0))
        + p9.labs(
            title='Composición del empleo por sector según nivel de renta (2023)',
            x='Grupo de renta (ancho proporcional a trabajadores)',
            y='% de trabajadores por sector',
            fill='Sector'
        )
        + p9.theme_minimal()
        + p9.theme(
            axis_text_x=p9.element_blank(),
            axis_ticks_x=p9.element_blank()
        )
    )
    return plot