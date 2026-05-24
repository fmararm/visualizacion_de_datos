def generar_plot(df):
    top15 = df.nlargest(15, 'renta_media').copy()
    top15['grupo'] = 'Top 15'
    bottom15 = df.nsmallest(15, 'renta_media').copy()
    bottom15['grupo'] = 'Bottom 15'
    df_plot = pd.concat([top15, bottom15], ignore_index=True)
    df_plot['renta_media'] = df_plot['renta_media'].astype(float)
    df_plot['municipio'] = df_plot['municipio'].astype(str)
    df_plot['grupo'] = df_plot['grupo'].astype(str)

    plot = (
        p9.ggplot(df_plot)
        + p9.geom_segment(
            p9.aes(
                x=0,
                xend='renta_media',
                y='reorder(municipio, renta_media)',
                yend='reorder(municipio, renta_media)'
            ),
            color='gray',
            size=0.6
        )
        + p9.geom_point(
            p9.aes(
                x='renta_media',
                y='reorder(municipio, renta_media)',
                color='grupo'
            ),
            size=3
        )
        + p9.labs(
            title='Los municipios más ricos y más pobres de Tenerife (2023)',
            x='Renta neta media por hogar (€)',
            y=''
        )
        + p9.theme_minimal()
        + p9.theme(legend_position='none')
    )
    return plot