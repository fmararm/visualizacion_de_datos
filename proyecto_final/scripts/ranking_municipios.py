def generar_plot(df):
    top15 = df.nlargest(15, 'renta_media').copy()
    top15['grupo'] = 'Top 15'
    bottom15 = df.nsmallest(15, 'renta_media').copy()
    bottom15['grupo'] = 'Bottom 15'
    data = pd.concat([top15, bottom15])
    data['municipio'] = data['municipio'].astype(str)
    data['grupo'] = pd.Categorical(data['grupo'], categories=['Top 15', 'Bottom 15'])
    data['renta_fmt'] = data['renta_media'].apply(lambda x: f'{x:,.0f}€')

    plot = (
        p9.ggplot(data, p9.aes(x='reorder(municipio, renta_media)', y='renta_media', fill='grupo'))
        + p9.geom_col()
        + p9.geom_text(p9.aes(label='renta_fmt'), ha='left', size=7, nudge_y=200)
        + p9.coord_flip()
        + p9.scale_fill_manual(values=['#2196F3', '#FF5722'])
        + p9.labs(
            title='Los municipios más ricos y más pobres de Tenerife (2023)',
            x='',
            y='Renta neta media por hogar (€)'
        )
        + p9.theme_minimal()
        + p9.theme(legend_position='none')
    )
    return plot