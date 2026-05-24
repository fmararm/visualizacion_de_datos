def generar_plot(df):
    orden = df.sort_values('desviacion')['ocupacion'].tolist()
    df = df.copy()
    df['ocupacion'] = pd.Categorical(df['ocupacion'], categories=orden, ordered=True)

    plot = (
        p9.ggplot(df, p9.aes(x='ocupacion', y='desviacion', fill='mayoria'))
        + p9.geom_col(width=0.6)
        + p9.coord_flip()
        + p9.geom_hline(yintercept=0, color='black', size=0.7)
        + p9.scale_fill_manual(values={'Más mujeres': '#d73027', 'Más hombres': '#4575b4'})
        + p9.scale_y_continuous(labels=lambda lst: [f'{v:+.1f}%' for v in lst])
        + p9.labs(
            title='Brecha de género por categoría de ocupación en Tenerife (2023)',
            y='Desviación respecto a la paridad (% mujeres − 50%)',
            x='',
            fill=''
        )
        + p9.theme_minimal()
        + p9.theme(figure_size=(10, 5), axis_text_y=p9.element_text(size=8))
    )
    return plot