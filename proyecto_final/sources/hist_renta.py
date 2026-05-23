def generar_plot(df):
    import numpy as np

    mediana = df['renta_neta'].median()

    counts, bin_edges = np.histogram(df['renta_neta'], bins=40)
    y_max = counts.max()
    y_annot = y_max * 0.90

    plot = (
        p9.ggplot(df, p9.aes(x='renta_neta'))
        + p9.geom_histogram(bins=40, fill='#2166ac', color='white', alpha=0.85)
        + p9.geom_vline(xintercept=mediana, color='#d73027', linetype='dashed', size=0.9)
        + p9.annotate('text', x=mediana, y=y_annot, label=f'Mediana: {mediana:,.0f} €', color='#d73027', ha='left')
        + p9.labs(
            title='Distribución de la renta por sección censal en Tenerife (2023)',
            x='Renta neta media por hogar (€)',
            y='Número de secciones'
        )
        + p9.theme_minimal()
    )
    return plot