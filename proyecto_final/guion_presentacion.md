# Guión de presentación: "Dos Tenerifes"

---

## Introducción

Este dashboard analiza la desigualdad económica en el archipiélago canario a partir de datos censales de renta, ocupación y género de 2021 a 2023. Cada sección responde a una pregunta concreta sobre la fractura social que los datos evidencian.

---

## 1. Lollipop — Ranking de municipios por renta

**Qué muestra:** Los 15 municipios con mayor y menor renta neta media de Tenerife (2023).

**Gramática de gráficos:**
- *Marcas*: segmento (`geom_segment`) más punto (`geom_point`).
- *Mapeos estéticos*: `x` = renta media (cuantitativa continua), `y` = municipio reordenado por valor (nominal con orden inducido por los datos), `color` = grupo (Top 15 / Bottom 15) mapeado a escala discreta manual (azul/rojo).
- *Estadística*: identidad, sin transformación; los valores se leen directamente.
- *Coordenadas*: cartesianas con eje y categórico.

**Principios Gestalt:**
- *Figura-Fondo*: la línea delgada actúa como fondo que guía la mirada; el punto es la figura que concentra la lectura del valor preciso.
- *Punto focal*: el extremo del punto rompe la uniformidad del segmento y dirige automáticamente la atención al número concreto.
- *Similitud*: el color agrupa sin necesidad de leyenda. El rojo agrupa los municipios pobres, el azul los ricos. La lectura es inmediata.

---

## 2. Histograma — Distribución de la renta

**Qué muestra:** Distribución de la renta neta media por hogar en todas las secciones censales de la provincia (2023).

**Gramática de gráficos:**
- *Marcas*: barras (`geom_histogram`).
- *Mapeos estéticos*: `x` = renta (continua), altura de barra = frecuencia (transformación estadística `stat_bin`).
- *Estadística*: binning; el rango continuo se discretiza en intervalos iguales para revelar la forma de la distribución.
- *Escalas*: eje x lineal en euros, eje y de recuento.

**Principios Gestalt:**
- *Proximidad*: los bins adyacentes que representan rangos contiguos se perciben como una forma continua, no como barras aisladas. El ojo lee la campana asimétrica como una unidad.
- *Figura-Fondo*: el fondo neutro realza la silueta de la distribución, poniendo en primer plano la cola derecha (las secciones más ricas) que de otra forma pasaría inadvertida.

---

## 3. Heatmap — Recuperación post-COVID por isla

**Qué muestra:** Evolución de la renta media por municipio de 2021 a 2023, con una pestaña por isla.

**Gramática de gráficos:**
- *Marcas*: teselas (`geom_tile`).
- *Mapeos estéticos*: `x` = año (ordinal discreta con tres valores), `y` = municipio (nominal, ordenado por renta en 2023), `fill` = renta media (cuantitativa continua).
- *Escala de color*: gradiente divergente rojo-verde (`scale_fill_gradient`), donde el rojo codifica renta baja y el verde renta alta.
- *Estadística*: identidad; cada celda es un valor observado.

**Principios Gestalt:**
- *Similitud*: celdas del mismo tono se perciben como municipios en situación económica equivalente, independientemente de su posición en el eje y.
- *Cambio temporal*: la organización en columnas cronológicas hace que el ojo compare cada fila de izquierda a derecha, leyendo la evolución de cada municipio sin cálculo explícito.
- *Continuidad*: la secuencia de tres columnas crea una línea de lectura natural que sigue la trayectoria temporal de cada municipio.

---

## 4. Boxplots — Desigualdad intramunicipal

**Qué muestra:** Dispersión de renta entre secciones censales de cada municipio, por isla.

**Gramática de gráficos:**
- *Marcas*: cajas con bigotes (`geom_boxplot`) y puntos para outliers (`geom_jitter`).
- *Mapeos estéticos*: `x` = municipio (nominal), `y` = renta de cada sección censal (cuantitativa continua).
- *Estadística*: resumen de cinco números (mínimo, Q1, mediana, Q3, máximo) más identificación de outliers.

**Principios Gestalt:**
- *Distribución*: la caja codifica simultáneamente la mediana, el rango intercuartílico y los extremos. Cuatro estadísticos en un solo elemento gráfico.
- *Punto focal*: la línea de la mediana dentro de la caja actúa como punto de referencia central para la comparación entre municipios.
- *Proximidad*: los outliers representados como puntos aislados fuera de los bigotes se perciben como casos excepcionales, separados visualmente de la distribución principal.

---

## 5. Scatter — Ocupaciones elementales y renta

**Qué muestra:** Correlación entre el porcentaje de trabajadores en ocupaciones elementales y la renta media de cada sección censal.

**Gramática de gráficos:**
- *Marcas*: puntos (`geom_point`) más línea de tendencia (`geom_smooth`).
- *Mapeos estéticos*: `x` = porcentaje de ocupaciones elementales (continua), `y` = renta media (continua), `size` = número total de trabajadores en la sección (tercera variable codificada en área).
- *Estadística*: el modelo de tendencia es una transformación estadística aplicada sobre los datos brutos.

**Principios Gestalt:**
- *Destino común*: los puntos que forman la nube orientada negativamente señalan un destino compartido: a más ocupaciones elementales, menor renta. El patrón se percibe como dirección colectiva.
- *Figura-Fondo*: la línea de tendencia actúa como figura que sintetiza el patrón subyacente; los puntos individuales son el fondo que da soporte empírico a esa figura.

---

## 6. Waterfall — Fuentes de ingresos y actividad económica

**Qué muestra (izquierda):** Composición porcentual media de los ingresos de los hogares tinerfeños.
**Qué muestra (derecha):** Distribución de trabajadores por sector CNAE.

**Gramática de gráficos:**
- *Marcas*: barras apiladas (`geom_col`, posición apilada).
- *Mapeos estéticos*: `fill` = fuente de ingreso o sector (nominal con escala discreta de color), longitud de segmento = porcentaje (cuantitativa).
- *Estadística*: parte de un todo; cada segmento es una fracción del 100%.

**Principios Gestalt:**
- *Continuidad*: las barras encadenadas forman una línea de acumulación continua. El ojo lee la estructura de composición de izquierda a derecha sin saltos.
- *Parte-Todo*: cada segmento se percibe como componente de un conjunto cerrado. La anchura relativa comunica de inmediato la dominancia de cada categoría (en actividad, el sector Servicios ocupa más del 85%).

---

## 7. Grouped bars — Qué trabajan el Q1 y el Q5

**Qué muestra:** Distribución del sector de actividad y categoría de ocupación según quintil de renta.

**Gramática de gráficos:**
- *Marcas*: barras agrupadas (`geom_col`, posición `dodge`).
- *Mapeos estéticos*: `x` = sector o categoría (nominal), `y` = porcentaje (continua), `fill` = quintil de renta (ordinal con gradiente de color de Q1 a Q5).
- *Estadística*: identidad sobre proporciones calculadas en la transformación de datos.

**Principios Gestalt:**
- *Magnitud*: la altura de las barras codifica directamente la comparación cuantitativa entre quintiles. El ojo estima diferencias de altura de forma inmediata y precisa.
- *Proximidad*: las barras del mismo quintil están físicamente agrupadas, facilitando la comparación intragrupo antes que la intergrupal.
- *Similitud*: el gradiente de color del Q1 al Q5 refuerza el orden y permite rastrear visualmente la evolución de cada categoría de un quintil al siguiente.

---

## 8. Diverging bar — Brecha de género por ocupación

**Qué muestra:** Desviación porcentual respecto a la paridad (50/50) por categoría de ocupación.

**Gramática de gráficos:**
- *Marcas*: barras horizontales divergentes (`geom_col`) centradas en el eje de paridad (0%).
- *Mapeos estéticos*: `x` = desviación respecto a 50% (positiva = más mujeres, negativa = más hombres), `y` = categoría de ocupación (nominal, ordenada por desviación).
- *Escala*: eje x simétrico centrado en cero; la referencia visual es la línea de paridad, no el cero absoluto.

**Principios Gestalt:**
- *Destino común*: las barras que se extienden hacia el mismo lado pertenecen al mismo grupo de desequilibrio (dominancia masculina o femenina), percibidas sin necesidad de color adicional.
- *Simetría*: el diseño divergente explota la simetría como expectativa visual: donde hay paridad, las barras se compensan. Donde la simetría está rota, el desequilibrio se percibe de forma inmediata e impactante.

---

## 9. Mapa interactivo de secciones censales

**Qué muestra:** Tres capas temáticas a nivel de sección censal: renta neta media, porcentaje de ingresos por desempleo y porcentaje de trabajadores en Servicios.

**Gramática de gráficos:**
- *Marcas*: polígonos geoespaciales (coropletas).
- *Mapeos estéticos*: `fill` = valor de cada capa temática (continua), `geometry` = fronteras de la sección censal.
- *Escala*: colormap lineal por capa (`LinearColormap` de Branca), con dominio ajustado al rango observado de cada variable.
- *Interactividad*: control de capas (LayerControl de Leaflet), tooltips con valores exactos al pasar el cursor.

**Principios Gestalt:**
- *Figura-Fondo*: el gradiente de color sobre el fondo neutro gris del mapa base enfoca la atención en las diferencias espaciales, no en las fronteras administrativas.
- *Punto focal*: las secciones con valores extremos son visualmente dominantes y dirigen la lectura hacia las zonas de mayor interés analítico.
- *Proximidad*: las secciones con valores similares y contiguas en el mapa se perciben como zonas cohesionadas, revelando clustering espacial que los gráficos de barras no pueden mostrar.

---

## Cierre

La gramática de gráficos garantiza que cada canal visual codifica exactamente una variable. Los principios Gestalt garantizan que el lector la decodifica sin esfuerzo consciente. El resultado es un dashboard donde la estructura visual y la narrativa económica son inseparables.
