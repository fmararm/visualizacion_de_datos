# Principios Gestalt aplicados al proyecto

---

## 1. Figura-Fondo

El sistema visual separa automáticamente lo que percibe como objeto principal (figura) de lo que percibe como entorno (fondo). La figura tiene forma definida y parece estar delante; el fondo es amorfo y retrocede.

En visualización, se aplica eligiendo fondos neutros que no compiten con los datos y marcas visualmente destacadas que el ojo interpreta como el mensaje. En el lollipop, el segmento delgado es el fondo y el punto es la figura. En el mapa, el gris del cartoDB es el fondo y el gradiente de color de las secciones censales es la figura.

---

## 2. Punto Focal

Un elemento que rompe la uniformidad de su entorno atrae la atención de forma involuntaria. No es un principio Gestalt clásico, sino una derivación del contraste: el cerebro interpreta la discontinuidad como señal relevante.

En el lollipop, el punto al final del segmento es el único elemento circular en un gráfico de líneas, lo que lo convierte en el destino natural de la lectura. En los boxplots, la línea de mediana contrasta con la caja y actúa como referencia de comparación entre municipios.

---

## 3. Similitud

Los elementos que comparten propiedades visuales (color, forma, tamaño, orientación) se perciben como pertenecientes al mismo grupo, aunque no estén contiguos.

En el lollipop, el color rojo agrupa todos los municipios pobres y el azul todos los ricos sin necesidad de etiqueta explícita. En el heatmap, las celdas del mismo tono se perciben como municipios en situación económica equivalente. En los grouped bars, el gradiente de color del Q1 al Q5 permite seguir visualmente cada quintil a través de los distintos sectores.

---

## 4. Proximidad

Los elementos cercanos entre sí se perciben como un grupo, independientemente de si comparten otras propiedades visuales.

En el histograma, los bins adyacentes forman una silueta continua aunque cada barra sea un elemento independiente. En los boxplots, los outliers separados de los bigotes se perciben como casos excepcionales por su distancia al resto. En los grouped bars, las barras del mismo quintil están físicamente agrupadas, facilitando la comparación intragrupo antes que la intergrupal.

---

## 5. Continuidad

El ojo tiende a seguir la dirección más suave y continua, prolongando líneas y formas más allá de donde terminan, y agrupando los elementos que forman una trayectoria coherente.

En los waterfall charts, las barras encadenadas crean una línea de acumulación que el ojo sigue de izquierda a derecha como si fuera una sola figura en movimiento. En el heatmap, la secuencia de tres columnas cronológicas crea una línea de lectura natural que sigue la trayectoria temporal de cada municipio fila a fila.

---

## 6. Destino Común

Los elementos que se mueven o se orientan en la misma dirección se perciben como un grupo con un propósito compartido, aunque no estén próximos ni sean similares.

En el scatter, la nube de puntos orientada negativamente comunica que todos los datos convergen hacia el mismo patrón: a más ocupaciones elementales, menor renta. La dirección compartida es el mensaje. En el diverging bar, las barras que se extienden hacia el mismo lado pertenecen al mismo grupo de desequilibrio de género.

---

## 7. Magnitud

La diferencia de tamaño entre elementos se percibe directamente como diferencia de cantidad. Es uno de los canales de codificación más precisos para variables cuantitativas continuas.

En los grouped bars de quintiles, la altura de cada barra codifica el porcentaje de trabajadores en esa categoría. El ojo estima diferencias de altura de forma inmediata, lo que permite comparar los quintiles sin leer los valores numéricos.

---

## 8. Parte-Todo

Cuando varios elementos se perciben como componentes de un conjunto cerrado, el cerebro interpreta cada parte en relación al todo, no de forma absoluta.

En los waterfall charts, cada segmento coloreado es una fracción del 100% total. La anchura relativa comunica de inmediato el peso de cada categoría: el sector Servicios ocupa más del 85% de la barra de actividad sin necesidad de etiqueta numérica.

---

## 9. Simetría

El ojo espera simetría y la interpreta como equilibrio o normalidad. Cuando la simetría se rompe, la asimetría se percibe como una anomalía que exige explicación.

En el diverging bar de brecha de género, el diseño establece como referencia visual el eje de paridad (0%). Si hubiera igualdad perfecta, todas las barras se compensarían simétricamente. La simetría rota en cada categoría evidencia el desequilibrio sin necesidad de texto adicional.

---

## 10. Cambio Temporal (Similitud temporal)

Extensión del principio de similitud al eje del tiempo: elementos que cambian de forma similar a lo largo del tiempo se perciben como un grupo con comportamiento compartido.

En el heatmap, la organización en columnas cronológicas (2021, 2022, 2023) permite leer la evolución de cada municipio comparando el color de izquierda a derecha. Los municipios que mantienen el mismo tono a lo largo de las tres columnas se perciben como estables; los que cambian de color revelan recuperación o deterioro relativo.

---

## 11. Distribución

La caja del boxplot es un caso especial: codifica cinco estadísticos en un solo elemento gráfico (mínimo, Q1, mediana, Q3, máximo). El cerebro lo procesa como una forma unitaria que representa la distribución completa de los datos, no como cinco valores separados.

Este principio no figura en las listas clásicas de Gestalt pero es una aplicación directa del principio de cierre: el rectángulo cerrado de la caja se percibe como un objeto con significado propio, más que como la suma de sus líneas.
