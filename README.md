## 1. Importación de módulos:

En esta sección, se importan los módulos necesarios para el funcionamiento del script, incluyendo los módulos de PySpark, "requests", "datetime", y otros.

## 2. Definición de variables y lista de provincias:

En esta parte, se define la API key de OpenWeatherMap y se crea una lista llamada "provincias" que contiene los nombres de las provincias de Argentina para las cuales se obtendrán los datos climáticos.

## 3. Clase ETL_Clima:

En este bloque, se define la clase "ETL_Clima" que realizará todo el proceso de ETL. Esta clase hereda de otra clase llamada "ETL_Spark".

## 4. Constructor y atributos de la clase:

En esta sección, se define el constructor de la clase "ETL_Clima" y se inicializa el atributo "process_date" con la fecha actual.

## 5. Método run():

El método "run()" es llamado desde la sección "main". Dentro de este método, se obtiene la fecha actual y se ejecuta el método "execute()" con la fecha de procesamiento.

## 6. Método extract():

El método "extract()" se encarga de extraer los datos climáticos de cada provincia mediante llamadas a la API de OpenWeatherMap. Crea un DataFrame en Spark con los datos obtenidos de cada provincia y muestra el esquema y los datos del DataFrame resultante.

## 7. Método transform():

El método transform() se encarga de realizar diversas transformaciones en el DataFrame obtenido del método extract() para limpiar y preparar los datos climáticos antes de cargarlos en la base de datos Redshift. A continuación, se describen las transformaciones en detalle:

### División de la columna 'coord':
Se divide la columna 'coord' en dos columnas separadas llamadas 'lon' y 'lat', que representan las coordenadas de longitud y latitud, respectivamente. La columna 'coord' se elimina del DataFrame final.

Mantener solo la columna 'description' de la columna 'weather':
Dentro de la columna 'weather', hay varias subcolumnas que contienen información sobre el clima, como 'description', 'icon', etc. En esta transformación, se extrae solo la columna 'description' que contiene la descripción del clima (por ejemplo, "cielo despejado", "lluvioso", etc.). El resto de la columna 'weather' se elimina del DataFrame final.

División de la columna 'main':
La columna 'main' contiene múltiples atributos relacionados con el clima, como la temperatura, la humedad, la presión, etc. En esta transformación, se divide la columna 'main' en varias columnas individuales para cada atributo relevante: 'temp', 'feels_like', 'temp_min', 'temp_max', 'pressure', y 'humidity'. Las subcolumnas de 'main' se eliminan del DataFrame final.

División de la columna 'wind':
La columna 'wind' contiene información sobre la velocidad del viento. En esta transformación, se divide la columna 'wind' en una nueva columna llamada 'wind_speed', que representa la velocidad del viento en la provincia. La columna 'wind' se elimina del DataFrame final.

Eliminación de columnas innecesarias:
En esta etapa, se eliminan algunas columnas innecesarias que no aportan información relevante para el análisis climático. Las columnas eliminadas son: 'clouds', 'sys', 'id', 'cod', 'base', y 'dt'.

Formato de datos y cálculos adicionales:
Agregar el símbolo porcentual a la columna 'humidity': Se concatena el símbolo de porcentaje '%' a la columna 'humidity' para indicar que representa un porcentaje (por ejemplo, "50%" para una humedad del 50%).

Agregar una columna nueva que sea la amplitud térmica: Se crea una nueva columna llamada 'amplitud_termica' que representa la diferencia entre la temperatura máxima ('temp_max') y la temperatura mínima ('temp_min') de la provincia.

Agregar una columna nueva que sea la temperatura promedio de la provincia: Se crea una nueva columna llamada 'temp_promedio' que representa la temperatura promedio entre la temperatura máxima y la temperatura mínima de la provincia.

7. Mostrar los datos transformados:

Una vez que se completan todas las transformaciones, el DataFrame resultante se muestra en la consola para que se pueda verificar cómo se han aplicado las modificaciones.

8. Retorno del DataFrame transformado:

Finalmente, el método devuelve el DataFrame transformado, que contiene los datos climáticos limpios y preparados para su carga en la base de datos Redshift.

Espero que esta descripción más detallada te ayude a comprender mejor las transformaciones que se realizan en el código. Si tienes alguna otra pregunta o necesitas más detalles sobre alguna parte específica de las transformaciones, no dudes en preguntar.
## 8. Método load():

El método "load()" se encarga de cargar los datos transformados en una tabla llamada "clima" dentro de la base de datos Redshift. Añade una columna adicional llamada "process_date" que indica la fecha de procesamiento. Utiliza las variables de entorno para establecer la conexión con Redshift y guardar los datos en la tabla.

## 9. Sección "main":

En esta sección, se crea una instancia de la clase "ETL_Clima" y se ejecuta el método "run()" para iniciar todo el proceso de ETL.

