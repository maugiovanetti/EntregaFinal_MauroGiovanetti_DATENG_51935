##1. Importación de módulos:

En esta sección, se importan los módulos necesarios para el funcionamiento del script, incluyendo los módulos de PySpark, "requests", "datetime", y otros.

##2. Definición de variables y lista de provincias:

En esta parte, se define la API key de OpenWeatherMap y se crea una lista llamada "provincias" que contiene los nombres de las provincias de Argentina para las cuales se obtendrán los datos climáticos.

##3. Clase ETL_Clima:

En este bloque, se define la clase "ETL_Clima" que realizará todo el proceso de ETL. Esta clase hereda de otra clase llamada "ETL_Spark".

##4. Constructor y atributos de la clase:

En esta sección, se define el constructor de la clase "ETL_Clima" y se inicializa el atributo "process_date" con la fecha actual.

##5. Método run():

El método "run()" es llamado desde la sección "main". Dentro de este método, se obtiene la fecha actual y se ejecuta el método "execute()" con la fecha de procesamiento.

##6. Método extract():

El método "extract()" se encarga de extraer los datos climáticos de cada provincia mediante llamadas a la API de OpenWeatherMap. Crea un DataFrame en Spark con los datos obtenidos de cada provincia y muestra el esquema y los datos del DataFrame resultante.

##7. Método transform():

El método "transform()" realiza diversas transformaciones en el DataFrame obtenido del método "extract()". Estas transformaciones incluyen dividir las columnas, mantener solo ciertas columnas, eliminar columnas innecesarias, agregar símbolos a las columnas, y agregar nuevas columnas con cálculos derivados.

##8. Método load():

El método "load()" se encarga de cargar los datos transformados en una tabla llamada "clima" dentro de la base de datos Redshift. Añade una columna adicional llamada "process_date" que indica la fecha de procesamiento. Utiliza las variables de entorno para establecer la conexión con Redshift y guardar los datos en la tabla.

##9. Sección "main":

En esta sección, se crea una instancia de la clase "ETL_Clima" y se ejecuta el método "run()" para iniciar todo el proceso de ETL.

