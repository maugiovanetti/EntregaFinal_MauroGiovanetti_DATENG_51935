# Entrega Final DATENG 51935

## Comentarios de Configuración Inicial:

* El entorno del DAG esta confeccionado sobre el Docker-Compose del entregable 3. En este, se incorporó un archivo de configuración, que debería estar ubicado a la misma altura que el ``docker-compose.yml`` 
  En este archivo se debe realizar la configuración  ``[smtp] `` 
  Linea agregada al Docker del Entregable 3 es la siguiente:

```python

    ./airflow.cfg:/opt/airflow/airflow.cfg
```

* En la carpeta dags, se debería incluir un mail en las configuraciones del DAG.
  
```python
  with DAG(
    dag_id="etl_clima",
    default_args={
        "owner": "Mauro Giovanetti",
        "start_date": datetime(2023, 7, 1),
        "retries": 0,
        "retry_delay": timedelta(seconds=5),
        'catchup': False,
        'email': [''], #Completar Mail
        'email_on_failure': True,
        'email_on_retry': True,
    },
    description="ETL de la tabla clima",
    schedule_interval="@daily",
    catchup=False,
) as dag:

```

* En la carpeta scripts, en `` ETL_Clima.py ``. Se debe agregar la contraseña recibida en la entrega.
 
  
```python

# Configurar la API key de OpenWeatherMap

api_key = ""

```



# Descripción del proceso ETL_Clima

# 1. Importación de módulos:


En esta sección, se importan los módulos necesarios para el funcionamiento del script, incluyendo los módulos de PySpark, "requests", "datetime", y otros.

```python


import requests
from datetime import datetime, timedelta
from os import environ as env

from pyspark.sql.functions import concat, col, lit, when, expr, to_date, round
from pyspark.sql.types import StringType

from commons import ETL_Spark

```

# 2. Definición de variables y lista de provincias:

En esta parte, se define la API key de OpenWeatherMap y se crea una lista llamada "provincias" que contiene los nombres de las provincias de Argentina para las cuales se obtendrán los datos climáticos.

```python

# Configurar la API key de OpenWeatherMap
api_key = #API_Key

# Definir listado de provincias de Argentina

provincias = [
    "Buenos Aires",
    "Cordoba",
    "Santa Fe",
    "Mendoza",
    "Tucuman",
    "Parana",
    "Salta",
    "Resistencia",
    "Corrientes",
    "Misiones",
    "Santiago del Estero",
    "San Juan",
    "San Salvador de Jujuy",
    "Viedma",
    "Formosa",
    "Neuquen",
    "Rawson",
    "San Luis",
    "Catamarca",
    "La Rioja, AR",
    "Santa Rosa, AR",
    "Río Gallegos",
    "Ushuaia",
]

```

# 3. Clase ETL_Clima:

En este bloque, se define la clase "ETL_Clima" que realizará todo el proceso de ETL. Esta clase hereda de otra clase llamada "ETL_Spark".


```python

class ETL_Clima(ETL_Spark):
```

# 4. Constructor y atributos de la clase:

En esta sección, se define el constructor de la clase "ETL_Clima" y se inicializa el atributo "process_date" con la fecha actual.


```python

def __init__(self, job_name=None):
        super().__init__(job_name)
        self.process_date = datetime.now().strftime("%Y-%m-%d")

```


# 5. Método run():

El método "run()" es llamado desde la sección "main". Dentro de este método, se obtiene la fecha actual y se ejecuta el método "execute()" con la fecha de procesamiento.


```python

def run(self):
        process_date = datetime.now().strftime("%Y-%m-%d")
        self.execute(process_date)

```

# 6. Método extract():

El método "extract()" se encarga de extraer los datos climáticos de cada provincia mediante llamadas a la API de OpenWeatherMap. Crea un DataFrame en Spark con los datos obtenidos de cada provincia y muestra el esquema y los datos del DataFrame resultante.


```python

    def extract(self):
        """
        # Extrae datos de la API
        #"""
        print(">>> [E] Extrayendo datos de la API...")

        # Realiza una consulta para establecer esquema

        url_esquema = (
            f"https://api.openweathermap.org/data/2.5/weather?q={provincias[0]}"
            f"&appid={api_key}&units=metric"
        )

        respuesta_esquema = requests.get(url_esquema)
        ejemplo_esquema = respuesta_esquema.json()

        # Obtener el esquema a partir de los datos de la primera provincia

        esquema = self.spark.read.json(
            self.spark.sparkContext.parallelize([ejemplo_esquema]), multiLine=True
        ).schema

        # Crear el DataFrame "df" vacío con el esquema obtenido
        df = self.spark.createDataFrame([], esquema)

        # Leer los datos para cada provincia y agregarlos al DataFrame
        for provincia in provincias:
            url = (
                f"https://api.openweathermap.org/data/2.5/weather?q={provincia}"
                f"&appid={api_key}&units=metric"
            )
            respuesta = requests.get(url)
            data = respuesta.json()

            # Crear un DataFrame con los datos de la provincia actual

            df_prov = self.spark.read.json(
                self.spark.sparkContext.parallelize([data]),
                multiLine=True,
                schema=df.schema,
            )
            # Agregar los datos de la provincia actual al DataFrame final

            df = df.union(df_prov)

        # Mostrar el resultado

        df.printSchema()
        df.show()

        return df


```

# 7. Método transform():

El método transform() se encarga de realizar diversas transformaciones en el DataFrame obtenido del método extract() para limpiar y preparar los datos                 climáticos antes de cargarlos en la base de datos Redshift. A continuación, se describen las transformaciones en detalle:
        
#### A. División de la columna 'coord':
Se divide la columna 'coord' en dos columnas separadas llamadas 'lon' y 'lat', que representan las coordenadas de longitud y latitud, respectivamente. La              columna 'coord' se elimina del DataFrame final.
        
#### B. Mantener solo la columna 'description' de la columna 'weather':
Dentro de la columna 'weather', hay varias subcolumnas que contienen información sobre el clima, como 'description', 'icon', etc. En esta                              transformación, se extrae solo la columna 'description' que contiene la descripción del clima (por ejemplo, "cielo despejado", "lluvioso", etc.). El                   resto de la columna 'weather' se elimina del DataFrame final.
        
#### C. División de la columna 'main':
La columna 'main' contiene múltiples atributos relacionados con el clima, como la temperatura, la humedad, la presión, etc. En esta transformación, se                 divide la columna 'main' en varias columnas individuales para cada atributo relevante: 'temp', 'feels_like', 'temp_min', 'temp_max', 'pressure', y                     'humidity'. Las subcolumnas de 'main' se eliminan del DataFrame final.
        
#### D. División de la columna 'wind':
La columna 'wind' contiene información sobre la velocidad del viento. En esta transformación, se divide la columna 'wind' en una nueva columna llamada                 'wind_speed', que representa la velocidad del viento en la provincia. La columna 'wind' se elimina del DataFrame final.
        
#### E. Eliminación de columnas innecesarias:
En esta etapa, se eliminan algunas columnas innecesarias que no aportan información relevante para el análisis climático. Las columnas eliminadas son:                 'clouds', 'sys', 'id', 'cod', 'base', y 'dt'.
        
#### F. Formato de datos y cálculos adicionales:
1. Agregar el símbolo porcentual a la columna 'humidity': Se concatena el símbolo de porcentaje '%' a la columna 'humidity' para indicar que                              representa un porcentaje (por ejemplo, "50%" para una humedad del 50%).
        
2. Agregar una columna nueva que sea la amplitud térmica: Se crea una nueva columna llamada 'amplitud_termica' que representa la diferencia                               entre la temperatura máxima ('temp_max') y la temperatura mínima ('temp_min') de la provincia.
        
3. Agregar una columna nueva que sea la temperatura promedio de la provincia: Se crea una nueva columna llamada 'temp_promedio' que representa                            la temperatura promedio entre la temperatura máxima y la temperatura mínima de la provincia.
        
#### G. Mostrar los datos transformados:
Una vez que se completan todas las transformaciones, el DataFrame resultante se muestra en la consola para que se pueda verificar cómo se han                         aplicado las modificaciones.
        
#### H.  Retorno del DataFrame transformado:
Finalmente, el método devuelve el DataFrame transformado, que contiene los datos climáticos limpios y preparados para su carga en la base de datos                     Redshift.
        

```python

def transform(self, df_original):
        """
        Transforma los datos
        """
        print(">>> [T] Transformando datos...")

            
        # 1. Dividir la columna 'coord' en 'lon' y 'lat'
        df = df_original.withColumn("lon", col("coord.lon")) \
                        .withColumn("lat", col("coord.lat")) \
                        .drop("coord")

        # 2. Mantener solo la columna 'description' de la columna 'weather'
        df = df.withColumn("weather_description", col("weather.description").cast("string")) \
                                     .drop("weather")

        # 3. Dividir la columna 'main' en nuevas columnas y eliminar la columna original
        df = df.withColumn("temp", col("main.temp")) \
                                     .withColumn("feels_like", col("main.feels_like")) \
                                     .withColumn("temp_min", col("main.temp_min")) \
                                     .withColumn("temp_max", col("main.temp_max")) \
                                     .withColumn("pressure", col("main.pressure")) \
                                     .withColumn("humidity", col("main.humidity")) \
                                     .drop("main")

        # 4. Dividir la columna 'wind' en nuevas columnas y eliminar la columna original
        df = df.withColumn("wind_speed", col("wind.speed")) \
                                     .drop("wind")

        # 5. Eliminar la columna 'clouds', 'sys', 'id' y 'cod'
        df = df.drop("clouds", "sys", "id", "cod", "base", "dt")



                # Agregar el símbolo porcentual a la columna "humedad"
        df = df .withColumn("humidity", concat(col("humidity"), lit("%")))

                # Agregar una columna nueva que sea la amplitud térmica
        df = df .withColumn("amplitud_termica", round(col("temp_max") - col("temp_min"), 2))

                # Agregar una columna nueva que sea la temperatura promedio de la provincia
        df = df .withColumn("temp_promedio", round((col("temp_min") + col("temp_max")) / 2, 2))
        df.show()

        return df

```
# 8. Método load():

El método "load()" se encarga de cargar los datos transformados en una tabla llamada "clima" dentro de la base de datos Redshift. Añade una columna adicional llamada "process_date" que indica la fecha de procesamiento. Utiliza las variables de entorno para establecer la conexión con Redshift y guardar los datos en la tabla.


```python

 def load(self, df_final):
        """
        Carga los datos transformados en Redshift
        """
        print(">>> [L] Cargando datos en Redshift...")

        # add process_date column
        df_final = df_final.withColumn("process_date", lit(self.process_date))

        df_final.write \
            .format("jdbc") \
            .option("url", env['REDSHIFT_URL']) \
            .option("dbtable", f"{env['REDSHIFT_SCHEMA']}.clima") \
            .option("user", env['REDSHIFT_USER']) \
            .option("password", env['REDSHIFT_PASSWORD']) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        
        print(">>> [L] Datos cargados exitosamente")


```

# 9. Sección "main":

En esta sección, se crea una instancia de la clase "ETL_Clima" y se ejecuta el método "run()" para iniciar todo el proceso de ETL.


```python

if __name__ == "__main__":
    print("Corriendo script")
    etl = ETL_Clima()
    etl.run()

```
