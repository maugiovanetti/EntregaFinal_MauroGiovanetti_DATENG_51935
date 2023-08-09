# Este script está pensado para correr en Spark y hacer el proceso de ETL de la tabla Clima

import requests
from datetime import datetime, timedelta
from os import environ as env

from pyspark.sql.functions import concat, col, lit, when, expr, to_date, round
from pyspark.sql.types import StringType

from commons import ETL_Spark


# Configurar la API key de OpenWeatherMap

api_key = "cb3c7af6f8a3112d069b2cd42e3d2651"

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


class ETL_Clima(ETL_Spark):
    def __init__(self, job_name=None):
        super().__init__(job_name)
        self.process_date = datetime.now().strftime("%Y-%m-%d")

    def run(self):
        process_date = datetime.now().strftime("%Y-%m-%d")
        self.execute(process_date)

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

    def transform(self, df_original):
        """
        Transforma los datos
        """
        print(">>> [T] Transformando datos...")

        # Dividir la columna 'coord' en 'lon' y 'lat'

        df = (
            df_original.withColumn("lon", col("coord.lon"))
            .withColumn("lat", col("coord.lat"))
            .drop("coord")
        )

        # Mantener solo la columna 'description' de la columna 'weather'

        df = df.withColumn(
            "weather_description", col("weather.description").cast("string")
        ).drop("weather")

        # Dividir la columna 'main' en nuevas columnas y eliminar la columna original

        df = (
            df.withColumn("temp", col("main.temp"))
            .withColumn("feels_like", col("main.feels_like"))
            .withColumn("temp_min", col("main.temp_min"))
            .withColumn("temp_max", col("main.temp_max"))
            .withColumn("pressure", col("main.pressure"))
            .withColumn("humidity", col("main.humidity"))
            .drop("main")
        )

        # Dividir la columna 'wind' en nuevas columnas y eliminar la columna original

        df = df.withColumn("wind_speed", col("wind.speed")).drop("wind")

        # Eliminar la columna 'clouds', 'sys', 'id' y 'cod'

        df = df.drop("clouds", "sys", "id", "cod", "base", "dt")

        # Agregar el símbolo porcentual a la columna "humedad"

        df = df.withColumn("humidity", concat(col("humidity"), lit("%")))

        # Agregar una columna nueva que sea la amplitud térmica

        df = df.withColumn(
            "amplitud_termica", round(col("temp_max") - col("temp_min"), 2)
        )

        # Agregar una columna nueva que sea la temperatura promedio de la provincia

        df = df.withColumn(
            "temp_promedio", round((col("temp_min") + col("temp_max")) / 2, 2)
        )
        df.show()

        return df

    def load(self, df_final):
        """
        Carga los datos transformados en Redshift
        """
        print(">>> [L] Cargando datos en Redshift...")

        # add process_date column

        df_final = df_final.withColumn("process_date", lit(self.process_date))

        df_final.write.format("jdbc").option("url", env["REDSHIFT_URL"]).option(
            "dbtable", f"{env['REDSHIFT_SCHEMA']}.clima"
        ).option("user", env["REDSHIFT_USER"]).option(
            "password", env["REDSHIFT_PASSWORD"]
        ).option(
            "driver", "org.postgresql.Driver"
        ).mode(
            "append"
        ).save()

        print(">>> [L] Datos cargados exitosamente")


if __name__ == "__main__":
    print("Corriendo script")
    etl = ETL_Clima()
    etl.run()
