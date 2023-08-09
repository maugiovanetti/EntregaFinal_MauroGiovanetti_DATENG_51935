# Este es el DAG que orquesta el ETL de la tabla Clima

from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


from airflow.models import Variable

from datetime import datetime, timedelta

QUERY_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS clima (
    name VARCHAR(50),
    timezone INT,
    visibility INT,
    lon FLOAT,
    lat FLOAT,
    weather_description VARCHAR(300),
    temp FLOAT, 
    feels_like FLOAT,
    temp_min FLOAT, 
    temp_max FLOAT, 
    pressure INT,
    humidity VARCHAR(50),
    wind_speed FLOAT,
    amplitud_termica FLOAT,
    temp_promedio FLOAT,
    process_date VARCHAR(10)
) 
DISTKEY(process_date)
SORTKEY(process_date, name);
"""

QUERY_CLEAN_PROCESS_DATE = """
DELETE FROM clima WHERE process_date = '{{ ti.xcom_pull(key="process_date") }}';
"""


# create function to get process_date and push it to xcom
def get_process_date(**kwargs):
    # If process_date is provided take it, otherwise take today
    if (
        "process_date" in kwargs["dag_run"].conf
        and kwargs["dag_run"].conf["process_date"] is not None
    ):
        process_date = kwargs["dag_run"].conf["process_date"]
    else:
        process_date = kwargs["dag_run"].conf.get(
            "process_date", datetime.now().strftime("%Y-%m-%d")
        )
    kwargs["ti"].xcom_push(key="process_date", value=process_date)


defaul_args = {
    "owner": "Mauro Giovanetti",
    "start_date": datetime(2023, 7, 1),
    "retries": 0,
    "retry_delay": timedelta(seconds=5),
    'catchup': False,
}

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
    
    # Tareas
    get_process_date_task = PythonOperator(
        task_id="get_process_date",
        python_callable=get_process_date,
        provide_context=True,
        dag=dag,
    )

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="redshift_default",
        sql=QUERY_CREATE_TABLE,
        dag=dag,
    )

    clean_process_date = SQLExecuteQueryOperator(
        task_id="clean_process_date",
        conn_id="redshift_default",
        sql=QUERY_CLEAN_PROCESS_DATE,
        dag=dag,
    )

    spark_etl_clima = SparkSubmitOperator(
        task_id="spark_etl_clima",
        application=f'{Variable.get("spark_scripts_dir")}/ETL_Clima.py',
        conn_id="spark_default",
        dag=dag,
        driver_class_path=Variable.get("driver_class_path"),
    )

    get_process_date_task >> create_table >> clean_process_date >> spark_etl_clima
