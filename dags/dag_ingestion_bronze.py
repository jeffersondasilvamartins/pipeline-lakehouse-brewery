#
'''
Script: dag_ingestion_bronze.py
Author: Jefferson da S Martins
Version: 1.0.0
'''
#
#[START import libs]
import os
import pendulum
from datetime import datetime, timedelta
import random

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
#[END import libs]

#[START variable definition]
local_tz = pendulum.timezone("America/Sao_Paulo")
table = 'brewery'
bronze_bucket = Variable.get("BRONZE_BUCKET")
script_path_filename= Variable.get("ARTIFACTS_BUCKET")
script_path_filename += "/spark/data_ingestion_bronze_table.py"

default_args = {
    'onwer': 'data engineering',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 20, tzinfo=local_tz),
    'email': ['email@email.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
}
#[END variable definition]


with DAG(dag_id=f'dag_ingestion_bronze',
            description=f'Ingestion Bronze layer',
            default_args=default_args,
            schedule_interval=None,
            catchup=False,
            max_active_tasks=1,
            max_active_runs=1,
            tags=['spark', 'bronze','bees']
) as dag:

    start_pipeline = DummyOperator(task_id = 'start_pipeline')

    spark_submit = BashOperator(
        task_id="spark_submit",
        bash_command=f"spark-submit \
            --master local[*]  \
            --packages io.delta:delta-core_2.12:2.1.0 \
            {script_path_filename}\
            {bronze_bucket} \
            {table}",
    )

    end_pipeline = DummyOperator(task_id = 'end_pipeline')


start_pipeline >> spark_submit  >> end_pipeline
