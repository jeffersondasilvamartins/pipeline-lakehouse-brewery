#
'''
Script: dag_delivery_gold.py
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

silver_table = 'brewery'
gold_table = 'store_by_location'

gold_bucket = Variable.get("GOLD_BUCKET")
silver_bucket = Variable.get("SILVER_BUCKET")

script_home='/root/scripts'
script_pathname=f'{script_home}/data_delivery_gold_table.py'
sql_name='type_location.sql'

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


with DAG(dag_id=f'dag_delivery_gold',
            description=f'Ingestion Gold layer',
            default_args=default_args,
            schedule_interval=None,
            catchup=False,
            max_active_tasks=10,
            max_active_runs=1,
            tags=['spark', 'gold','bees']
) as dag:

    start_pipeline = DummyOperator(task_id = 'start_pipeline')

    spark_submit = BashOperator(
        task_id="spark_submit",
        bash_command=f"spark-submit --master local[*]  --packages io.delta:delta-core_2.12:2.1.0 {script_pathname} {silver_bucket} {gold_bucket} {silver_table} {gold_table} {sql_name}",
    )

    end_pipeline = DummyOperator(task_id = 'end_pipeline')


start_pipeline >> spark_submit  >> end_pipeline
