#
'''
Script: dag_delivery_location_by_brewery_type.py
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
gold_table = 'location_by_brewery_type'

gold_bucket = Variable.get("GOLD_BUCKET")
silver_bucket = Variable.get("SILVER_BUCKET")

artifacts_bucket = Variable.get("ARTIFACTS_BUCKET")
script_path_filename = artifacts_bucket + "/spark/data_delivery_gold_table.py"
sql_path_filename = artifacts_bucket + '/sql/location_by_brewery_type.sql'

default_args = {
    'onwer': 'data engineering',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 20, tzinfo=local_tz),
    'email': ['email@email.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
}
#[END variable definition]


with DAG(dag_id=f'dag_delivery_location_by_brewery_type',
            description=f'Gold table-location_by_brewery_type',
            default_args=default_args,
            schedule_interval=None,
            catchup=False,
            max_active_tasks=1,
            max_active_runs=1,
            tags=['spark', 'gold','bees']
) as dag:

    start_pipeline = DummyOperator(task_id = 'start_pipeline')

    spark_submit = BashOperator(
        task_id="spark_submit",
        bash_command=f"spark-submit \
            --master local[*]  \
            --packages io.delta:delta-core_2.12:2.1.0 \
            {script_path_filename} \
            {silver_bucket} \
            {gold_bucket} \
            {silver_table} \
            {gold_table} \
            {sql_path_filename}",
    )

    end_pipeline = DummyOperator(task_id = 'end_pipeline')


start_pipeline >> spark_submit  >> end_pipeline
