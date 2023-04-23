#
'''
Script: dag_pipeline_lakehouse.py
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
silver_table = 'brewery'

gold_table_01 = 'location_by_brewery_type'
gold_table_02 = 'brewery_type_by_state'

bronze_bucket = Variable.get("BRONZE_BUCKET")
silver_bucket = Variable.get("SILVER_BUCKET")
gold_bucket = Variable.get("GOLD_BUCKET")

artifacts_bucket = Variable.get("ARTIFACTS_BUCKET")

bronze_script_path_filename = artifacts_bucket + "/spark/data_ingestion_bronze_table.py"
silver_script_pathfilename = artifacts_bucket + "/spark/data_processing_silver_table.py"
gold_script_path_filename = artifacts_bucket + "/spark/data_delivery_gold_table.py"

sql_1_path_filename = artifacts_bucket + '/sql/location_by_brewery_type.sql'
sql_2_path_filename = artifacts_bucket + "/sql/brewery_type_by_state.sql"

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


with DAG(dag_id=f'dag_pipeline_lakehouse',
            description=f'Ingestion Silver layer',
            default_args=default_args,
            schedule_interval=None,
            catchup=False,
            max_active_tasks=10,
            max_active_runs=1,
            tags=['spark', 'silver','bees']
) as dag:

    start_pipeline = DummyOperator(task_id = 'start_pipeline')

    bronze_layer = BashOperator(
        task_id="bronze_layer",
        bash_command=f"spark-submit \
                --master local[*] \
                --packages io.delta:delta-core_2.12:2.1.0 \
                {bronze_script_path_filename} \
                {bronze_bucket} \
                {table}",
    )

    silver_layer = BashOperator(
        task_id="silver_layer",
        bash_command=f"spark-submit \
                --master local[*] \
                --packages io.delta:delta-core_2.12:2.1.0 \
                {silver_script_pathfilename} \
                {bronze_bucket} \
                {silver_bucket} \
                {table}",
    )

    gold_location_by_brewery_type = BashOperator(
        task_id="gold_location_by_brewery_type",
        bash_command=f"spark-submit \
            --master local[*]  \
            --packages io.delta:delta-core_2.12:2.1.0 \
            {gold_script_path_filename} \
            {silver_bucket} \
            {gold_bucket} \
            {silver_table} \
            {gold_table_01} \
            {sql_1_path_filename}",
    )

    gold_brewery_type_by_state = BashOperator(
        task_id="gold_brewery_type_by_state",
        bash_command=f"spark-submit \
            --master local[*]  \
            --packages io.delta:delta-core_2.12:2.1.0 \
            {gold_script_path_filename} \
            {silver_bucket} \
            {gold_bucket} \
            {silver_table} \
            {gold_table_02} \
            {sql_2_path_filename}",
    )

    end_pipeline = DummyOperator(task_id = 'end_pipeline')

start_pipeline >> bronze_layer >> silver_layer >> [gold_brewery_type_by_state, gold_location_by_brewery_type]  >> end_pipeline
