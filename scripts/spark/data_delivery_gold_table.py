#
'''
Script: data_delivery_gold_table.py
Author: Jefferson da S Martins
Version: 1.0.0
'''
#
#import libraries
import logging as log
import requests
import json
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
from delta.tables import *

def read_sql_file(path_file_name):
    with open(f"{path_file_name}") as fr:
        query = fr.read()
    return query

def main():
    spark = (SparkSession
                .builder
                .appName('data_processing')
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .getOrCreate()
    )

    setLogLevel = "ERROR"
    spark.sparkContext.setLogLevel(setLogLevel)

    #Read SILVER delta table
    log.info(f'Loading Silver table - {silver_bucket}/{silver_table}')
    delta_silver_table = DeltaTable.forPath(spark, f'{silver_bucket}/{silver_table}')
    df = delta_silver_table.toDF()

    #Create temporary table
    df.createOrReplaceTempView("tb_brewery")
    sql = read_sql_file(sql_file)

    #SQL execute
    df_gold = spark.sql(sql)

    log.info(f'Create Gold table - {gold_bucket}/{gold_table}')
    (df_gold.write
        .format('delta')
        .option("overwriteSchema", "true")
        .mode("overwrite")
        .save(f'{gold_bucket}/{gold_table}')
    )

    #just print all the dataset result
    df_gold.show(df_gold.count(), False)

    #stop session
    spark.stop()

if __name__ == "__main__":
    log.basicConfig(format='[%(asctime)s] - [APP] ### %(message)s', level=log.INFO, datefmt='%Y-%m-%d %H:%M:%S')

    silver_bucket = sys.argv[1]
    gold_bucket = sys.argv[2]
    silver_table = sys.argv[3]
    gold_table = sys.argv[4]
    sql_file = sys.argv[5]

    start_time = datetime.now()
    log.info(f'Start load - Gold layer')

    main()

    end_time = datetime.now()
    log.info(f'End load - Elapsed time: {end_time - start_time}')
