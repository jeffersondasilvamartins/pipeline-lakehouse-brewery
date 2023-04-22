#
'''
Script: data_processing_silver_table.py
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

    #Read bronze delta table
    log.info(f'Loading Bronze table - {bronze_bucket}/{table}')
    delta_bronze_table = DeltaTable.forPath(spark, f'{bronze_bucket}/{table}')
    df_brewery = delta_bronze_table.toDF()

    #Normalize columns & metadata
    df_brewery_select = df_brewery.select (
        F.col('address_1'), 
        F.col('address_2'), 
        F.col('address_3'), 
        F.col('brewery_type'), 
        F.col('city'), 
        F.col('country'), 
        F.col('id'), 
        F.col('latitude'), 
        F.col('longitude'), 
        F.col('name'), 
        F.col('phone'), 
        F.col('postal_code'), 
        F.trim(F.col('state')).alias('state'), 
        F.col('state_province'), 
        F.col('street'), 
        F.col('website_url'), 
        F.col('hash_field'), 
        F.col('ingestion_time'), 
        F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).alias('event_time'),
        F.col('system_name').alias('system_name'),
    )

    try:
        #Read silver delta table
        delta_silver_table = DeltaTable.forPath(spark, f'{silver_bucket}/{table}')
        log.info(f'Incremental load - Merge table - {silver_bucket}/{table}')
        (delta_silver_table.alias('target')
            .merge(df_brewery_select.alias('source'),'source.id = target.id')
            .whenMatchedUpdate(
                condition="source.hash_field <> target.hash_field",
                set = {
                    'address_1': df_brewery_select.address_1,
                    'address_2': df_brewery_select.address_2,
                    'address_3': df_brewery_select.address_3,
                    'brewery_type': df_brewery_select.brewery_type,
                    'city': df_brewery_select.city,
                    'country': df_brewery_select.country,
                    'latitude': df_brewery_select.latitude,
                    'longitude': df_brewery_select.longitude,
                    'name': df_brewery_select.name,
                    'phone': df_brewery_select.phone,
                    'postal_code': df_brewery_select.postal_code,
                    'state': df_brewery_select.state,
                    'state_province': df_brewery_select.state_province,
                    'street': df_brewery_select.street,
                    'website_url': df_brewery_select.website_url,
                    'hash_field': df_brewery_select.hash_field,
                    'ingestion_time': df_brewery_select.ingestion_time,
                    'system_name': df_brewery_select.system_name,
                }
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    except:
        log.info(f'Full load - New table - {silver_bucket}/{table}')

        (df_brewery_select.write
            .format('delta')
            .option("overwriteSchema", "true")
            .partitionBy(*['latitude', 'longitude'])
            .mode("overwrite")
            .save(f'{silver_bucket}/{table}')
        )

    spark.stop()

if __name__ == "__main__":
    log.basicConfig(format='[%(asctime)s] - [APP] ### %(message)s', level=log.INFO, datefmt='%Y-%m-%d %H:%M:%S')

    bronze_bucket = sys.argv[1]
    silver_bucket = sys.argv[2]
    table = sys.argv[3]

    start_time = datetime.now()
    log.info(f'Start load - Silver layer')

    main()

    end_time = datetime.now()
    log.info(f'End load - Elapsed time: {end_time - start_time}')
