#
'''
Script: data_ingestion_bronze_table.py
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
from pyspark.sql.functions import lit, sha2, concat_ws
from datetime import datetime

def main():
    spark = (SparkSession
                .builder
                .appName('data_ingestion')
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .getOrCreate()
    )

    setLogLevel = "ERROR"
    spark.sparkContext.setLogLevel(setLogLevel)

    sc = spark.sparkContext

    payload = {}
    headers = {}

    #Get metadata
    #Total the pages and records by page
    url= "https://api.openbrewerydb.org/v1/breweries/meta"
    response = requests.request("GET", url, headers=headers, data=payload)

    total = int(response.json()['total'])
    per_page=200 #limit value by page

    breweries= []

    #Call API
    log.info(f'Get request to the API ')
    for page in range(round(total/per_page)):
        #print("-----------")
        #print("Page=",  page)
        url=f"https://api.openbrewerydb.org/v1/breweries?page={page+1}&per_page={per_page}"
        response = requests.request("GET", url, headers=headers, data=payload)
        data = response.json()
        breweries.extend(data)

    #Convert into dataframe
    df = spark.read\
            .option('multiline', "true")\
            .json(sc.parallelize([json.dumps(breweries)]))

    #metadata & hash field
    schema = df.dtypes
    df_source = df\
                .withColumn("ingestion_time", lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))\
                .withColumn("system_name", lit("api.openbrewerydb"))\
                .withColumn("hash_field", sha2(concat_ws(",",*[col for col in df.columns]),256))

    #Write delta table
    log.info(f'Write Bronze delta table')    
    (df_source.write
        .format('delta')
        .option("overwriteSchema", "true")
        .mode("overwrite")
        .save(f'{bucket}/{table}')
    )

    spark.stop()

if __name__ == "__main__":
    log.basicConfig(format='[%(asctime)s] - [APP] ### %(message)s', level=log.INFO, datefmt='%Y-%m-%d %H:%M:%S')

    bucket = sys.argv[1]
    table = sys.argv[2]

    start_time = datetime.now()
    log.info(f'Start load - Bronze layer')

    main()

    end_time = datetime.now()
    log.info(f'End load - Elapsed time: {end_time - start_time}')