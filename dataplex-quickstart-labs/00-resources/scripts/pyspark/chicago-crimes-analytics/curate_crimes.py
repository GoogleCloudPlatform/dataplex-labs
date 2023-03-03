# ............................................................
# Curate Chicago Crimes
# ............................................................
# This script -
# 1. subsets raw Chicago crimes data
# 2. augments with temporal attributes 
# 3. persists to GCS in the curated zone as parquet
# 4. creates an external table in HMS over #2
# ............................................................

import sys,logging,argparse
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime
from google.cloud import storage


def fnParseArguments():
# {{ Start 
    """
    Purpose:
        Parse arguments received by script
    Returns:
        args
    """
    argsParser = argparse.ArgumentParser()
    argsParser.add_argument(
        '--projectID',
        help='Project ID',
        type=str,
        required=True)
    argsParser.add_argument(
        '--tableFQN',
        help='Table fully qualified name',
        type=str,
        required=True)
    argsParser.add_argument(
        '--peristencePath',
        help='GCS location',
        type=str,
        required=True)
    return argsParser.parse_args()
# }} End fnParseArguments()

def fnDeleteSuccessFlagFile(bucket_uri):
# {{ Start 
    """Deletes a blob from the bucket."""
    # bucket_name = "your-bucket-name"
    # blob_name = "your-object-name"

    storage_client = storage.Client()
    bucket_name = bucket_uri.split("/")[2]
    object_name = "/".join(bucket_uri.split("/")[3:]) 

    print(f"Bucket name: {bucket_name}")
    print(f"Object name: {object_name}")

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f"{object_name}_SUCCESS")
    blob.delete()

    print(f"_SUCCESS file deleted.")
# }} End

def fnMain(logger, args):
# {{ Start main

    # 1. Capture Spark application input
    projectID = args.projectID
    tableFQN = args.tableFQN
    peristencePath = args.peristencePath

    # 2. Create Spark session
    logger.info('....Initializing spark & spark configs')
    spark = SparkSession.builder.appName("Curate Chicago Crimes").getOrCreate()
    logger.info('....===================================')

    # 3. Create curated crimes SQL
    # 3.1. Read data from BigQuery
    logger.info('....Creating a base DF off of a BigQuery table')
    baseDF = spark.read \
    .format('bigquery') \
    .load(f'{projectID}.oda_raw_zone.crimes_raw')
    logger.info('....===================================')

    # 3.2. Register temp table
    logger.info('....Creating a temp table')
    baseDF.createOrReplaceTempView("crimes_raw")
    baseDF.count()
    logger.info('....===================================')

    # 3.3. Then create the curate crimes SQL
    curatedCrimesSQL="SELECT case_number,primary_type as case_type,date as case_date,year AS case_year,date_format(date, 'MMM') AS case_month,date_format(date,'E') AS case_day_of_week, hour(date) AS case_hour_of_day FROM crimes_raw;"
    print(f"Curated Crimes SQL: {curatedCrimesSQL}")
    logger.info('....===================================')
    
    try:
        # 4. Drop table if exists
        logger.info('....Dropping table if it exists')
        spark.sql(f"DROP TABLE IF EXISTS {tableFQN}").show(truncate=False)
        logger.info('....===================================')
        
        # 5. Curate crimes
        logger.info('....Creating dataframe')
        curatedCrimesDF = spark.sql(curatedCrimesSQL)
        curatedCrimesDF.dropDuplicates()
        curatedCrimesDF.count()
        logger.info('....===================================')
    
        # 6. Persist to the data lake bucket in the curated zone
        logger.info('....Persisting dataframe in overwrite mode')
        print(f"peristencePath is {peristencePath}")
        curatedCrimesDF.repartition(17).write.parquet(peristencePath, mode='overwrite')
        logger.info('....===================================')
    
        # 7. Create table definition
        logger.info('....Create table')
        CREATE_TABLE_DDL=f"CREATE TABLE IF NOT EXISTS {tableFQN}(case_number string, case_type string,case_date timestamp, case_year long, case_month string, case_day_of_week string, case_hour_of_day integer) STORED AS PARQUET LOCATION \"{peristencePath}\";"
        print(f"Create Curated Crimes DDL: {CREATE_TABLE_DDL}")
        spark.sql(CREATE_TABLE_DDL).show(truncate=False)
        logger.info('....===================================')

        # 8. Refresh table 
        logger.info('....Refresh table')
        spark.sql(f"REFRESH TABLE {tableFQN};").show(truncate=False)
        logger.info('....===================================')

        # 9. Remove _SUCCESS file
        logger.info('....Deleting _SUCCESS')
        fnDeleteSuccessFlagFile(peristencePath)
        logger.info('....===================================')

    except RuntimeError as coreError:
            logger.error(coreError)
    else:
        logger.info('Successfully completed curating Chicago crimes!')
# }} End fnMain()

def fnConfigureLogger():
# {{ Start 
    """
    Purpose:
        Configure a logger for the script
    Returns:
        Logger object
    """
    logFormatter = logging.Formatter('%(asctime)s - %(filename)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("data_engineering")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    logStreamHandler = logging.StreamHandler(sys.stdout)
    logStreamHandler.setFormatter(logFormatter)
    logger.addHandler(logStreamHandler)
    return logger
# }} End fnConfigureLogger()

if __name__ == "__main__":
    arguments = fnParseArguments()
    logger = fnConfigureLogger()
    fnMain(logger, arguments)