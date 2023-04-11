# ............................................................
# Generate NYC Taxi trips
# ............................................................
# This script -
# 1. Reads a BQ table with NYC Taxi trips
# 2. Persists to GCS in the curated zone as parquet
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
    spark = SparkSession.builder.appName("NYC Taxi trip dataset generator").getOrCreate()
    logger.info('....===================================')

    # 3. Read base data in BigQuery
    logger.info('....Creating a base DF off of a BigQuery table')
    baseDF = spark.read \
    .format('bigquery') \
    .load(tableFQN)
    logger.info('....===================================')
   
    try:
        # 4. Persist to Cloud Storage
        
        logger.info('....Persisting dataframe in overwrite mode')
        baseDF.coalesce(2).write.partitionBy("trip_year","trip_month","trip_day").parquet(peristencePath, mode='overwrite')
        logger.info('....===================================')
        
        # 5. Delete flag files
        logger.info('....Deleting _SUCCESS')
        fnDeleteSuccessFlagFile(peristencePath)
        logger.info('....===================================')
    
       
    except RuntimeError as coreError:
            logger.error(coreError)
    else:
        logger.info('Successfully completed persisting NYC taxi trips!')
        
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
