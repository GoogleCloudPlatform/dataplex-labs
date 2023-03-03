# ............................................................
# Create Crime Trend Report
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
        '--projectNbr',
        help='The project number',
        required=True)
    argsParser.add_argument(
        '--projectID',
        help='The project id',
        type=str,
        required=True)
    argsParser.add_argument(
        '--reportDirGcsURI',
        help='The GCS URI for the report',
        required=True)
    argsParser.add_argument(
        '--reportName',
        help='The report name',
        required=True)
    argsParser.add_argument(
        '--reportSQL',
        help='The report SQL',
        required=True)
    argsParser.add_argument(
        '--reportPartitionCount',
        help='The spark partition count',
        required=True)
    argsParser.add_argument(
        '--reportTableFQN',
        help='The report table FQN',
        required=True)
    argsParser.add_argument(
        '--reportTableDDL',
        help='The report table DDL',
        required=True)
    return argsParser.parse_args()
# }} End fnParseArguments()

def fnMain(logger, args):
# {{ Start main

    logger.info('....Inside main')

    # 1. Capture Spark application input
    projectNbr=args.projectNbr
    projectID=args.projectID
    reportDirGcsURI=args.reportDirGcsURI
    reportName=args.reportName
    reportSQL=args.reportSQL
    reportPartitionCount=args.reportPartitionCount
    reportTableFQN=args.reportTableFQN
    reportTableDDL=args.reportTableDDL

    print("Arguments")
    print("........................")
    print(f"projectNbr: {projectNbr}")
    print(f"projectID: {projectID}")
    print(f"reportDirGcsURI: {reportDirGcsURI}")
    print(f"reportName: {reportName}")
    print(f"reportSQL: {reportSQL}")
    print(f"reportPartitionCount: {reportPartitionCount}")
    print(f"reportTableFQN: {reportTableFQN}")


    # 2. Create Spark session
    logger.info('....Initializing spark & spark configs')
    spark = SparkSession.builder.appName(f"reportName: {reportName}").getOrCreate()
    logger.info('....===================================')

    try:

        # 3. Drop table if exists
        logger.info('....drop table if exists')
        spark.sql(f"DROP TABLE IF EXISTS {reportTableFQN}").show(truncate=False)
        logger.info('....===================================')

        # 4. Create dataframe off of the SQL & drop duplicates
        logger.info('....creating dataframe')
        reportDF = spark.sql(reportSQL)
        reportDF.dropDuplicates()
        logger.info('....===================================')
    
        # 5. Persist to the data lake as a table in the curated zone
        logger.info('....persisting dataframe to table')
        reportDF.repartition(int(reportPartitionCount)).write.parquet(reportDirGcsURI, mode='overwrite')
        logger.info('....completed persisting dataframe to table')
        logger.info('....===================================')

        # 6. Create external table
        logger.info('....Create table')
        print(f"Create Curated Crimes DDL: {reportTableDDL}")
        spark.sql(reportTableDDL).show(truncate=False)
        logger.info('....===================================')

        # 7. Refresh table 
        logger.info('....Refresh table')
        spark.sql(f"REFRESH TABLE {reportTableFQN};").show(truncate=False)
        logger.info('....===================================')

        # 8. Remove _SUCCESS file
        logger.info('....Deleting _SUCCESS')
        fnDeleteSuccessFlagFile(reportDirGcsURI)
        logger.info('....===================================')

    
    except RuntimeError as coreError:
            logger.error(coreError)
    else:
        logger.info(f"Successfully completed generating {reportName}!")
# }} End fnMain()


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
    blob = bucket.blob(f"{object_name}/_SUCCESS")
    blob.delete()

    print(f"_SUCCESS file deleted.")
# }} End

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