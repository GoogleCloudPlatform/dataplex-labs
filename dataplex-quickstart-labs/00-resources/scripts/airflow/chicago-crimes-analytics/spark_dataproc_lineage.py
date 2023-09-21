# ======================================================================================
# ABOUT
# This script orchestrates the execution of the Chicago crimes reports
# It also includes custom lineage
# ======================================================================================

import os
from airflow.models import Variable
from datetime import datetime
from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (DataprocCreateBatchOperator,DataprocGetBatchOperator)
from datetime import datetime
from airflow.utils.dates import days_ago
import string
import random 
from airflow.operators import dummy_operator
from airflow.utils import trigger_rule
from airflow.composer.data_lineage.entities import BigQueryTable
from airflow.lineage import AUTO

# Read environment variables into local variables
PROJECT_ID = models.Variable.get('project_id')
PROJECT_NBR = models.Variable.get('project_nbr')
REGION = models.Variable.get("region")
UMSA = models.Variable.get("umsa")
SUBNET  = models.Variable.get("subnet")

# User Managed Service Account FQN
UMSA_FQN=UMSA+"@"+PROJECT_ID+".iam.gserviceaccount.com"

# PySpark script files in GCS, of the individual Spark applications in the pipeline
GCS_URI_CURATE_CRIMES_PYSPARK= f"gs://raw-code-{PROJECT_NBR}/pyspark/chicago-crimes-analytics/curate_crimes.py"
GCS_URI_CRIME_TRENDS_REPORT_PYSPARK= f"gs://raw-code-{PROJECT_NBR}/pyspark/chicago-crimes-analytics/crimes_report.py"

# Dataproc Metastore Resource URI
DPMS_RESOURCE_URI = f"projects/{PROJECT_ID}/locations/{REGION}/services/lab-dpms-{PROJECT_NBR}"

# Define DAG name
dag_name= "Chicago_Crime_Trends_From_Spark_With_Custom_Lineage"

# Generate Pipeline ID
randomizerCharLength = 10 
BATCH_ID = ''.join(random.choices(string.digits, k = randomizerCharLength))

# Report bases
REPORT_BASE_NM_CRIMES_YEAR="crimes-by-year"
REPORT_BASE_NM_CRIMES_MONTH="crimes-by-month"
REPORT_BASE_NM_CRIMES_DAY="crimes-by-day"
REPORT_BASE_NM_CRIMES_HOUR="crimes-by-hour"
REPORT_BASE_DIR=f"gs://product-data-{PROJECT_NBR}"
REPORT_CRIMES_YEAR_LOCATION=f"{REPORT_BASE_DIR}/{REPORT_BASE_NM_CRIMES_YEAR}-spark"
REPORT_CRIMES_MONTH_LOCATION=f"{REPORT_BASE_DIR}/{REPORT_BASE_NM_CRIMES_MONTH}-spark"
REPORT_CRIMES_DAY_LOCATION=f"{REPORT_BASE_DIR}/{REPORT_BASE_NM_CRIMES_DAY}-spark"
REPORT_CRIMES_HOUR_LOCATION=f"{REPORT_BASE_DIR}/{REPORT_BASE_NM_CRIMES_HOUR}-spark"


# 1a. Curate Crimes Spark application args
CURATE_CRIMES_ARGS_ARRAY = [ 
        f"--projectID={PROJECT_ID}", \
        f"--tableFQN=oda_curated_zone.crimes_curated_spark", \
        f"--peristencePath=gs://curated-data-{PROJECT_NBR}/crimes-curated-spark/"]

# 1b. Curate Crimes Spark application conf
CURATE_CRIMES_DATAPROC_SERVERLESS_BATCH_CONFIG = {
    "pyspark_batch": {
        "main_python_file_uri": GCS_URI_CURATE_CRIMES_PYSPARK,
        "args": CURATE_CRIMES_ARGS_ARRAY
        #,"jar_file_uris": ["gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.22.2.jar"]
    },
    "runtime_config":{
        "version": "1.1"
    },
    "environment_config":{
        "execution_config":{
              "service_account": UMSA_FQN,
            "subnetwork_uri": SUBNET
        },
        "peripherals_config": {
            "metastore_service": DPMS_RESOURCE_URI
        },
    },
}

# 2a. Crimes By Year Spark application args
CRIMES_BY_YEAR_ARGS_ARRAY = [f"--projectNbr={PROJECT_NBR} ",  \
        f"--projectID={PROJECT_ID} ",  \
        f"--reportDirGcsURI={REPORT_CRIMES_YEAR_LOCATION}",  \
        f"--reportName=Chicago Crime Trend by Year ",  \
        f"--reportSQL=SELECT cast(case_year as int) case_year,count(*) AS crime_count FROM oda_curated_zone.crimes_curated_spark GROUP BY case_year; ",  \
        f"--reportPartitionCount=1",  \
        f"--reportTableFQN=oda_product_zone.crimes_by_year_spark ",  \
        f"--reportTableDDL=CREATE TABLE IF NOT EXISTS oda_product_zone.crimes_by_year_spark (case_year int, crime_count long) STORED AS PARQUET LOCATION \"{REPORT_CRIMES_YEAR_LOCATION}\"" 
        ]

# 2b. Crimes By Year Spark application conf
CRIMES_BY_YEAR_DATAPROC_SERVERLESS_BATCH_CONFIG = {
    "pyspark_batch": {
        "main_python_file_uri": GCS_URI_CRIME_TRENDS_REPORT_PYSPARK,
        "args": CRIMES_BY_YEAR_ARGS_ARRAY
        #,"jar_file_uris": ["gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.22.2.jar"]
    },
    "runtime_config":{
        "version": "1.1"
    },
    "environment_config":{
        "execution_config":{
              "service_account": UMSA_FQN,
            "subnetwork_uri": SUBNET
        },
        "peripherals_config": {
            "metastore_service": DPMS_RESOURCE_URI
        },
    },
}

# 3a. Crimes By Month Spark application args
CRIMES_BY_MONTH_ARGS_ARRAY = [f"--projectNbr={PROJECT_NBR} " ,  \
        f"--projectID={PROJECT_ID} ",  \
        f"--reportDirGcsURI={REPORT_CRIMES_MONTH_LOCATION}",  \
        f"--reportName=Chicago Crime Trend by Month ",  \
        f"--reportSQL=SELECT case_month,count(*) AS crime_count FROM oda_curated_zone.crimes_curated_spark GROUP BY case_month; ",  \
        f"--reportPartitionCount=1",  \
        f"--reportTableFQN=oda_product_zone.crimes_by_month_spark ",  \
        f"--reportTableDDL=CREATE TABLE IF NOT EXISTS oda_product_zone.crimes_by_month_spark(case_month string, crime_count long) STORED AS PARQUET LOCATION \"{REPORT_CRIMES_MONTH_LOCATION}\"" 
        ]

# 3b. Crimes By Month Spark application conf
CRIMES_BY_MONTH_DATAPROC_SERVERLESS_BATCH_CONFIG = {
    "pyspark_batch": {
        "main_python_file_uri": GCS_URI_CRIME_TRENDS_REPORT_PYSPARK,
        "args": CRIMES_BY_MONTH_ARGS_ARRAY
        #,"jar_file_uris": ["gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.22.2.jar"]
    },
    "runtime_config":{
        "version": "1.1"
    },
    "environment_config":{
        "execution_config":{
            "service_account": UMSA_FQN,
            "subnetwork_uri": SUBNET
        },
        "peripherals_config": {
            "metastore_service": DPMS_RESOURCE_URI  
        },
    },
}

# 4a. Crimes By Day Spark application args
CRIMES_BY_DAY_ARGS_ARRAY = [f"--projectNbr={PROJECT_NBR} " ,   \
        f"--projectID={PROJECT_ID} ",   \
        f"--reportDirGcsURI={REPORT_CRIMES_DAY_LOCATION}" , \
        f"--reportName=Chicago Crime Trend by Day " ,  \
        f"--reportSQL=SELECT case_day_of_week,count(*) AS crime_count FROM oda_curated_zone.crimes_curated_spark GROUP BY case_day_of_week; " , \
        f"--reportPartitionCount=1" , \
        f"--reportTableFQN=oda_product_zone.crimes_by_day_spark ",  \
        f"--reportTableDDL=CREATE TABLE IF NOT EXISTS  oda_product_zone.crimes_by_day_spark (case_day_of_week string, crime_count long) STORED AS PARQUET LOCATION \"{REPORT_CRIMES_DAY_LOCATION}\"" 
        ]

# 4b. Crimes By Day Spark application conf
CRIMES_BY_DAY_DATAPROC_SERVERLESS_BATCH_CONFIG = {
    "pyspark_batch": {
        "main_python_file_uri": GCS_URI_CRIME_TRENDS_REPORT_PYSPARK,
        "args": CRIMES_BY_DAY_ARGS_ARRAY 
        #,"jar_file_uris": ["gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.22.2.jar"]
    },
    "runtime_config":{
        "version": "1.1"
    },
    "environment_config":{
        "execution_config":{
            "service_account": UMSA_FQN,
            "subnetwork_uri": SUBNET
        },
        "peripherals_config": {
            "metastore_service": DPMS_RESOURCE_URI     
        }
    }
}

# 5a. Crimes By Hour Spark application args
CRIMES_BY_HOUR_ARGS_ARRAY = [f"--projectNbr={PROJECT_NBR} " ,   \
        f"--projectID={PROJECT_ID} ",   \
        f"--reportDirGcsURI={REPORT_CRIMES_HOUR_LOCATION}" , \
        f"--reportName=Chicago Crime Trend by Hour " ,  \
        f"--reportSQL=SELECT CAST(case_hour_of_day AS int) case_hour_of_day,count(*) AS crime_count FROM oda_curated_zone.crimes_curated_spark GROUP BY case_hour_of_day; " , \
        f"--reportPartitionCount=1",  \
        f"--reportTableFQN=oda_product_zone.crimes_by_hour_spark ",  \
        f"--reportTableDDL=CREATE TABLE IF NOT EXISTS oda_product_zone.crimes_by_hour_spark(case_hour_of_day int, crime_count long) STORED AS PARQUET LOCATION \"{REPORT_CRIMES_HOUR_LOCATION}\" " 
        ]

# 5b. Crimes By Hour Spark application conf
CRIMES_BY_HOUR_DATAPROC_SERVERLESS_BATCH_CONFIG = {
    "pyspark_batch": {
        "main_python_file_uri": GCS_URI_CRIME_TRENDS_REPORT_PYSPARK,
        "args": CRIMES_BY_HOUR_ARGS_ARRAY
        #,"jar_file_uris": ["gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.22.2.jar"]
    },
    "runtime_config":{
        "version": "1.1"
    },
    "environment_config":{
        "execution_config":{
              "service_account": UMSA_FQN,
            "subnetwork_uri": SUBNET
        },
        "peripherals_config": {
            "metastore_service": DPMS_RESOURCE_URI
                
        },
    }
}


# Build the pipeline
with models.DAG(
    dag_name,
    schedule_interval=None,
    start_date = days_ago(2),
    catchup=False,
) as dag_serverless_batch:

    start = dummy_operator.DummyOperator(
        task_id='start',
        trigger_rule='all_success'
    )
    curate_chicago_crimes = DataprocCreateBatchOperator(
        task_id="CURATE_CRIMES",
        project_id=PROJECT_ID,
        region=REGION,
        batch=CURATE_CRIMES_DATAPROC_SERVERLESS_BATCH_CONFIG,
        batch_id=f"chicago-crimes-curate-af-scl-{BATCH_ID}",
        inlets=[BigQueryTable(
        project_id=PROJECT_ID,
        dataset_id='oda_raw_zone',
        table_id='crimes_raw',
        )],
        outlets=[BigQueryTable(
        project_id=PROJECT_ID,
        dataset_id='oda_curated_zone',
        table_id='crimes_curated_spark',
        )]
    )
    trend_by_year = DataprocCreateBatchOperator(
        task_id="CRIME_TREND_BY_YEAR",
        project_id=PROJECT_ID,
        region=REGION,
        batch=CRIMES_BY_YEAR_DATAPROC_SERVERLESS_BATCH_CONFIG,
        batch_id=f"chicago-crimes-trend-by-year-af-scl-{BATCH_ID}",
        inlets=[BigQueryTable(
        project_id=PROJECT_ID,
        dataset_id='oda_curated_zone',
        table_id='crimes_curated_spark',
        )],
        outlets=[BigQueryTable(
        project_id=PROJECT_ID,
        dataset_id='oda_product_zone',
        table_id='crimes_by_year_spark',
        )]
    )

    trend_by_month = DataprocCreateBatchOperator(
        task_id="CRIME_TREND_BY_MONTH",
        project_id=PROJECT_ID,
        region=REGION,
        batch=CRIMES_BY_MONTH_DATAPROC_SERVERLESS_BATCH_CONFIG,
        batch_id=f"chicago-crimes-trend-by-month-af-scl-{BATCH_ID}",
        inlets=[BigQueryTable(
        project_id=PROJECT_ID,
        dataset_id='oda_curated_zone',
        table_id='crimes_curated_spark',
        )],
        outlets=[BigQueryTable(
        project_id=PROJECT_ID,
        dataset_id='oda_product_zone',
        table_id='crimes_by_month_spark',
        )]
    )

    trend_by_day = DataprocCreateBatchOperator(
        task_id="CRIME_TREND_BY_DAY",
        project_id=PROJECT_ID,
        region=REGION,
        batch=CRIMES_BY_DAY_DATAPROC_SERVERLESS_BATCH_CONFIG,
        batch_id=f"chicago-crimes-trend-by-day-af-scl-{BATCH_ID}",
        inlets=[BigQueryTable(
        project_id=PROJECT_ID,
        dataset_id='oda_curated_zone',
        table_id='crimes_curated_spark',
        )],
        outlets=[BigQueryTable(
        project_id=PROJECT_ID,
        dataset_id='oda_product_zone',
        table_id='crimes_by_day_spark',
        )]
    )

    trend_by_hour = DataprocCreateBatchOperator(
        task_id="CRIME_TREND_BY_HOUR",
        project_id=PROJECT_ID,
        region=REGION,
        batch=CRIMES_BY_HOUR_DATAPROC_SERVERLESS_BATCH_CONFIG,
        batch_id=f"chicago-crimes-trend-by-hour-af-scl-{BATCH_ID}",
        inlets=[BigQueryTable(
        project_id=PROJECT_ID,
        dataset_id='oda_curated_zone',
        table_id='crimes_curated_spark',
        )],
        outlets=[BigQueryTable(
        project_id=PROJECT_ID,
        dataset_id='oda_product_zone',
        table_id='crimes_by_hour_spark',
        )]
    )

    end = dummy_operator.DummyOperator(
        task_id='end',
        trigger_rule='all_done'
    )


start >> curate_chicago_crimes >> [trend_by_year, trend_by_month, trend_by_day, trend_by_hour] >> end


