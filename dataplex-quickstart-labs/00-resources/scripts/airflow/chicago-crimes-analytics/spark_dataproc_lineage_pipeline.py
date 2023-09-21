# ======================================================================================
# ABOUT
# This script orchestrates the execution of the Chicago crimes reports
# It showcases Dataproc lineage
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

UMSA_FQN=UMSA+"@"+PROJECT_ID+".iam.gserviceaccount.com"
SUBNET_URI=f"projects/{PROJECT_ID}/regions/{REGION}/subnetworks/{SUBNET}"
DPGCE_CLUSTER_NM=f"lineage-enabled-spark-cluster-{PROJECT_NBR}"

# PySpark script files in GCS, of the individual Spark applications in the pipeline
GCS_URI_CURATE_CRIMES_PYSPARK= f"gs://raw-code-{PROJECT_NBR}/pyspark/chicago-crimes-analytics/curate_crimes.py"
GCS_URI_CRIME_TRENDS_REPORT_PYSPARK= f"gs://raw-code-{PROJECT_NBR}/pyspark/chicago-crimes-analytics/crimes_report.py"

# Dataproc Metastore Resource URI
DPMS_RESOURCE_URI = f"projects/{PROJECT_ID}/locations/{REGION}/services/lab-dpms-{PROJECT_NBR}"

# Define DAG name
dag_name= "Chicago_Crime_Trends_From_Spark_With_Dataproc_OOB_Lineage"

# Generate Pipeline ID
randomizerCharLength = 10 
JOB_ID = ''.join(random.choices(string.digits, k = randomizerCharLength))

# Report bases
REPORT_BASE_NM_CRIMES_YEAR="crimes-by-year-spark-dataproc"
REPORT_BASE_NM_CRIMES_MONTH="crimes-by-month-spark-dataproc"
REPORT_BASE_NM_CRIMES_DAY="crimes-by-day-spark-dataproc"
REPORT_BASE_NM_CRIMES_HOUR="crimes-by-hour-spark-dataproc"
REPORT_BASE_DIR=f"gs://product-data-{PROJECT_NBR}"
REPORT_CRIMES_YEAR_LOCATION=f"{REPORT_BASE_DIR}/{REPORT_BASE_NM_CRIMES_YEAR}"
REPORT_CRIMES_MONTH_LOCATION=f"{REPORT_BASE_DIR}/{REPORT_BASE_NM_CRIMES_MONTH}"
REPORT_CRIMES_DAY_LOCATION=f"{REPORT_BASE_DIR}/{REPORT_BASE_NM_CRIMES_DAY}"
REPORT_CRIMES_HOUR_LOCATION=f"{REPORT_BASE_DIR}/{REPORT_BASE_NM_CRIMES_HOUR}"


# 1a. Curate Crimes Spark application args
CURATE_CRIMES_ARGS_ARRAY = [ 
        f"--projectID={PROJECT_ID}", \
        f"--tableFQN=oda_curated_zone.crimes_curated_spark_dataproc", \
        f"--peristencePath=gs://curated-data-{PROJECT_NBR}/crimes-curated-spark-dataproc/"]

# 1b. Curate Crimes Spark application conf
CURATE_CRIMES_DATAPROC_GCE_JOB_CONFIG = {
    "reference": {"job_id": REPORT_BASE_NM_CRIMES_YEAR + "-JOB_ID","project_id": PROJECT_ID},
    "placement": {"cluster_name": DPGCE_CLUSTER_NM},
    "pyspark_job": {"main_python_file_uri": GCS_URI_CURATE_CRIMES_PYSPARK,
                    "jar_file_uris": [ "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.22.2.jar"],
                    "args": CURATE_CRIMES_ARGS_ARRAY
                    }
}

# 2a. Crimes By Year Spark application args
CRIMES_BY_YEAR_ARGS_ARRAY = [f"--projectNbr={PROJECT_NBR} ",  \
        f"--projectID={PROJECT_ID} ",  \
        f"--reportDirGcsURI={REPORT_CRIMES_YEAR_LOCATION}",  \
        f"--reportName=Chicago Crime Trend by Year ",  \
        f"--reportSQL=SELECT cast(case_year as int) case_year,count(*) AS crime_count FROM oda_curated_zone.crimes_curated_spark_dataproc GROUP BY case_year; ",  \
        f"--reportPartitionCount=1",  \
        f"--reportTableFQN=oda_product_zone.crimes_by_year_spark_dataproc ",  \
        f"--reportTableDDL=CREATE TABLE IF NOT EXISTS oda_product_zone.crimes_by_year_spark_dataproc(case_year int, crime_count long) STORED AS PARQUET LOCATION \"{REPORT_CRIMES_YEAR_LOCATION}\"" 
        ]

# 2b. Crimes By Year Spark application conf
CRIMES_BY_YEAR_DATAPROC_GCE_JOB_CONFIG = {
    "reference": {"job_id": REPORT_BASE_NM_CRIMES_YEAR + "-JOB_ID","project_id": PROJECT_ID},
    "placement": {"cluster_name": DPGCE_CLUSTER_NM},
    "pyspark_job": {"main_python_file_uri": GCS_URI_CRIME_TRENDS_REPORT_PYSPARK,
                    "jar_file_uris": [ "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.22.2.jar"],
                    "args": CRIMES_BY_YEAR_ARGS_ARRAY
                    }
}

# 3a. Crimes By Month Spark application args
CRIMES_BY_MONTH_ARGS_ARRAY = [f"--projectNbr={PROJECT_NBR} " ,  \
        f"--projectID={PROJECT_ID} ",  \
        f"--reportDirGcsURI={REPORT_CRIMES_MONTH_LOCATION}",  \
        f"--reportName=Chicago Crime Trend by Month ",  \
        f"--reportSQL=SELECT case_month,count(*) AS crime_count FROM oda_curated_zone.crimes_curated_spark_dataproc GROUP BY case_month; ",  \
        f"--reportPartitionCount=1",  \
        f"--reportTableFQN=oda_product_zone.crimes_by_month_spark_dataproc ",  \
        f"--reportTableDDL=CREATE TABLE IF NOT EXISTS oda_product_zone.crimes_by_month_spark_dataproc(case_month string, crime_count long) STORED AS PARQUET LOCATION \"{REPORT_CRIMES_MONTH_LOCATION}\"" 
        ]

# 3b. Crimes By Month Spark application conf
CRIMES_BY_MONTH_DATAPROC_GCE_JOB_CONFIG = {
    "reference": {"job_id": REPORT_BASE_NM_CRIMES_MONTH + "-JOB_ID","project_id": PROJECT_ID},
    "placement": {"cluster_name": DPGCE_CLUSTER_NM},
    "pyspark_job": {"main_python_file_uri": GCS_URI_CRIME_TRENDS_REPORT_PYSPARK,
                    "jar_file_uris": [ "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.22.2.jar"],
                    "args": CRIMES_BY_MONTH_ARGS_ARRAY
                    }
}

# 4a. Crimes By Day Spark application args
CRIMES_BY_DAY_ARGS_ARRAY = [f"--projectNbr={PROJECT_NBR} " ,   \
        f"--projectID={PROJECT_ID} ",   \
        f"--reportDirGcsURI={REPORT_CRIMES_DAY_LOCATION}" , \
        f"--reportName=Chicago Crime Trend by Day " ,  \
        f"--reportSQL=SELECT case_day_of_week,count(*) AS crime_count FROM oda_curated_zone.crimes_curated_spark_dataproc GROUP BY case_day_of_week; " , \
        f"--reportPartitionCount=1" , \
        f"--reportTableFQN=oda_product_zone.crimes_by_day_spark_dataproc ",  \
        f"--reportTableDDL=CREATE TABLE IF NOT EXISTS  oda_product_zone.crimes_by_day_spark_dataproc(case_day_of_week string, crime_count long) STORED AS PARQUET LOCATION \"{REPORT_CRIMES_DAY_LOCATION}\"" 
        ]

# 4b. Crimes By Day Spark application conf
CRIMES_BY_DAY_DATAPROC_GCE_JOB_CONFIG = {
    "reference": {"job_id": REPORT_BASE_NM_CRIMES_DAY + "-JOB_ID","project_id": PROJECT_ID},
    "placement": {"cluster_name": DPGCE_CLUSTER_NM},
    "pyspark_job": {"main_python_file_uri": GCS_URI_CRIME_TRENDS_REPORT_PYSPARK,
                    "jar_file_uris": [ "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.22.2.jar"],
                    "args": CRIMES_BY_DAY_ARGS_ARRAY
                    }
}

# 5a. Crimes By Hour Spark application args
CRIMES_BY_HOUR_ARGS_ARRAY = [f"--projectNbr={PROJECT_NBR} " ,   \
        f"--projectID={PROJECT_ID} ",   \
        f"--reportDirGcsURI={REPORT_CRIMES_HOUR_LOCATION}" , \
        f"--reportName=Chicago Crime Trend by Hour " ,  \
        f"--reportSQL=SELECT CAST(case_hour_of_day AS int) case_hour_of_day,count(*) AS crime_count FROM oda_curated_zone.crimes_curated_spark_dataproc GROUP BY case_hour_of_day; " , \
        f"--reportPartitionCount=1",  \
        f"--reportTableFQN=oda_product_zone.crimes_by_hour_spark_dataproc ",  \
        f"--reportTableDDL=CREATE TABLE IF NOT EXISTS oda_product_zone.crimes_by_hour_spark_dataproc(case_hour_of_day int, crime_count long) STORED AS PARQUET LOCATION \"{REPORT_CRIMES_HOUR_LOCATION}\" " 
        ]

# 5b. Crimes By Hour Spark application conf
CRIMES_BY_HOUR_DATAPROC_GCE_JOB_CONFIG = {
    "reference": {"job_id": REPORT_BASE_NM_CRIMES_HOUR + "-JOB_ID","project_id": PROJECT_ID},
    "placement": {"cluster_name": DPGCE_CLUSTER_NM},
    "pyspark_job": {"main_python_file_uri": GCS_URI_CRIME_TRENDS_REPORT_PYSPARK,
                    "jar_file_uris": [ "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.22.2.jar"],
                    "args": CRIMES_BY_HOUR_ARGS_ARRAY
                    }
}


# Build the pipeline
with models.DAG(
    dag_name,
    schedule_interval=None,
    start_date = days_ago(2),
    catchup=False,
) as DAG_DATAPROC_GCE_JOB:

    start = dummy_operator.DummyOperator(
        task_id='start',
        trigger_rule='all_success'
    )
        
    curate_chicago_crimes = DataprocSubmitJobOperator(
        task_id="CURATE_CRIMES",
        project_id=PROJECT_ID,
        region=REGION,
        job=CURATE_CRIMES_DATAPROC_GCE_JOB_CONFIG,
    )
        
    trend_by_year = DataprocSubmitJobOperator(
        task_id="CRIME_TREND_BY_YEAR",
        project_id=PROJECT_ID,
        region=REGION,
        job=CRIMES_BY_YEAR_DATAPROC_GCE_JOB_CONFIG
    )

    trend_by_month =  DataprocSubmitJobOperator(
        task_id="CRIME_TREND_BY_MONTH",
        project_id=PROJECT_ID,
        region=REGION,
        job=CRIMES_BY_MONTH_DATAPROC_GCE_JOB_CONFIG
    )

    trend_by_day = DataprocSubmitJobOperator(
        task_id="CRIME_TREND_BY_DAY",
        project_id=PROJECT_ID,
        region=REGION,
        job=CRIMES_BY_DAY_DATAPROC_GCE_JOB_CONFIG
    )

    trend_by_hour =  DataprocSubmitJobOperator(
        task_id="CRIME_TREND_BY_HOUR",
        project_id=PROJECT_ID,
        region=REGION,
        job=CRIMES_BY_HOUR_DATAPROC_GCE_JOB_CONFIG
    )

    end = dummy_operator.DummyOperator(
        task_id='end',
        trigger_rule='all_done'
    )


start >> curate_chicago_crimes >> [trend_by_year, trend_by_month, trend_by_day, trend_by_hour] >> end


