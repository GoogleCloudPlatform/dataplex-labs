import datetime
from airflow import models
from airflow.operators import bash
from airflow.operators import email
from airflow.providers.google.cloud.operators import bigquery
from airflow.providers.google.cloud.transfers import bigquery_to_gcs
from airflow.utils import trigger_rule
from airflow.models.baseoperator import chain
import datetime
from airflow.operators import bash
import uuid
import os
from airflow import models
from airflow.models.baseoperator import chain
from airflow.providers.google.cloud.operators.dataplex import (
    DataplexCreateTaskOperator,
    DataplexDeleteTaskOperator,
    DataplexGetTaskOperator,
    DataplexListTasksOperator,
)
from airflow.providers.google.cloud.sensors.dataplex import DataplexTaskStateSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import logging
import io
from airflow.operators import dummy_operator
import google.auth
from requests_oauth2 import OAuth2BearerToken
import requests
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
import time
import json
import csv


# --------------------------------------------------------------------------------
# Set variables
# --------------------------------------------------------------------------------
IMPERSONATION_CHAIN = models.Variable.get('gcp_merchants_sa_acct')
REGION = models.Variable.get('gcp_project_region')
PROJECT_ID_DW = models.Variable.get('gcp_dw_project')
PROJECT_ID_DG = models.Variable.get('gcp_dg_project')
GCP_DG_NUMBER = models.Variable.get('gcp_dg_number')
DATAPLEX_REGION = models.Variable.get('merchant_dplx_region')
LAKE_ID = models.Variable.get('merchant_dplx_lake_id')
ZONE_ID = models.Variable.get('merchant_dplx_zone_id')
DATAPLEX_ENDPOINT = models.Variable.get('dplx_api_end_point')
ENTITY_LIST_FILE_PATH = models.Variable.get('merchant_entity_list_file_path')
TAG_TEMPLATE = models.Variable.get('tag_template_data_product_quality')
TAG_INPUT_FILE = models.Variable.get('merchant_dq_input_file')
TAG_INPUT_PATH = models.Variable.get('merchant_dq_info_input_path')
SUB_NETWORK = models.Variable.get('gcp_sub_net')
TAG_JAR = models.Variable.get('gdc_tag_jar')
DPLX_TASK_PREFIX = "airflow-merch-dq"
R_DPLX_TASK_PREFIX = "airflow-merch-etl"
TAG_MAIN_CLASS = models.Variable.get('data_quality_main_class')

INPUT_DQ_YAML = models.Variable.get('merchant_dq_raw_input_yaml')
BQ_REGION = models.Variable.get('dq_bq_region')
GCP_BQ_DATASET_ID = models.Variable.get('dq_dataset_id')
TARGET_BQ_SUMMARY_TABLE = models.Variable.get('dq_target_summary_table')

#input_tbl_cust = models.Variable.get('input_tbl_cust')
#input_tbl_cc_cust = models.Variable.get('input_tbl_cc_cust')
partition_date = models.Variable.get('partition_date')

IMPERSONATION_CHAIN_DQ= f"f{GCP_DG_NUMBER}-admin-sa@{PROJECT_ID_DG}.iam.gserviceaccount.com"
# --------------------------------------------------------------------------------
# Set default arguments
# --------------------------------------------------------------------------------


yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

default_args = {
    'owner': 'airflow',
    'start_date': yesterday,
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

CREATE_MERCHANT_CORE_DATA_PRODUCT_TABLE = f"""
CREATE TABLE IF NOT EXISTS `{PROJECT_ID_DW}.merchants_data_product.core_merchants`
(
  merchant_id STRING,
  merchant_name STRING,
  mcc INT64,
  email STRING,
  street STRING,
  city STRING,
  state STRING,
  country STRING,
  zip STRING,
  latitude FLOAT64,
  longitude FLOAT64,
  owner_id STRING,
  owner_name STRING,
  terminal_ids STRING,
  Description STRING,
  Market_Segment STRING,
  Industry_Code_Description STRING,
  Industry_Code STRING,
  ingest_date DATE
);
"""

#INSERT_MERCHANT_CORE_DATA_PRODUCT_TABLE = f"""
#INSERT INTO  `{PROJECT_ID_DW}.merchants_data_product.core_merchants`
#SELECT
#merchant_id ,
#  merchant_name ,
#  merchants.mcc as mcc ,
#  email ,
#  street ,
#  CASE
#    WHEN merchants.city is null THEN zip.city
#    ELSE merchants.city 
#    END
#    AS city ,
#  CASE
#    WHEN merchants.state is null THEN zip.state_name
#    ELSE merchants.state 
#    END
#    AS state,
#   CASE
#    WHEN merchants.country is null THEN "USA"
#    ELSE merchants.country 
#    END
#    AS  country ,
#   CASE
#    WHEN merchants.zip is null THEN zip.zip_code
#    ELSE merchants.zip 
#    END
#    AS zip ,
#  latitude ,
#  longitude ,
#  owner_id ,
#  owner_name ,
#  terminal_ids ,
#  description ,
#  market_Segment ,
#  Industry_Code_Descritption as industry_Code_description ,
#  industry_Code ,
#  merchants.ingest_date  as ingest_date

#FROM
#`{PROJECT_ID_DW}.merchants_refined_data.merchants_data` as merchants,
#  `bigquery-public-data.geo_us_boundaries.zip_codes` AS zip
#left outer join 
# `{PROJECT_ID_DW}.merchants_ref_data.mcc_codes` mcc
# on 
# merchants.mcc=mcc.mcc
# WHERE
#  ST_WITHIN(ST_GEOGPOINT(merchants.latitude,
#      merchants.longitude ),
#    zip.zip_code_geom)
#    AND 
#    merchants.ingest_date='{partition_date}';
#"""



INSERT_MERCHANT_CORE_DATA_PRODUCT_TABLE = f"""
INSERT INTO  `{PROJECT_ID_DW}.merchants_data_product.core_merchants`
SELECT
merchant_id ,
  merchant_name ,
  merchants.mcc as mcc ,
  email ,
  street ,
  CASE
    WHEN merchants.city is null THEN NULL
    ELSE merchants.city 
    END
    AS city ,
  CASE
    WHEN merchants.state is null THEN NULL
    ELSE merchants.state 
    END
    AS state,
   CASE
    WHEN merchants.country is null THEN "USA"
    ELSE merchants.country 
    END
    AS  country ,
   CASE
    WHEN merchants.zip is null THEN NULL
    ELSE merchants.zip 
    END
    AS zip ,
  latitude ,
  longitude ,
  owner_id ,
  owner_name ,
  terminal_ids ,
  description ,
  market_Segment ,
  Industry_Code_Descritption as industry_Code_description ,
  industry_Code ,
  merchants.ingest_date  as ingest_date

FROM
`{PROJECT_ID_DW}.merchants_refined_data.merchants_data` as merchants

left outer join 
 `{PROJECT_ID_DW}.merchants_ref_data.mcc_code` mcc
 on 
 merchants.mcc=mcc.mcc
 WHERE

    merchants.ingest_date='{partition_date}';
"""





def get_uuid():
    return str(uuid.uuid4())


def get_clouddq_task_status(task_id):
    """
    This method will return the job status for the task.
    Args:
    Returns: str
    """
    session, headers = get_session_headers()
    res = session.get(
        f"{DATAPLEX_ENDPOINT}/v1/projects/{PROJECT_ID_DG}/locations/{DATAPLEX_REGION}/lakes/{LAKE_ID}/tasks/{task_id}/jobs", headers=headers)
    print(res.status_code)
    print(res.text)
    resp_obj = json.loads(res.text)
    if res.status_code == 200:

        if (
            "jobs" in resp_obj
            and len(resp_obj["jobs"]) > 0
            and "state" in resp_obj["jobs"][0]
        ):
            task_status = resp_obj["jobs"][0]["state"]
            return task_status
    else:
        return "FAILED"


def get_session_headers():
    """
    This method is to get the session and headers object for authenticating the api requests using credentials.
    Args:
    Returns: tuple
    """
    # getting the credentials and project details for gcp project
    credentials, your_project_id = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"])

    # getting request object
    auth_req = google.auth.transport.requests.Request()

    print(credentials.valid)  # logger.debugs False
    credentials.refresh(auth_req)  # refresh token
    # cehck for valid credentials
    print(credentials.valid)  # logger.debugs True
    auth_token = credentials.token
    print(auth_token)

    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + auth_token
    }

    with requests.Session() as session:
        session.auth = OAuth2BearerToken(auth_token)

    return (session, headers)


def _get_dataplex_job_state(**kwargs):
    """
    This method will try to get the status of the job till it is in either 'SUCCEEDED' or 'FAILED' state.
    Args:
    Returns: str
    """
    task_status = get_clouddq_task_status(kwargs['dplx_task_id'])
    while (task_status != 'SUCCEEDED' and task_status != 'FAILED'):
        print(time.ctime())
        time.sleep(30)
        task_status = get_clouddq_task_status(kwargs['dplx_task_id'])
        print(f"Cloud Data Quality tag task status is {task_status}")
    return task_status + "_{}".format(kwargs['entity_val'])


with models.DAG(
        'etl_with_dq_merchant_data_product_wf',
        catchup=False,
        schedule_interval=None,  # datetime.timedelta(days=1),
        default_args=default_args) as dag:

    pre_task_id = "merchant-raw-dq"

    start = dummy_operator.DummyOperator(
        task_id='start',
        trigger_rule='all_success'
    )

    end = dummy_operator.DummyOperator(
        task_id='end',
        trigger_rule='all_done'
    )
    
#    generate_uuid_dq_check = PythonOperator(
#        task_id='{}'.format(pre_task_id),
#        python_callable=get_uuid,
#        trigger_rule='all_success'
#    )
#
#    create_dataplex_dq_check_task = DataplexCreateTaskOperator(
#        task_id='merchant-dq-check-job',
#        project_id=PROJECT_ID_DG,
#        region=REGION,
#        lake_id=LAKE_ID,
#
#        dataplex_task_id=f"{DPLX_TASK_PREFIX}-{{{{ ti.xcom_pull(task_ids='{pre_task_id}', key='return_value') }}}}",
#
#        asynchronous=False,
#        impersonation_chain=IMPERSONATION_CHAIN,
#
#        body={
#                "trigger_spec": {"type_": 'ON_DEMAND'},
#                "execution_spec": {
#                    "service_account": IMPERSONATION_CHAIN,
#                    "args": {
#                        "TASK_ARGS": f"""clouddq-executable.zip, ALL,{INPUT_DQ_YAML}, --gcp_project_id={PROJECT_ID_DG}, --gcp_region_id={BQ_REGION}, --gcp_bq_dataset_id={GCP_BQ_DATASET_ID}, --target_bigquery_summary_table={TARGET_BQ_SUMMARY_TABLE}
#                    """
#                    }
#                },
#            "spark": {
#                    "file_uris": [f"gs://dataplex-clouddq-artifacts-us-central1/clouddq-executable.zip", "gs://dataplex-clouddq-artifacts-us-central1/clouddq-executable.zip.hashsum", f"{INPUT_DQ_YAML}"],
#                    "python_script_file": 'gs://dataplex-clouddq-artifacts-us-central1/clouddq_pyspark_driver.py',
#                    "infrastructure_spec": {"vpc_network": {"sub_network": f"{SUB_NETWORK}"}},
#                    },
#        }
#    )
#
#    dataplex_task_state = BranchPythonOperator(
#        task_id="dataplex_task_state_{}".format(pre_task_id),
#        python_callable=_get_dataplex_job_state,
#        provide_context=True,
#        op_kwargs={
#            'dplx_task_id': f"{DPLX_TASK_PREFIX}-{{{{ ti.xcom_pull(task_ids='{pre_task_id}', key='return_value') }}}}", 'entity_val': f"{pre_task_id}"}
#
#    )
#
#    dataplex_task_success = BashOperator(
#        task_id="SUCCEEDED_{}".format(pre_task_id),
#        bash_command="echo 'Job Completed Successfully'",
#        dag=dag,
#    )
#    dataplex_task_failed = BashOperator(
#        task_id="FAILED_{}".format(pre_task_id),
#        bash_command="echo 'Job Failed'",
#        dag=dag,
#    )
#

    """
    Start of Move Merchant Raw to Refined layer
    """

    r_pre_task_id = "merchant-raw-to-refined-layer"

    r_start = dummy_operator.DummyOperator(
        task_id='r_start',
        trigger_rule='all_success'
    )

    r_end = dummy_operator.DummyOperator(
        task_id='r_end',
        trigger_rule='all_done'
    )

    r_generate_uuid_dq_check = PythonOperator(
        task_id='{}'.format(r_pre_task_id),
        python_callable=get_uuid,
        trigger_rule='all_success'
    )

    create_dataplex_raw_to_refined_task = DataplexCreateTaskOperator(
        task_id='merchant-raw-to-refined-task',
        project_id=PROJECT_ID_DG,
        region=REGION,
        lake_id=LAKE_ID,

        dataplex_task_id=f"{R_DPLX_TASK_PREFIX}-{{{{ ti.xcom_pull(task_ids='{r_pre_task_id}', key='return_value') }}}}",

        asynchronous=False,
        impersonation_chain=IMPERSONATION_CHAIN,

        body={
                "trigger_spec": {"type_": 'ON_DEMAND'},
                "execution_spec": {
                    "service_account": IMPERSONATION_CHAIN,
                    "args": {
                        "TASK_ARGS": f"""--template=DATAPLEXGCSTOBQ,--templateProperty=project.id={PROJECT_ID_DG},--templateProperty=dataplex.gcs.bq.target.dataset=merchants_refined_data,--templateProperty=gcs.bigquery.temp.bucket.name={PROJECT_ID_DG}_dataplex_temp,--templateProperty=dataplex.gcs.bq.save.mode=append,--templateProperty=dataplex.gcs.bq.incremental.partition.copy=yes,--dataplexEntity=projects/{PROJECT_ID_DG}/locations/us-central1/lakes/consumer-banking--merchant--domain/zones/merchant-raw-zone/entities/merchants_data,--partitionField=ingest_date,--partitionType=DAY,--targetTableName=merchants_data,--customSqlGcsPath=gs://{PROJECT_ID_DG}_dataplex_process/code/merchant-source-configs/merchantcustom.sql
                    """
                    }
                },
            "spark": {
                    "file_uris": [f"gs://{PROJECT_ID_DG}_dataplex_process/common/log4j-spark-driver-template.properties"],
                    "main_jar_file_uri": f"gs://{PROJECT_ID_DG}_dataplex_process/common/dataproc-templates-1.0-SNAPSHOT.jar",
                    "main_class":'com.google.cloud.dataproc.templates.main.DataProcTemplate',
                    "infrastructure_spec": {"vpc_network": {"sub_network": f"{SUB_NETWORK}"} , "container_image" : {"java_jars": [f"gs://{PROJECT_ID_DG}_dataplex_process/common/dataproc-templates-1.0-SNAPSHOT.jar"] } },
                    },
        }
    )

    r_dataplex_task_state = BranchPythonOperator(
        task_id="dataplex_task_state_{}".format(r_pre_task_id),
        python_callable=_get_dataplex_job_state,
        provide_context=True,
        op_kwargs={
            'dplx_task_id': f"{R_DPLX_TASK_PREFIX}-{{{{ ti.xcom_pull(task_ids='{r_pre_task_id}', key='return_value') }}}}", 'entity_val': f"{r_pre_task_id}"}

    )

    r_dataplex_task_success = BashOperator(
        task_id="SUCCEEDED_{}".format(r_pre_task_id),
        bash_command="echo 'Job Completed Successfully'",
        dag=dag,
    )
    r_dataplex_task_failed = BashOperator(
        task_id="FAILED_{}".format(r_pre_task_id),
        bash_command="echo 'Job Failed'",
        dag=dag,
    )

    """
    End of Move Customer Raw to Refined layer
    """




    bq_create_merchant_dp_tbl = bigquery.BigQueryInsertJobOperator(
        task_id="bq_create_merchant_dp_tbl",
        impersonation_chain=IMPERSONATION_CHAIN,
        configuration={
            "query": {
                "query": CREATE_MERCHANT_CORE_DATA_PRODUCT_TABLE,
                "useLegacySql": False
            }
        }
    )

    bq_insert_merchant_dp_tbl = bigquery.BigQueryInsertJobOperator(
        task_id="bq_insert_merchant_dp_tbl",
        impersonation_chain=IMPERSONATION_CHAIN,
        configuration={
            "query": {
                "query": INSERT_MERCHANT_CORE_DATA_PRODUCT_TABLE,
                "useLegacySql": False
            }
        }
    )


    # [END composer_bigquery]

    #chain(start >> bq_create_merchant_dp_tbl >> generate_uuid_dq_check >> create_dataplex_dq_check_task >> dataplex_task_state >> [dataplex_task_failed, dataplex_task_success] >> end >> bq_insert_merchant_dp_tbl)

    chain(start >> bq_create_merchant_dp_tbl  >> r_generate_uuid_dq_check >> r_start >> create_dataplex_raw_to_refined_task >> r_dataplex_task_state >> [r_dataplex_task_failed, r_dataplex_task_success] >> r_end >> bq_insert_merchant_dp_tbl >> end)
