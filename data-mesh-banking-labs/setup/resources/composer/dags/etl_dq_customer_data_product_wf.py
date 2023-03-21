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

#from airflow.composer.data_lineage.entities import BigQueryTable

#from airflow.lineage import AUTO


# --------------------------------------------------------------------------------
# Set variables
# --------------------------------------------------------------------------------
IMPERSONATION_CHAIN = models.Variable.get('gcp_customer_sa_acct')
REGION = models.Variable.get('gcp_project_region')
PROJECT_ID_DW = models.Variable.get('gcp_dw_project')
PROJECT_ID_DG = models.Variable.get('gcp_dg_project')
GCP_DG_NUMBER = models.Variable.get('gcp_dg_number')
DATAPLEX_REGION = models.Variable.get('customer_dplx_region')
LAKE_ID = models.Variable.get('customer_dplx_lake_id')
ZONE_ID = models.Variable.get('customer_dplx_zone_id')
DATAPLEX_ENDPOINT = models.Variable.get('dplx_api_end_point')
ENTITY_LIST_FILE_PATH = models.Variable.get('cust_entity_list_file_path')
TAG_TEMPLATE = models.Variable.get('tag_template_data_product_quality')
TAG_INPUT_FILE = models.Variable.get('customer_dq_input_file')
TAG_INPUT_PATH = models.Variable.get('customer_dq_info_input_path')
SUB_NETWORK = models.Variable.get('gcp_sub_net')
TAG_JAR = models.Variable.get('gdc_tag_jar')
DPLX_TASK_PREFIX = "airflow-cust-dq"
R_DPLX_TASK_PREFIX = "airflow-cust-raw-refined"
TAG_MAIN_CLASS = models.Variable.get('data_quality_main_class')

INPUT_DQ_YAML = models.Variable.get('customer_dq_raw_input_yaml')
BQ_REGION = models.Variable.get('dq_bq_region')
GCP_BQ_DATASET_ID = models.Variable.get('dq_dataset_id')
TARGET_BQ_SUMMARY_TABLE = models.Variable.get('dq_target_summary_table')

input_tbl_cust = models.Variable.get('input_tbl_cust')
input_tbl_cc_cust = models.Variable.get('input_tbl_cc_cust')
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

CUST_REF_DATA_TABLE = f"""
CREATE TABLE IF NOT EXISTS `{PROJECT_ID_DW}.customer_refined_data.{input_tbl_cust}`
(
  client_id STRING,
  ssn STRING,
  first_name STRING,
  last_name STRING,
  gender STRING,
  street STRING,
  city STRING,
  state STRING,
  zip INT64,
  latitude FLOAT64,
  longitude FLOAT64,
  city_pop INT64,
  job STRING,
  dob STRING,
  email STRING,
  phonenum STRING,
  profile STRING,
  dt STRING,
  ingest_date DATE
)
PARTITION BY ingest_date;

CREATE TABLE IF NOT EXISTS `{PROJECT_ID_DW}.customer_refined_data.customer_keysets`
(
  client_id STRING,
  ssn STRING,
  first_name STRING,
  last_name STRING,
  gender STRING,
  street STRING,
  city STRING,
  state STRING,
  zip INT64,
  latitude FLOAT64,
  longitude FLOAT64,
  city_pop INT64,
  job STRING,
  dob DATE,
  email STRING,
  phonenum STRING,
  keyset BYTES,
  ingest_date DATE
)
PARTITION BY ingest_date;

        """

CUST_DATA_PRODUCT_TABLE = f"""
CREATE TABLE IF NOT EXISTS `{PROJECT_ID_DW}.customer_data_product.customer_data`
(
  client_id STRING,
  ssn STRING,
  first_name STRING,
  middle_name INT64,
  last_name STRING,
  dob DATE,
  gender STRING,
  address_with_history ARRAY<STRUCT<status STRING, street STRING, city STRING, state STRING, zip_code INT64, WKT GEOGRAPHY, modify_date INT64>>,
  phone_num ARRAY<STRUCT<primary STRING, secondary INT64, modify_date INT64>>,
  email ARRAY<STRUCT<status STRING, primary STRING, secondary INT64, modify_date INT64>>, 
  ingest_date DATE
)
        """

CUST_TOKENIZED_DATA_PRODUCT_TABLE = f"""
CREATE TABLE IF NOT EXISTS  `{PROJECT_ID_DW}.customer_data_product.tokenized_customer_data`
(
  client_id STRING,
  ssn STRING,
  first_name STRING,
  middle_name INT64,
  last_name STRING,
  dob STRING,
  gender STRING,
  address_with_history ARRAY<STRUCT<status STRING, street STRING, city STRING, state STRING, zip_code STRING, WKT STRING, modify_date INT64>>,
  phone_num ARRAY<STRUCT<primary STRING, secondary INT64, modify_date INT64>>,
  email ARRAY<STRUCT<status STRING, primary STRING, secondary INT64, modify_date INT64>>,
  ingest_date DATE
)

        """

CC_CUST_REF_TABLE = f"""
CREATE TABLE IF NOT EXISTS  `{PROJECT_ID_DW}.customer_refined_data.{input_tbl_cc_cust}`
  (
  cc_number INT64,
  cc_expiry STRING,
  cc_provider STRING,
  cc_ccv INT64,
  cc_card_type STRING,
  client_id STRING,
  token STRING,
  dt STRING,
  ingest_date DATE )
PARTITION BY
  ingest_date
OPTIONS(
  partition_expiration_days=365,
  require_partition_filter=false
)

        """

CC_CUST_DATA_PRODUCT_TABLE = f"""
CREATE TABLE IF NOT EXISTS  `{PROJECT_ID_DW}.customer_data_product.cc_customer_data`
(
  cc_number INT64,
  cc_expiry STRING,
  cc_provider STRING,
  cc_ccv INT64,
  cc_card_type STRING,
  client_id STRING,
  token STRING,
  ingest_date DATE )

        """

CUST_DP_INSERT = f"""
INSERT INTO
  `{PROJECT_ID_DW}.customer_data_product.customer_data`
SELECT
  client_id AS client_id,
  ssn AS ssn,
  first_name AS first_name,
  NULL AS middle_name,
  last_name AS last_name,
  PARSE_DATE("%F",
    dob) AS dob,
    gender,
  [STRUCT('current' AS status,
    cdd.street AS street,
    cdd.city,
    cdd.state,
    cdd.zip AS zip_code,
    ST_GeogPoint(cdd.latitude,
      cdd.longitude) AS WKT,
    NULL AS modify_date)] AS address_with_history,
  [STRUCT(cdd.phonenum AS primary,
    NULL AS secondary,
    NULL AS modify_date)] AS phone_num,
  [STRUCT('current' AS status,
    cdd.email AS primary,
    NULL AS secondary,
    NULL AS modify_date)] AS email,
  ingest_date AS ingest_date
FROM (
  SELECT
    * EXCEPT(rownum)
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY client_id, ssn, first_name, last_name, gender, street, city, state, zip, city_pop, job, dob, email, phonenum, profile ORDER BY client_id ) rownum
    FROM
      `{PROJECT_ID_DW}.customer_refined_data.{input_tbl_cust}`
    WHERE
      ingest_date='{partition_date}' )
  WHERE
    rownum = 1 ) cdd;
    """

CUST_PRIVATE_KEYSET = f"""
INSERT INTO  `{PROJECT_ID_DW}.customer_refined_data.customer_keysets`
SELECT 
client_id as client_id,
  ssn as ssn,
  first_name  as first_name,
  last_name as last_name,
  gender as gender,
  street as street,
  city as city,
  state as state,
  zip as zip,
  latitude as latitude,
  longitude as longitude,
  city_pop as city_pop,
  job as job,
  PARSE_DATE("%F",dob) as dob,
  email as email,
  phonenum as phonenum,
KEYS.NEW_KEYSET('AEAD_AES_GCM_256') AS keyset ,
ingest_date  as ingest_date

FROM
( SELECT 
distinct client_id ,
  ssn ,
  first_name ,
  last_name ,
  gender ,
  street ,
  city ,
  state ,
  zip ,
  latitude ,
  longitude ,
  city_pop ,
  job ,
  dob ,
  email ,
  phonenum ,
  profile,
  ingest_date
  from 
  `{PROJECT_ID_DW}.customer_refined_data.{input_tbl_cust}` where ingest_date='{partition_date}') cdd
  ;


"""

TOKENIZED_CUST_DATA = f"""
INSERT INTO  `{PROJECT_ID_DW}.customer_data_product.tokenized_customer_data`
SELECT
  TO_BASE64(AEAD.ENCRYPT(keyset,'dummy_value',cast(client_id as String)))  AS client_id,
  TO_BASE64(AEAD.ENCRYPT(keyset,'dummy_value',cast(ssn  as String)) ) AS ssn,
  TO_BASE64(AEAD.ENCRYPT(keyset,'dummy_value',cast(first_name as String)) ) AS first_name,
  NULL AS middle_name,
  TO_BASE64(AEAD.ENCRYPT(keyset,'dummy_value',cast(last_name  as String)) ) AS last_name,
  TO_BASE64 (AEAD.ENCRYPT(keyset,'dummy_value',cast(dob as String)) ) AS dob,
  TO_BASE64 (AEAD.ENCRYPT(keyset,'dummy_value',cast(gender as String)) ) as gender,
  [STRUCT('current' AS status,
    TO_BASE64(AEAD.ENCRYPT(keyset,'dummy_value',cast(cdk.street as String)) ) AS street,
    TO_BASE64(AEAD.ENCRYPT(keyset,'dummy_value',cast(cdk.city as String)) ) AS city,
    TO_BASE64(AEAD.ENCRYPT(keyset,'dummy_value',cast(cdk.state  as String)) ) AS state,
    TO_BASE64(AEAD.ENCRYPT(keyset,'dummy_value',cast(cdk.zip as String)) ) AS zip_code,
    TO_BASE64(AEAD.ENCRYPT(keyset,'dummy_value',cast(ST_GEOHASH(ST_GeogPoint(cdk.latitude, cdk.longitude))  as String)) ) as WKT,
    null AS modify_date)] AS address_with_history,
  [STRUCT( TO_BASE64(AEAD.ENCRYPT(keyset,'dummy_value',cast(cdk.phonenum  as String)) ) AS primary,
    NULL AS secondary,
    NULL AS modify_date)] AS phone_num,
  [STRUCT('current' AS status,
    TO_BASE64(AEAD.ENCRYPT(keyset,'dummy_value',cast(cdk.email  as String)) ) AS primary,
    NULL AS secondary,
    NULL AS modify_date)] AS email,
      ingest_date as ingest_date

FROM
`{PROJECT_ID_DW}.customer_refined_data.customer_keysets` cdk where ingest_date='{partition_date}';
"""

CC_CUST_DATA = f"""
INSERT INTO  `{PROJECT_ID_DW}.customer_data_product.cc_customer_data`
SELECT 
  cc_number ,
  cc_expiry ,
  cc_provider ,
  cc_ccv ,
  cc_card_type ,
  client_id ,
  token ,
  ingest_date 
  from 
  `{PROJECT_ID_DW}.customer_refined_data.{input_tbl_cc_cust}`
where 
ingest_date='{partition_date}';
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
        'etl_with_dq_customer_data_product_wf',
        catchup=False,
        schedule_interval=None,  # datetime.timedelta(days=1),
        default_args=default_args) as dag:

    """
    Start of Move Customer Raw to Refined layer
    """

    r_pre_task_id = "customer-raw-to-refined-layer"

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
        task_id='customer-raw-to-refined-task',
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
                        "TASK_ARGS": f"""--template=DATAPLEXGCSTOBQ,--templateProperty=project.id={PROJECT_ID_DG},--templateProperty=dataplex.gcs.bq.target.dataset=customer_refined_data,--templateProperty=gcs.bigquery.temp.bucket.name={PROJECT_ID_DG}_dataplex_temp,--templateProperty=dataplex.gcs.bq.save.mode=append,--templateProperty=dataplex.gcs.bq.incremental.partition.copy=yes,--dataplexEntity=projects/{PROJECT_ID_DG}/locations/us-central1/lakes/consumer-banking--customer--domain/zones/customer-raw-zone/entities/customers_data,--partitionField=ingest_date,--partitionType=DAY,--targetTableName=customers_data,--customSqlGcsPath=gs://{PROJECT_ID_DG}_dataplex_process/code/customer-source-configs/customercustom.sql
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


    bq_create_customer_ref_tbl = bigquery.BigQueryInsertJobOperator(
        task_id="bq_create_customer_ref_tbl",
        impersonation_chain=IMPERSONATION_CHAIN,
        configuration={
            "query": {
                "query": CUST_REF_DATA_TABLE,
                "useLegacySql": False
            }
        }
       # ,
       # inlets=[BigQueryTable(
       # project_id=PROJECT_ID_DG,
       # dataset_id='customer_raw_zone',
       # table_id='customers_data',
       # )],
       # outlets=[BigQueryTable(
       # project_id=PROJECT_ID_DG,
       # dataset_id='customer_refined_data',
       # table_id='customers_data',
       # )]
    )

    bq_create_customer_dp_tbl = bigquery.BigQueryInsertJobOperator(
        task_id="bq_create_customer_dp_tbl",
        impersonation_chain=IMPERSONATION_CHAIN,
        configuration={
            "query": {
                "query": CUST_DATA_PRODUCT_TABLE,
                "useLegacySql": False
            }
        }
    )

    bq_create_tokenized_customer_dp_tbl = bigquery.BigQueryInsertJobOperator(
        task_id="bq_create_tokenized_customer_dp_tbl",
        impersonation_chain=IMPERSONATION_CHAIN,
        configuration={
            "query": {
                "query": CUST_TOKENIZED_DATA_PRODUCT_TABLE,
                "useLegacySql": False
            }
        }
    )

    bq_create_cc_customer_ref_tbl = bigquery.BigQueryInsertJobOperator(
        task_id="bq_create_cc_customer_ref_tbl",
        impersonation_chain=IMPERSONATION_CHAIN,
        configuration={
            "query": {
                "query": CC_CUST_REF_TABLE,
                "useLegacySql": False
            }
        }
    )

    bq_create_cc_customer_dp_tbl = bigquery.BigQueryInsertJobOperator(
        task_id="bq_create_cc_customer_dp_tbl",
        impersonation_chain=IMPERSONATION_CHAIN,
        configuration={
            "query": {
                "query": CC_CUST_DATA_PRODUCT_TABLE,
                "useLegacySql": False
            }
        }
    )


    bq_insert_customer_dp_tbl = bigquery.BigQueryInsertJobOperator(
        task_id="bq_insert_customer_dp_tbl",
        impersonation_chain=IMPERSONATION_CHAIN,
        configuration={
            "query": {
                "query": CUST_DP_INSERT,
                "useLegacySql": False
            }
        }
    )

    bq_insert_customer_keyset_dp_tbl = bigquery.BigQueryInsertJobOperator(
        task_id="bq_insert_customer_keyset_dp_tbl",
        impersonation_chain=IMPERSONATION_CHAIN,
        configuration={
            "query": {
                "query": CUST_PRIVATE_KEYSET,
                "useLegacySql": False
            }
        }
    )

    bq_insert_customer_tokenized_dp_tbl = bigquery.BigQueryInsertJobOperator(
        task_id="bq_insert_customer_tokenized_dp_tbl",
        impersonation_chain=IMPERSONATION_CHAIN,
        configuration={
            "query": {
                "query": TOKENIZED_CUST_DATA,
                "useLegacySql": False
            }
        }
    )

    bq_insert_cc_customer_dp_tbl = bigquery.BigQueryInsertJobOperator(
        task_id="bq_insert_cc_customer_dp_tbl",
        impersonation_chain=IMPERSONATION_CHAIN,
        configuration={
            "query": {
                "query": CC_CUST_DATA,
                "useLegacySql": False
            }
        }
    )

    # [END composer_bigquery]
    
    chain( bq_create_customer_ref_tbl >> bq_create_customer_dp_tbl >> bq_create_tokenized_customer_dp_tbl >> bq_create_cc_customer_ref_tbl >> bq_create_cc_customer_dp_tbl >> r_generate_uuid_dq_check >> r_start >> create_dataplex_raw_to_refined_task >> r_dataplex_task_state >> [r_dataplex_task_failed, r_dataplex_task_success] >> r_end >>  bq_insert_customer_dp_tbl >> bq_insert_customer_keyset_dp_tbl >> bq_insert_customer_tokenized_dp_tbl >> bq_insert_cc_customer_dp_tbl)