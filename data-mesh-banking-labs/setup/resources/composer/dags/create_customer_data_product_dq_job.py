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
IMPERSONATION_CHAIN = models.Variable.get('gcp_customer_sa_acct')
REGION = models.Variable.get('gcp_project_region')
PROJECT_ID_DW = models.Variable.get('gcp_dw_project')
PROJECT_ID_DG = models.Variable.get('gcp_dg_project')
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
TAG_MAIN_CLASS = models.Variable.get('data_quality_main_class')
INPUT_DQ_YAML = models.Variable.get('customer_dq_dp_input_yaml')
BQ_REGION = models.Variable.get('dq_bq_region')
GCP_BQ_DATASET_ID = models.Variable.get('dq_dataset_id')
TARGET_BQ_SUMMARY_TABLE = models.Variable.get('dq_target_summary_table')

input_tbl_cust = models.Variable.get('input_tbl_cust')
input_tbl_cc_cust = models.Variable.get('input_tbl_cc_cust')
partition_date = models.Variable.get('partition_date')

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
        'data_governane_dq_customer_data_product_wf',
        catchup=False,
        schedule_interval=None,  # datetime.timedelta(days=1),
        default_args=default_args) as dag:
### Customer Data Product Quality Job 

    pre_task_id = "gen_uuid_dq_job"

    start = dummy_operator.DummyOperator(
        task_id='start',
        trigger_rule='all_success'
    )

    end = dummy_operator.DummyOperator(
        task_id='end',
        trigger_rule='all_done'
    )

    generate_uuid_dq_check = PythonOperator(
        task_id='{}'.format(pre_task_id),
        python_callable=get_uuid,
        trigger_rule='all_success'
    )

    create_dataplex_dq_check_task = DataplexCreateTaskOperator(
        task_id='customer-dq-check-job',
        project_id=PROJECT_ID_DG,
        region=REGION,
        lake_id=LAKE_ID,

        dataplex_task_id=f"{DPLX_TASK_PREFIX}-{{{{ ti.xcom_pull(task_ids='{pre_task_id}', key='return_value') }}}}",

        asynchronous=False,
        impersonation_chain=IMPERSONATION_CHAIN,

        body={
                "trigger_spec": {"type_": 'ON_DEMAND'},
                "execution_spec": {
                    "service_account": IMPERSONATION_CHAIN,
                    "args": {
                        "TASK_ARGS": f"""clouddq-executable.zip, ALL,{INPUT_DQ_YAML}, --gcp_project_id={PROJECT_ID_DG}, --gcp_region_id={BQ_REGION}, --gcp_bq_dataset_id={GCP_BQ_DATASET_ID}, --target_bigquery_summary_table={TARGET_BQ_SUMMARY_TABLE} , --summary_to_stdout
                    """
                    }
                },
            "spark": {
                    "file_uris": [f"gs://dataplex-clouddq-artifacts-us-central1/clouddq-executable.zip", "gs://dataplex-clouddq-artifacts-us-central1/clouddq-executable.zip.hashsum", f"{INPUT_DQ_YAML}"],
                    "python_script_file": 'gs://dataplex-clouddq-artifacts-us-central1/clouddq_pyspark_driver.py',
                    "infrastructure_spec": {"vpc_network": {"sub_network": f"{SUB_NETWORK}"}},
                    },
        }
    )

    dataplex_task_state = BranchPythonOperator(
        task_id="dataplex_task_state_{}".format(pre_task_id),
        python_callable=_get_dataplex_job_state,
        provide_context=True,
        op_kwargs={
            'dplx_task_id': f"{DPLX_TASK_PREFIX}-{{{{ ti.xcom_pull(task_ids='{pre_task_id}', key='return_value') }}}}", 'entity_val': f"{pre_task_id}"}

    )

    dataplex_task_success = BashOperator(
        task_id="SUCCEEDED_{}".format(pre_task_id),
        bash_command="echo 'Job Completed Successfully'",
        dag=dag,
    )
    dataplex_task_failed = BashOperator(
        task_id="FAILED_{}".format(pre_task_id),
        bash_command="echo 'Job Failed'",
        dag=dag,
    )
    chain(start  >> generate_uuid_dq_check >> create_dataplex_dq_check_task >> dataplex_task_state >> [dataplex_task_failed, dataplex_task_success] >> end)




