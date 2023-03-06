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
DATAPLEX_REGION= models.Variable.get('merchant_dplx_region')
LAKE_ID = models.Variable.get('merchant_dplx_lake_id')
ZONE_ID = models.Variable.get('merchant_dplx_zone_id')
DATAPLEX_ENDPOINT = models.Variable.get('dplx_api_end_point')
ENTITY_LIST_FILE_PATH = models.Variable.get('merchant_entity_list_file_path')
TAG_TEMPLATE = models.Variable.get('tag_template_data_product_quality') 
TAG_INPUT_FILE = models.Variable.get('merchant_dq_input_file')
TAG_INPUT_PATH = models.Variable.get('merchant_dq_info_input_path')
SUB_NETWORK=models.Variable.get('gcp_sub_net')
TAG_JAR=models.Variable.get('gdc_tag_jar')
DPLX_TASK_PREFIX="merchant-quality-tag"
TAG_MAIN_CLASS=models.Variable.get('data_quality_main_class')

#'https://dataplex.googleapis.com'


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
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=5),
}

# --------------------------------------------------------------------------------
# Set task specific variables
# --------------------------------------------------------------------------------


#projects/mdm-dg/locations/us-central1/tagTemplates/data_product_data_quality"

# --------------------------------------------------------------------------------
# Set GCP logging
# --------------------------------------------------------------------------------

logger = logging.getLogger('customer_merchant_dp_info_tag')

def get_uuid():
    return str(uuid.uuid4())

def read_entities_list(table_list_file):
    """
    Reads the table list file that will help in creating Airflow tasks in
    the DAG dynamically.
    :param table_list_file: (String) The file location of the table list file,
    e.g. '/home/airflow/framework/table_list.csv'
    :return table_list: (List) List of tuples containing the source and
    target tables.
    """
    table_list = []
    logger.info('Reading table_list_file from : %s' % str(table_list_file))
    try:
        with io.open(table_list_file, 'rt', encoding='utf-8') as csv_file:
            csv_reader = csv.reader(csv_file)
            next(csv_reader)  # skip the headers
            for row in csv_reader:
                logger.info(row)
                table_tuple = {
                    'entity_name': row[0]
                }
                table_list.append(table_tuple)
            return table_list
    except IOError as e:
        logger.error('Error opening table_list_file %s: ' % str(
            table_list_file), e)

def read_entity_list(entity_list_file):
    """
    Reads the table list file that will help in creating Airflow tasks in
    the DAG dynamically.
    :param entity_list_file: (String) The file location of the table list file,
    e.g. '/home/airflow/framework/entity_list.csv'
    :return entity_list: (List) List of tuples containing the source and
    target tables.
    """
    entity_list = []
    logger.info('Reading entity_list_file from : %s' % str(entity_list_file))
    try:
        with io.open(entity_list_file, 'rt', encoding='utf-8') as file:

            for row in file:
                entity_list.append(row)
            return entity_list
    except IOError as e:
        logger.error('Error opening table_list_file %s: ' % str(
            entity_list_file), e)


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
        print(f"Cloud Data quality tag task status is {task_status}")
    return task_status+ "_{}".format(kwargs['entity_val'])


with models.DAG(
        'data_governance_merchant_quality_tag',
        schedule_interval=None, #datetime.timedelta(days=1),
        default_args=default_args) as dag:
            
    start = dummy_operator.DummyOperator(
        task_id='start',
        trigger_rule='all_success'
    )

    end = dummy_operator.DummyOperator(
        task_id='end',
        trigger_rule='all_success'
    )

    # Get the table list from main file
    all_entities = read_entities_list(ENTITY_LIST_FILE_PATH)

    for row in all_entities:

        entity = row['entity_name']
        logger.info('Generating Tagging tasks for entity id: {}'.format(entity))

        pre_task_id = "gen_uuid_dp_info_tag_" + entity

       # start_op = dummy_operator.DummyOperator(
       # task_id='start_{}'.format(entity),
       # trigger_rule='all_success'
    #)

        generate_uuid_dq_check = PythonOperator(
            task_id='gen_uuid_dp_info_tag_{}'.format(entity),
            python_callable=get_uuid,
            trigger_rule='all_success'
        )
    
        create_dataplex_dp_info_tag_task = DataplexCreateTaskOperator(
            task_id='run_dplx_tag_{}'.format(entity),
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
                    "args": {"TASK_ARGS": f" --tag_template_id={TAG_TEMPLATE}, --project_id={PROJECT_ID_DG},--location={DATAPLEX_REGION}, --lake_id={LAKE_ID},--zone_id={ZONE_ID}, --entity_id={entity},--input_file={TAG_INPUT_FILE}"
                             }
                },
                "spark": {
                    "main_class": f"{TAG_MAIN_CLASS}",
                    "file_uris": [f"{TAG_INPUT_PATH}/{TAG_INPUT_FILE}"],
                    "infrastructure_spec": {"vpc_network": {"sub_network": f"{SUB_NETWORK}"},
                                            "container_image": {
                        "java_jars": [f"{TAG_JAR}"]

                    },
                    },

                }
            }
        )

        dataplex_task_state = BranchPythonOperator(
            task_id="dataplex_task_state_{}".format(entity),
            python_callable=_get_dataplex_job_state,
            provide_context=True,
            op_kwargs={'dplx_task_id': f"{DPLX_TASK_PREFIX}-{{{{ ti.xcom_pull(task_ids='{pre_task_id}', key='return_value') }}}}",'entity_val':f"{entity}"}

        )

        dataplex_task_success = BashOperator(
            task_id="SUCCEEDED_{}".format(entity),
            bash_command="echo 'Job Completed Successfully'",
            dag=dag,
        )
        dataplex_task_failed = BashOperator(
            task_id="FAILED_{}".format(entity),
            bash_command="echo 'Job Failed'",
            dag=dag,
        )

        start >> generate_uuid_dq_check >> create_dataplex_dp_info_tag_task >> dataplex_task_state >> [dataplex_task_success,dataplex_task_failed] >> end
