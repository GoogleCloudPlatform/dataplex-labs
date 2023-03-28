'''
 Copyright 2022 Google LLC
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
'''



import datetime, time
import json, csv, random, string 
import uuid, os, logging, io
import requests, google.auth

from requests_oauth2 import OAuth2BearerToken
from airflow.operators import dummy_operator
from airflow.operators import bash
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.operators import bigquery
from airflow.providers.google.cloud.operators.dataplex import (
    DataplexCreateTaskOperator,
    DataplexDeleteTaskOperator,
    DataplexGetTaskOperator,
    DataplexListTasksOperator,
)
from airflow.utils import trigger_rule
from airflow import models
from airflow.models.baseoperator import chain


# Variables
PROJECT_ID = models.Variable.get('project_id')
PROJECT_NBR = models.Variable.get('project_nbr')
DATAPLEX_LOCATION = models.Variable.get('region')
BQ_LOCATION = models.Variable.get('region_multi')
SUBNET_URI = f"projects/{PROJECT_ID}/regions/{DATAPLEX_LOCATION}/subnetworks/{models.Variable.get('subnet')}"
UMSA_FQN = f"{models.Variable.get('umsa')}@{PROJECT_ID}.iam.gserviceaccount.com"
LAKE_ID = "oda-lake"
ZONE_ID = "oda-dq-zone"
DATAPLEX_ENDPOINT = "https://dataplex.googleapis.com"


# Dataplex Task Body & Vars
DQ_TASK_YAML_FQP = f"gs://oda-dq-bucket-{PROJECT_NBR}/dq-yaml/customer_master_dq.yaml"
DQ_BQ_REGION = {BQ_LOCATION}
DQ_BQ_DATASET_ID = "oda_dq_scratch_ds"
TARGET_DQ_RESULTS_TABLE = f"{PROJECT_ID}.oda_dq_scratch_ds.dq_results_customer_master"
SOURCE_DQ_TABLE = "customer_master"
GENERATE_UUID_TASK_NM = "Generate_UUID"
DATAPLEX_DQ_TASK_PREFIX="af-cust-master-dq"
DATAPLEX_DQ_TASK_BODY = {
    "spark": {
        "file_uris": [f"gs://dataplex-clouddq-artifacts-us-central1/clouddq-executable.zip", "gs://dataplex-clouddq-artifacts-us-central1/clouddq-executable.zip.hashsum", f"{DQ_TASK_YAML_FQP}"],
        "python_script_file": 'gs://dataplex-clouddq-artifacts-us-central1/clouddq_pyspark_driver.py',
        "infrastructure_spec": {"vpc_network": {"sub_network": f"{SUBNET_URI}"}},
    },
    "execution_spec": {
        "service_account": UMSA_FQN,
        "args": {
            "TASK_ARGS": f"""clouddq-executable.zip, ALL,{DQ_TASK_YAML_FQP}, --gcp_project_id={PROJECT_ID}, --gcp_region_id={BQ_LOCATION}, --gcp_bq_dataset_id={DQ_BQ_DATASET_ID}, --target_bigquery_summary_table={TARGET_DQ_RESULTS_TABLE}, --summary_to_stdout"""
        }
    },

    "trigger_spec": {
        "type_": 'ON_DEMAND'
    },
}

'''
gcloud dataplex tasks create \
--location="${DATAPLEX_LOCATION}" \
--lake="${LAKE_ID}" \
--trigger-type=ON_DEMAND \
--vpc-sub-network-name="lab-snet" \
--execution-service-account=$UMSA_FQN \
--spark-main-class="com.google.cloud.dataplex.templates.dataquality.DataProductQuality" \
--spark-file-uris="$TAG_POPULATION_YAML_FILE_FQP" \
--container-image-java-jars="$TAG_MANAGER_UTIL_JAR_FQP" \
--execution-args=^::^TASK_ARGS="--tag_template_id=projects/${PROJECT_ID}/locations/${DATAPLEX_LOCATION}/tagTemplates/data_product_quality, --project_id=${PROJECT_ID},--location=${DATAPLEX_LOCATION},--lake_id=$LAKE_ID,--zone_id=$DATA_QUALITY_ZONE_ID,--entity_id=customer_master,--input_file=data-quality-scores-tag.yaml" \
"customer-master-populate-tags-$RANDOM"

'''


# Dataplex Tagging Task Body & Vars
DATAPLEX_TAGGING_TASK_PREFIX="af-cust-master-dq-tag"
TAGGER_MAIN_CLASS="com.google.cloud.dataplex.templates.dataquality.DataProductQuality"
TAG_MANAGER_JAR_FQP = f"gs://oda-dq-bucket-{PROJECT_NBR}/dq-utils/tagmanager-1.0-SNAPSHOT.jar"
TAG_POPULATION_TEMPLATE_FILE_FQP = f"gs://oda-dq-bucket-{PROJECT_NBR}/dq-tag-population-yaml/data-quality-scores-tag.yaml"
TAG_POPULATION_TEMPLATE_FILE_NM = "data-quality-scores-tag.yaml"
TAG_TEMPLATE_RESOURCE_URI = f"projects/{PROJECT_ID}/locations/{DATAPLEX_LOCATION}/tagTemplates/data_product_quality"
DATAPLEX_TAGGING_TASK_BODY = {
    "spark": {
        "main_class": f"{TAGGER_MAIN_CLASS}",
        "file_uris": [f"{TAG_POPULATION_TEMPLATE_FILE_FQP}"],
        "infrastructure_spec": {
            "vpc_network": {"sub_network": f"{SUBNET_URI}"},
            "container_image": {"java_jars": [f"{TAG_MANAGER_JAR_FQP}"]},
        }
    },
    "execution_spec": {
        "service_account": UMSA_FQN,
        "args": {"TASK_ARGS": f"--tag_template_id={TAG_TEMPLATE_RESOURCE_URI}, --project_id={PROJECT_ID},--location={DATAPLEX_LOCATION},--lake_id={LAKE_ID},--zone_id={ZONE_ID},--entity_id=customer_master,--input_file={TAG_POPULATION_TEMPLATE_FILE_NM}"}
    },
    "trigger_spec": {"type_": 'ON_DEMAND'}
}


# Other
YESTERDAY = datetime.datetime.combine(datetime.datetime.today() - datetime.timedelta(1),datetime.datetime.min.time())


# DAG Args
default_args = {
    'owner': 'airflow',
    'start_date': YESTERDAY,
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

# Async Dataplex Task Polling function 1 - DO NOT ALTER
def get_clouddq_task_status(task_id):
    """
    This method will return the job status for the task.
    Args:
    Returns: str
    """
    session, headers = get_session_headers()
    res = session.get(
        f"{DATAPLEX_ENDPOINT}/v1/projects/{PROJECT_ID}/locations/{DATAPLEX_LOCATION}/lakes/{LAKE_ID}/tasks/{task_id}/jobs", headers=headers)
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

# Async Dataplex Task Polling function 2 - DO NOT ALTER
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
    # check for valid credentials
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


# Async Dataplex Task Polling function 3 - DO NOT ALTER
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


# Dataproc Serverless (execution env for Dataplex Task requires a unique ID per task)
def get_uuid():
    return str(uuid.uuid4())


# DAG
# {{
with models.DAG(
        'Customer_Master_Data_Quality_E2E_Workflow',
        catchup=False,
        schedule_interval=None, 
        default_args=default_args) as dag:



    start = dummy_operator.DummyOperator(
        task_id='start',
        trigger_rule='all_success'
    )

    end = dummy_operator.DummyOperator(
        task_id='end',
        trigger_rule='all_done'
    )

    generate_uuid_for_dq_task = PythonOperator(
        task_id='{}'.format(GENERATE_UUID_TASK_NM),
        python_callable=get_uuid,
        trigger_rule='all_success'
    )


    submit_dataplex_dq_task = DataplexCreateTaskOperator(
        task_id='Validate_Customer_Master_Data_Quality',
        project_id=PROJECT_ID,
        region=DATAPLEX_LOCATION,
        lake_id=LAKE_ID,
        dataplex_task_id=f"{DATAPLEX_DQ_TASK_PREFIX}-{{{{ ti.xcom_pull(task_ids='{GENERATE_UUID_TASK_NM}', key='return_value') }}}}",
        body=DATAPLEX_DQ_TASK_BODY
    )


    poll_async_dataplex_dq_task = BranchPythonOperator(
        task_id="Poll_Async_Data_Quality_Task",
        python_callable=_get_dataplex_job_state,
        provide_context=True,
        op_kwargs={
            'dplx_task_id': f"{DATAPLEX_DQ_TASK_PREFIX}-{{{{ ti.xcom_pull(task_ids='{GENERATE_UUID_TASK_NM}', key='return_value') }}}}", 'entity_val': f"{DATAPLEX_DQ_TASK_PREFIX}"}
    )

    dataplex_dq_task_success = BashOperator(
        task_id="SUCCEEDED_{}".format(DATAPLEX_DQ_TASK_PREFIX),
        bash_command="echo 'Job Completed Successfully'",
        dag=dag,
    )

    dataplex_dq_task_failed = BashOperator(
        task_id="FAILED_{}".format(DATAPLEX_DQ_TASK_PREFIX),
        bash_command="echo 'Job Failed'",
        dag=dag,
    )


    submit_dataplex_tagging_task = DataplexCreateTaskOperator(
        task_id='Publish_Data_Quaity_Scores_To_Tags',
        project_id=PROJECT_ID,
        region=DATAPLEX_LOCATION,
        lake_id=LAKE_ID,
        dataplex_task_id=f"{DATAPLEX_TAGGING_TASK_PREFIX}-{{{{ ti.xcom_pull(task_ids='{GENERATE_UUID_TASK_NM}', key='return_value') }}}}",
        body=DATAPLEX_TAGGING_TASK_BODY
    )


    poll_async_dataplex_tagging_task = BranchPythonOperator(
        task_id="Poll_Async_Tagging_Task",
        python_callable=_get_dataplex_job_state,
        provide_context=True,
        op_kwargs={
            'dplx_task_id': f"{DATAPLEX_TAGGING_TASK_PREFIX}-{{{{ ti.xcom_pull(task_ids='{GENERATE_UUID_TASK_NM}', key='return_value') }}}}", 'entity_val': f"{DATAPLEX_TAGGING_TASK_PREFIX}"}
    )

    dataplex_tagging_task_success = BashOperator(
        task_id="SUCCEEDED_{}".format(DATAPLEX_TAGGING_TASK_PREFIX),
        bash_command="echo 'Job Completed Successfully'",
        dag=dag,
    )
    dataplex_tagging_task_failed = BashOperator(
        task_id="FAILED_{}".format(DATAPLEX_TAGGING_TASK_PREFIX),
        bash_command="echo 'Job Failed'",
        dag=dag,
    )


    start  >> generate_uuid_for_dq_task >> submit_dataplex_dq_task >> poll_async_dataplex_dq_task >> [dataplex_dq_task_failed, dataplex_dq_task_success] 
    dataplex_dq_task_success >> submit_dataplex_tagging_task >> poll_async_dataplex_tagging_task >> [dataplex_tagging_task_failed, dataplex_tagging_task_success] >> end

# }} End of DAG
