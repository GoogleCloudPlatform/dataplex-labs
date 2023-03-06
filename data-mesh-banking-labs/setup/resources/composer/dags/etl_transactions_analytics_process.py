import datetime
from airflow import models
from airflow.operators import bash
from airflow.operators import email
from airflow.providers.google.cloud.operators import bigquery
from airflow.providers.google.cloud.transfers import bigquery_to_gcs
from airflow.utils import trigger_rule
from airflow.models.baseoperator import chain

IMPERSONATION_CHAIN = models.Variable.get('gcp_transactions_consumer_sa_acct')
REGION = models.Variable.get('gcp_project_region')
PROJECT_ID_DW = models.Variable.get('gcp_dw_project')
PROJECT_ID_DG = models.Variable.get('gcp_dg_project')
partition_date = models.Variable.get('transactions_partition_date')

CREATE_DP_CC_TRANS_ANALYTICS_DP = f"""
CREATE TABLE IF NOT EXISTS
  `{PROJECT_ID_DW}.cc_analytics_data_product.credit_card_transaction_data` ( 
    originating_event STRUCT<card_read_type STRUCT<code INT,
    entry_mode STRING>,
    trans_type STRUCT<trans_type INT64, 
    value STRING>,
    payment_method STRUCT<pym_type_code INT64,
    pymt_name STRING>,
    swipe_type STRUCT<swipe_code INT64,
    swipe_value STRING>,
    trans_start_ts TIMESTAMP,
    trans_end_ts TIMESTAMP,
    amount STRUCT<trans_amount STRING,
    trans_currency STRING>,
    authorization_response STRUCT<trans_auth_code INT64,
    trans_auth_date FLOAT64,
    origination INT64,
    is_pin_entry INT64,
    is_signed INT64,
    is_unattended INT64>,
    event_ids STRING,
    event STRING>,
    card_info STRUCT<cc_token STRING,
    cc_number INT64,
    cc_expiry STRING,
    cc_provider STRING,
    cc_ccv INT64,
    cc_card_type STRING>,
    merchant_info STRUCT<merchant_id STRING,
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
    terminal_ids STRING>,
    customer_info STRUCT<client_id STRING>,
    version INT64,
    ingest_date DATE )
    PARTITION BY ingest_date;
    """

INSERT_DP_CC_TRANS_DATA = f"""
INSERT INTO
  `{PROJECT_ID_DW}.cc_analytics_data_product.credit_card_transaction_data` (originating_event,
    card_info,
    merchant_info,
    customer_info,
    version,
    ingest_date )
SELECT
  STRUCT( 
    STRUCT(card_read_type as code,
      entry_mode) AS card_read_type,
    STRUCT(trans_type,
      value ) AS trans_type,
    STRUCT(payment_method as pym_type_code,
      pymt_name ) AS payment_method,
    STRUCT(swipe_code,
      swipe_value ) AS swipe_type,
     trans_start_ts,
    trans_end_ts,
    STRUCT(trans_amount,
      trans_currency) AS amount,
    STRUCT(trans_auth_code,
      trans_auth_date,
      origination,
      is_pin_entry,
      is_signed,
      is_unattended) AS authorization_response,
    event_ids,
    event ) AS originating_event,
  STRUCT(auth.cc_token AS cc_token,
    cc_cust.cc_number AS cc_number,
    cc_cust.cc_expiry AS cc_expiry,
    cc_cust.cc_provider AS cc_provider,
    cc_cust.cc_ccv AS cc_ccv,
    cc_cust.cc_card_type AS cc_card_type ) AS card_info,
  STRUCT( merch.merchant_id AS merchant_id,
    merch.merchant_name AS merchant_name,
    merch.mcc AS mcc,
    merch.email AS email,
    merch.street AS street,
    merch.city AS city,
    merch.state AS state,
    merch.country AS country,
    merch.zip AS zip,
    merch.latitude AS lalitude,
    merch.longitude AS longitude,
    merch.owner_id AS owner_id,
    merch.owner_name AS owner_name,
    merch.terminal_ids AS terminal_ids ) AS merchant_info,
  STRUCT(client_id) AS customer_info,
  NULL AS version,
  auth.ingest_date as ingest_date
FROM
  `{PROJECT_ID_DW}.auth_data_product.auth_table` auth
LEFT OUTER JOIN
  `{PROJECT_ID_DW}.customer_data_product.cc_customer_data` cc_cust
ON
  auth.cc_token=cc_cust.token
LEFT OUTER JOIN
  `{PROJECT_ID_DW}.merchants_data_product.core_merchants` merch
ON
  auth.merchant_id = merch.merchant_id
  where auth.ingest_date='{partition_date}';
"""


yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

# [START composer_notify_failure]
default_dag_args = {
    'start_date': yesterday,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=1),
    'project_id': PROJECT_ID_DG,
    'region': REGION,

}

with models.DAG(
        'etl-transactions-analytics-process',
        catchup=False,
        schedule_interval=None,
        default_args=default_dag_args) as dag:


    bq_create_cc_analytics_dp_tbl = bigquery.BigQueryInsertJobOperator(
        task_id="bq_create_cc_analytics_dp_tbl",
        impersonation_chain=IMPERSONATION_CHAIN,
        configuration={
            "query": {
                "query": CREATE_DP_CC_TRANS_ANALYTICS_DP,
                "useLegacySql": False
            }
        }
    )

    bq_insert_cc_analytics_tbl = bigquery.BigQueryInsertJobOperator(
        task_id="bq_insert_cc_analytics_tbl",
        impersonation_chain=IMPERSONATION_CHAIN,
        configuration={
            "query": {
                "query": INSERT_DP_CC_TRANS_DATA,
                "useLegacySql": False
            }
        }
    )
    
    chain(bq_create_cc_analytics_dp_tbl >> bq_insert_cc_analytics_tbl)