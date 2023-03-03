from airflow import models
from airflow.operators import dummy_operator
from airflow.providers.google.cloud.operators import bigquery
from airflow.utils import trigger_rule
from datetime import datetime
from airflow.utils.dates import days_ago


PROJECT_ID = models.Variable.get('project_id')


CURATE_CHICAGO_CRIMES = f"""
CREATE OR REPLACE TABLE
  `{PROJECT_ID}.oda_curated_zone.crimes_curated` AS
SELECT
  *,
  CAST(year AS Integer) AS case_year,
  FORMAT_DATE('%B',date) AS case_month,
  FORMAT_DATE('%d',date) AS case_day_of_month,
  FORMAT_DATE('%k',date) AS case_hour,
  EXTRACT(DAYOFWEEK FROM date) AS case_day_of_week_nbr,
  FORMAT_DATE('%A',date) AS case_day_of_week_name
FROM
  oda_raw_zone.crimes_raw;
"""

TREND_BY_YEAR = f"""
CREATE OR REPLACE TABLE
  `{PROJECT_ID}.oda_product_zone.crimes_by_year` AS
SELECT
  case_year,
  COUNT(*) AS crime_count
FROM
  oda_curated_zone.crimes_curated
GROUP BY
  case_year;
"""

TREND_BY_MONTH = f"""
CREATE OR REPLACE TABLE
  `{PROJECT_ID}.oda_product_zone.crimes_by_month` AS
SELECT
  case_month AS month,
  COUNT(*) AS crime_count
FROM
  oda_curated_zone.crimes_curated
GROUP BY
  case_month;
"""


TREND_BY_DAY = f"""
CREATE OR REPLACE TABLE
  `{PROJECT_ID}.oda_product_zone.crimes_by_day` AS
SELECT
  case_day_of_week_name AS day,
  COUNT(*) AS crime_count
FROM
  oda_curated_zone.crimes_curated
GROUP BY
  case_day_of_week_name;
"""


TREND_BY_HOUR = f"""
CREATE OR REPLACE TABLE
  `{PROJECT_ID}.oda_product_zone.crimes_by_hour` AS
SELECT
  case_hour AS hour_of_day,
  COUNT(*) AS crime_count
FROM
  oda_curated_zone.crimes_curated
GROUP BY
  case_hour;
"""

with models.DAG(
        'Chicago_Crime_Trends_From_BQ_With_OOB_Lineage',
        schedule_interval=None,
        start_date = days_ago(2),
        catchup=False) as dag:

    start = dummy_operator.DummyOperator(
        task_id='start',
        trigger_rule='all_success'
    )

    end = dummy_operator.DummyOperator(
        task_id='end',
        trigger_rule='all_done'
    )

    curate_chicago_crimes = bigquery.BigQueryInsertJobOperator(
        task_id="Curate_Chicago_Crimes",
        configuration={
            "query": {
                "query": CURATE_CHICAGO_CRIMES,
                "useLegacySql": False
            }
        }
    )
    
    trend_by_year = bigquery.BigQueryInsertJobOperator(
        task_id="Trend_By_Year",
        configuration={
            "query": {
                "query": TREND_BY_YEAR,
                "useLegacySql": False
            }
        }
    )

    trend_by_month = bigquery.BigQueryInsertJobOperator(
        task_id="Trend_By_Month",
        configuration={
            "query": {
                "query": TREND_BY_MONTH,
                "useLegacySql": False
            }
        }
    )

    trend_by_day = bigquery.BigQueryInsertJobOperator(
        task_id="Trend_By_Day",
        configuration={
            "query": {
                "query": TREND_BY_DAY,
                "useLegacySql": False
            }
        }
    )
 
    trend_by_hour = bigquery.BigQueryInsertJobOperator(
        task_id="Trend_By_Hour",
        configuration={
            "query": {
                "query": TREND_BY_HOUR,
                "useLegacySql": False
            }
        }
    )
  
start >> curate_chicago_crimes >> [trend_by_year, trend_by_month, trend_by_day, trend_by_hour] >> end