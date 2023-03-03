/**
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/******************************************
Local variables declaration
 *****************************************/

locals {
project_id                  = "${var.project_id}"
project_nbr                 = "${var.project_number}"
admin_upn_fqn               = "${var.gcp_account_name}"
location                    = "${var.gcp_region}"
location_multi              = "${var.gcp_multi_region}"
zone                        = "${var.gcp_zone}"
umsa                        = "lab-sa"
umsa_fqn                    = "${local.umsa}@${local.project_id}.iam.gserviceaccount.com"

lab_dpms_nm                 = "lab-dpms-${local.project_nbr}"
lab_spark_bucket            = "lab-spark-bucket-${local.project_nbr}"
lab_spark_bucket_fqn        = "gs://dew-lab-spark-${local.project_nbr}"
lab_vpc_nm                  = "lab-vpc-${local.project_nbr}"
lab_subnet_nm               = "lab-snet"
lab_subnet_cidr             = "10.0.0.0/16"

lab_sensitive_data_bucket_raw= "raw-data-sensitive-${local.project_nbr}"

lab_data_bucket_raw         = "raw-data-${local.project_nbr}"
lab_code_bucket             = "raw-code-${local.project_nbr}"
lab_notebook_bucket         = "raw-notebook-${local.project_nbr}"
lab_model_bucket            = "raw-model-${local.project_nbr}"
lab_bundle_bucket           = "raw-model-mleap-bundle-${local.project_nbr}"
lab_metrics_bucket          = "raw-model-metrics-${local.project_nbr}"
lab_scheduled_output_bucket = "scheduled-runs-output-${local.project_nbr}"

lab_data_bucket_curated     = "curated-data-${local.project_nbr}"
lab_data_bucket_product = "product-data-${local.project_nbr}"

CC_GMSA_FQN                 = "service-${local.project_nbr}@cloudcomposer-accounts.iam.gserviceaccount.com"
GCE_GMSA_FQN                = "${local.project_nbr}-compute@developer.gserviceaccount.com"
CLOUD_COMPOSER2_IMG_VERSION = "${var.cloud_composer_image_version}"
bq_connector_jar_gcs_uri    = "${var.bq_connector_jar_gcs_uri}"

}

/******************************************
1. Enable Google APIs in parallel
 *****************************************/

 module "activate_service_apis" {
  source                      = "terraform-google-modules/project-factory/google//modules/project_services"
  project_id                     = var.project_id
  enable_apis                 = true

  activate_apis = [
    "compute.googleapis.com",
    "dataproc.googleapis.com",
    "bigqueryconnection.googleapis.com",
    "bigquerydatapolicy.googleapis.com",
    "storage-component.googleapis.com",
    "bigquerystorage.googleapis.com",
    "datacatalog.googleapis.com",
    "dataplex.googleapis.com",
    "bigquery.googleapis.com" ,
    "cloudresourcemanager.googleapis.com",
    "cloudidentity.googleapis.com",
    "storage.googleapis.com",
    "composer.googleapis.com",
    "metastore.googleapis.com",
    "orgpolicy.googleapis.com",
    "dlp.googleapis.com",
    "logging.googleapis.com",
    "monitoring.googleapis.com",
    "dataplex.googleapis.com",
    "datacatalog.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "datapipelines.googleapis.com",
    "cloudscheduler.googleapis.com",
    "datalineage.googleapis.com"
    ]

  disable_services_on_destroy = false
}
/*******************************************
Introducing sleep to minimize errors from
dependencies having not completed
********************************************/

resource "time_sleep" "sleep_after_activate_service_apis" {
  create_duration = "60s"

  depends_on = [
    module.activate_service_apis
  ]
}

/******************************************
2. Project-scoped Org Policy Updates
*****************************************/

resource "google_project_organization_policy" "bool-policies" {
  for_each = {
    "compute.requireOsLogin" : false,
    "compute.disableSerialPortLogging" : false,
    "compute.requireShieldedVm" : false
  }
  project    = var.project_id
  constraint = format("constraints/%s", each.key)
  boolean_policy {
    enforced = each.value
  }

  depends_on = [
    time_sleep.sleep_after_activate_service_apis
  ]

}

resource "google_project_organization_policy" "list_policies" {
  for_each = {
    "compute.vmCanIpForward" : true,
    "compute.vmExternalIpAccess" : true,
    "compute.restrictVpcPeering" : true
  }
  project     = var.project_id
  constraint = format("constraints/%s", each.key)
  list_policy {
    allow {
      all = each.value
    }
  }

  depends_on = [
    time_sleep.sleep_after_activate_service_apis
  ]

}

/*******************************************
Introducing sleep to minimize errors from
dependencies having not completed
********************************************/
resource "time_sleep" "sleep_after_apis_and_org_policies" {
  create_duration = "60s"

  depends_on = [
    google_project_organization_policy.bool-policies,
    google_project_organization_policy.list_policies,
    time_sleep.sleep_after_activate_service_apis
  ]
}

/******************************************
3. Create User Managed Service Account 
 *****************************************/
module "umsa_creation" {
  source     = "terraform-google-modules/service-accounts/google"
  project_id = local.project_id
  names      = ["${local.umsa}"]
  display_name = "User Managed Service Account"
  description  = "User Managed Service Account for Dataplex lab"
   depends_on = [time_sleep.sleep_after_apis_and_org_policies]
}

/******************************************
4a. Grant IAM roles to User Managed Service Account
 *****************************************/

module "umsa_role_grants" {
  source                  = "terraform-google-modules/iam/google//modules/member_iam"
  service_account_address = "${local.umsa_fqn}"
  prefix                  = "serviceAccount"
  project_id              = local.project_id
  project_roles = [
    
    "roles/iam.serviceAccountUser",
    "roles/iam.serviceAccountTokenCreator",
    "roles/storage.objectAdmin",
    "roles/storage.admin",
    "roles/metastore.admin",
    "roles/metastore.editor",
    "roles/metastore.user",
    "roles/metastore.metadataEditor",
    "roles/dataproc.worker",
    "roles/dataproc.editor",
    "roles/bigquery.dataEditor",
    "roles/bigquery.admin",
    "roles/viewer",
    "roles/composer.worker",
    "roles/composer.admin",
    "roles/serviceusage.serviceUsageConsumer"
  ]
  depends_on = [
    module.umsa_creation
  ]
}

# IAM role grants to Google Managed Service Account for Cloud Composer 2
module "gmsa_role_grants_cc" {
  source                  = "terraform-google-modules/iam/google//modules/member_iam"
  service_account_address = "${local.CC_GMSA_FQN}"
  prefix                  = "serviceAccount"
  project_id              = local.project_id
  project_roles = [
    
    "roles/composer.ServiceAgentV2Ext",
  ]
  depends_on = [
    module.umsa_role_grants
  ]
}

# IAM role grants to Google Managed Service Account for Compute Engine (for Cloud Composer 2 to download images)
module "gmsa_role_grants_gce" {
  source                  = "terraform-google-modules/iam/google//modules/member_iam"
  service_account_address = "${local.GCE_GMSA_FQN}"
  prefix                  = "serviceAccount"
  project_id              = local.project_id
  project_roles = [
    
    "roles/editor",
  ]
  depends_on = [
    module.umsa_role_grants
  ]
}


/******************************************************
5. Grant Service Account Impersonation privilege to yourself/Admin User
 ******************************************************/

module "umsa_impersonate_privs_to_admin" {
  source  = "terraform-google-modules/iam/google//modules/service_accounts_iam/"
  service_accounts = ["${local.umsa_fqn}"]
  project          = local.project_id
  mode             = "additive"
  bindings = {
    "roles/iam.serviceAccountUser" = [
      "user:${local.admin_upn_fqn}"
    ],
    "roles/iam.serviceAccountTokenCreator" = [
      "user:${local.admin_upn_fqn}"
    ]

  }
  depends_on = [
    module.umsa_creation
  ]
}

/******************************************************
6. Grant IAM roles to Admin User/yourself
 ******************************************************/

module "administrator_role_grants" {
  source   = "terraform-google-modules/iam/google//modules/projects_iam"
  projects = ["${local.project_id}"]
  mode     = "additive"

  bindings = {
    "roles/storage.admin" = [
      "user:${local.admin_upn_fqn}",
    ]
    "roles/metastore.admin" = [

      "user:${local.admin_upn_fqn}",
    ]
    "roles/dataproc.admin" = [
      "user:${local.admin_upn_fqn}",
    ]
    "roles/bigquery.admin" = [
      "user:${local.admin_upn_fqn}",
    ]
    "roles/bigquery.user" = [
      "user:${local.admin_upn_fqn}",
    ]
    "roles/bigquery.dataEditor" = [
      "user:${local.admin_upn_fqn}",
    ]
    "roles/bigquery.jobUser" = [
      "user:${local.admin_upn_fqn}",
    ]
    "roles/composer.environmentAndStorageObjectViewer" = [
      "user:${local.admin_upn_fqn}",
    ]
    "roles/iam.serviceAccountUser" = [
      "user:${local.admin_upn_fqn}",
    ]
    "roles/iam.serviceAccountTokenCreator" = [
      "user:${local.admin_upn_fqn}",
    ]
    "roles/composer.admin" = [
      "user:${local.admin_upn_fqn}",
    ]
     "roles/compute.networkAdmin" = [
      "user:${local.admin_upn_fqn}",
    ]
  }
  depends_on = [
    module.umsa_role_grants,
    module.umsa_impersonate_privs_to_admin
  ]
  }

/*******************************************
Introducing sleep to minimize errors from
dependencies having not completed
********************************************/
resource "time_sleep" "sleep_after_identities_permissions" {
  create_duration = "120s"
  depends_on = [
    module.umsa_creation,
    module.umsa_role_grants,
    module.umsa_impersonate_privs_to_admin,
    module.administrator_role_grants,
    module.gmsa_role_grants_cc,
    module.gmsa_role_grants_gce
  ]
}

/************************************************************************
7. Create VPC network & subnet 
 ***********************************************************************/
module "vpc_creation" {
  source                                 = "terraform-google-modules/network/google"
  project_id                             = local.project_id
  network_name                           = local.lab_vpc_nm
  routing_mode                           = "REGIONAL"

  subnets = [
    {
      subnet_name           = "${local.lab_subnet_nm}"
      subnet_ip             = "${local.lab_subnet_cidr}"
      subnet_region         = "${local.location}"
      subnet_range          = local.lab_subnet_cidr
      subnet_private_access = true
    }
  ]
  depends_on = [
    time_sleep.sleep_after_identities_permissions
  ]
}


/******************************************
8. Create Firewall rules 
 *****************************************/

resource "google_compute_firewall" "allow_intra_snet_ingress_to_any" {
  project   = local.project_id 
  name      = "allow-intra-snet-ingress-to-any"
  network   = local.lab_vpc_nm
  direction = "INGRESS"
  source_ranges = [local.lab_subnet_cidr]
  allow {
    protocol = "all"
  }
  description        = "Creates firewall rule to allow ingress from within subnet on all ports, all protocols"
  depends_on = [
    module.vpc_creation
  ]
}

/*******************************************
Introducing sleep to minimize errors from
dependencies having not completed
********************************************/
resource "time_sleep" "sleep_after_network_and_firewall_creation" {
  create_duration = "120s"
  depends_on = [
    module.vpc_creation,
    google_compute_firewall.allow_intra_snet_ingress_to_any
  ]
}

/******************************************
9. Create Storage bucket 
 *****************************************/

resource "google_storage_bucket" "lab_spark_bucket_creation" {
  project                           = local.project_id 
  name                              = local.lab_spark_bucket
  location                          = local.location
  uniform_bucket_level_access       = true
  force_destroy                     = true
  depends_on = [
      time_sleep.sleep_after_identities_permissions
  ]
}

resource "google_storage_bucket" "lab_data_bucket_raw_creation" {
  project                           = local.project_id 
  name                              = local.lab_data_bucket_raw
  location                          = local.location_multi
  uniform_bucket_level_access       = true
  force_destroy                     = true
  depends_on = [
      time_sleep.sleep_after_identities_permissions
  ]
}



resource "google_storage_bucket" "lab_sensitive_data_bucket_raw_creation" {
  project                           = local.project_id 
  name                              = local.lab_sensitive_data_bucket_raw
  location                          = local.location_multi
  uniform_bucket_level_access       = true
  force_destroy                     = true
  depends_on = [
      time_sleep.sleep_after_identities_permissions
  ]
}

resource "google_storage_bucket" "lab_code_bucket_creation" {
  project                           = local.project_id 
  name                              = local.lab_code_bucket
  location                          = local.location
  uniform_bucket_level_access       = true
  force_destroy                     = true
  depends_on = [
      time_sleep.sleep_after_identities_permissions
  ]
}

resource "google_storage_bucket" "lab_notebook_bucket_creation" {
  project                           = local.project_id 
  name                              = local.lab_notebook_bucket
  location                          = local.location
  uniform_bucket_level_access       = true
  force_destroy                     = true
  depends_on = [
      time_sleep.sleep_after_identities_permissions
  ]
}

resource "google_storage_bucket" "lab_model_bucket_creation" {
  project                           = local.project_id 
  name                              = local.lab_model_bucket
  location                          = local.location
  uniform_bucket_level_access       = true
  force_destroy                     = true
  depends_on = [
      time_sleep.sleep_after_identities_permissions
  ]
}

resource "google_storage_bucket" "lab_metrics_bucket_creation" {
  project                           = local.project_id 
  name                              = local.lab_metrics_bucket
  location                          = local.location
  uniform_bucket_level_access       = true
  force_destroy                     = true
  depends_on = [
      time_sleep.sleep_after_identities_permissions
  ]
}

resource "google_storage_bucket" "lab_bundle_bucket_creation" {
  project                           = local.project_id 
  name                              = local.lab_bundle_bucket
  location                          = local.location
  uniform_bucket_level_access       = true
  force_destroy                     = true
  depends_on = [
      time_sleep.sleep_after_identities_permissions
  ]
}

resource "google_storage_bucket" "lab_data_bucket_curated_creation" {
  project                           = local.project_id 
  name                              = local.lab_data_bucket_curated
  location                          = local.location_multi
  uniform_bucket_level_access       = true
  force_destroy                     = true
  depends_on = [
      time_sleep.sleep_after_identities_permissions
  ]
}

resource "google_storage_bucket" "lab_data_bucket_product_creation" {
  project                           = local.project_id 
  name                              = local.lab_data_bucket_product
  location                          = local.location_multi
  uniform_bucket_level_access       = true
  force_destroy                     = true
  depends_on = [
      time_sleep.sleep_after_identities_permissions
  ]
}


/*******************************************
Introducing sleep to minimize errors from
dependencies having not completed
********************************************/

resource "time_sleep" "sleep_after_bucket_creation" {
  create_duration = "60s"
  depends_on = [
    google_storage_bucket.lab_data_bucket_raw_creation,
    google_storage_bucket.lab_code_bucket_creation,
    google_storage_bucket.lab_notebook_bucket_creation,
    google_storage_bucket.lab_spark_bucket_creation,
    google_storage_bucket.lab_model_bucket_creation,
    google_storage_bucket.lab_metrics_bucket_creation,
    google_storage_bucket.lab_bundle_bucket_creation,
    google_storage_bucket.lab_data_bucket_curated_creation,
    google_storage_bucket.lab_data_bucket_product_creation,
    google_storage_bucket.lab_sensitive_data_bucket_raw_creation

  ]
}

/******************************************
10. Copy of datasets, scripts and notebooks to buckets
 ******************************************/

variable "notebooks_to_upload" {
  type = map(string)
  default = {
    "../notebooks/chicago-crimes-analysis/chicago-crimes-analytics.ipynb" = "chicago-crimes-analysis/chicago-crimes-analytics.ipynb",
    "../notebooks/icecream-sales-forecasting/icecream-sales-forecasting.ipynb" = "icecream-sales-forecasting/icecream-sales-forecasting.ipynb",
    "../notebooks/telco-customer-churn-prediction/preprocessing.ipynb" = "telco-customer-churn-prediction/preprocessing.ipynb",
    "../notebooks/telco-customer-churn-prediction/model_training.ipynb" = "telco-customer-churn-prediction/model_training.ipynb",
    "../notebooks/telco-customer-churn-prediction/hyperparameter_tuning.ipynb" = "telco-customer-churn-prediction/hyperparameter_tuning.ipynb",
    "../notebooks/telco-customer-churn-prediction/batch_scoring.ipynb" = "telco-customer-churn-prediction/batch_scoring.ipynb",  
    "../notebooks/retail-transactions-anomaly-detection/retail-transactions-anomaly-detection.ipynb" = "retail-transactions-anomaly-detection/retail-transactions-anomaly-detection.ipynb",
  }
}

resource "google_storage_bucket_object" "upload_to_gcs_notebooks" {
  for_each = var.notebooks_to_upload
  name     = each.value
  source   = "${path.module}/${each.key}"
  bucket   = "${local.lab_notebook_bucket}"
  depends_on = [
    time_sleep.sleep_after_bucket_creation
  ]

}

variable "csv_datasets_to_upload" {
  type = map(string)
  default = {
    "../datasets/cell-tower-anomaly-detection/reference_data/ctad_service_threshold_ref.csv"="cell-tower-anomaly-detection/reference_data/ctad_service_threshold_ref.csv",
    "../datasets/cell-tower-anomaly-detection/transactions_data/ctad_transactions.csv"="cell-tower-anomaly-detection/transactions_data/ctad_transactions.csv",
    "../datasets/icecream-sales-forecasting/isf_icecream_sales_transactions.csv"="icecream-sales-forecasting/isf_icecream_sales_transactions.csv",
    "../datasets/telco-customer-churn-prediction/machine_learning_scoring/tccp_customer_churn_score_candidates.csv"="telco-customer-churn-prediction/machine_learning_scoring/tccp_customer_churn_score_candidates.csv",
    "../datasets/telco-customer-churn-prediction/machine_learning_training/tccp_customer_churn_train_candidates.csv"="telco-customer-churn-prediction/machine_learning_training/tccp_customer_churn_train_candidates.csv",
    "../datasets/chicago-crimes/reference_data/crimes_chicago_iucr_ref.csv"="chicago-crimes/reference_data/crimes_chicago_iucr_ref.csv",
    }
}

resource "google_storage_bucket_object" "upload_to_gcs_datasets_raw" {
  for_each = var.csv_datasets_to_upload
  name     = each.value
  source   = "${path.module}/${each.key}"
  bucket   = "${local.lab_data_bucket_raw}"
  depends_on = [
    time_sleep.sleep_after_bucket_creation
  ]
}

variable "sensitive_csv_datasets_to_upload" {
  type = map(string)
  default = {
    "../datasets/banking/customers_raw/credit_card_customers/date=2022-05-01/credit_card_customers.csv"="banking/customers_raw/credit_card_customers/date=2022-05-01/credit_card_customers.csv",
    "../datasets/banking/customers_raw/customers/date=2022-05-01/customers.csv"="banking/customers_raw/customers/date=2022-05-01/customers.csv",
    }
}

resource "google_storage_bucket_object" "upload_to_gcs_sensitive_datasets_raw" {
  for_each = var.sensitive_csv_datasets_to_upload
  name     = each.value
  source   = "${path.module}/${each.key}"
  bucket   = "${local.lab_sensitive_data_bucket_raw}"
  depends_on = [
    time_sleep.sleep_after_bucket_creation
  ]
}


variable "parquet_datasets_to_upload" {
  type = map(string)
  default = {
    "../datasets/cell-tower-anomaly-detection/master_data/ctad_part-00000-fc7d6e20-dbda-4143-91b5-d9414310dfd1-c000.snappy.parquet"="cell-tower-anomaly-detection/master_data/ctad_part-00000-fc7d6e20-dbda-4143-91b5-d9414310dfd1-c000.snappy.parquet",
    "../datasets/cell-tower-anomaly-detection/master_data/ctad_part-00001-fc7d6e20-dbda-4143-91b5-d9414310dfd1-c000.snappy.parquet"="cell-tower-anomaly-detection/master_data/ctad_part-00001-fc7d6e20-dbda-4143-91b5-d9414310dfd1-c000.snappy.parquet",
    "../datasets/cell-tower-anomaly-detection/master_data/ctad_part-00002-fc7d6e20-dbda-4143-91b5-d9414310dfd1-c000.snappy.parquet"="cell-tower-anomaly-detection/master_data/ctad_part-00002-fc7d6e20-dbda-4143-91b5-d9414310dfd1-c000.snappy.parquet",
    "../datasets/cell-tower-anomaly-detection/master_data/ctad_part-00003-fc7d6e20-dbda-4143-91b5-d9414310dfd1-c000.snappy.parquet"="cell-tower-anomaly-detection/master_data/ctad_part-00003-fc7d6e20-dbda-4143-91b5-d9414310dfd1-c000.snappy.parquet"
    "../datasets/retail-transactions-anomaly-detection/rtad_sales.parquet"="retail-transactions-anomaly-detection/rtad_sales.parquet"

  }
}

resource "google_storage_bucket_object" "upload_to_gcs_datasets_curated" {
  for_each = var.parquet_datasets_to_upload
  name     = each.value
  source   = "${path.module}/${each.key}"
  bucket   = "${local.lab_data_bucket_curated}"
  depends_on = [
    time_sleep.sleep_after_bucket_creation
  ]

}

variable "code_to_upload" {
  type = map(string)
  default = {
    "../scripts/spark-sql/retail-transactions-anomaly-detection/retail-transactions-anomaly-detection.sql" = "spark-sql/retail-transactions-anomaly-detection/retail-transactions-anomaly-detection.sql"
    
    "../scripts/pyspark/chicago-crimes-analytics/curate_crimes.py" = "pyspark/chicago-crimes-analytics/curate_crimes.py"
    "../scripts/pyspark/chicago-crimes-analytics/crimes_report.py" = "pyspark/chicago-crimes-analytics/crimes_report.py"

    "../scripts/airflow/chicago-crimes-analytics/bq_lineage_pipeline.py" = "airflow/chicago-crimes-analytics/bq_lineage_pipeline.py"
    "../scripts/airflow/chicago-crimes-analytics/spark_custom_lineage_pipeline.py" = "airflow/chicago-crimes-analytics/spark_custom_lineage_pipeline.py"
  }
}

resource "google_storage_bucket_object" "upload_to_gcs_code_raw" {
  for_each = var.code_to_upload
  name     = each.value
  source   = "${path.module}/${each.key}"
  bucket   = "${local.lab_code_bucket}"
  depends_on = [
    time_sleep.sleep_after_bucket_creation
  ]

}

/*******************************************
Introducing sleep to minimize errors from
dependencies having not completed
********************************************/

resource "time_sleep" "sleep_after_network_and_storage_steps" {
  create_duration = "120s"
  depends_on = [
      time_sleep.sleep_after_network_and_firewall_creation,
      time_sleep.sleep_after_bucket_creation,
      google_storage_bucket_object.upload_to_gcs_datasets_raw,
      google_storage_bucket_object.upload_to_gcs_datasets_curated,
      google_storage_bucket_object.upload_to_gcs_notebooks,
      google_storage_bucket_object.upload_to_gcs_code_raw

  ]
}


/******************************************
11. Dataproc Metastore with gRPC endpoint
******************************************/

resource "google_dataproc_metastore_service" "datalake_metastore" {
  provider      = google-beta
  service_id    = local.lab_dpms_nm
  location      = local.location
  tier          = "DEVELOPER"

 maintenance_window {
    hour_of_day = 2
    day_of_week = "SUNDAY"
  }

 hive_metastore_config {
    version = "3.1.2"
    endpoint_protocol = "GRPC"
    
  }

  metadata_integration {
        data_catalog_config {
            enabled = true
        }
  }
  depends_on = [
    module.administrator_role_grants,
    time_sleep.sleep_after_network_and_storage_steps
  ]
}

/******************************************
12. Cloud Composer
******************************************/

resource "google_composer_environment" "create_cloud_composer_env" {
  name   = "oda-${local.project_nbr}-cc2"
  region = local.location
  provider = google-beta
  config {

    software_config {
      image_version = local.CLOUD_COMPOSER2_IMG_VERSION 
      env_variables = {
       
        AIRFLOW_VAR_PROJECT_ID      = "${local.project_id}"
        AIRFLOW_VAR_PROJECT_NBR     = "${local.project_nbr}"
        AIRFLOW_VAR_REGION          = "${local.location}"
        AIRFLOW_VAR_REGION_MULTI    = "${local.location_multi}"
        AIRFLOW_VAR_SUBNET          = "${local.lab_subnet_nm}"
        AIRFLOW_VAR_UMSA            = "${local.umsa}"
      }
    }

    node_config {
      network    = local.lab_vpc_nm
      subnetwork = local.lab_subnet_nm
      service_account = local.umsa_fqn
    }
  }

  depends_on = [
    time_sleep.sleep_after_network_and_firewall_creation
  ] 

  timeouts {
    create = "75m"
  } 
}


/*******************************************
Introducing sleep to minimize errors from
dependencies having not completed
********************************************/
resource "time_sleep" "sleep_after_composer_creation" {
  create_duration = "180s"
  depends_on = [
      google_composer_environment.create_cloud_composer_env
  ]
}

/******************************************
13. Cloud Composer 2 DAG bucket capture so we can upload DAG to it
******************************************/

output "CLOUD_COMPOSER_DAG_BUCKET" {
  value = google_composer_environment.create_cloud_composer_env.config.0.dag_gcs_prefix
}

/*******************************************
14. Upload Airflow DAG to Composer DAG bucket
******************************************/
variable "airflow_dags_to_upload" {
  type = map(string)
  default = {
    "../scripts/airflow/chicago-crimes-analytics/bq_lineage_pipeline.py" = "dags/chicago-crimes-analytics/bq_lineage_pipeline.py",
    "../scripts/airflow/chicago-crimes-analytics/spark_custom_lineage_pipeline.py" = "dags/chicago-crimes-analytics/spark_custom_lineage_pipeline.py"
  }
}

resource "google_storage_bucket_object" "upload_dags_to_airflow_dag_bucket" {
  for_each = var.airflow_dags_to_upload
  name     = each.value
  source   = "${path.module}/${each.key}"
  bucket   = substr(substr(google_composer_environment.create_cloud_composer_env.config.0.dag_gcs_prefix, 5, length(google_composer_environment.create_cloud_composer_env.config.0.dag_gcs_prefix)), 0, (length(google_composer_environment.create_cloud_composer_env.config.0.dag_gcs_prefix)-10))
  depends_on = [
    time_sleep.sleep_after_composer_creation
  ]
}





/******************************************
DONE
******************************************/
