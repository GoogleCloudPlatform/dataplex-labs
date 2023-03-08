#  Register Data Products

## 1. About

Data Catalog is a fully managed, scalable metadata management service within Dataplex. Data Catalog automatically catalogs the following Google Cloud assets: BigQuery datasets, tables, views, Pub/Sub topics, Dataplex lakes, zones, tables, and filesets, Analytics Hub linked datasets and (Public preview): Dataproc Metastore services, databases, and tables. Data Catalog handles two types of metadata: technical metadata and business metadata.

We can leverage Dataplex's Catalog to register data products by bringing in additional business context through tags for example - Data Ownership, Data Classification, Data Quality and so on. This can provide a single data disocvery experience for end user to discover high quality and secured data products. 

### 1.1. Prerequisites 
Successful completion of lab1, lab2 and lab4

### 1.2. Duration
~45  mins

### 1.3 Concepts

#### 1.3.1 Data Stewards
Data stewards associated with a data entry. A data steward for a data entry can be contacted to request more information about the data entry. A data steward does not require a specific IAM role. You can add a user with a non- Google email account as a data steward for a data entry. A data steward cannot perform any project-related activity within the console, unless the user is provided with explicit IAM permissions.

#### 1.3.2 Rich text overview
Rich text overview of a data entry that can include images, tables, links, and so on.

#### 1.3.3 Technical Metadata
An example of technical metadata related to a data entry, such as a BigQuery table, can include the following attributes: Project information, such as name and ID, Asset name and description, Google Cloud resource labels, Schema name and description for BigQuery tables and views

#### 1.3.4 Business Metadata
Business metadata for a data entry can be of the following types: Tags, Data Stewards, Rich text overview 

#### 1.3.5 Tags

Tags are a type of business metadata. Adding tags to a data entry helps provide meaningful context to anyone who needs to use the asset. For example, a tag can tell you information such as who is responsible for a particular data entry, whether it contains personally identifiable information (PII), the data retention policy for the asset, and a data quality score.

#### 1.3.6 Tag templates

To start tagging data, you first need to create one or more tag templates. A tag template can be a public or private tag template. When you create a tag template, the option to create a public tag template is the default and recommended option in the Google Cloud console. A tag template is a group of metadata key-value pairs called fields. Having a set of templates is similar to having a database schema for your metadata.

### 1.4. Scope of this lab

You will learn how to create bulk tags on the Dataplex Data Product entity across domains using Composer in this lab after the Data Products have been created as part of the above lab. You will use a Custom Dataple task based on [custom metadata tag library](https://github.com/mansim07/datamesh-templates/tree/main/metadata-tagmanager) to create 4 predefined tag templates - Data Classification, Data Quality, Data Exchange and Data product info(Onwership)


### 1.5. Note
You can also look at Tag Engine for tagging automation

### 1.6. Documentation
[About Data Catalog Metadata](https://cloud.google.com/data-catalog/docs/concepts/metadata)<br>

## 2. Lab
<hr>

### 2.1 Prevalidations 

#### 2.1.1 Validate Dataplex entities
If you have just run your previous lab i.e. Building your Data Product Lab, make sure the entities are visible in Dataplex before proceeding with the below steps.

In order to verify Go to Dataplex  → Manage tab → Click on Customer - Source Domain Lake → Click on Customer Data Product Zone

![entities](/data-mesh-banking-labs/lab7-register-data-products/resources/imgs/entities_screnshot.png)

Do this for all the other domain data product zones as well. 

#### 2.1.2 Validate Data Catalog tag template
Make sure tag templates are created in ${PROJECT_ID}  created. Go to Dataplex → Manage Catalog → Tag templates. You should see the below 4 Tag Templates. Open each one and look the schema: 
![tagtemplates](/data-mesh-banking-labs/lab7-register-data-products/resources/imgs/tag_templates.png)

#### 2.1.3  Validate the DLP results are populated
Make sure Data is populated by the DLP job profiler into ${PROJECT_ID}.central_dlp_data dataset. If the data has not been populated the data classification tags will fail. 
    ```bash 
    export PROJECT_ID=$(gcloud config get-value project)

    bq query --use_legacy_sql=false \
    "SELECT
    COUNT(*)
    FROM
    ${PROJECT_ID}.central_dlp_data.dlp_data_profiles"
    ```

### 2.2 Data Ownership Tag for Customer Data Product(Custom Spark Task)  

#### 2.2.1 Author and build a custom tag for a Customer Data Product 

1.  Open Cloud Shell and create a new file called “customer-tag.yaml” and copy and paste the below yaml into a file.

    ```bash 
    cd ~
    vim customer-tag.yaml
    ``` 
    Enter the below text 
    ```
    data_product_id: derived
    data_product_name: ""
    data_product_type: ""
    data_product_description: ""
    data_product_icon: ""
    data_product_category: ""
    data_product_geo_region: ""
    data_product_owner: ""
    data_product_documentation: ""
    domain: derived
    domain_owner: ""
    domain_type: ""
    last_modified_by: ""
    last_modify_date: ""
    ```

    **Make sure you always leave the last_modified_by and last _moddify date blank**

2. Upload the file to the temp gcs bucket

```bash 
gsutil cp ~/customer-tag.yaml gs://${PROJECT_ID}_dataplex_temp/
```

3. Run the below command to create the tag for customer_data product entity 

```bash 
gcloud dataplex tasks create customer-tag-job \
        --project=${PROJECT_ID} \
        --location=us-central1 \
        --vpc-sub-network-name=projects/${PROJECT_ID}/regions/us-central1/subnetworks/dataplex-default \
        --lake='consumer-banking--customer--domain' \
        --trigger-type=ON_DEMAND \
        --execution-service-account=customer-sa@${PROJECT_ID}.iam.gserviceaccount.com \
        --spark-main-class="com.google.cloud.dataplex.templates.dataproductinformation.DataProductInfo" \
        --spark-file-uris="gs://${PROJECT_ID}_dataplex_temp/customer-tag.yaml" \
        --container-image-java-jars="gs://${PROJECT_ID}_dataplex_process/common/tagmanager-1.0-SNAPSHOT.jar" \
        --execution-args=^::^TASK_ARGS="--tag_template_id=projects/${PROJECT_ID}/locations/us-central1/tagTemplates/data_product_information, --project_id=${PROJECT_ID},--location=us-central1,--lake_id=consumer-banking--customer--domain,--zone_id=customer-data-product-zone,--entity_id=customer_data,--input_file=customer-tag.yaml"

```

4.  Go to Dataplex UI → Process → Custom Spark tab → Monitor the job → you will find a job named “customer-tag-job”

5. Once job is successful, Go to Dataplex Search Tab and type this into the search bar - tag:data_product_information
![tag-search](/data-mesh-banking-labs/lab7-register-data-products/resources/imgs/tag-search.png)

6. Click on customer_data -> Go to the Tags section and make sure the data product information is created.

![dp_info_tag](/data-mesh-banking-labs/lab7-register-data-products/resources/imgs/dp_info_tag.png)

As you can see the automation utilities was able to derive most of the information. But at times, certain values may needs to be overriden. 

7.  Now let's update the input file 

```bash 
cd ~
vim customer-tag.yaml
```

Enter the below text
```
data_product_id: derived
data_product_name: ""
data_product_type: ""
data_product_description: "Adding Custom Description as part of demo"
data_product_icon: ""
data_product_category: ""
data_product_geo_region: ""
data_product_owner: "alexandra.gill@boma.com"
data_product_documentation: ""
domain: derived
domain_owner: "rebecca.piper@boma.com"
domain_type: ""
last_modified_by: ""
last_modify_date: ""
```

8. upload the updated file to the temp gcs bucket 

```
gsutil cp ~/customer-tag.yaml gs://${PROJECT_ID}_dataplex_temp
```

9.  Trigger another tagging job 

```bash 
gcloud dataplex tasks create customer-tag-job-2 \
        --project=${PROJECT_ID} \
        --location=us-central1 \
        --vpc-sub-network-name=projects/${PROJECT_ID}/regions/us-central1/subnetworks/dataplex-default \
        --lake='consumer-banking--customer--domain' \
        --trigger-type=ON_DEMAND \
        --execution-service-account=customer-sa@${PROJECT_ID}.iam.gserviceaccount.com \
        --spark-main-class="com.google.cloud.dataplex.templates.dataproductinformation.DataProductInfo" \
        --spark-file-uris="gs://${PROJECT_ID}_dataplex_temp/customer-tag.yaml" \
        --container-image-java-jars="gs://${PROJECT_ID}_dataplex_process/common/tagmanager-1.0-SNAPSHOT.jar" \
        --execution-args=^::^TASK_ARGS="--tag_template_id=projects/${PROJECT_ID}/locations/us-central1/tagTemplates/data_product_information, --project_id=${PROJECT_ID},--location=us-central1,--lake_id=consumer-banking--customer--domain,--zone_id=customer-data-product-zone,--entity_id=customer_data,--input_file=customer-tag.yaml"
```

10. Go to Dataplex UI → Process → Custom Spark tab → Monitor the job -> Wait till it completes. You will find a job with name “customer-tag-job-2” 

11.  Go to Dataplex-> search tab -> Refresh the tag and see if the updates has been propagated Once the job is successful 

### 2.3 Create bulk tags for  Customer Data Product(Composer )  
Above we learned how to create and update the tags manually, now let’s see how we can use composer to automate the tagging process end-to-end. Now you can easily add tagging jobs as a downstream dependy to your data pipeline 

    We have limitation on Spark Serverless capacity(8 vCore)  by default so makes sure you trigger the tags sequentially to avoid failures due  resource crunch**

1.  Go to **Composer** → Go to **Airflow UI** → Click on DAGs 
2. Click on DAGs and search for or go to **“data_governance_customer_dp_info_tag”** DAG and click on it 
3. Trigger the DAG Manually by clicking on the Run button
4.  Monitor and wait for the jobs to complete. You can also go to Dataplex UI to monitor the jobs 
5. Trigger the **“master_dag_customer_dq”**  and wait for its completion. This first runs a data quality job and then publishes the data quality score tag. 
6.  Trigger the **“data_governance_customer_exchange_tag”** dag and wait for its completion 
7. Trigger the  **“data_governance_customer_classification_tag”** and wait for its completion 

### 2.4 Create bulk tags for Merchants Domain(Composer )  

- **Step1**: Follow the same steps you used above for triggering the below set of DAGs: and wait for its completion. Execute the below DAGs in airflow 
    - **master_dag_merchant_dq**
    - **data_governance_merchant_classification_tag**
    - **data_governance_merchant_dp_info_tag**
    - **data_governance_merchant_exchange_tag**

### 2.4 Create bulk tags for ransactions Domain(Composer )  
This task is optional and finish this task only if you have time left for your lab. 

- **Step1**: Follow the same steps you used above for triggering the below set of DAGs: and wait for its completion. Execute the below DAGs in airflow
    - **data_governance_transactions_classification_tag**
    - **data_governance_transactions_dp_info_tag**
    - **data_governance_transactions_exchange_tag**
    - **data_governance_transactions_quality_tag**

<hr>
This concludes the lab module. Either proceed to the [main menu](../README.md) or to the [next module](../lab8-data-discovery-lineage/README.md) you will learn to discover data products and look at data lineage 
<hr>