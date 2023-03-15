# Manage Data Security through Dataplex 

    This is a critial lab. Make sure you follow step-by-step and finish applying each of the security policies. 
## 1. About

[Cloud Dataplex](https://cloud.google.com/dataplex/docs/lake-security) provides a single control plane for managing data security for distribued data. It translates and propagates  data roles to the underlying storage resource, setting the correct roles for each storage resource. The benefit is that you can grant a single Dataplex data role at the lake hierarchy (for example, a lake), and Dataplex maintains the specified access to data on all resources connected to that lake (for example, Cloud Storage buckets and BigQuery datasets are referred to by assets in the underlying zones). You can specify data roles at lake, zone and asset level. 

### 1.1. Prerequisites
Lab1-data-organization successfully completed.

### 1.2. Duration
~20 mins

### 1.3 Concepts
tbd

### 1.4. Scope of this lab

![Lab flow](/data-mesh-banking-labs/lab2-data-security/resources/imgs/Lab-2-flow.png)

While most part of data security policy application is already taken care as part of terraform setup, in this lab,  
 - you will grant data roles to the "customer-sa" service accounts created by terraform to own and manage the customer domain data
 - you will learn various ways to monitor the security policy propagation
 - you will learn to apply the security policies both through the Dataplex UI as well Dataplex APIs 
 - you will learn how to publish cloud audit logs to bigquery for further analysis and reporting

**Service Accounts and Access Overview**

![Dataplex Security](/data-mesh-banking-labs/lab2-data-security/resources/imgs/dataplex-security-lab.png)

### 1.5. Note
Attribute store which allows you to set column-level, row-level and table-level policies is a preview feature and is on the roadmap to be part of this lab. 

    Note: ADD "Service Account Token Creator" for active account principle before proceeeding

### 1.6. Documentation
[Secure your lake](https://cloud.google.com/dataplex/docs/lake-security)
<hr>

## 2. Lab

### 2.1. Set up data security audit sink 

Cloud logging sink to capture the audit data which we can later query to run and visualize audit reports.
- **Step 1:**  Create the Cloud Logging sink to capture the Dataplex Audit logs into a BigQuery table. <br>

     In Cloud shell, run the below command-

    ```
    export PROJECT_ID=$(gcloud config get-value project)
    
    gcloud logging --project=${PROJECT_ID} sinks create audits-to-bq bigquery.googleapis.com/projects/${PROJECT_ID}/datasets/central_audit_data --log-filter='resource.type="audited_resource" AND resource.labels.service="dataplex.googleapis.com" AND protoPayload.serviceName="dataplex.googleapis.com"'
    ```

    Sample output of the author: 

    ```
    Created [https://logging.googleapis.com/v2/projects/mbdatagov-05/sinks/audits-to-bq].
    Please remember to grant `serviceAccount:p52065135315-549975@gcp-sa-logging.iam.gserviceaccount.com` the BigQuery Data Editor role on the dataset.
    More information about sinks can be found at https://cloud.google.com/logging/docs/export/configure_export
    ```

- **Step 2:**  Validate: Go to Cloud Logging -> Logs Router and you should see a sink called “audits-to-bq” as shown below

- **Step 3:** Grant the Google Managed Cloud Logging Sink Service Account requisite permissions through Dataplex 
 Open Cloud Shell execute the below command: 

    ```
    export PROJECT_ID=$(gcloud config get-value project)

    LOGGING_GMSA=`gcloud logging sinks describe audits-to-bq | grep writerIdentity | grep serviceAccount | cut -d":" -f3`
    
    echo $LOGGING_GMSA

    curl -X \
    POST -H \
    "Authorization: Bearer $(gcloud auth print-access-token)" -H \
    "Content-Type: application.json" https://dataplex.googleapis.com/v1/projects/${PROJECT_ID}/locations/us-central1/lakes/central-operations--domain/zones/operations-data-product-zone/assets/audit-data:setIamPolicy -d "{\"policy\":{\"bindings\":[{\"role\":\"roles/dataplex.dataOwner\",\"members\":[\"serviceAccount:$LOGGING_GMSA\"]}]}}" 
    ```

<hr>

### 2.2. Apply Customer Domain Data Owner Policy(Lake Level push down) - UI

Here you will grant "customer-sa@" service account the data owner role for customer domain.

- **Step 1:** Pre-validation <br>

    Pre-verify data access. Make sure your active account has the Service Account Token Creator role for impersonation. 

    Open Cloud shell and execute the below command to list the tables in the "customer_raw_zone" dataset

    ``` 
    export PROJECT_ID=$(gcloud config get-value project)

    curl -X \
    GET -H \
    "Authorization: Bearer $(gcloud auth print-access-token --impersonate -service-account=customer-sa@${PROJECT_ID}.iam.gserviceaccount.com)" -H \
    "Content-Type: application.json"  https://bigquery.googleapis.com/bigquery/v2/projects/${PROJECT_ID}/datasets/customer_refined_data/tables?maxResults=10
    ```
    Sample output: 
    ![permission denied](/data-mesh-banking-labs/lab2-data-security/resources/imgs/permission-dnied.png)

- **Step 2:** Security policy application <br>

    In Dataplex, let's grant the customer user managed service account, access to the “Consumer Banking - Customer Domain” (lake). For this we will use the Lakes Permission feature to apply policy. 

    1. Go to **Dataplex** in the Google Cloud console.
    2. On the left navigation bar, click on **Manage** menu under **Manage Lakes**.
    3. Click on the **“Consumer Banking - Customer Domain”** lake.
    4. Click on the "**PERMISSIONS**" tab.
    5. Click on **+GRANT ACCESS**
    6. Choose **“customer-sa@your-project-id.iam.gserviceaccount.com”**  as principal
    7. Assign **Dataplex Data Owner** role.
    8. Click the **save** button
    9. Verify Dataplex data owner roles appear under the permissions 


- **Step 3** : Monitoring security policy propagation <br>

    Monitor the security policy propagation, you have various options to monitor the security access porpation centrally. Use any of the below methods. Method#3 is safest. 
    
    - **Method 1:** Using Dataplex UI 

        - Go to Dataplex -> Manage sub menu -> Go to "Consumer Banking - Customer Domain" lake --> Click on "Customer Raw Zone" --> Click on the Customer Raw Data Asset
            ![Dataplex Verify Image](/data-mesh-banking-labs/lab2-data-security/resources/imgs/dataplex-security-status-ui.png)

            You can also look at the Asset Status section at the lake level. 

            ![dataplex security status lake](/data-mesh-banking-labs/lab2-data-security/resources/imgs/dataplex-security-status-lake.png)

    -  **Method 2:** Using Dataplex APIs

        - Open Cloud Shell and execute the below command: 

            ```bash 
             curl -X \
            GET -H \
            "Authorization: Bearer $(gcloud auth print-access-token)" -H \
            "Content-Type: application.json" https://dataplex.googleapis.com/v1/projects/${PROJECT_ID}/locations/us-central1/lakes/consumer-banking--customer--domain/zones/customer-raw-zone/assets/customer-raw-data
            ```
            
            ![Dataplex Verify Image](/data-mesh-banking-labs/lab2-data-security/resources/imgs/dataplex-security-status-api.png)

    - **Method 3:** Check the permissions of the underlying asset

        - Here is an example of the policy for underlying GCS bucket 

            ![Dataplex Verify Image](/data-mesh-banking-labs/lab2-data-security/resources/imgs/dataplex-security-status-underlying-assets.png)


- **Step 4**: Post-Validation<br>

    After the access policies has been propagated by Dataplex, rerun the commands in Step1 and verify the service account is able to access underlying data

    Open Cloud shell and execute the below command which should now execute successfully. 

    ```bash 
    export PROJECT_ID=$(gcloud config get-value project)

    curl -X \
    GET -H \
    "Authorization: Bearer $(gcloud auth print-access-token --impersonate-service-account=customer-sa@${PROJECT_ID}.iam.gserviceaccount.com)" -H \
    "Content-Type: application.json"  https://bigquery.googleapis.com/bigquery/v2/projects/${PROJECT_ID}/datasets/customer_refined_data/tables?maxResults=10
    ```

    Sample Output:

    ![successful output](/data-mesh-banking-labs/lab2-data-security/resources/imgs/dataplex-security-result.png)

<br>

### 2.3. Apply Data Reader Policy for Customer Domain (Zone level push down) - UI
<hr>

Grant the Credit card analytics consumer sa read access to the Customer Data product zone(Zone Level security pushdown).**

Using “Secure View” to provide the credit card analytics consumer domain access to the Customer Data Products. For this we will use the "Secure" functionality to the apply policy

 1. Go to **Dataplex** in the Google Cloud console.
2. Navigate to the **Manage**->**Secure** on the left menu.
3. Under the **RESOURCE-CENTRIC** tab, find and expand on your project
4. Expand on the **"**Consumer Banking -  Customer Domain"** lake
5. Click on the **"Customer Data Product Zone"**
6. Click on **+Grant Access**
7. Choose "cc-trans-consumer-sa@<your-project-id>.iam.gserviceaccount.com as the principle
8. Add the **Dataplex Data reader** roles
9. Click on the Save button 
10. Verify Dataplex Data Reader roles appear for the principal. Use one of the methods outlined in Step#3 above. 


### 2.4. Manage Security Policies for Central Operations domain -  Dataplex APIs

- **Step 1:**  Define and apply security policy to grant read access to all the domain service accounts(customer-sa@, cc-trans-consumer-sa@, cc-trans-sa@, merchant-sa@) to central managed common utilities (Central Operations Domain Lake -> COMMON UTILITIES zone) housed in the gcs bucket e.g. libs, jars, log files etc. As you observe this has been applied at the zone-level.

    -  Open Cloud shell and execute the below commands: 

        ```bash 
        export PROJECT_ID=$(gcloud config get-value project)

        # 1. CREATE POLICY
        export central_common_util_policy="{\"policy\":{
        \"bindings\": [
        {
            \"role\": \"roles/dataplex.dataReader\",
            \"members\": [
            \"serviceAccount:cc-trans-consumer-sa@${PROJECT_ID}.iam.gserviceaccount.com\",
        \"serviceAccount:cc-trans-sa@${PROJECT_ID}.iam.gserviceaccount.com\",   \"serviceAccount:customer-sa@${PROJECT_ID}.iam.gserviceaccount.com\",    \"serviceAccount:merchant-sa@${PROJECT_ID}.iam.gserviceaccount.com\"
            ]
        }
        ]
        }
        }"

        echo " "
        # 2. VIEW POLICY
        echo "==========="
        echo "The policy we just created is "
        echo "==========="
        echo " "
        echo $central_common_util_policy


        echo " "
        # 3. APPLY POLICY
        echo "==========="
        curl -X POST -H "Authorization: Bearer $(gcloud auth print-access-token)" -H "Content-Type: application.json" https://dataplex.googleapis.com/v1/projects/${PROJECT_ID}/locations/us-central1/lakes/central-operations--domain/zones/common-utilities:setIamPolicy -d "${central_common_util_policy}"
        echo " "
        echo "==========="
        ```



### 2.5. Analyze Dataplex Audit Data  

Go to BigQuery and perform analysis on the audit data to analyze and report 

 - Open BigQuery UI, change the processing location to us-central1 and execute the below query after replacing the ${PROJECT_ID}
    ```bash 
    SELECT protopayload_auditlog.methodName,   protopayload_auditlog.resourceName,  protopayload_auditlog.authenticationInfo.principalEmail,  protopayload_auditlog.requestJson, protopayload_auditlog.responseJson FROM `${PROJECT_ID}.central_audit_data.cloudaudit_googleapis_com_activity_*` LIMIT 1000
    ```

## Summary 
In this lab you have learned:

1. the different ways you can apply the security policies(terraform will be supported in future) <br>1.1 using Dataplex UI - both using the PERMISSIONS tab within Lakes, Zones and Assets and also using the "SECURE" tab under Manage <br>1.2 using Dataplex API
2. how to route the Dataplex audit logs into BigQuery for further analysis and reporting
3. that using Dataplex you can simply data security policy using a single policy for both buckets and datasets

<hr>

This concludes the lab module. Either proceed to the [main menu](../README.md) or to the [next module](../lab3-data-curation/README.md) you will curate and standardize the data using Dataplex dataflow templates based task. 

<hr>