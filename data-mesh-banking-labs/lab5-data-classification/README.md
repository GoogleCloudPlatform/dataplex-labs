# Data Classification using DLP 

## 1. About
Cloud DLP Data Profiler in this lab so that it can awhich will automatically scan all BigQuery tables and columns across the entire organization, individual folders, and projects. It then creates data profiles at table. column and project level. 

### 1.1. Prerequisites
Lab2-data-security successfully completed.

### 1.2. Duration
~15 mins

### 1.3 Concepts

#### 1.3.1 Inspection templates
Templates for saving configuration information for inspection scan jobs, including what predefined or custom detectors to use.

#### 1.3.2 InfoTypes
Cloud Data Loss Prevention uses information types—or infoTypes—to define what it scans for. An infoType is a type of sensitive data, such as a name, email address, telephone number, identification number, credit card number, and so on. An infoType detector is the corresponding detection mechanism that matches on an infoType's matching criteria.

### 1.4. Scope of this lab

In this lab, we'll use a Terraform template to configure the DLP Data profiler. We will carry out steps 1, 2, and 3 of this lab based on the architecture diagram below. Step 4,5 & 7 will be done in the lab7-register-data-product. 

![data classification](/data-mesh-banking-labs/lab5-data-classification/resources/imgs/dc-technical-architecture.png)


### 1.5. Note
In a future release, will add a lab for sending Cloud DLP results to Dataplex Catalog

### 1.6. Documentation
[Data Profiles](https://cloud.google.com/dlp/docs/data-profiles)<br>
[DLP result Analysis](https://cloud.google.com/dlp/docs/querying-findings)<br>
[Send DLP Results To Catalog](https://cloud.google.com/dlp/docs/sending-results-to-dc)

## 2. Lab 

### 2.1 Configure DLP Data Profiler 
Follow the below instructions to setup the DLP Auto profiler job. 

- **Step1**: Add IAM permissions for DLP Service Account 

    Open Cloud Shell and run the below command: 

    ```
     export PROJECT_ID=$(gcloud config get-value project)

     export project_num=$(gcloud projects list --filter="${PROJECT_ID}" --format="value(PROJECT_NUMBER)")

    gcloud projects add-iam-policy-binding ${PROJECT_ID} --member="serviceAccount:service-${project_num}@dlp-api.iam.gserviceaccount.com" --role="roles/dlp.admin"
    ```
- **Step2**: Go to "Data Loss Prevention" service under Security in Google Console
- **Step3**: Click on "SCAN CONFIGURATIONS" tab 
- **Step4**: Click on +CREATE CONFIGURATION 
- **Step5**: For **select resource to scan** select **Scan the entire project**
- **Step6**: Click Continue 
- **Step7**: Under Manage Schedules (Optional)
    - Click on Edit Schedule, Choose “Reprofile daily” for both When Schema Changes and When Table Changes
    ![dlp options](/data-mesh-banking-labs/lab5-data-classification/resources/imgs/dlp_options.png)
    - Click “Done” and hit continue
- **Step8**: Under Select inspection template
    - Choose “Select existing inspection template” and provide this value 
    
        **template name**: projects/${PROJECT_ID}/inspectTemplates/marsbank_dlp_template

        **location**: global
    then click "Continue"
- **Step9**: Under “Add Actions”
    - Choose Save data profile copies to BigQuery and provide these values
		Project id: ${PROJECT_ID}
		Dataset id: central_dlp_data
		Table id: dlp_data_profiles

       ![dlp_bq_specs](/data-mesh-banking-labs/lab5-data-classification/resources/imgs/dlp_bq_profile.png)
    - Click continue

- **Step10**: Under Set location to store configuration
    **Resource location**: Iowa (us-central1)
   Click continue

- **Step11**: Leave "Review and Create" at default and click Create
- **Step12**: Make sure configuration has been successfully created 

     ![scan config](/data-mesh-banking-labs/lab5-data-classification/resources/imgs/dlp_scan_configuration.png)
- **Step13** After a few minutes check to make sure the data profile is available in the "DATA PROFILE" tab, choose the Iowa region and the central_dlp_table dataset has been poupulated in Bigquery. Meanwhile feel free to move to the next lab. 

   ![dlp profile](/data-mesh-banking-labs/lab5-data-classification/resources/imgs/dlp_profile.png)

In a later lab, we will use these results to annotate the Data products with the Data classification info. 

<hr>

This concludes the lab module. Either proceed to the [main menu](../README.md) or to the [next module](../lab6-data-quality) where you will learn to implement data quality using Dataplex. 

<hr>