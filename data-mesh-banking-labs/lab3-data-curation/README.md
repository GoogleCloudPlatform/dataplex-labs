# Using Dataplex's Data Curation Task

## 1. About

Dataplex provides templates, powered by Dataflow, to perform common data processing tasks like data ingestion, processing, and managing the data lifecycle. In this lab, you will discover how to leverage common Dataplex templates to curate raw data and translate it into standardized formats like parquet and Avro in the Data Curation lane. This demonstrates how domain teams may quickly process data in a serverless manner and begin consuming it. 

### 1.1. Prerequisites
Lab2-data-security successfully completed.

### 1.2. Duration
~20 mins

### 1.3 Concepts
tbd

### 1.4. Scope of this lab
As part of this lab we will curate the Customer Data using Dataplex's Curation Task 

### 1.5. Note
This lab is optional

### 1.6. Documentation
[Practioner Guide](https://cloud.google.com/dataplex/docs/task-templates)

## 2. Lab
<hr>

### 2.1 Convert raw to curate for Customer Domain 

The Customer Raw data has 2 feeds coming into the gcs raw bucket. We will convert both the feeds from CSV to parquet while preserving the data and the partitions. 

- **Step1:** Create the Dataflow pipeline 
    - Go to Dataplex UI --> Navigate to **Manage Lakes** --> **Process** menu --> Click on **Create Task** button 

    - Then, Select Create Dataflow Pipeline  in Convert to Curated Formats panel

        ![dataplex tasks](/data-mesh-banking-labs/lab3-data-curation/resources/imgs/dataplex-tasks.png)

- **Step2:** Enter the Base  Parameters 

    - **Dataplex Lake:** : Choose the "Customer Domain" 

    - **Task name:**: curate-customer-raw-data
    
    - **Regional Endpoint** : us-central1(Iowa) 

    - **Sample Screenshot**:
        ![Base parameters](/data-mesh-banking-labs/lab3-data-curation/resources/imgs/base-parameter.png)


- **Step3:** Enter the Required Parameters 
   
        DON'T FORGET TO REPLACE THE PROJECT_ID WITH YOURS
    
    - **Dataplex asset name or Dataplex entity names for the files to be converted**: projects/${PROJECT_ID}/locations/us-central1/lakes/consumer-banking--customer--domain/zones/customer-raw-zone/assets/customer-raw-data

    - **Output file format in GCS**: PARQUET
    
    - **Dataplex asset name for the destination GCS bucket**: projects/${PROJECT_ID}/locations/us-central1/lakes/consumer-banking--customer--domain/zones/customer-curated-zone/assets/customer-curated-data

    - **Sample Screenshot** 
        ![required parameters](/data-mesh-banking-labs/lab3-data-curation/resources/imgs/required_parameters.png)

- **Step4:** Enter the Optional Parameters **Very critical for job success**

        DON'T FORGET TO REPLACE THE PROJECT_ID WITH YOURS
     Open "Show Optional Parameters" and add the following-
    - **Service Account Email**: customer-sa@${PROJECT_ID}.iam.gserviceaccount.com
    - **Subnetwork**: https://www.googleapis.com/compute/v1/projects/${PROJECT_ID}/regions/us-central1/subnetworks/dataplex-default
    - **Sample ScreenShot**:

        ![optional_parameter](/data-mesh-banking-labs/lab3-data-curation/resources/imgs/Optional-parameter.png)

- **Step5:** Set Schedule <br>

    - Choose "custom" as Repeats adn set Custom schedule to "0 0 1 1 *"
    - Click the **Create** button.  

- **Step6:** Click **Run** button ad then **Run Pipeline**

     ![run_button](/data-mesh-banking-labs/lab3-data-curation/resources/imgs/run_button.png)

- **Step7:** Give it a couple of seconds, and after click on the refresh button. Shows the current job that was scheduled and is in pending/running status


    ![jobstatatus](/data-mesh-banking-labs/lab3-data-curation/resources/imgs/job_status.png)

- **Step8:** Monitor the job. Click on the refresh button again after a few minutes. 

    ![pipline status](/data-mesh-banking-labs/lab3-data-curation/resources/imgs/monitor_pipeline.png)

- **Step9:** Validate the output 

    - Navigate to Dataplex--> Manage --> "Consumer Banking - Customer Domain" Lake
    - Select "Customer Curated Zone" 
    - Select "Customer Curated Data" Asset 
    - Scroll down to Resource details and click the External URL link
        ![external_url](/data-mesh-banking-labs/lab3-data-curation/resources/imgs/external_url.png)
    - Open customers_data folder 
    - Open the dt=2022-01-01 folder
    - You should see the "customer.parquet" file created here

- **Step10:** Validate the metadata 

        (Note: Metadata for curated data may not show up immediately as metadata refreshes typically takes an hour)

    - Go to Dataplex UI 

    - Navigate to the Discover menu, Search optin 

    - Open 'Consumer banking - Customer Domain' and select 'Customer Curated Zone' to filter the assets in the curated layer. 
     ![filtered search](/data-mesh-banking-labs/lab3-data-curation/resources/imgs/filter_search.png)

    - Select **customer_curated_data** from the list of assets shown on the right panel to see the following display. Validate the entry details to see the parquet format file information 
    ![curated metadata](/data-mesh-banking-labs/lab3-data-curation/resources/imgs/curated_metadata.png)

Suggestion: As a homework, try to curate the merchants data.  

## Summary 
In this lab, you learned how to use the built-in one-click templatized Dataplex task to quick standardize your data. This can be a common Data Management task that can be executed without the need of understanding any underlying data. You can also leverage built-in scheduler to execute the workflow either on-demand or on-schedule. 

<hr>

This concludes the lab module. Either proceed to the [main menu](../README.md) or to the [next module](../lab4-build-data-products/README.md) you will use build the data products i.e move from refined to data product layer. 

<hr>



## Common Issues

- **Issue#1:** Error creating a Dataflow task with Error message "Create task failed The principal(user or service account) lacks IAm permission 'cloudscheduler.jobs.create' for the resource 'project/..." (or the resource may not exist)

    **Solution**: Re-run the job usually fixes the issue 

- **Issue#2:** Nothing happens or screen frozen when hit Run 
  
  **Resolution**: Use the refresh button 

- **Issue#3:** Failed to start the VM, launcher-20220903094028585565812401460001, used for launching because of status code: INVALID_ARGUMENT, reason: Error: Message: Invalid value for field 'resource.networkInterfaces[0]': '{ "network": "global/networks/default", "accessConfig": [{ "type": "ONE_TO_ONE_NAT", "name":...'. Subnetwork should be specified for custom subnetmode network HTTP Code: 400.

    **Resolution**: Make sure you have set the correct subnet. 

- **Issue#4:** After trying to clone a job and re-submit, the Create Button disappears and it keeps buffereing

    **Resolution**: UI issue.  Just click on a radio button eg. Pipeline option - Switch to Streaming and back to batch

- **Issue#5:** :  "message" : "customer-sa@mbdatagov-896507121.iam.gserviceaccount.com does not have storage.objects.list access to the Google Cloud Storage bucket.",

    **Resolution**: Make sure the customer-sa has the right privileges on the bucket. 