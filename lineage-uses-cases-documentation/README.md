# Data Lineage Use-Cases Documentation
This repository contains the code required to set up the environments described in Lineage Use-Case based documentation. Use this code to follow along each use-case.

## Prerequisites
*   A Google Cloud Project with Billing enabled.
*   Google Cloud CLI (`gcloud`, `bq`) installed and configured (or use Cloud Shell).
*   Sufficient IAM permissions in your project (e.g., BigQuery Admin, Data Lineage Viewer).
*   The following APIs must be enabled in your project:
    *   BigQuery API
    *   BigQuery Data Transfer API
    *   Data Lineage API

You can enable APIs via the console or `gcloud services enable <service.googleapis.com>`.


Moreover, in order to set the pre-built state in your google cloud console for the various lineage use-cases, you need to follow the steps in this section related to: 
* Create a  GitHub Personal Access Token (PAT) and add it to Secret Manager
* Create a Dataform repository connected 

### 1. Create a  GitHub Personal Access Token (PAT) and add it to Secret Manager

#### Step 1: Create a GitHub Token (PAT)
In order to connect  Google Cloud, you need a "key" from GitHub. Follow this steps to create one: 

* In GitHub, go to Settings -> Developer Settings -> Personal Access Tokens -> Tokens (classic).
* Click Generate new token (classic).
* Select Name: "Dataform-Client-Access".
* Expiration: Select "No expiration" (or a long duration).
* Scopes: Check the box for repo (this allows Dataform to see your code).
* Copy the Token: Save it somewhere safe. You will never see it again!

#### Step 2: Save the Token in Secret Manager
Now, you need to store that key securely in Google Cloud.

* In the Google Cloud Console, search for Secret Manager.
* Click Create Secret.
* Give a name Name, such as: github-token.
* Secret Value: Paste the token you just copied from GitHub.
* Click Create.

#### Step 3: Give Dataform Permission to Read the Secret
By default, Dataform isn't allowed to see your secrets. You have to give it "Permission to peek."

* In Google Cloud's Secret Manager click on the github-token secret you just created.
* Go to the Permissions tab.
* Click Grant Access and add the Dataform Service Acount. Note: This usually looks like: service-[PROJECT_NUMBER]@gcp-sa-dataform.iam.gserviceaccount.com. As role select 'Select Secret Manager Secret Accessor.'
* Click Save.


### 2. Connect Dataform to the Github repository

In order to have the pre-built state necessary for each use-case, you need to follow the steps of this section:

* Fork this github repository 
* In Google Cloud's dataform page, select 'Create Repository', fill the 'Repository ID' and 'Region' Fields, and click on 'Create'
* In Dataform, go to your Repository Settings.
* Click Connect with Git and fill the fields: 
  * Fill the Remote Git URL with the yout forked repository URL.
  * Choose Default Branch: main (or master).
  * Secret: Select the github-token secret you just created from the dropdown.
  * Click Link.

After these steps you will be able to see github's code in your dataform repository.

## How to run each use case

To run the defined code for each use case:
1. Ensure that you have changed 'defaultProject' to your GCP PROJECT NAME in 'workflow_settings.yaml'
2. With the repository code open in datafrom, click Start Execution -> Actions -> Multiple Actions.
3. Choose to run a selection of tags.
   * For use case 'Impact Analysis' you will choose the tag 'impact-analysis'
   * For use case 'Optimize Costs' you will choose the tag 'optimize-costs'
   * For use case 'PII Leakage' you will choose the tag 'pii-leakage'
4. Click Start Execution.

