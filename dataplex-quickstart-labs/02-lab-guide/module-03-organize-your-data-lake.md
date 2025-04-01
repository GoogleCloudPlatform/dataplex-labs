# M3: Organize your enterprise assets into Lakes and Zones Using Dataplex Catalog Aspects

In this lab module, we will first go over concepts and then into the lab, and try out the concepts.

### Prerequisites
Completion of prior modules

### Approximate duration
20 minutes or less

### Pictorial overview of work to be completed in the lab module

![IAM](../01-images/m3-00.png)   
<br><br>

### Targeted Data Lake Layout

We will create a DataLake with Lakes and Zones as shown below-

![IAM](../01-images/m3-01.png)   
<br><br>

<hr>

## 1. Concepts

### 1.1. Intelligent data to AI governance
Centrally discover, manage, monitor, and govern data and AI artifacts across your data platform, providing access to trusted data and powering analytics and AI at scale.


### 1.2. Discover, Organize and Enrich


### 1.3. Dataplex "Lake" Aspect

A Lake is a logical metadata abstraction on top of your assets (structured and unstructured). There are various capabiltiies offered in Dataplex over a Data Lake that we will cover in subsequent labs. It is analogous to a "domain". And is also analogous to a "Data Lake" - a cordoned off repository of your assets.

A Lake, although provisioned in a GCP project, can span multiple projects. 

To create and assign the "Lake" aspect, one must have the IAM permissions ```dataplex.aspectTypes.create```
and ```dataplex.aspectTypes.use```.  These permissions are granted to the predefined role ```roles/dataplex.catalogEditor```.

### 1.3. Dataplex "Zone" Aspect
Data zones are named entities within a Dataplex "Lake". They are logical groupings of unstructured, semi-structured, and structured data, consisting of multiple assets, such as Cloud Storage buckets, BigQuery datasets, and BigQuery tables.

A lake can include one or more zones. a Zone may contain assets that point to resources that are part of projects outside of its parent project.  

In this example we have defined six different types of zones.  The name and number of zones are flexible and are dependent on your needs.   For example, some implementers might use the medallion naming convention and have three zones called "bronze", "silver" and "gold"

In this example, we use the following six zones:<BR>
<BR>
1. "oda-misc-zone"
2. "oda-raw-zone"
3. "oda-dq-zone"
4. "oda-curated-zone"
5. "oda-product-zone"
6. "oda-raw-sensitive-zone"


#### 1.3.3. IAM permisions for Dataplex Zone creation

TBD

<hr>

<hr>

## 2. Lab - Foundations

In this lab module, we will organize all the lab assets into a Dataplex Lake and into Zones.

### 2.1. Declare variables

In Cloud Shell, while in the lab's project scope, paste the following-
```
PROJECT_ID=`gcloud config list --format "value(core.project)" 2>/dev/null`
PROJECT_NBR=`gcloud projects describe $PROJECT_ID | grep projectNumber | cut -d':' -f2 |  tr -d "'" | xargs`
USER_PROJECT_OVERRIDE=true
GOOGLE_BILLING_PROJECT=$PROJECT_ID  
UMSA_FQN="lab-sa@${PROJECT_ID}.iam.gserviceaccount.com"
YOUR_GCP_REGION="us-central1"
```

<hr>

### 2.2. IAM permissions

As part of the Terraform provisioned, we created a user managed service account (UMSA) to which we granted ourselves impersonation privileges. We will use this UMSA as the creator of the Dataplex constructs in this lab.

In Cloud Shell, paste the below to grant the UMSA, Dataplex admin privileges-

```
gcloud projects add-iam-policy-binding $PROJECT_ID --member=serviceAccount:$UMSA_FQN \
--role="roles/dataplex.admin"
```

![IAM](../01-images/03-03.png)   
<br><br>

<hr>

### 2.3. Enable Data Catalog Sync in the Dataproc Metastore Service if its not already enabled

We created a Dataproc Metastore Service with GRPC endpoint via Terraform. We enabled the Data Catalog sync so that databases and tables metadata in Dataproc Metastore are automatically hydrated in Data Catalog. 

![IAM](../01-images/03-04.png)   
<br><br>

Documentation: https://cloud.google.com/dataproc-metastore/docs/data-catalog-sync

## 3. Lab

### 3.1. Getting set up for the lab (assumes you already completed the provision step)

```
cd ~/dataplex-quickstart-labs/00-resources/terraform/provision

terraform init
```
<hr>

### 3.2. Organize the data using Terraform

rm -rf dataplex-quickstart-lab.output

```
terraform apply \
  -var="project_id=${PROJECT_ID}" \
  -var="project_number=${PROJECT_NBR}" \
  -var="gcp_region=${YOUR_GCP_REGION}" \
  --auto-approve >> dataplex-quickstart-lab.output
```
<hr>

### 3.3. Layout 

![IAM](../01-images/m3-02.png)   
<br><br>

#### 3.3.3. Pictorial of zones created


![LAKE](../01-images/03-22.png)   
<br><br>

<hr>

This concludes the lab module. Proceed to the [next module](module-04-register-assets-into-zones.md).

<hr>


