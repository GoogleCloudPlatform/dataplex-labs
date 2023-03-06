# Data Profiling in Dataplex

## 1. About

Dataplex data profiling lets you identify common statistical characteristics of the columns of your BigQuery tables. This information helps data consumers understand their data better, which makes it possible to analyze data more effectively. Dataplex also uses this information to recommend rules for data quality.

### 1.1. Options for Data Profiling in Dataplex
1. Auto Data Profiling
2. User Configured Data Profiling

### 1.2. Scope of this lab
User Configured Data Profiling

### 1.3. Note
1. This feature is currently supported only for BigQuery tables.
2. Data profiling compute used is Google managed, so you don't need to plan for/or handle any infrastructure complexity.

### 1.4. Documentation
[About](https://cloud.google.com/dataplex/docs/data-profiling-overview#limitations_in_public_preview) | 
[Practitioner's Guide](https://cloud.google.com/dataplex/docs/use-data-profiling)

### 1.5. User Configured Dataplex Profiling - what's involved

| # | Step | 
| -- | :--- |
| 1 | A User Managed Service Account is needed with ```roles/dataplex.dataScanAdmin``` to run the profiling job|
| 2 | A scan profile needs to be created against a table|
| 3 | In the scan profile creation step, you can select a full scan or incremental|
| 4 | In the scan profile creation step, you can configure profiing to run on schedue or on demand|
| 5 | Profiling results are visually displayed|
| 6 | [Configure RBAC](https://cloud.google.com/dataplex/docs/use-data-profiling#datascan_permissions_and_roles) for running scan versus viewing results |

### 1.6. User Configured Dataplex Profiling - what's supported

![supported](/data-mesh-banking-labs/lab9-data-profiling/resources/imgs/lab-profiling-01.png)

### 1.7. Roles for Data Profiling - what's available

```
role/dataplex.dataScanAdmin: Full access to DataScan resources.
role/dataplex.dataScanEditor: Write access to DataScan resources.
role/dataplex.dataScanViewer: Read access to DataScan resources, excluding the results.
role/dataplex.dataScanDataViewer: Read access to DataScan resources, including the results.
```
[Documentation on RBAC](https://cloud.google.com/dataplex/docs/use-data-profiling#datascan_permissions_and_roles)


### 1.8. Provisioning Data Profiling tasks - what's supported

At the time of authoring of this lab, Console and REST API only


<hr>

## 2. Lab

### 2.1. Grant requisite IAM permissions

The User Managed Service Account customer-sa@ needs privileges to create and run profiling scans. From the Cloud Shell scoped to your project, run the below:

```
PROJECT_ID=`gcloud config list --format "value(core.project)" 2>/dev/null`
CUSTOMER_UMSA_FQN="customer-sa@${PROJECT_ID}.iam.gserviceaccount.com"

gcloud projects add-iam-policy-binding $PROJECT_ID --member=serviceAccount:$CUSTOMER_UMSA_FQN \
--role="roles/dataplex.dataScanAdmin"
```

### 2.2. Create a Scan Profile from the Dataplex "Profile" UI

#### 2.2.1. Navigate to the "Profile" UI from the Dataplex landing page

![navigate](/data-mesh-banking-labs/lab9-data-profiling/resources/imgs/lab-profiling-02.png)

<br><br>

#### 2.2.2. Create a profile scan as depicted below

![navigate](/data-mesh-banking-labs/lab9-data-profiling/resources/imgs/lab-profiling-03.png)

<br><br>

#### 2.2.3. Review the profile scan list & click on the scan profile created

![navigate](/data-mesh-banking-labs/lab9-data-profiling/resources/imgs/lab-profiling-04.png)

<br><br>

### 2.3. Run the Data Profiling scan created

![navigate](/data-mesh-banking-labs/lab9-data-profiling/resources/imgs/lab-profiling-06.png)

<br><br>

![navigate](/data-mesh-banking-labs/lab9-data-profiling/resources/imgs/lab-profiling-07.png)

<br><br>



### 2.4. Review the Data Profiling Results

![navigate](/data-mesh-banking-labs/lab9-data-profiling/resources/imgs/lab-profiling-08.png)

<br><br>

### 2.5. Run multiple times

Note how you cannot switch to incremental mode.

![navigate](/data-mesh-banking-labs/lab9-data-profiling/resources/imgs/lab-profiling-08.png)

<br><br>

### 2.6. Challenge

Create a partitioned BQ table, run the profiling in an incremental mode, add some data and run profiling again and observe the results.

<br><br>

<hr>

This concludes the lab module. Proceed to the main menu.

<hr>