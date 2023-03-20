
# M11-1e: Auto Data Quality - date of birth validity with SQL ROW checks

The focus of this lab module is auto data quality -  checks with BYO SQL, where you can specify column and have autoDQ check for specific criteria.

### Prerequisites

Successful completion of prior modules

### Duration

5 minutes or less

### Documentation 

[Data Quality Overview](https://cloud.google.com/dataplex/docs/data-quality-overview)<br>
[About Auto Data Quality](https://cloud.google.com/dataplex/docs/auto-data-quality-overview)<br>
[Use Auto Data Quality](https://cloud.google.com/dataplex/docs/use-auto-data-quality)<br>


### Learning goals

1. Understand options for data quality in Dataplex
2. Practical knowledge of running Auto Data Quality - SQL ROW checks feature


### Lab flow

![LF](../01-images/m11-1e-landing-flow.png)   
<br><br>


<hr>

# LAB

<hr>
<hr>

## 1. Target data for Data Quality checks

We will use the same table as in the Data Profiling lab module.

![ADQ-3](../01-images/module-10-1-04.png)   
<br><br>

Familiarize yourself with the table, from the BigQuery UI by running the SQL below-

```
SELECT * FROM oda_dq_scratch_ds.customer_master LIMIT 20

```

<hr>

## 2. Create a Data Quality scan with uniqueness checks on client_id column

### 2.1. Navigate to Auto Data Quality in Dataplex UI

![ADQ-3](../01-images/module-11-1-11.png)   
<br><br>

### 2.2. Click on Create Data Quality Scan

![ADQ-3](../01-images/module-11-1e-00.png)   
<br><br>



### 2.3. Define Data Quality Rules - SQL ROW checks to ensure validity of DOB (should be less than current date)

Click on the scan and define rules and follow the screnshots and submit.



![ADQ-3](../01-images/module-11-1e-12.png)   
<br><br>

![ADQ-3](../01-images/module-11-1e-13.png)   
<br><br>

![ADQ-3](../01-images/module-11-1e-14.png)   
<br><br>


![ADQ-3](../01-images/module-11-1e-15.png)   
<br><br>


### 2.4. Run Data Quality Rules - DOB validity with SQL ROW checks

Lets check all the fields for quality scan and click on "run now".

![ADQ-3](../01-images/module-11-1e-16.png)   
<br><br>

### 2.5. Job for Data Quality Rules - DOB validity with SQL ROW checks




![ADQ-3](../01-images/module-11-1e-11.png)   
<br><br>

### 2.6. Click on the DQ job that completed & review the results

![ADQ-3](../01-images/module-11-1e-17.png)   
<br><br>


<hr>

This concludes the module. Proceed to the [next module](module-11-1f-auto-dq-sql-aggregates.md).

<hr>




