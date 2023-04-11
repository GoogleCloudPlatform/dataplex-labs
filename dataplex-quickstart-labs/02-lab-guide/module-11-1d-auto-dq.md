
# M11-1c: Auto Data Quality - VALUE SET checks

The focus of this lab module is auto data quality - value set checks, where you can specify an allowed set of values and have autoDQ check for invalid values.

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
2. Practical knowledge of running Auto Data Quality - value set checks feature

<hr>
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

## 2. Create a Data Quality scan with value set checks on important columns

### 2.1. Navigate to Auto Data Quality in Dataplex UI

![ADQ-3](../01-images/module-11-1-11.png)   
<br><br>

### 2.2. Click on Create Data Quality Scan

![ADQ-3](../01-images/module-11-1c-00.png)   
<br><br>

![ADQ-3](../01-images/module-11-1c-01.png)   
<br><br>

### 2.3. Define Data Quality Rules - VALUE SET checks

Click on the scan and define rules. Lets start with recommendations from Data profiling results.

![ADQ-3](../01-images/module-11-1c-02.png)   
<br><br>

![ADQ-3](../01-images/module-11-1c-03.png)   
<br><br>

![ADQ-3](../01-images/module-11-1c-04.png)   
<br><br>

![ADQ-3](../01-images/module-11-1c-05.png)   
<br><br>

![ADQ-3](../01-images/module-11-1c-06.png)   
<br><br>


### 2.4. Run Data Quality Rules - VALUE SET checks

Lets check all the fields for quality scan and click on "run now".

![ADQ-3](../01-images/module-11-1c-10.png)   
<br><br>


![ADQ-3](../01-images/module-11-1c-07.png)   
<br><br>

### 2.5. Job for Data Quality Rules - VALUE SET checks gets submitted

![ADQ-3](../01-images/module-11-1c-08.png)   
<br><br>

### 2.6. Click on the DQ - VALUE SET job that completed & review the results

![ADQ-3](../01-images/module-11-1c-09.png)   
<br><br>


<hr>

<hr>

This concludes the module. Proceed to the next module.

<hr>




