# Dataplex Catalog: Data discovery, search and Data Lineage 

## 1. About

Dataplex makes data easily searchable and discoverable based on a number of facets - including business domains, classification, and operational metrics such as data quality. As a data consumer you simply search for the data you are looking for - based on its domain, classification, ownership, name, and so on. 

Now it's time to explore how you can use the Dataplex catalog to perform advanced data discovery by being to trigger searches based on business metadata, get 360 degree context of your data product, provide business overview and explore data lineage. 

### 1.1. Prerequisites 
Successful completion of lab1, lab2, lab4 and Lab7 

### 1.2. Duration
~20  mins

### 1.3 Concepts


### 1.4. Scope of this lab

### 1.5. Note


### 1.6. Documentation

## 2. Lab

### 2.1 Add rich text overview to your data product

1. Go to Dataplex UI --> Search under Discover --> Type this in the search bar "tag:data_product_information customer data"
2. Click on the customer_data entry 
3. Under **OVERVIEW** -> Click on **+ADD OVERVIEW** -> paste the below text and you also insert a sample icon to represent your data product. 
```
Customer Demograhics Data Product 
This customer data table contains the data for customer demographics of all Bank of Mars retail banking customers. It contains PII information that can be accessed on "need-to-know" basis. 

Customer data is the information that customers give us while interacting with our business through websites, applications, surveys, social media, marketing initiatives, and other online and offline channels. A good business plan depends on customer data. Data-driven businesses recognize the significance of this and take steps to guarantee that they gather the client data points required to enhance client experience and fine-tune business strategy over time.
```
- Click Save 
- Sample screenshot 
![overview](/lab8-data-discovery-lineage/resources/imgs/cust_data_overview.png)
- Now as you see you have a 360 degree view of the customer data product - including technical metadata like schemas and fields , business metadata like wiki style product overview, business metadata such as - classification info, dq scores, data ownership..

![dp-overview](/lab8-data-discovery-lineage/resources/imgs/dp-overview.png) 


### 2.2 Data discovery and Search 

1. Use business filters to search for data products. Try out the below search string: 

    | What is your end user looking for?  | Search String |
    | ----------------------- | ------------- |
    | Search for data products  | tag:data_product_information |
    | Search for all data products that belong to the consumer banking domain | tag:data_product_information.domain:Consumer Banking  |
    | Search for all data products that are owned by Rebecca Piper (Customer Domain Owner) | tag:data_product_information.domain_owner:hannah.anderson@boma.com |
    | Search based on quality Score | tag:data_product_quality.data_quality_score>50 |
    | List all the data products with PII info | tag:data_product_classification.is_pii=true |
    | List data products which meets the SLA | tag:data_product_quality.timeliness_score=100 |
    | List all data products hosted on Analytics Hub | tag:data_product_exchange.data_exchange_platform:Analytics Hub |
    | List of data products that don’t have Data Quality Scores | tag:data_product_quality.data_quality_score=-1 |
    | Search for data products that our Master Data | tag:data_product_information.data_product_category="Master Data" | 

### 2.3 Explore Data lineage

1. Go to Dataplex UI --> Search under Discover --> Type this in the search bar "system=bigquery credit_card_transaction_data"
2.  Click on the credit_card_transaction_data entry
3.  Click on Data Lineage to explore the lineage
![lineage](/lab8-data-discovery-lineage/resources/imgs/lineage.png)


<hr>

This concludes the lab module. Either proceed to the [main menu](../README.md) or to the [next module](../lab8-data-discovery-lineage/) where you will learn to trigger a auto data profiling job

<hr>