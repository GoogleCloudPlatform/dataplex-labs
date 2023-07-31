# M8-6: Business Glossary the Dataplex Catalog for a Catalog Entry

A business glossary is a collection of business terminology with description/definitions that organizations can standardize on. Dataplex offers a Business Glossary feature. In this module we will learn how to create a business glossary and enters items into it.

You can use Dataplex business glossary to do the following:

1. Create and maintain business glossaries and terms.
2. Establish relationships between terms as synonyms or related terms.
3. Attach terms to data entry columns.
4. Browse and filter terms within business glossary.
5. Display related terms and data entries for a given term.
6. Search for data entries tagged by a particular term.
7. Display related terms for a given data entry.

At the time of authoring of this lab (July 2023), the feature is in preview and needs explicity allow-listing.

### Prerequisites for the lab

Successful completion of prior modules

### Approximate duration

10 minutes or less to complete

### Pictorial overview of the lab flow

![CE](../01-images/m086-bg-00.png)   
<br><br>

### Documentation

https://cloud.google.com/dataplex/docs/create-glossary

<hr>

## 1. Concepts

### 1.1. Dataplex terminology levelset for Business Glossary feature
The following terminology is used in this module:

#### Business glossary
Business glossary is a repository that provides appropriate vocabularies, and governs the business terms of an enterprise, along with the associated term definitions and relationships between the terms. It provides a single place to maintain and manage business terms and their definitions. A business glossary has the following property displayed in the console:

#### Overview: 
A free-form rich-text description of business glossary purpose and content.

#### Term
A term is defined within a business glossary. It describes a concept used in a particular branch of business within an enterprise. A term has the following properties displayed in the console:

#### Description
A free-form rich text business definition of the term.
Data Steward: Specifies the person responsible for maintaining the term. This is a descriptive property and it doesn't affect the permissions on the term.
You can attach terms with data entry columns to indicate that the columns are described by the respective terms.

#### Synonym
A synonym is a relationship used to indicate semantic similarity or equivalence between two different terms. It is used when two similar terms are defined by different teams in different glossaries. For example, earnings and income.

#### Related term
A related term is a relationship used to indicate that two terms are semantically related, yet different. For example, profit and income.

## 1.2. Limitations

https://cloud.google.com/dataplex/docs/create-glossary#limitations

## 1.3. IAM permissions

To get the permissions that you need to create and manage glossaries, ask your administrator to grant you the following IAM roles on the project:

- Full access to glossaries and terms: DataCatalog Glossary Owner (roles/datacatalog.glossaryOwner)
- Read glossaries and terms, create attachments between terms, and create attachments between terms and data entries: DataCatalog Glossary User (roles/datacatalog.glossaryUser)
- Read-only access to glossaries and terms: DataCatalog Entry Viewer (roles/datacatalog.entryViewer)
- You also need to have the role that permits creation of entry groups (roles/datacatalog.entryGroupCreator)

<hr>


# 2. Lab

## 2.1. Create a Business Glossary

### 2.1.1. Grant yourself IAM permissions to create/work with a Business Glossary manually

Paste the below in Cloud Shell-
```

PROJECT_ID=`gcloud config list --format "value(core.project)" 2>/dev/null`
YOUR_GCP_ACCOUNT_NAME=`gcloud auth list --filter=status:ACTIVE --format="value(account)"`

gcloud projects add-iam-policy-binding $PROJECT_ID --member user:$YOUR_GCP_ACCOUNT_NAME --role roles/datacatalog.entryGroupCreator
gcloud projects add-iam-policy-binding $PROJECT_ID --member user:$YOUR_GCP_ACCOUNT_NAME --role roles/datacatalog.glossaryOwner
```
<hr>

### 2.1.2. Create a Business Glossary manually

Navigate to Dataplex Glossary UI and follow the screenshots below to create a glossary.<br>
The following is the text entered by the author into the Glossary named "Chicago Crimes Business Glossary" -<br>
```
**Chicago Crimes**
State of Illinois, City of Chicago has published Chicago crimes as a public dataset.
It includes records from the Crimes - 2001 to Present dataset for the indicated year.

 The dataset is available for download at:
https://data.cityofchicago.org/Public-Safety/Crimes-2022/9hwr-2zxp/data
```

![CE](../01-images/m086-bg-01.png)   
<br><br>


![CE](../01-images/m086-bg-02.png)   
<br><br>


![CE](../01-images/m086-bg-03.png)   
<br><br>


![CE](../01-images/m086-bg-04.png)   
<br><br>




<hr>

## 2.2. Add terms to the Business Glossary, and attach to entries, manually


### 2.2.1. Add terms to the Business Glossary manually

A CSV containing glossary terms for Chicago Crimes is available at:<br>
[https://github.com/GoogleCloudPlatform/dataplex-labs/blob/main/dataplex-quickstart-labs/00-resources/datasets/chicago-crimes/business_glossary/chicago_crimes_glossary.csv](https://github.com/GoogleCloudPlatform/dataplex-labs/blob/main/dataplex-quickstart-labs/00-resources/datasets/chicago-crimes/business_glossary/chicago_crimes_glossary.csv)<br>

Follow the steps below to add a couple terms. We are going to learn what the experience is and then delete and bulk upload from a CSV as shown in section 2.2.2.<br>

![CE](../01-images/m086-bg-05.png)   
<br><br>

![CE](../01-images/m086-bg-06.png)   
<br><br>

![CE](../01-images/m086-bg-07.png)   
<br><br>

![CE](../01-images/m086-bg-08.png)   
<br><br>

<hr>

### 2.2.2. Attach terms in the Business Glossary manually to Catalog entries manually





### 2.2.3. Bulk upload CSV spreadsheet with terms into the Business Glossart manually




<hr>



This concludes the lab module. You can proceed to the [next module](module-09-1-data-lineage-with-bigquery.md).

<hr>
