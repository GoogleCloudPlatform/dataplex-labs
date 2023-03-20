# M8-5: Business Glossary the Dataplex Catalog for a Catalog Entry

A business glossary is a collection of business terminology with description/definitions that organizations can standardize on. Dataplex offers a Business Glossary feature. In this module we will learn how to create a business glossary and enters items into it.

You can use Dataplex business glossary to do the following:

1. Create and maintain business glossaries and terms.
2. Establish relationships between terms as synonyms or related terms.
3. Attach terms to data entry columns.
4. Browse and filter terms within business glossary.
5. Display related terms and data entries for a given term.
6. Search for data entries tagged by a particular term.
7. Display related terms for a given data entry.


### Prerequisites for the lab

Successful completion of prior modules

### Approximate duration

10 minutes or less to complete

### Pictorial overview

![CE](../01-images/m086-00.png)   
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

<hr>
<hr>

## 2. Lab


### 2.1. Grant IAM permissions to work with Business Glossary feature

<hr>

This concludes the lab module. You can proceed to the [next module](module-09-1-data-lineage-with-bigquery.md).

<hr>
