# M8-2: Create a custom entry in Dataplex Catalog & designate a steward

In this lab module, we will learn how to create entries in Cloud Catalog, manually, using the Metadata API. We will first create an entry group and then an entry belonging to the group. In a subsequent lab, we will use it to demonstrate lineage. We will designate a steward for the entry.

### Prerequisites
Successful completion of prior modules

### Approximate duration
15 minutes or less to complete

### Pictorial overview of lab

![IAM](../01-images/m082-00.png)   
<br><br>

<hr>

## LAB

## 1. Create custom entry group

In this section, we will create an entry group called "Government Datasets". Paste the below in Cloud Shell.

```
PROJECT_ID=`gcloud config list --format "value(core.project)" 2>/dev/null`
PROJECT_NBR=`gcloud projects describe $PROJECT_ID | grep projectNumber | cut -d':' -f2 |  tr -d "'" | xargs`
LOCATION="us-central1"

ENTRY_GROUP_ID="government_datasets"

rm -rf requestCustomEntryGroup.json
echo "{ \"displayName\": \"Government Datasets\" }" >>  requestCustomEntryGroup.json

curl -X POST \
    -H "Authorization: Bearer $(gcloud auth print-access-token)" \
    -H "x-goog-user-project: $PROJECT_ID" \
    -H "Content-Type: application/json; charset=utf-8" \
    -d @requestCustomEntryGroup.json \
    "https://datacatalog.googleapis.com/v1/projects/$PROJECT_ID/locations/$LOCATION/entryGroups?entryGroupId=$ENTRY_GROUP_ID"

```

![CE](../01-images/M07-Entry-01.png)   
<br><br>


![CE](../01-images/M07-Entry-02.png)   
<br><br>



<hr>

## 2. Create custom entry

In this section, we will create an entry, as detailed in the table below, for the "Chicago Crimes Dataset", as part of the entry group "Government Datasets", above. 

| Key | Value  | 
| -- | :--- | 
| Entry ID | chicago_crimes_dataset |
| Entry Group | government_datasets |
| User Specified Type | CSV |
| User Specified System | Internet |
| Display Name | Chicago Crimes Public Dataset |
| Decsctiption | Chicago Crimes Data available at https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2 |



Paste the below in Cloud Shell.
```
ENTRY_ID="chicago_crimes_dataset"

rm -rf requestCustomEntry.json
echo "{\"description\": \"Chicago Crimes Data available at https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2\",\"displayName\": \"Chicago Crimes Public Dataset\",\"user_specified_type\": \"CSV\",\"user_specified_system\": \"Internet\"}" >>  requestCustomEntry.json

curl -X POST \
    -H "Authorization: Bearer $(gcloud auth print-access-token)" \
    -H "x-goog-user-project: $PROJECT_ID" \
    -H "Content-Type: application/json; charset=utf-8" \
    -d @requestCustomEntry.json \
    "https://datacatalog.googleapis.com/v1/projects/$PROJECT_ID/locations/$LOCATION/entryGroups/$ENTRY_GROUP_ID/entries?entryId=$ENTRY_ID"
    
```

![CE](../01-images/M07-Entry-03.png)   
<br><br>

<hr>

## 3. Catalog Search UI walkthrough 

Navigate to Dataplex in the Cloud Console and to "Search" on the left navigation menu.

![CE](../01-images/M07-Entry-04.png)   
<br><br>

![CE](../01-images/M07-Entry-05.png)   
<br><br>

![CE](../01-images/M07-Entry-06.png)   
<br><br>

<hr>

## 4. Add a Data Steward for the custom Catalog entry

Where we left off in the Catalog Entry listing, click on the steward icon on the extreme right and assign yourself as the steward, as shown below.

![CE](../01-images/M07-Entry-07.png)   
<br><br>

![CE](../01-images/M07-Entry-08.png)   
<br><br>

![CE](../01-images/M07-Entry-09.png)   
<br><br>



<hr>

This concludes the lab module. You can proceed to the [next module](module-08-3-create-tag-template-for-catalog-entry.md).

<hr>
