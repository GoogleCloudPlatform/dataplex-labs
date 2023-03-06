#!/bin/bash

export PROJECT_ID=$(gcloud config get-value project)

bq mk \
    --schema ../resources/scripts/customer-schema.json \
    ${PROJECT_ID}:customer_data_product.customer_data


bq mk \
    --schema merchant_id:STRING,merchant_name:STRING,mcc:INT64,email:STRING,street:STRING,city:STRING,state:STRING,country:STRING,zip:STRING,latitude:FLOAT64,longitude:FLOAT64,owner_id:STRING,owner_name:STRING,terminal_ids:STRING,Description:STRING,Market_Segment:STRING,Industry_Code_Description:STRING,Industry_Code:STRING,ingest_date:DATE \
${PROJECT_ID}:merchants_data_product.core_merchants


bq mk \
--schema cc_token:STRING,merchant_id:STRING,card_read_type:INT64,entry_mode:STRING,trans_type:INT64,value:STRING,payment_method:INT64,pymt_name:STRING,swipe_code:INT64,swipe_value:STRING,trans_start_ts:TIMESTAMP,trans_end_ts:TIMESTAMP,trans_amount:STRING,trans_currency:STRING,trans_auth_code:INT64,trans_auth_date:FLOAT64,origination:INT64,is_pin_entry:INT64,is_signed:INT64,is_unattended:INT64,event_ids:STRING,event:STRING,version:INT64,ingest_date:DATE \
--time_partitioning_field ingest_date \
${PROJECT_ID_DW}:auth_data_product.auth_table

bq mk \
    --schema cc_number:INT64,cc_expiry:STRING,cc_provider:STRING,cc_ccv:INT64,cc_card_type:STRING,client_id:STRING,token:STRING,ingest_date:DATE \
    ${PROJECT_ID}:customer_data_product.cc_customer_data
