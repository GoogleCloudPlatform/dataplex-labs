#!/bin/sh

cd ~/dataplex-quickstart-labs/00-resources/datasets/banking


rm -rf credit_card_reference_data_raw
tar -xvzf credit_card_reference_data_raw.tgz
rm credit_card_reference_data_raw.tgz

rm -rf credit_card_transactions_raw
tar -xvzf credit_card_transactions_raw.tgz
rm credit_card_transactions_raw.tgz

rm -rf customers_raw
tar -xvzf customers_raw.tgz
rm customers_raw.tgz

rm -rf merchants_raw
tar -xvzf merchants_raw.tgz
rm merchants_raw.tgz

echo "Completed untarring datasets successfully!"
