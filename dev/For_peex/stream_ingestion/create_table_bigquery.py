from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import os

def bq_create_dataset(dataset):
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset(dataset)

    try:
        dataset = bigquery_client.get_dataset(dataset_ref)
        print('Dataset {} already exists.'.format(dataset))
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = 'US'
        dataset = bigquery_client.create_dataset(dataset)
        print('Dataset {} created.'.format(dataset.dataset_id))
    return dataset

#Function to create a dataset in Table
def bq_create_table(dataset, table_name):
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset(dataset)

    # Prepares a reference to the table
    table_ref = dataset_ref.table(table_name)

    try:
        table = bigquery_client.get_table(table_ref)
        print('table {} already exists.'.format(table))
    except NotFound:
        schema = [
            bigquery.SchemaField('sys_transaction_time', 'TIMESTAMP', mode='REQUIRED'),
            bigquery.SchemaField('first_name', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('last_name', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('bithday', 'DATE', mode='REQUIRED'),
            bigquery.SchemaField('age', 'INTEGER', mode='REQUIRED'),
            bigquery.SchemaField('card_number', 'INTEGER', mode='REQUIRED'),
            bigquery.SchemaField('total_sum', 'FLOAT', mode='REQUIRED'),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table = bigquery_client.create_table(table)
        print('table {} created.'.format(table.table_id))
    return table

if __name__ == "__main__":
    #creating bigquery object
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'mykh_key.json'
    bigquery_client = bigquery.Client()
    dataset = "demo_dataset"
    table_name = "demo_table"
    data = bq_create_dataset(dataset)
    table = bq_create_table(dataset, table_name)
