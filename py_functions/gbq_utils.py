# -*- coding: utf-8 -*-
"""
Created on Thu Oct 26 22:54:12 2023

@author: Arvin Jay
"""
import time,json
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd


def authenticate_bq(acct):
    """
    Authenticate to Google BigQuery personal trial service account.

    Parameters:
    acct (dict): secrets.json file from generated key

    Returns:
    tuple: A tuple containing the BigQuery client and credentials.
    """
    credentials = service_account.Credentials.from_service_account_info(
        acct
    )
    client = bigquery.Client(credentials=credentials,project=credentials.project_id)
    return client,credentials


def check_dataset(client,project_id,dataset_name):
    """
    Create a bigquery dataset if it does not exist.

    Parameters:
    client (bigquery.Client): The BigQuery client.
    project_id (str): The BigQuery project ID.
    dataset_name (str): The name of the dataset to check/create.

    Returns:
    None but status is printed in console 
    """
    datasets = [i.dataset_id for i in list(client.list_datasets())]
    if dataset_name not in datasets:
        platform_dataset = f"{project_id}.{dataset_name}" #format "project_id.platform" ie "lica-rdbms.rapide"
        dataset = bigquery.Dataset(platform_dataset)
        dataset.location = "US"
        dataset = client.create_dataset(dataset, timeout=30)
        print("Created dataset {}".format(platform_dataset))
        datasets = [i.dataset_id for i in list(client.list_datasets())]
        print("Updated GCP-Bigquery datasets")
        print(datasets)
    else:
        print("{} already in GCP-Bigquery".format(dataset_name.title()))
        pass


def bq_write(df,credentials,dataset_name,table_name,client,date_field='timestamp'):
    """
    Write a pandas DataFrame to a BigQuery table and is partitioned based on
    the date_field column per DAY. This saves us costs when we are dealing
    with a load of data.

    Parameters:
    df (pd.DataFrame): The DataFrame to write.
    credentials (google.auth.credentials.Credentials): credentials object 
    generated earlier.
    dataset_name (str): BigQuery dataset name
    table_name (str): BigQuery table name
    client (bigquery.Client): BigQuery client.
    date_field (str): The name of the date field in the DataFrame 
    to be partitioned.

    Returns:
    tuple: A tuple containing success (bool) and an error message (str).
    """
    success = False
    error = None
    if len(df)==0:
        error = 'No dataframe to be transferred'
        return success,error
    
    job_config = bigquery.LoadJobConfig(
                write_disposition = 'WRITE_TRUNCATE',
                source_format=bigquery.SourceFormat.CSV,
                autodetect=True,
                time_partitioning=bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.DAY,
                        field=date_field
                    )
                )
    target_table_id = f"{credentials.project_id}.{dataset_name}.{table_name}"
    job = client.load_table_from_dataframe(
                df, 
                target_table_id,
                job_config=job_config)
    while job.state != "DONE":
        time.sleep(2)
        job.reload()
    print(job.result())
    table =client.get_table(target_table_id)
    if table.num_rows==len(df):
        success = True
    print(
          'Loaded {} rows and {} columns to {}'.format(
              table.num_rows, len(table.schema), target_table_id)
          )
    return success,error


def secrets_configs(config = 'config.json'):
    """
    Load secrets and configuration which are json files.

    Parameters:
    secret (str): The path to the secrets JSON file.
    config (str): The path to the configuration JSON file.

    Returns:
    tuple: A tuple containing secrets (dict), dataset_name (str), and table_name (str).
    """
    config = open('config.json')
    config = json.load(config)
    dataset_name = config['tm_dataset']
    table_name = config['tm_table']
    return dataset_name,table_name


def write_process(df,secret):
    """
    Main algorithm for writing a table in Google Bigquery. this contains the
    functions described above

    Parameters:
    df (pd.DataFrame): The DataFrame to write.

    Returns:
    tuple: A tuple containing success (bool) and an error message (str).
    """
    dataset_name,table_name = secrets_configs()
    client,credentials = authenticate_bq(secret)
    check_dataset(client,credentials.project_id,dataset_name)
    success,error = bq_write(df,credentials,dataset_name,table_name,client)
    return success,error


def generate_table_id(credentials,dataset_name,table_name):
    """
    Generate the table ID for a BigQuery table to prepare for querying
    GBQ table.

    Parameters:
    credentials (google.auth.credentials.Credentials): Google Cloud credentials.
    dataset_name (str): The name of the BigQuery dataset.
    table_name (str): The name of the BigQuery table.

    Returns:
    str: The full table ID.
    """
    return f'{credentials.project_id}.{dataset_name}.{table_name}'


def query_bq_table(secret):
    """
    Query an entire table in GBQ. Although it's not standard to query
    an entire table, I thought it would be better to cache the query result
    instead of making the query again and again over a small dataset

    Returns:
    pd.DataFrame: The queried data as a DataFrame.
    """
    dataset_name,table_name = secrets_configs()
    client,credentials = authenticate_bq(secret)
    table_id = generate_table_id(credentials,dataset_name,table_name)
    query_str = f"""
                SELECT * FROM `{table_id}`
    """
    response = client.query(query_str).to_dataframe()
    return response

# """
# Sample implementation
# """
if __name__ == '__main__':
    dataset_name,table_name = secrets_configs()
    client,credentials = authenticate_bq(secret)
    check_dataset(client,credentials.project_id,dataset_name)
    df = pd.DataFrame()
    success,error = bq_write(df,credentials,dataset_name,table_name,client)

