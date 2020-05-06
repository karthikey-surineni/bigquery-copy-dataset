import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)),"src")))
import threading
import collections
from bigquery import bigquery_connection, migration_plan
from config import default_config
from storage import storage_transfer
from google.cloud import bigquery

# tables = ["incidents_2008",
# "incidents_2009",
# "incidents_2010",
# "incidents_2011",
# ]
# source_config = {}
# destination_config = {}
# source_bucket = "test_bucket_london"
# sink_bucket = "test_bucket_au"
# for _ in tables:
#     source_config[_] = {"dataset":"test_london","bucket_name":source_bucket,"table":f"{_}"}
#     destination_config[_] = {"dataset":"test_au","bucket_name":sink_bucket,"table":f"{_}"}

def extract_transfer_tables(info_list):
    return (_['table_name'] for _ in info_list)

def table_export(client,dataset_ref,dataset_location,table,bucket):
    destination_uri = "gs://{}/{}-*.avro".format(bucket,table)
    job_config = bigquery.job.ExtractJobConfig()
    job_config.destination_format = bigquery.DestinationFormat.AVRO
    table_ref = dataset_ref.table(table)
    extract_job = client.extract_table(
    table_ref,
    destination_uri,
    job_config=job_config,
    location=dataset_location,
    )
    extract_job.result()

    print(
        "Exported {}:{}.{} to {}".format(client.project, dataset_ref.dataset_id, table, destination_uri)
    )

def table_import(client,dataset_ref,dataset_location,table,bucket):
    uri = "gs://{}/{}-*.avro".format(bucket,table)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.AVRO
    load_job = client.load_table_from_uri(
        uri,
        dataset_ref.table(table),
        location=dataset_location,
        job_config=job_config,
    )
    print("Starting job {}".format(load_job.job_id))

    load_job.result()
    print("Job finished.")

    destination_table = client.get_table(dataset_ref.table(table))
    print("Loaded {} rows.".format(destination_table.num_rows))

def transfer_files(source_bucket,sink_bucket):
    transfer = storage_transfer.StorageTransfer(source_bucket=source_bucket,sink_bucket=sink_bucket)
    transfer.create_transfer()
    x = {}
    while(x == {} or x['operations'][0]['metadata']['status']!='IN_PROGRESS'):
        x = transfer.get_transfer_status()
        if(x!={}):
            print("{}..".format(x['operations'][0]['metadata']['status']))

def create_transfer(table_list,source_dataset_ref,source_dataset_location,destination_dataset_ref,destination_dataset_location,source_bucket,sink_bucket):
    queue_export_tables = []
    queue_import_tables = []
    for table_name in table_list:
        print("Adding {} table for transfer".format(table_name))
        queue_export_tables.append(threading.Thread(target=table_export, args=(client,source_dataset_ref,source_dataset_location,table_name,source_bucket)))
        queue_import_tables.append(threading.Thread(target=table_import, args=(client,destination_dataset_ref,destination_dataset_location,table_name,sink_bucket)))
    #Step 1 - Run Export for all tables in source dataset
    run_all_tasks(queue_export_tables)
    #Step 2 - Run Transfer for all files across buckets
    transfer_files(source_bucket,sink_bucket)
    #Step 3 - Run Import for all files to tables in destination dataset
    run_all_tasks(queue_import_tables)

def run_all_tasks(tasks):
    for thread in tasks:
        thread.start()
    for thread in tasks:
        thread.join()

if __name__ == "__main__":
    # try:
        m = migration_plan.MigrationPlan("London_To_Australia")
        client = m.bigquery_client
        source_dataset_id = m.migration_config["source_dataset"]
        destination_dataset_id = m.migration_config["destination_dataset"]
        source_bucket = m.migration_config["source_bucket"]
        sink_bucket = m.migration_config["sink_bucket"]

        m.retrieve_information_schema("tables")
        # m.show_information_schema_assets()
        table_list = extract_transfer_tables(m.filter_base_tables())

        # # Source dataset for export
        source_dataset_ref = client.dataset(source_dataset_id, project=client.project)
        source_dataset = client.get_dataset(client.project+'.'+source_dataset_id)

        # # Destination dataset for export
        destination_dataset_ref = client.dataset(destination_dataset_id, project=client.project)
        destination_dataset = client.get_dataset(client.project+'.'+destination_dataset_id)
        create_transfer(table_list,source_dataset_ref,source_dataset.location,destination_dataset_ref,destination_dataset.location,source_bucket,sink_bucket)

    # except KeyError:
    #     print("Available Keys : ",default_config.MIGRATION_CONFIG.keys())
