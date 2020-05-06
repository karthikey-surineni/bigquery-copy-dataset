import os
import sys
sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "src")))

import logging
from google.cloud import bigquery
from storage import storage_transfer
from config import default_config
from bigquery import bigquery_connection, migration_plan
import collections
import threading

# tables = ["incidents_2008",
# "incidents_2009",
# "incidents_2010",
# "incidents_2011",
# ]
log = logging.getLogger(__file__)
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))

def extract_transfer_tables(info_list):
    return (_['table_name'] for _ in info_list)


def table_export(client, dataset_ref, dataset_location, table, bucket):
    destination_uri = "gs://{}/{}-*.avro".format(bucket, table)
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

    log.info(
        "Exported {}:{}.{} to {}".format(
            client.project, dataset_ref.dataset_id, table, destination_uri)
    )


def table_import(client, dataset_ref, dataset_location, table, bucket):
    uri = "gs://{}/{}-*.avro".format(bucket, table)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.AVRO
    load_job = client.load_table_from_uri(
        uri,
        dataset_ref.table(table),
        location=dataset_location,
        job_config=job_config,
    )
    log.info("Starting job: {} for table: {}".format(load_job.job_id,table))

    load_job.result()
    log.info("Job finished.")

    destination_table = client.get_table(dataset_ref.table(table))
    log.info("Loaded {} rows to table {}".format(destination_table.num_rows,table))


def transfer_files(source_bucket, sink_bucket):
    transfer = storage_transfer.StorageTransfer(
        source_bucket=source_bucket, sink_bucket=sink_bucket)
    transfer.create_transfer()
    response = {}
    while(response == {} or response['operations'][0]['metadata']['status'] == 'IN_PROGRESS'):
        response = transfer.get_transfer_status()
        if(response != {}):
            log.info("{}..".format(response['operations'][0]['metadata']['status']))
    if (response['operations'][0]['metadata']['status']=='SUCCESS'):
        transfer.remove_transfer()


def create_views(client, source_dataset_ref, destination_dataset_ref, dataset_location, view_meta):
    view_ref = destination_dataset_ref.table(view_meta["table_name"])
    view = bigquery.Table(view_ref)
    view.view_query = view_meta["view_definition"].replace(
        source_dataset_ref.dataset_id, destination_dataset_ref.dataset_id)
    view = client.create_table(view)
    log.info("Successfully created view at {}".format(view.full_table_id))

# def create_access_entries()


def create_transfer(table_list, view_list, source_dataset_ref, source_dataset_location, destination_dataset_ref, destination_dataset_location, source_bucket, sink_bucket):
    queue_export_tables = []
    queue_import_tables = []
    queue_views = []
    for table_name in table_list:
        log.info("Adding {} table for transfer".format(table_name))
        queue_export_tables.append(threading.Thread(target=table_export, args=(
            client, source_dataset_ref, source_dataset_location, table_name, source_bucket)))
        queue_import_tables.append(threading.Thread(target=table_import, args=(
            client, destination_dataset_ref, destination_dataset_location, table_name, sink_bucket)))
    for view in view_list:
        log.info("Adding {} view for transfer".format(view["table_name"]))
        queue_views.append(threading.Thread(target=create_views, args=(
            client, source_dataset_ref, destination_dataset_ref, destination_dataset_location, view)))

    # Step 1 - Run Export for all tables in source dataset
    run_all_tasks(queue_export_tables)
    # Step 2 - Run Transfer for all files across buckets
    transfer_files(source_bucket, sink_bucket)
    # Step 3 - Run Import for all files to tables in destination dataset
    run_all_tasks(queue_import_tables)
    # Step 4 - Run Import for all files to tables in destination dataset
    # run_all_tasks(queue_views)


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

    # Extracting Base tables
    # -----------------------------
    m.retrieve_information_schema("tables")
    # m.show_information_schema_assets()
    table_list = extract_transfer_tables(m.filter_base_tables())

    # # Source dataset for export
    source_dataset_ref = client.dataset(
        source_dataset_id, project=client.project)
    source_dataset = client.get_dataset(client.project+'.'+source_dataset_id)

    # # Destination dataset for export
    destination_dataset_ref = client.dataset(
        destination_dataset_id, project=client.project)
    destination_dataset = client.get_dataset(
        client.project+'.'+destination_dataset_id)

    # Extracting Views
    # -----------------------------
    m.retrieve_information_schema("views")
    view_list = m.information_schema_map["views"]
    create_transfer(table_list, view_list, source_dataset_ref, source_dataset.location,
                    destination_dataset_ref, destination_dataset.location, source_bucket, sink_bucket)

    # except KeyError:
    #     log.info("Available Keys : ",default_config.MIGRATION_CONFIG.keys())
