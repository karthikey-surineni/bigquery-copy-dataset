import os
import sys
sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "src")))
import logging
from google.cloud import bigquery
from storage import storage_transfer
from config import default_config
from bigquery import bigquery_connection, migration_plan
from dbt import dbt_config_creator
import yaml
import collections
import threading
import re
import time

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
    log.info("Starting job: {} for table: {}".format(load_job.job_id, table))

    load_job.result()
    log.info("Job finished.")

    destination_table = client.get_table(dataset_ref.table(table))
    log.info("Loaded {} rows to table {}".format(
        destination_table.num_rows, table))


def transfer_files(source_bucket, sink_bucket):
    transfer = storage_transfer.StorageTransfer(
        source_bucket=source_bucket, sink_bucket=sink_bucket)
    transfer.create_transfer()
    response = {}
    while(response == {} or response['operations'][0]['metadata']['status'] == 'IN_PROGRESS'):
        time.sleep(12)
        response = transfer.get_transfer_status()
        if(response != {}):
            log.info("{}..".format(
                response['operations'][0]['metadata']['status']))
    if (response['operations'][0]['metadata']['status'] == 'SUCCESS'):
        transfer.remove_transfer()

def filter_views_referencing_base_tables(table_list, view_list, source_dataset_ref):
    v_base_table = []
    v_complex = []
    for table in table_list:
        for view in view_list:
            search_view_str = f"{view['view_definition']}"
            search_table_str = f"{source_dataset_ref.dataset_id}.{table}"
            if search_table_str in search_view_str:
                v_base_table.append(view)
    for item in view_list:
        if item not in v_base_table:
            v_complex.append(item)
    return [v_base_table, v_complex]

def create_dbt_schema(destination_dataset_ref,table_list):
    table_config_list = []
    # TEST_SOURCES = [{'name':'test_AU','tables':[{'name':'incidents_2008'},{'name':'incidents_2008'}]}]
    dbt_config = dbt_config_creator.DBTConfig(path=destination_dataset_ref.dataset_id)
    for table in table_list:
        table_config_list.append({'name':str(table)})
    # print([{'name':destination_dataset_ref.dataset_id,'tables':table_config_list}])
    source_dict = [{'name':destination_dataset_ref.dataset_id,'tables':table_config_list}]
    dbt_config.write_sources(source_dict)
    dbt_config.write_config()
    return dbt_config

def create_dbt_model_views(client, source_dataset_ref, destination_dataset_ref, dataset_location, dbt_config, view_meta, is_base_view=False):

    # Creating Jinja template tags
    JINJA_COMPLEX_VIEW_REPL = "{{{{ ref('{reference}') }}}}"
    JINJA_BASE_VIEW_REPL = "{{{{ source("+f"'{destination_dataset_ref.dataset_id}'"+",'{reference}') }}}}"
    if (is_base_view == True):
        jinja_replacement_str = JINJA_BASE_VIEW_REPL
    else:
        jinja_replacement_str = JINJA_COMPLEX_VIEW_REPL

    # dbt_config = create_dbt_schema(destination_dataset_ref,table_list)
    view_query = view_meta["view_definition"].replace(
            source_dataset_ref.dataset_id, destination_dataset_ref.dataset_id)
    match_list = re.findall(r"FROM.*`(.*)`",view_query)
    for match in match_list:
        if(match.split(".")[-2]!=destination_dataset_ref.dataset_id):
            log.info("Unsuccessfull at saving view query '{}' reference to '{}' dataset".format(f"{view_meta['table_name']}",match.split(".")[-2]))
            return
        if(match.split(".")[-3]!=destination_dataset_ref.project):
            log.info("Unsuccessfull at saving view query '{}' reference to '{}' project".format(f"{view_meta['table_name']}",match.split(".")[-3]))
            return
        test = ""
        test = re.sub(f'`{match}`',jinja_replacement_str.format(reference=match.split(".")[-1]),view_query)
        view_query = test
    dbt_config.write_model_sql(view_meta['table_name'],view_query)
    log.info("Successfully saved view query {}".format(f"{view_meta['table_name']}"))


def create_views(client, source_dataset_ref, destination_dataset_ref, dataset_location, view_meta):
    view_ref = destination_dataset_ref.table(view_meta["table_name"])
    view = bigquery.Table(view_ref)
    view.view_query = view_meta["view_definition"].replace(
        source_dataset_ref.dataset_id, destination_dataset_ref.dataset_id)
    view = client.create_table(view)
    log.info("Successfully created view at {}".format(view.full_table_id))

# def create_access_entries()


def create_transfer(table_list, view_base_table_list, view_complex_list, source_dataset_ref, source_dataset_location, destination_dataset_ref, destination_dataset_location, source_bucket, sink_bucket):
    queue_export_tables = []
    queue_import_tables = []
    queue_complex_views = []
    queue_base_table_views = []

    for table_name in table_list:
        log.info("Adding {} table for transfer".format(table_name))
        queue_export_tables.append(threading.Thread(target=table_export, args=(
            client, source_dataset_ref, source_dataset_location, table_name, source_bucket)))
        queue_import_tables.append(threading.Thread(target=table_import, args=(
            client, destination_dataset_ref, destination_dataset_location, table_name, sink_bucket)))
    # for view_base in view_base_table_list:
    #     log.info("Adding {} view for transfer".format(view_base["table_name"]))
    #     # TODO: Change create_dbt_views to create_views
    #     queue_base_table_views.append(threading.Thread(target=create_views, args=(
    #         client, source_dataset_ref, destination_dataset_ref, destination_dataset_location, view_base)))

    dbt_config = create_dbt_schema(destination_dataset_ref,table_list)
    for view_base in view_base_table_list:
        log.info("Adding {} view for dbt deploy".format(view_base["table_name"]))
        # TODO: Change create_dbt_views to create_views
        queue_base_table_views.append(threading.Thread(target=create_dbt_model_views, args=(
            client, source_dataset_ref, destination_dataset_ref, destination_dataset_location, dbt_config, view_base, True)))

    for view_complex in view_complex_list:
        log.info("Adding {} view for dbt deploy".format(view_complex["table_name"]))
        # TODO: Change create_dbt_views to create_views
        queue_complex_views.append(threading.Thread(target=create_dbt_model_views, args=(
            client, source_dataset_ref, destination_dataset_ref, destination_dataset_location, dbt_config, view_complex, False)))

    # Step 1 - Run Export for all tables in source dataset
    run_all_tasks(queue_export_tables)
    # Step 2 - Run Transfer for all files across buckets
    transfer_files(source_bucket, sink_bucket)
    # Step 3 - Run Import for all files to tables in destination dataset
    run_all_tasks(queue_import_tables)
    # Step 5 - Run View Creation for Base Tables for all files to tables in destination dataset
    run_all_tasks(queue_base_table_views)
    # Step 4 - Run View Creation for Base Tables for all files to tables in destination dataset
    run_all_tasks(queue_complex_views)


def run_all_tasks(tasks):
    for thread in tasks:
        thread.start()
    for thread in tasks:
        thread.join()


if __name__ == "__main__":
    # try:
    tables_to_transfer = []
    m = migration_plan.MigrationPlan("London_To_Australia")
    client = m.bigquery_client
    source_dataset_id = m.migration_config["source_dataset"]
    destination_dataset_id = m.migration_config["destination_dataset"]
    source_bucket = m.migration_config["source_bucket"]
    sink_bucket = m.migration_config["sink_bucket"]

    # Extracting Base tables
    # -----------------------------
    m.retrieve_information_schema("tables")
    # log.info("AFTER TABLES EXTRACT")
    # m.show_information_schema_assets()
    # m.show_information_schema_assets()
    table_list = extract_transfer_tables(m.filter_base_tables())
    for table in table_list:
        tables_to_transfer.append(table)

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
    # log.info("AFTER VIEWS EXTRACT")
    # m.show_information_schema_assets()
    view_list = m.information_schema_map["views"]
    [view_base_table_list, view_complex_list] = filter_views_referencing_base_tables(
        tables_to_transfer, view_list, source_dataset_ref)

    print("------BASE------\n",view_base_table_list)
    print("------COMPLEX------\n",view_complex_list)
    create_transfer(tables_to_transfer, view_base_table_list, view_complex_list, source_dataset_ref, source_dataset.location,
                    destination_dataset_ref, destination_dataset.location, source_bucket, sink_bucket)

    # except KeyError:
    #     log.info("Available Keys : ",default_config.MIGRATION_CONFIG.keys())
