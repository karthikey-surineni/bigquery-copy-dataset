import os
import sys
import logging
from bigquery import bigquery_connection
from config import default_config
from storage import storage_connection, storage_transfer

log = logging.getLogger(__file__)
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))


class MigrationPlan():
    def __init__(self, plan):
        self.__plan = plan
        self.__migration_config = default_config.MIGRATION_CONFIG[plan]
        self.__bigquery_client = bigquery_connection.BigQueryClient(
            project=self.__migration_config["source_project"]).bq_client
        # self.__storage_client = StorageConnection.StorageConnection(project=self.__migration_config["source_project"]).storage_client
        self.__asset_map = {}
        self.__information_schema_map = {}
        super().__init__()

    @property
    def migration_config(self):
        return self.__migration_config

    @property
    def bigquery_client(self):
        return self.__bigquery_client

    @property
    def information_schema_map(self):
        return self.__information_schema_map

    @property
    def asset_map(self):
        return self.__asset_map

    def retrieve_information_schema(self, asset_type):
        information_schema_asset_list = []
        dataset_entity = self.__bigquery_client.get_dataset(
            self.__bigquery_client.project+'.'+self.__migration_config["source_dataset"])
        job_config = bigquery_connection.BigQueryJobConfig(
            client=self.__bigquery_client,
            args=default_config.INFORMATION_SCHEMA_CONFIG).job_config

        query_job = self.__bigquery_client.query(
            query=default_config.INFORMATION_SCHEMA_QUERIES[asset_type].format(
                source_dataset=self.__migration_config["source_dataset"]),
            location=dataset_entity.location,
            job_config=job_config)
        log.info("Executing information schema query : {}".format(default_config.INFORMATION_SCHEMA_QUERIES[asset_type].format(
            source_dataset=self.__migration_config["source_dataset"]),
            location=dataset_entity.location,
            job_config=job_config))
        result = query_job.result()
        for row in query_job:
            schema_value_map = {}
            for id in range(len(row)):
                schema_value_map[result.schema[id].name] = row[id]
            information_schema_asset_list.append(schema_value_map)
        self.__information_schema_map[asset_type] = information_schema_asset_list

    def filter_base_tables(self):
        log.info("Filtering base tables")
        return list(filter(lambda x: x['table_type'] == 'BASE TABLE', self.__information_schema_map["tables"]))

    def show_information_schema_assets(self):
        log.info("Showing information schema assets")
        for k, v in self.__information_schema_map.items():
            print(k)
            for item in v:
                print(item)

    def extract_source_assets(self):
        log.info("Extracting source assets: tables, views and access entries")
        for dataset in self.__bigquery_client.list_datasets():
            dataset_entity = self.__bigquery_client.get_dataset(
                dataset.full_dataset_id.replace(":", "."))
            if self.__migration_config["source_dataset"] == dataset.dataset_id:
                for _ in self.__bigquery_client.list_tables(dataset.reference):
                    self.__asset_map['tables'] = (
                        _ for _ in self.__bigquery_client.list_tables(dataset.reference))
                    self.__asset_map['access_entries'] = (
                        _ for _ in dataset_entity.access_entries)

    def show_source_assets(self):
        log.info("Displaying source assets: tables, views and access entries")
        for asset_type in self.__asset_map.keys():
            print(asset_type)
            for _ in self.__asset_map[asset_type]:
                if asset_type == 'tables':
                    print(_.reference.to_api_repr())
                elif asset_type == 'access_entries':
                    if _.entity_type in ['userByEmail', 'userByGroup', 'view']:
                        print(_)

    # def create_transfer_config(self):
    #     transfer = {"Name" : "BLAH"}
    #     transfer_config = bigquery_connection.BigQueryTransferConfig(transfer)
