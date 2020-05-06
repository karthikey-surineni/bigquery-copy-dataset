import json
import os
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import storage
import config.default_config as default_config


class BigQueryClient():
    def __init__(self, project=default_config.PROJECT, location=default_config.LOCATION, sa_path=default_config.SA_PATH):
        self.__project = project
        self.__location = location
        self.__sa_path = sa_path
        super().__init__()

    @property
    def bq_client(self):
        try:
            credentials = service_account.Credentials.from_service_account_file(
                filename=self.__sa_path,
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            client = bigquery.Client(
                project=self.__project, credentials=credentials, location=self.__location)
            return client
        except Exception as e:
            print(e)


class BigQueryJobConfig():
    def __init__(self, client, destination="", args={}):
        self.__client = client
        self.__job_config = bigquery.QueryJobConfig()
        # For Information Schema queries below
        if destination is not "":
            self.__job_config.create_disposition = args["create_disposition"]
            self.__job_config.write_disposition = args["write_disposition"]
            self.__job_config.destination_dataset = self.__client.dataset(
                args["destination_dataset"])
            self.__job_config.destination = self.__job_config.destination_dataset.table(
                destination)
        super().__init__()

    @property
    def job_config(self):
        return self.__job_config

#Service in alpha
# class BigQueryDataTransfer():
#     def __init__(self,bq_client,args):
#         self.__client = bigquery_datatransfer.DataTransferServiceClient()
#         self.__project = bq_client.project
#         self.__transfer_config = args
#         super().__init__()

#     def create_transfer_config(self):
#         for key in self.__transfer_config.keys():
#             if key not in default_config.TRANSFER_CONFIG.keys():
#                 raise KeyError("Transfer Config parameter: {} not found\nAvailable keys: ".format(key,default_config.TRANSFER_CONFIG.keys()))

#     @property
#     def transfer_config(self):
#         parent = self.__client.project_path(self.__project)
#         transfer_config = create_transfer_config()
        # response = self.__client.create_transfer_config(parent, transfer_config)
