import argparse
import datetime
import json
import os
import logging
import config.default_config as default_config
from google.oauth2 import service_account
from pprint import pprint
import googleapiclient.discovery

log = logging.getLogger(__file__)
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))

class StorageTransfer():
    def __init__(self, **args):
        if not os.path.isfile(default_config.SA_PATH):
            self.__storagetransfer = googleapiclient.discovery.build(
                'storagetransfer', 'v1')
        else:
            credentials = service_account.Credentials.from_service_account_file(
                filename=default_config.SA_PATH,
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            self.__storagetransfer = googleapiclient.discovery.build(
                'storagetransfer', 'v1', credentials=credentials)
        # request = self.__storagetransfer.googleServiceAccounts().get(projectId=default_config.PROJECT)
        # response = request.execute()
        # pprint(response)
        self.__transfer_job = default_config.STORAGE_TRANSFER_CONFIG
        self.__transfer_job['transferSpec']['gcsDataSource']['bucket_name'] = args["source_bucket"]
        self.__transfer_job['transferSpec']['gcsDataSink']['bucket_name'] = args["sink_bucket"]
        self.__transfer_job['description'] = "Transfer from gs://{} to gs://{}".format(
            args["source_bucket"], args["sink_bucket"])
        self.__job_name = ""
        super().__init__()

    def create_transfer(self):
        result = self.__storagetransfer.transferJobs().create(
            body=self.__transfer_job).execute()
        self.__job_name = result["name"]
        # print('Returned transferJob: {}'.format(
        #     json.dumps(result, indent=4)))

    def get_transfer_status(self):
        filterString = (
            '{{"project_id": "{project_id}", '
            '"job_names": ["{job_name}"]}}'
        ).format(project_id=self.__transfer_job['projectId'], job_name=self.__job_name)

        result = self.__storagetransfer.transferOperations().list(
            name="transferOperations",
            filter=filterString).execute()
        # print('Result of transferOperations/list: {}'.format(
        #     json.dumps(result, indent=4, sort_keys=True)))
        return result

    def remove_transfer(self):
        update_transfer_job_request_body = {
            "projectId": self.__transfer_job['projectId'],
            "update_transfer_job_field_mask": "status",
            "transferJob": {"status": "DELETED"}
        }
        request = self.__storagetransfer.transferJobs().patch(
            jobName=self.__job_name, body=update_transfer_job_request_body)
        response = request.execute()
        print('Result of transferJob/patch: {}'.format(
            json.dumps(response, indent=4, sort_keys=True)))
        return response
