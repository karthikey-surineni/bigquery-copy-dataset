import argparse
import datetime
import json
import config.default_config as default_config
from google.oauth2 import service_account
from pprint import pprint
import googleapiclient.discovery
# from google_auth_httplib2 import AuthorizedHttp
# from oauth2client.service_account import ServiceAccountCredentials

class StorageTransfer():
    def __init__(self,**args):
        # credentials = ServiceAccountCredentials.from_json_keyfile_name(
        # default_config.SA_PATH, scopes=['https://www.googleapis.com/auth/cloud-platform'])
        credentials = service_account.Credentials.from_service_account_file(
            filename=default_config.SA_PATH,
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        self.__storagetransfer = googleapiclient.discovery.build('storagetransfer', 'v1',credentials=credentials)
        # request = self.__storagetransfer.googleServiceAccounts().get(projectId=default_config.PROJECT)
        # response = request.execute()
        # pprint(response)
        self.__transfer_job = default_config.STORAGE_TRANSFER_CONFIG
        self.__transfer_job['transferSpec']['gcsDataSource']['bucket_name'] = args["source_bucket"]
        self.__transfer_job['transferSpec']['gcsDataSink']['bucket_name'] = args["sink_bucket"]
        self.__transfer_job['description'] = "Transfer from gs://{} to gs://{}".format(args["source_bucket"],args["sink_bucket"])
        self.__job_name = ""
        super().__init__()

    def create_transfer(self):
        result = self.__storagetransfer.transferJobs().create(body=self.__transfer_job).execute()
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


# if __name__ == '__main__':
#     parser = argparse.ArgumentParser(
#         description=__doc__,
#         formatter_class=argparse.RawDescriptionHelpFormatter)
#     parser.add_argument('description', help='Transfer description.')
#     parser.add_argument('project_id', help='Your Google Cloud project ID.')
#     parser.add_argument('start_date', help='Date YYYY/MM/DD.')
#     parser.add_argument('start_time', help='UTC Time (24hr) HH:MM:SS.')
#     parser.add_argument('source_bucket', help='Standard GCS bucket name.')
#     parser.add_argument('sink_bucket', help='Nearline GCS bucket name.')

#     args = parser.parse_args()
#     start_date = datetime.datetime.strptime(args.start_date, '%Y/%m/%d')
#     start_time = datetime.datetime.strptime(args.start_time, '%H:%M:%S')

#     main(
#         args.description,
#         args.project_id,
#         start_date,
#         start_time,
#         args.source_bucket,
#         args.sink_bucket)