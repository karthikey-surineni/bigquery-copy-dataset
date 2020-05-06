from google.cloud import storage
from google.oauth2 import service_account
import config.default_config as default_config


class StorageConnection():
    def __init__(self, project=default_config.PROJECT, sa_path=default_config.SA_PATH):
        self.__project = project
        self.__sa_path = sa_path
        super().__init__()

    @property
    def storage_client(self):
        try:
            credentials = service_account.Credentials.from_service_account_file(
                filename=self.__sa_path,
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            client = storage.Client(
                project=self.__project, credentials=credentials)
            return client
        except Exception as e:
            print(e)
