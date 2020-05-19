import os
import logging
import config.job_config as job_config
from datetime import datetime
from datetime import timedelta

log = logging.getLogger(__file__)
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))

PROJECT = "dataflow-pubsub-django"
LOCATION = "australia-southeast1"
SA_PATH = os.path.join(os.path.dirname(os.path.abspath(
    __file__)), "..", "..", "dataflow-pubsub-django-f64b5bb7d05c.json")

# PROJECT = "karthikeysurineni-215500"
# SA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),"..","..","karthikeysurineni-215500-8212ecdeeb34.json")
MIGRATION_CONFIG = {
    "London_To_Australia": {
        "source_project": f"{PROJECT}",
        "source_dataset": "test_London",
        "source_bucket": "test_bucket_london",
        "destination_project": f"{PROJECT}",
        "destination_dataset": "test_AU",
        "sink_bucket": "test_bucket_au",
        "create_disposition": job_config.JobConfig.CREATE_IF_NEEDED,
        "write_disposition": job_config.JobConfig.WRITE_TRUNCATE
    },
    "Australia_To_London": {
        "source_project": f"{PROJECT}",
        "source_dataset": "DPD_dataset",
        "source_bucket": "test_bucket_au",
        "destination_project": f"{PROJECT}",
        "destination_dataset": "test_London",
        "sink_bucket": "test_bucket_london",
        "create_disposition": job_config.JobConfig.CREATE_IF_NEEDED,
        "write_disposition": job_config.JobConfig.WRITE_TRUNCATE
    }
}
INFORMATION_SCHEMA_CONFIG = {
    "source_project": f"{PROJECT}",
    "source_dataset": "DPD_dataset",
    "destination_project": f"{PROJECT}",
    "destination_dataset": "",
    "create_disposition": job_config.JobConfig.CREATE_NEVER,
    "write_disposition": job_config.JobConfig.WRITE_TRUNCATE,
}

INFORMATION_SCHEMA_QUERIES = {
    "views": "select * from {source_dataset}.INFORMATION_SCHEMA.VIEWS",
    "tables": "select * from {source_dataset}.INFORMATION_SCHEMA.TABLES",
    "columns": "select * from {source_dataset}.INFORMATION_SCHEMA.COLUMNS"
}
utc_date = datetime.utcnow() + timedelta(seconds=30)
STORAGE_TRANSFER_CONFIG = {
    'description': "",
    'status': 'ENABLED',
    'projectId': f"{PROJECT}",
    'schedule': {
        'scheduleStartDate': {
            'day': utc_date.day,
            'month': utc_date.month,
            'year': utc_date.year
        },
        'startTimeOfDay': {
            'hours': utc_date.hour,
            'minutes': utc_date.minute,
            'seconds': utc_date.second
        }
    },
    'transferSpec': {
        'gcsDataSource': {
            'bucketName': ""
        },
        'gcsDataSink': {
            'bucketName': ""
        },
        'objectConditions': {
            'minTimeElapsedSinceLastModification': '0s'  # '2592000s' 30 days
        },
        'transferOptions': {
            'deleteObjectsFromSourceAfterTransfer': 'false'
        }
    }
}

# TABLE_TRANSFER = {
#     "dataset": "",
#     "bucket": "",
# }

#Service in alpha
# TRANSFER_CONFIG = {
#     "name":"",
#     "destination":"",
#     "destination_dataset_id":"",
#     "display_name":"",
#     "display_name":"",
#     "schedule":"",
#     "schedule_options":"",
#     "data_refresh_window_days":"",
#     "disabled":"",
#     "notification_pubsub_topic":"",
#     "email_preferences":""
# }
