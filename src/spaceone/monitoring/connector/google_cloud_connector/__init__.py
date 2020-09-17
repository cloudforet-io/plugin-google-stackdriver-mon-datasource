import logging
import google.oauth2.service_account
import googleapiclient
import googleapiclient.discovery
from spaceone.core.connector import BaseConnector
from spaceone.monitoring.connector.google_cloud_connector.stackdriver import StackDriver
from pprint import pprint
__all__ = ['GoogleCloudConnector']
_LOGGER = logging.getLogger(__name__)


class GoogleCloudConnector(BaseConnector):
    def __init__(self, transaction=None, config=None):
        self.client = None
        self.project_id = None
        super().__init__(transaction, config)

    def get_connect(self, options: dict, secret_data: dict):
        """
        cred(dict)
            - type: ..
            - project_id: ...
            - token_uri: ...
            - ...
        """
        try:
            self.project_id = secret_data.get('project_id')
            credentials = google.oauth2.service_account.Credentials.from_service_account_info(secret_data)
            self.client = googleapiclient.discovery.build('monitoring', 'v3', credentials=credentials)

        except Exception as e:
            print(e)
            raise self.client(message='connection failed. Please check your authentication information.')

    def list_metrics(self, *args, **kwargs):
        stack_driver = StackDriver(self.client, self.project_id)
        return stack_driver.list_metrics(*args, **kwargs)

    def get_metric_data(self, *args, **kwargs):
        stack_driver = StackDriver(self.client, self.project_id)
        return stack_driver.get_metric_data(*args, **kwargs)
