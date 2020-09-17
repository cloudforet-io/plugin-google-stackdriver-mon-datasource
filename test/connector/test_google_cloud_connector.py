import unittest
import os
import json
from datetime import datetime, timedelta
from spaceone.tester import TestCase
from spaceone.core.unittest.runner import RichTestRunner
from spaceone.core import config
from spaceone.core.transaction import Transaction
from spaceone.monitoring.connector.google_cloud_connector import GoogleCloudConnector

GOOGLE_APPLICATION_CREDENTIALS_PATH = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', None)

if GOOGLE_APPLICATION_CREDENTIALS_PATH is None:
    print("""
        ##################################################
        # ERROR 
        #
        # Configure your GCP credential first for test
        # https://console.cloud.google.com/apis/credentials

        ##################################################
        example)

        export GOOGLE_APPLICATION_CREDENTIALS="<PATH>" 
    """)
    exit


def _get_credentials():
    with open(GOOGLE_APPLICATION_CREDENTIALS_PATH) as json_file:
        json_data = json.load(json_file)
        return json_data

class TestGoogleCloudStackDriverConnector(TestCase):
    @classmethod
    def setUpClass(cls):
        config.init_conf(package='spaceone.monitoring')
        cls.secret_data = _get_credentials() if _get_credentials() is not None else {}
        cls.gcp_connector = GoogleCloudConnector(Transaction(), {})
        super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()

    def test_get_connect_with_google_service_key(self):
        options = {}
        secret_data = self.secret_data
        self.gcp_connector.set_connect(options, secret_data)

    def test_list_metrics(self):
        gcp_mgr = GoogleCloudManager()
        self.gcp_connector.create_session({}, self.aws_credentials)
        metrics_info = self.gcp_connector.list_metrics(namespace, dimensions)

        print_data(metrics_info, 'test_list_metrics')

    def test_get_metric_data(self):
        gcp_mgr = GoogleCloudManager()
        namespace, dimensions = gcp_mgr._get_stackdriver_query(self.resource)
        self.aws_credentials['region_name'] = self.resource.get('region_name')

        end = datetime.utcnow()
        start = end - timedelta(minutes=60)

        period = gcp_mgr._make_period_from_time_range(start, end)
        stat = gcp_mgr._convert_stat('AVERAGE')

        self.gcp_connector.create_session({}, self.aws_credentials)
        metric_data_info = self.gcp_connector.get_metric_data(namespace, dimensions, self.metric,
                                                              start, end, period, stat)

        print_data(metric_data_info, 'test_get_metric_data')

    # def test_all_metric_data(self):
    #     gcp_mgr = GoogleCloudManager()
    #     namespace, dimensions = gcp_mgr._get_cloudwatch_query(self.resource)
    #     self.aws_credentials['region_name'] = self.resource.get('region_name')
    #
    #     end = datetime.utcnow()
    #     start = end - timedelta(minutes=60)
    #
    #     period = gcp_mgr._make_period_from_time_range(start, end)
    #     stat = gcp_mgr._convert_stat('AVERAGE')
    #
    #     self.gcp_connector.create_session({}, self.aws_credentials)
    #     metrics_info = self.gcp_connector.list_metrics(namespace, dimensions)
    #
    #     for metric_info in metrics_info.get('metrics', []):
    #         metric_data_info = self.gcp_connector.get_metric_data(namespace, dimensions, metric_info['key'],
    #                                                               start, end, period, stat)
    #
    #         print_data(metric_data_info, f'test_all_metric_data.{metric_info["key"]}')


if __name__ == "__main__":
    unittest.main(testRunner=RichTestRunner)
