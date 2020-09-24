import unittest
import os
import json
from datetime import datetime, timedelta
from spaceone.tester import TestCase
from spaceone.core.unittest.runner import RichTestRunner
from spaceone.core import config
from spaceone.core.unittest.result import print_data
from spaceone.core.transaction import Transaction
from spaceone.monitoring.connector.google_cloud_connector import GoogleCloudConnector
from spaceone.monitoring.manager.google_cloud_manager import GoogleCloudManager
from pprint import pprint

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
        self.gcp_connector.set_connect({}, options, secret_data)

    def test_list_metrics(self):
        options = {}
        secret_data = self.secret_data
        self.gcp_connector.set_connect({}, options, secret_data)

        resource = {
                       'type': 'gce_instance',
                       'filters': [{
                           'key': 'metric.labels.instance_name',
                           'value': 'stackdriver-jhsong-01'
                       }]
                   }

        metrics_info = self.gcp_connector.list_metrics(resource)
        print_data(metrics_info, 'test_list_metrics')

    def test_get_metric_data(self):

        end = datetime.utcnow()
        start = end - timedelta(minutes=60)

        options = {}
        secret_data = self.secret_data
        self.gcp_connector.set_connect({}, options, secret_data)

        options = {'metric': 'compute.googleapis.com/instance/cpu/utilization',
                   'resource': {
                       'type': 'gce_instance',
                       'filters': [{
                           'key': 'resource.labels.instance_id',
                           'value': '1873022307818018997'
                       }]
                   },
                   'aligner': 'ALIGN_SUM',
                   'start': start,
                   'end': end,
                   'interval': '360s'
                   }

        metrics_info = self.gcp_connector.get_metric_data(
            options.get('resource'),
            options.get('metric'),
            options.get('start'),
            options.get('end'),
            options.get('interval'),
            options.get('aligner'),
        )
        print_data(metrics_info, 'test_list_metrics')

    def test_all_metric_data(self):
        options = {}
        secret_data = self.secret_data
        self.gcp_connector.set_connect({}, options, secret_data)
        resource = {
                                'type': 'gce_instance',
                                'filters': [{
                                     'key': 'metric.labels.instance_name',
                                     'value': 'stackdriver-jhsong-01'
                                }]
                }

        metrics_info = self.gcp_connector.list_metrics(resource)

        end = datetime.utcnow()
        start = end - timedelta(days=30)

        gcp_mgr = GoogleCloudManager()
        period = gcp_mgr._make_period_from_time_range(start, end)
        stat = gcp_mgr._convert_stat('SUM')

        for metric_info in metrics_info.get('metrics', []):
            metric_data_info = self.gcp_connector.get_metric_data(
                resource,
                metric_info.get('key', ''),
                start,
                end,
                period,
                stat,
            )
            print_data(metric_data_info, f'test_all_metric_data.{metric_info.get("type")}')


if __name__ == "__main__":
    unittest.main(testRunner=RichTestRunner)
