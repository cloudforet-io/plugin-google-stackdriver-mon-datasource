import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

from spaceone.core.unittest.result import print_data
from spaceone.core.unittest.runner import RichTestRunner
from spaceone.core import config
from spaceone.monitoring.error import *
from spaceone.monitoring.connector.google_cloud_connector import GoogleCloudConnector
from spaceone.monitoring.manager.google_cloud_manager import GoogleCloudManager


class TestMetricManager(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        config.init_conf(package='spaceone.monitoring')
        super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()

    @patch.object(GoogleCloudConnector, '__init__', return_value=None)
    def test_convert_stat(self, *args):
        google_cloud_mgr = GoogleCloudManager()
        stat = google_cloud_mgr._convert_stat('MEAN')
        print_data(stat, 'test_convert_stat')

    @patch.object(GoogleCloudConnector, '__init__', return_value=None)
    def test_convert_stat_with_invalid_stat(self, *args):
        google_cloud_mgr = GoogleCloudManager()
        with self.assertRaises(ERROR_NOT_SUPPORT_STAT):
            google_cloud_mgr._convert_stat('AVERAGE')

    @patch.object(GoogleCloudConnector, '__init__', return_value=None)
    def test_make_period_from_time_range(self, *args):
        google_cloud_mgr = GoogleCloudManager()

        end = datetime.utcnow()
        start = end - timedelta(days=1)

        period = google_cloud_mgr._make_period_from_time_range(start, end)
        print_data(period, 'test_make_period_from_time_range')


if __name__ == "__main__":
    unittest.main(testRunner=RichTestRunner)
