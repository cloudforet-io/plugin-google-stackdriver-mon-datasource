import logging
import time

from spaceone.core.manager import BaseManager
from spaceone.monitoring.error import *
from spaceone.monitoring.connector.google_cloud_connector import GoogleCloudConnector
_LOGGER = logging.getLogger(__name__)

_STAT_MAP = {
    'AVERAGE': 'Average',
    'MAX': 'Maximum',
    'MIN': 'Minimum',
    'SUM': 'Sum'
}


class GoogleCloudManager(BaseManager):
    google_cloud_connector = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def verify(self, schema, options, secret_data):
        """ Check connection
        """
        self.google_cloud_connector: GoogleCloudConnector = self.locator.get_connector('GoogleCloudConnector')
        self.google_cloud_connector.set_connect(schema, options, secret_data)

    def set_connector(self, schema, secret_data):
        self.google_cloud_connector: GoogleCloudConnector = self.locator.get_connector('GoogleCloudConnector')
        self.google_cloud_connector.set_connect(schema, {}, secret_data)

    def list_metrics(self, schema, options, secret_data, resource):
        namespace, dimensions = self._get_stackdriver_query(resource)

        self.google_cloud_connector.set_connect(schema, options, secret_data)
        return self.google_cloud_connector.list_metrics(namespace, dimensions)

    def get_metric_data(self, schema, options, secret_data, resource, metric, start, end, period, stat):
        if 'region_name' in resource:
            secret_data['region_name'] = resource.get('region_name')

        namespace, dimensions = self._get_stackdriver_query(resource)

        if period is None:
            period = self._make_period_from_time_range(start, end)

        stat = self._convert_stat(stat)

        self.google_cloud_connector.set_connect(schema, options, secret_data)
        return self.google_cloud_connector.get_metric_data(namespace, dimensions, metric, start, end, period, stat)

    @staticmethod
    def _convert_stat(stat):
        if stat is None:
            stat = 'AVERAGE'

        if stat not in _STAT_MAP.keys():
            raise ERROR_NOT_SUPPORT_STAT(supported_stat=' | '.join(_STAT_MAP.keys()))

        return _STAT_MAP[stat]

    @staticmethod
    def _make_period_from_time_range(start, end):
        start_time = int(time.mktime(start.timetuple()))
        end_time = int(time.mktime(end.timetuple()))
        time_delta = end_time - start_time

        # Max 60 point in start and end time range
        if time_delta <= 60*60:         # ~ 1h
            return 60
        elif time_delta <= 60*60*6:     # 1h ~ 6h
            return 60*10
        elif time_delta <= 60*60*12:    # 6h ~ 12h
            return 60*20
        elif time_delta <= 60*60*24:    # 12h ~ 24h
            return 60*30
        elif time_delta <= 60*60*24*3:  # 1d ~ 2d
            return 60*60
        elif time_delta <= 60*60*24*7:  # 3d ~ 7d
            return 60*60*3
        elif time_delta <= 60*60*24*14:  # 1w ~ 2w
            return 60*60*6
        elif time_delta <= 60*60*24*14:  # 2w ~ 4w
            return 60*60*12
        else:                            # 4w ~
            return 60*60*24

    @staticmethod
    def _get_stackdriver_query(resource):
        return resource.get('namespace'), resource.get('dimensions')
