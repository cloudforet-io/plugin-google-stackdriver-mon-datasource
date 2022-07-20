import logging
import time

from spaceone.core.manager import BaseManager
from spaceone.monitoring.error import *
from spaceone.monitoring.connector.google_cloud_connector import GoogleCloudConnector

_LOGGER = logging.getLogger(__name__)

_STAT_MAP = {
    'MEAN': 'ALIGN_MEAN',
    'MAX': 'ALIGN_MAX',
    'MIN': 'ALIGN_MIN',
    'SUM': 'ALIGN_SUM'
}


class GoogleCloudManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.google_cloud_connector: GoogleCloudConnector = self.locator.get_connector('GoogleCloudConnector')

    def verify(self, schema, options, secret_data):
        """ Check connection
        """
        self.google_cloud_connector.set_connect(schema, options, secret_data)

    def set_connector(self, schema, secret_data):
        self.google_cloud_connector.set_connect(schema, {}, secret_data)

    def list_metrics(self, schema, options, secret_data, query):
        self.google_cloud_connector.set_connect(schema, options, secret_data)
        return self.google_cloud_connector.list_metrics(query)

    def get_metric_data(self, schema, options, secret_data, metric_query, metric, start, end, period, stat):
        interval = self._make_period_from_time_range(start, end) if period is None else str(period) + 's'
        stat = self._convert_stat(stat)
        self.google_cloud_connector.set_connect(schema, options, secret_data)
        return self.google_cloud_connector.get_metric_data(metric_query, metric, start, end, interval, stat)

    @staticmethod
    def _get_metric_filters(resource):
        return resource.get('type', None), resource.get('filters', [])

    @staticmethod
    def _convert_stat(stat):
        if stat is None:
            stat = 'MEAN'

        if stat not in _STAT_MAP.keys():
            raise ERROR_NOT_SUPPORT_STAT(supported_stat=' | '.join(_STAT_MAP.keys()))

        return _STAT_MAP[stat]

    @staticmethod
    def _make_period_from_time_range(start, end):
        start_time = int(time.mktime(start.timetuple()))
        end_time = int(time.mktime(end.timetuple()))
        time_delta = end_time - start_time
        interval = 0
        # Max 60 point in start and end time range
        if time_delta <= 60*60:         # ~ 1h
            interval = 60
        elif time_delta <= 60*60*6:     # 1h ~ 6h
            interval = 60*10
        elif time_delta <= 60*60*12:    # 6h ~ 12h
            interval = 60*20
        elif time_delta <= 60*60*24:    # 12h ~ 24h
            interval = 60*30
        elif time_delta <= 60*60*24*3:  # 1d ~ 2d
            interval = 60*60
        elif time_delta <= 60*60*24*7:  # 3d ~ 7d
            interval = 60*60*3
        elif time_delta <= 60*60*24*14:  # 1w ~ 2w
            interval = 60*60*6
        elif time_delta <= 60*60*24*14:  # 2w ~ 4w
            interval = 60*60*12
        else:                            # 4w ~
            interval = 60*60*24

        return str(interval)+'s'

    @staticmethod
    def _get_chart_info(namespace, dimensions, metric_name):
        return 'line', {}