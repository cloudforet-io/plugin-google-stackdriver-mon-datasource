import logging
from spaceone.core.service import *
from spaceone.monitoring.manager.google_cloud_manager import GoogleCloudManager
from spaceone.monitoring.manager.metric_manager import MetricManager

_LOGGER = logging.getLogger(__name__)
DEFAULT_SCHEMA = 'google_oauth2_credentials'


@authentication_handler
@authorization_handler
@event_handler
class MetricService(BaseService):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.google_mgr: GoogleCloudManager = self.locator.get_manager('GoogleCloudManager')
        self.metric_mgr: MetricManager = self.locator.get_manager('MetricManager')

    @transaction
    @check_required(['options', 'secret_data', 'query'])
    def list(self, params):
        """Get Google Cloud Monitoring metrics

        Args:
            params (dict): {
                'schema': 'str',
                'options': 'dict',
                'secret_data': 'dict',
                'query': 'dict'
            }

        Returns:
            plugin_metrics_response (dict)
        """
        metrics_info = self.google_mgr.list_metrics(
            params.get('schema', DEFAULT_SCHEMA), params['options'],
            params['secret_data'], params['query'])

        return self.metric_mgr.make_metrics_response(metrics_info)

    @transaction
    @check_required(['options', 'secret_data', 'metric_query', 'start', 'end'])
    @change_timestamp_value(['start', 'end'], timestamp_format='iso8601')
    def get_data(self, params):
        """Get Google StackDriver metric data

        Args:
            params (dict): {
                'schema': 'str',
                'options': 'dict',
                'secret_data': 'dict',
                'metric_query': 'dict',
                'metric': 'str',
                'start': 'timestamp',
                'end': 'timestamp',
                'period': 'int',
                'stat': 'str'
            }

        Returns:
            plugin_metric_data_response (dict)
        """
        metric_data_info = self.google_mgr.get_metric_data(params.get('schema', DEFAULT_SCHEMA), params['options'],
                                                params['secret_data'],
                                                params['metric_query'], params['metric'],
                                                params['start'], params['end'], params.get('period'),
                                                params.get('stat'))

        return self.metric_mgr.make_metric_data_response(metric_data_info)
