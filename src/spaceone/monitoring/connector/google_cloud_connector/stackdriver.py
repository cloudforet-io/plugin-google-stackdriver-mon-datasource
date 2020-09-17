import logging
import time
from datetime import datetime, timedelta

from spaceone.core import utils
from pprint import pprint
from spaceone.monitoring.error import *

__all__ = ['StackDriver']

_LOGGER = logging.getLogger(__name__)


class StackDriver(object):

    def __init__(self, client, project_id):
        self.client = client
        self.project_id = project_id

    def get_metric_descriptors(self):
        name = f'projects/{self.project_id}'
        response = self.client.metricDescriptors().list(name=name, filter=self._get_metric_filter()).execute()
        return response.get('metricDescriptors', [])

    def list_metrics(self):
        list_google_cloud_metrics = self.get_metric_descriptors()
        metrics_info = []

        for response in list_google_cloud_metrics:
            print('----metrics--')
            print()
            pprint(response)
            print()
            print('------------')
            # list_google_cloud_metrics =
            #     metric_info = {
            #         'key': metric_name,
            #         'name': metric_name,
            #         'unit': unit,
            #         'chart_type': chart_type,
            #         'chart_options': chart_option
            #     }
            #
            #     metrics_info.append(metric_info)
        return {
            'metrics': metrics_info
        }

    @staticmethod
    def _get_metric_filter():
        resource_type = {
            "metrics": [
                "agent.googleapis.com/cpu/utilization",
                "agent.googleapis.com/cpu/usage_time",
                "agent.googleapis.com/cpu/load_5m",
                "agent.googleapis.com/cpu/load_1m",
                "agent.googleapis.com/cpu/load_15m",
                "agent.googleapis.com/memory/percent_used",
                "agent.googleapis.com/memory/bytes_used",
                "agent.googleapis.com/disk/write_bytes_count",
                "agent.googleapis.com/disk/weighted_io_time",
                "agent.googleapis.com/disk/read_bytes_count",
                "agent.googleapis.com/disk/percent_used",
                "agent.googleapis.com/disk/pending_operations",
                "agent.googleapis.com/disk/operation_time",
                "agent.googleapis.com/disk/operation_count",
                "agent.googleapis.com/disk/merged_operations",
                "agent.googleapis.com/disk/io_time",
                "agent.googleapis.com/disk/bytes_used",
                "agent.googleapis.com/interface/traffic",
                "agent.googleapis.com/interface/packets",
                "agent.googleapis.com/interface/errors",
                "compute.googleapis.com/instance/network/sent_packets_count",
                "compute.googleapis.com/instance/network/sent_bytes_count",
                "compute.googleapis.com/instance/network/received_packets_count",
                "compute.googleapis.com/instance/network/received_bytes_count"
            ]
        }
        return "metric.type = one_of(" + "'" + "','".join(resource_type.get('metrics', [])) + "'"

    def get_metric_data(self, namespace, dimensions, metric_name, start, end, period, stat, limit=None):
        metric_id = f'metric_{utils.random_string()[:12]}'

        extra_opts = {}

        if limit:
            extra_opts['MaxDatapoints'] = limit

        response = self.client.get_metric_data(
            MetricDataQueries=[{
                'Id': metric_id,
                'MetricStat': {
                    'Metric': {
                        'Namespace': namespace,
                        'MetricName': metric_name,
                        'Dimensions': dimensions
                    },
                    'Period': period,
                    'Stat': stat
                }
            }],
            StartTime=start,
            EndTime=end,
            ScanBy='TimestampAscending',
            **extra_opts
        )

        metric_data_info = {
            'labels': [],
            'values': []
        }

        for metric_data in response.get('MetricDataResults', []):
            metric_data_info['labels'] = list(map(self._convert_timestamp, metric_data['Timestamps']))
            metric_data_info['values'] += metric_data['Values']

        return metric_data_info

    @staticmethod
    def _convert_timestamp(metric_datetime):
        timestamp = int(time.mktime(metric_datetime.timetuple()))
        return {
            'seconds': timestamp
        }

    def _get_metric_unit(self, namespace, dimensions, metric_name):
        end = datetime.utcnow()
        start = end - timedelta(minutes=60)

        response = self.client.get_metric_statistics(
            Namespace=namespace,
            Dimensions=dimensions,
            MetricName=metric_name,
            StartTime=start,
            EndTime=end,
            Period=600,
            Statistics=['SampleCount']
        )

        return_dict = {
            'x': 'Timestamp',
            'y': ''
        }

        for data_point in response.get('Datapoints', []):
            unit = data_point['Unit']

            if unit != 'None':
                return_dict['y'] = unit
                return return_dict

        return return_dict

    @staticmethod
    def _get_chart_info(namespace, dimensions, metric_name):
        return 'line', {}
