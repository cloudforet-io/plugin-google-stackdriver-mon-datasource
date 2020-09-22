import logging
import time
from datetime import datetime, timezone, timedelta

from spaceone.core import utils
from pprint import pprint
from spaceone.monitoring.error import *

__all__ = ['StackDriver']
_LOGGER = logging.getLogger(__name__)


class StackDriver(object):

    def __init__(self, client, project_id):
        self.client = client
        self.project_id = project_id

    def list_metrics(self, resource_type=None):
        list_google_cloud_metrics = self.list_metric_descriptors(resource_type)
        metrics_info = []

        for gc_metric in list_google_cloud_metrics:
            gc_metric_info = {
                'name': gc_metric.get('name', ''),
                'type': gc_metric.get('type', ''),
                'description': gc_metric.get('description', ''),
                'view': 'FULL',
                'resource': 'gce_instance'
            }

            metrics_info.append(gc_metric_info)

        return {
            'metrics': metrics_info
        }

    def get_metric_data(self, metric_type, resource, aligner, start, end, interval):

        response = self.list_metrics_time_series(metric_type, resource, aligner, start, end, interval)

        metric_data_info = {
            'labels': [],
            'values': []
        }

        for metric_data in response:
            metric_points = metric_data.get('points', [])
            m_points = sorted(metric_points, key=lambda point: (point['interval']['startTime']))
            time_stamps = []
            values = []
            for metric_point in m_points:
                interval = metric_point.get('interval', {})
                value = metric_point.get('value', {})
                time_stamps.append(self._get_time_stamps(interval))
                values.append(value.get('doubleValue', 0))

            metric_data_info['labels'] = list(map(self._convert_timestamp, time_stamps))
            metric_data_info['values'] = values
        return metric_data_info

    def list_metric_descriptors(self, resource_type, **query):
        query = self.get_list_metric_query(resource_type, **query)
        response = self.client.projects().metricDescriptors().list(**query).execute()
        return response.get('metricDescriptors', [])

    def list_metrics_time_series(self, metric_type, resource, aligner, start, end, interval, **query):
        query = self.get_metric_data_query(metric_type, resource, aligner, start, end, interval, **query)
        try:
            response = self.client.projects().timeSeries().list(**query).execute()
            return response.get('timeSeries', [])
        except Exception as e:
            print(e)

    def get_list_metric_query(self, resource_type, **query):
        '''
            name: projects/project_id
            filter: metric.type= one_of()
            pageSize : [optional]
            PageToken : [optional]
            reference : https://cloud.google.com/monitoring/api/ref_v3/rest/v3/projects.metricDescriptors/list
        '''
        query.update({
            'name': self._get_name(self.project_id),
            'filter': self._get_list_metric_filter(resource_type)
        })
        return query

    def get_metric_data_query(self, metric_type: str, resource: dict, aligner: str, start, end, interval, **query):

        '''
            SAMPLE
            "name": 'projects/286919713412',
            "aggregation.alignmentPeriod": '362880s',
            "aggregation.crossSeriesReducer": 'REDUCE_NONE',
            "aggregation.perSeriesAligner": 'ALIGN_SUM',
            "filter": 'metric.type="compute.googleapis.com/instance/cpu/utilization" AND resource.type="gce_instance"',
            "interval.endTime" : 2020-08-09T04:48:00Z,
            "interval.startTime": 2020-08-06T00:00:00Z,
            "view": 'FULL'
        '''

        query.update({
            'name': self._get_name(self.project_id),
            'filter': self._get_metric_data_filter(metric_type, resource),
            'aggregation_alignmentPeriod': interval,
            'aggregation_crossSeriesReducer': 'REDUCE_NONE',
            'aggregation_perSeriesAligner': aligner,
            'interval_endTime': end,
            'interval_startTime': start,
            'view': 'FULL'
        })
        return query

    @staticmethod
    def _convert_timestamp(metric_datetime):
        timestamp = int(time.mktime(metric_datetime.timetuple()))
        return {
            'seconds': timestamp
        }

    @staticmethod
    def _get_name(project_id):
        return f'projects/{project_id}'

    @staticmethod
    def _get_list_metric_filter(filter_type):
        metric_list_in_str = []

        if isinstance(filter_type, list):
            metric_list_in_str = '","'.join(filter_type)
        elif isinstance(filter_type, str):
            metric_list_in_str = '","'.join([filter_type])

        all_metrics_list = f'metric.type = one_of("{metric_list_in_str}")'

        return all_metrics_list

    @staticmethod
    def _get_time_stamps(intervals: dict):
        time_stamp = None
        try:
            start_time = datetime.strptime(intervals.get('startTime', ''), '%Y-%m-%dT%H:%M:%SZ')
            end_time = datetime.strptime(intervals.get('endTime', ''), '%Y-%m-%dT%H:%M:%SZ')
            sub_difference = (end_time.replace(tzinfo=timezone.utc) - start_time.replace(tzinfo=timezone.utc))/2
            time_stamp = start_time + sub_difference
            time_stamp.replace(tzinfo=timezone.utc)

        except Exception as e:
            print(e)
            time_stamp = datetime.strptime(intervals.get('endTime', ''), '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)

        return time_stamp

    @staticmethod
    def _get_metric_data_filter(metric_type: str, resource: dict):
        try:
            resource_type = resource.get('resource_type')  # VM_instance, gce_instance
            resource_key = resource.get('resource_key')  # resource.labels.instance_id
            resource_value = resource.get('resource_value')  # 1873022307818018997 => ids
            return f'metric.type="{metric_type}" AND resource.type = "{resource_type}" AND {resource_key} = "{resource_value}"'

        except Exception as e:
            raise ERROR_NOT_SUPPORT_RESOURCE()
