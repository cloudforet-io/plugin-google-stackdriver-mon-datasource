import logging
import time
from pprint import pprint
from datetime import datetime, timezone
from spaceone.monitoring.error import *

__all__ = ['StackDriver']
_LOGGER = logging.getLogger(__name__)


class StackDriver(object):

    def __init__(self, client, project_id):
        self.client = client
        self.project_id = project_id

    def list_metrics(self, filter_resource=None):
        list_google_cloud_metrics = self.list_metric_descriptors(filter_resource)
        metrics_info = []

        for gc_metric in list_google_cloud_metrics:
            metric_kind = gc_metric.get('metricKind', '')
            value_type = gc_metric.get('valueType', '')
            if metric_kind == 'GAUGE':
                gc_metric_info = {
                    'name': gc_metric.get('name', ''),
                    'type': gc_metric.get('type', ''),
                    'description': gc_metric.get('description', ''),
                    'unit': gc_metric.get('unit'),
                    'labels': gc_metric.get('labels', []),
                    'view': 'FULL',
                    'resource': gc_metric.get('monitoredResourceTypes', [])
                }
                metrics_info.append(gc_metric_info)

        return {'metrics': metrics_info}

    def get_metric_data(self, resource, metric, start, end, period, stat):
        start = self.date_time_to_iso(start)
        end = self.date_time_to_iso(end)
        responses = self.list_metrics_time_series(metric, resource, start, end, period, stat)

        metric_data_info = {
            'labels': [],
            'values': []
        }

        if responses:
            for metric_data in responses:
                metric_points = metric_data.get('points', [])
                sorted_metric_points = sorted(metric_points, key=lambda point: (point['interval']['startTime']))
                time_stamps = []
                values = []
                for metric_point in sorted_metric_points:
                    interval = metric_point.get('interval', {})
                    value = metric_point.get('value', {})
                    time_stamps.append(self._get_time_stamps(interval))
                    values.append(value.get('doubleValue', 0))

                metric_data_info['labels'] = list(map(self._convert_timestamp, time_stamps))
                metric_data_info['values'] = values

        return metric_data_info

    def list_metric_descriptors(self, filter_resource, **query):
        query = self.get_list_metric_query(filter_resource, **query)
        response = self.client.projects().metricDescriptors().list(**query).execute()
        return response.get('metricDescriptors', [])

    def list_metrics_time_series(self, metric, resource, start, end, period, stat, **query):
        query = self.get_metric_data_query(metric, resource, start, end, period, stat, **query)
        try:
            response = self.client.projects().timeSeries().list(**query).execute()
            return response.get('timeSeries', [])
        except Exception as e:
            print(e)

    def get_list_metric_query(self, filter_resource, **query):
        '''
            name: projects/project_id
            filter: metric.type= one_of()
            pageSize : [optional]
            PageToken : [optional]
            reference : https://cloud.google.com/monitoring/api/ref_v3/rest/v3/projects.metricDescriptors/list
        '''
        query.update({
            'name': self._get_name(self.project_id),
            'filter': self._get_list_metric_filter(filter_resource)
        })
        return query

    def get_metric_data_query(self, metric: str, resource: dict, start, end, period, stat, **query):
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
            'filter': self._get_metric_data_filter(metric, resource),
            'aggregation_alignmentPeriod': period,
            'aggregation_crossSeriesReducer': 'REDUCE_NONE',
            'aggregation_perSeriesAligner': stat,
            'interval_endTime': end,
            'interval_startTime': start,
            'view': 'FULL'
        })
        return query

    @staticmethod
    def _convert_timestamp(metric_datetime):
        timestamp = int(time.mktime(metric_datetime.timetuple()))
        return {'seconds': timestamp}

    @staticmethod
    def _get_name(project_id):
        return f'projects/{project_id}'

    @staticmethod
    def _get_list_metric_filter(filter_resource):
        filtering_list = []
        for idx, filter_single in enumerate(filter_resource):
            key = filter_single.get('key', None)
            value = filter_single.get('value', None)
            if key is not None and value is not None:
                if isinstance(value, list):
                    metric_list_in_str = '","'.join(value)
                    filtering_list.append(f'{key} = one_of("{metric_list_in_str}")')
                elif isinstance(value, str):
                    filtering_list.append(f'{key} = "{value}"')

        all_metrics_list = ' AND '.join(filtering_list)

        return all_metrics_list

    @staticmethod
    def _get_time_stamps(intervals: dict):
        time_stamp = None
        try:
            start_time = datetime.strptime(intervals.get('startTime', ''), '%Y-%m-%dT%H:%M:%S.%fZ')
            end_time = datetime.strptime(intervals.get('endTime', ''), '%Y-%m-%dT%H:%M:%S.%fZ')
            sub_difference = (end_time.replace(tzinfo=timezone.utc) - start_time.replace(tzinfo=timezone.utc))/2
            time_stamp = start_time + sub_difference
            time_stamp.replace(tzinfo=timezone.utc)

        except Exception as e:
            print(e)
            time_stamp = datetime.strptime(intervals.get('endTime', ''), '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc)

        return time_stamp

    @staticmethod
    def date_time_to_iso(date_time):
        return date_time.isoformat() + 'Z'

    @staticmethod
    def _get_metric_data_filter(metric_type: str, resource: dict):
        try:
            resource_type = resource.get('resource_type', None)     # VM_instance, gce_instance
            resource_key = resource.get('resource_key', None)       # resource.labels.instance_id
            resource_value = resource.get('resource_value', None)   # 1873022307818018997 => ids
            metric_filter = f'metric.type="{metric_type}"'

            if resource_type is not None:
                metric_filter = metric_filter + f' AND resource.type = "{resource_type}"'
            if resource_key is not None and resource_value is not None:
                metric_filter = metric_filter + f' AND {resource_key} = "{resource_value}"'

            return metric_filter

        except Exception as e:
            raise ERROR_NOT_SUPPORT_RESOURCE()
