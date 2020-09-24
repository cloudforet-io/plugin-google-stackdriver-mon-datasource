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

    def list_metrics(self, resource=None):
        list_google_cloud_metrics = self.list_metric_descriptors(resource)
        metrics_info = []

        for gc_metric in list_google_cloud_metrics:
            metric_kind = gc_metric.get('metricKind', '')
            value_type = gc_metric.get('valueType', '')
            if metric_kind == 'GAUGE' and value_type in ['DOUBLE', 'INT64']:
                chart_type, chart_option = self._get_chart_info(resource)
                chart_option.update({
                    'resource': gc_metric.get('monitoredResourceTypes', []),
                    'description': gc_metric.get('description', ''),
                    'labels': gc_metric.get('labels', []),
                    'view': 'FULL',
                })

                gc_metric_info = {
                    'key': gc_metric.get('type', ''),
                    'name': gc_metric.get('name', ''),
                    'unit': self._get_metric_unit(gc_metric.get('unit')),
                    'chart_type': chart_type,
                    'chart_option': chart_option
                }
                metrics_info.append(gc_metric_info)

        return {'metrics': metrics_info}

    #resource, metric, start, end, period, stat
    def get_metric_data(self, resource, metric, start, end, period, stat):
        start = self.date_time_to_iso(start)
        end = self.date_time_to_iso(end)
        responses = self.list_metrics_time_series(metric, resource, start, end, period, stat)

        metric_data_info = {
            'labels': [],
            'values': []
        }
        # data_source_id, resource_type, resources(list), metric, start(datetime), end(datetime), [period(int)], [stat], domain_id(meta)
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
                    values.append(self._get_value(value))

                metric_data_info['labels'] = list(map(self._convert_timestamp, time_stamps))
                metric_data_info['values'] = values

        pprint(metric_data_info)

        return metric_data_info

    def list_metric_descriptors(self, resource, **query):
        query = self.get_list_metric_query(resource, **query)
        response = self.client.projects().metricDescriptors().list(**query).execute()
        return response.get('metricDescriptors', [])

    def list_metrics_time_series(self, metric, resource, start, end, period, stat, **query):
        query = self.get_metric_data_query(metric, resource, start, end, period, stat, **query)
        try:
            response = self.client.projects().timeSeries().list(**query).execute()
            return response.get('timeSeries', [])
        except Exception as e:
            print(e)

    def get_list_metric_query(self, resource, **query):
        '''
            name: projects/project_id
            filter: metric.type= one_of()
            pageSize : [optional]
            PageToken : [optional]
            reference : https://cloud.google.com/monitoring/api/ref_v3/rest/v3/projects.metricDescriptors/list
        '''
        query.update({
            'name': self._get_name(self.project_id),
            'filter': self._get_list_metric_filter(resource)
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
    def _get_list_metric_filter(resource):
        filtering_list = []
        # {
        #     'type': 'gce_instance',
        #     'filters': [{
        #         'key': 'resource.labels.instance_id',
        #         'value': '1873022307818018997'
        #     }]
        # },
        filters = resource.get('filters', [])
        for filter_single in filters:
            key = filter_single.get('key', None)
            value = filter_single.get('value', None)
            if key and value:
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
    def _get_metric_data_filter(metric: str, resource: dict):
        try:
            metric_filter = f'metric.type="{metric}"'

            resource_type = resource.get('type', None)     # VM_instance, gce_instance
            resource_filters = resource.get('filters', [])       # resource.labels.instance_id

            if resource_type is not None:
                metric_filter = metric_filter + f' AND resource.type = "{resource_type}"'

            for resource_filter in resource_filters:
                key = resource_filter.get('key', None)
                value = resource_filter.get('value', None)

                if key and value :
                    metric_filter = metric_filter + f' AND {key} = "{value}"'

            return metric_filter

        except Exception as e:
            raise ERROR_NOT_SUPPORT_RESOURCE()

    @staticmethod
    def _get_chart_info(namespace):
        return 'line', {}

    @staticmethod
    def _get_value(value):
        metric_value = 0
        double = value.get('doubleValue', None)
        int_64 = value.get('int64Value', None)
        if double is not None:
            metric_value = double
        elif int_64 is not None:
            metric_value = int_64

        return metric_value


    @staticmethod
    def _get_metric_unit(unit):
        unit_name = unit
        if unit == 's':
            unit_name = 'seconds'
        elif unit == 'By':
            unit_name = 'Bytes'

        return {
            'x': 'Timestamp',
            'y': unit_name
        }