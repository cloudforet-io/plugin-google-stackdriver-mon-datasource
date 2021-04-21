import logging

from spaceone.core import utils
from datetime import datetime, timezone
from spaceone.monitoring.error import *
from pprint import pprint

__all__ = ['StackDriver']
_LOGGER = logging.getLogger(__name__)
PERCENT_METRIC = ['10^2.%']

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
            labels = gc_metric.get('labels', [])
            key = gc_metric.get('type', '')
            if metric_kind in ['DELTA', 'GAUGE'] \
                    and value_type in ['DOUBLE', 'INT64'] \
                    and self._metric_filters(labels, gc_metric.get('type', '')):
                chart_type, chart_option = self._get_chart_info(resource)
                # for later if Front-end has something to do for the future
                # chart_option.update({
                #     'resource': gc_metric.get('monitoredResourceTypes', []),
                #     'description': gc_metric.get('description', ''),
                #     'labels': gc_metric.get('labels', []),
                #     'view': 'FULL',
                # })

                self._set_filtered_metric_unit(gc_metric.get('unit'), key)

                gc_metric_info = {
                    'key': key,
                    'name': gc_metric.get('displayName', ''),
                    'unit': self._get_metric_unit(gc_metric.get('unit')),
                    'chart_type': chart_type,
                    'chart_options': chart_option
                }

                metrics_info.append(gc_metric_info)

        print(f'total number of metrics: {len(metrics_info)}')
        return {'metrics': metrics_info}

    def get_metric_data(self, resource, metric, start, end, period, stat):

        start = self.date_time_to_iso(start)
        end = self.date_time_to_iso(end)
        response_data = self.list_metrics_time_series(resource, metric, start, end, period, stat)
        current_unit = response_data.get('unit', '')
        multiply = True if current_unit in PERCENT_METRIC else False

        # print()
        # print('--------PARAMS--------')
        # print()
        # print(f'resource: {resource}')
        # print(f'  metric: {metric}')
        # print(f'   start: {start}')
        # print(f'     end: {end}')
        # print(f'  period: {period}')
        # print(f'    stat: {stat}')
        # print('--------response--------')
        # print()
        # pprint(response_data)
        # print()
        # print('========================')

        metric_data_info = {
            'labels': [],
            'resource_values': {}
        }

        if response_data:
            metric_data_set = response_data.get('time_series') if isinstance(response_data, dict) else []
            resource_ids = response_data.get('resource_ids', {})
            for metric_data in metric_data_set:
                metric_points = metric_data.get('points', [])
                label_resource = metric_data.get('resource', {}).get('labels', {})
                sorted_metric_points = sorted(metric_points, key=lambda point: (point['interval']['startTime']))
                time_stamps = []
                values = []
                for metric_point in sorted_metric_points:
                    interval = metric_point.get('interval', {})
                    value = metric_point.get('value', {})
                    time_stamps.append(self._get_time_stamps(interval))
                    values.append(self._get_value(value, multiply))

                if not metric_data_info.get('labels'):
                    metric_data_info['labels'] = list(map(self._convert_timestamp, time_stamps))
                instance_id = label_resource.get('instance_id', '')

                metric_data_info['resource_values'].update({resource_ids[instance_id]: values})

        return metric_data_info

    def list_metric_descriptors(self, resource, **query):
        query = self.get_list_metric_query(resource, **query)
        response = self.client.projects().metricDescriptors().list(**query).execute()
        return response.get('metricDescriptors', [])

    def list_metrics_time_series(self, resource, metric, start, end, period, stat, **query):
        query_format_resource = self._get_metric_filter_in_google_cloud_format(resource)

        query = self.get_metric_data_query(query_format_resource, metric, start, end, period, stat, **query)

        try:
            response = self.client.projects().timeSeries().list(**query).execute()

            time_series = response.get('timeSeries', None)
            unit = response.get('unit', None)

            if time_series and unit:
                return {
                    'time_series': time_series,
                    'unit': unit,
                    'resource_ids': query_format_resource.get('resource_ids', {})
                }
            else:
                return []

        except Exception as e:
            print(f'====Error to get Metric=====')
            print(f'metric_tye with {metric}')
            print(f'response: {response}')
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

    def get_metric_data_query(self, resource: dict, metric: str, start, end, period, stat, **query):
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
        metric_filter = self._get_metric_data_filter(metric, resource)

        query.update({
            'name': self._get_name(self.project_id),
            'filter': metric_filter,
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
        return utils.datetime_to_iso8601(metric_datetime)

    @staticmethod
    def _get_name(project_id):
        return f'projects/{project_id}'

    @staticmethod
    def _get_list_metric_filter(resource):
        filtering_list = []

        print('----metric Resource with func: list_metric----')
        print(resource)
        resource_type = resource.get('type', None)
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

        all_metrics_list = ' AND '.join(filtering_list) + f' AND resource.type = "{resource_type}"' \
            if resource_type else ' AND '.join(filtering_list)

        return all_metrics_list

    @staticmethod
    def _get_time_stamps(intervals: dict):
        time_stamp = None
        try:
            sd_string = intervals.get('startTime', '').upper()
            ed_string = intervals.get('endTime', '').upper()
            formatter = '%Y-%m-%dT%H:%M:%S.%fZ' if len(sd_string[sd_string.find('T'):sd_string.find('Z')]) > 9 \
                else '%Y-%m-%dT%H:%M:%SZ'
            start_time = datetime.strptime(sd_string, formatter)
            end_time = datetime.strptime(ed_string, formatter)
            sub_difference = (end_time.replace(tzinfo=timezone.utc) - start_time.replace(tzinfo=timezone.utc))/2
            time_stamp = start_time + sub_difference
            time_stamp.replace(tzinfo=timezone.utc)

        except Exception as e:
            print(e)
            time_stamp = datetime.strptime(intervals.get('endTime', ''), '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc)

        return time_stamp

    @staticmethod
    def date_time_to_iso(date_time):
        date_format = date_time.isoformat()
        return date_format[0:date_format.find('+')] + 'Z' if '+' in date_format else date_format + 'Z'

    @staticmethod
    def _get_metric_data_filter(metric: str, resource: dict):

        try:
            metric_filter = f'metric.type="{metric}"'

            resource_type = resource.get('type', None)     # VM_instance, gce_instance
            resource_filters = resource.get('filters', [])       # resource.labels.instance_id

            if resource_type is not None:
                metric_filter = metric_filter + f' AND resource.type = "{resource_type}"'

            filtering_list = []

            for resource_filter in resource_filters:
                key = resource_filter.get('key', None)
                value = resource_filter.get('value', None)

                if key and value:
                    if isinstance(value, list):
                        metric_list_in_str = '","'.join(value)
                        filtering_list.append(f'{key} = one_of("{metric_list_in_str}")')
                    elif isinstance(value, str):
                        filtering_list.append(f'{key} = "{value}"')

            all_metrics_list = ' AND '.join(filtering_list)

            return metric_filter + f' AND {all_metrics_list}'

        except Exception as e:
            raise ERROR_NOT_SUPPORT_RESOURCE()

    @staticmethod
    def _get_chart_info(namespace):
        return 'line', {}

    @staticmethod
    def _get_value(value, multiply):
        metric_value = 0
        double = value.get('doubleValue', None)
        int_64 = value.get('int64Value', None)
        if double is not None:
            metric_value = double
        elif int_64 is not None:
            metric_value = int_64

        return metric_value * 100 if multiply else metric_value

    @staticmethod
    def _metric_filters(labels, key):
        is_proper_metric = False
        if any(d['key'] == 'instance_name' for d in labels) and any(d['key'] == 'storage_type' for d in labels):
            is_proper_metric = False if 'guest' in key else True
        elif len(labels) == 1 and labels[0].get('key') == "instance_name":
            is_proper_metric = False if 'guest' in key else True
        return is_proper_metric

    @staticmethod
    def _get_metric_filter_in_google_cloud_format(resources):
        resource_filter_key = None
        resource_filter_values = []
        resource_ids = {}
        resource_type = None

        for resource in resources.get('resources', []):
            if resource_type is None:
                resource_type = resource.get('type')
            for single_filter in resource.get('filters', []):
                key = single_filter.get('key')
                value = single_filter.get('value')
                resource_ids.update({value:resource.get('resource_id')})
                if resource_filter_key is None:
                    resource_filter_key = key
                resource_filter_values.append(value)

        return {
            'filters': [{
                'key': resource_filter_key,
                'value': resource_filter_values}],
            'resource_ids': resource_ids,
            'type': resource_type
        }

    @staticmethod
    def _set_filtered_metric_unit(unit, metric_name):
        if unit == '10^2.%' and metric_name not in PERCENT_METRIC:
            PERCENT_METRIC.append(metric_name)

    @staticmethod
    def _get_metric_unit(unit):
        unit_name = unit
        if unit == 's':
            unit_name = 'Seconds'
        elif unit == 'By':
            unit_name = 'Bytes'
        elif unit == '10^2.%':
            unit_name = 'Percentage'
        elif unit == '1' or unit == 1:
            unit_name = 'Count'
        elif unit == 's{idle}':
            unit_name = 'Idle/s'
        elif unit == 's{uptime}':
            unit_name = 'Uptime/s'
        elif unit == 's{CPU}':
            unit_name = 'CPU/s'

        return {
            'x': 'Timestamp',
            'y': unit_name
        }