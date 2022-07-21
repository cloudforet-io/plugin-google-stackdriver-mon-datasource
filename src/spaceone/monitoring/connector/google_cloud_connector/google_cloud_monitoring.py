import logging

from spaceone.core import utils
from datetime import datetime, timezone
from spaceone.monitoring.error import *

__all__ = ['GoogleCloudMonitoring']
_LOGGER = logging.getLogger(__name__)
PERCENT_METRIC = ['10^2.%']


class GoogleCloudMonitoring(object):

    def __init__(self, client, project_id):
        self.client = client
        self.project_id = project_id

    def list_metrics(self, query):
        metrics_info = []

        if 'name' in query:
            for metric_filter in query.get('filters', []):
                _query = {
                    'name': query['name'],
                    'filter': self.set_metric_filter(metric_filter)
                }

                for gc_metric in self.list_metric_descriptors(_query):
                    metric_kind = gc_metric.get('metricKind', '')
                    value_type = gc_metric.get('valueType', '')
                    key = gc_metric.get('type', '')

                    if metric_kind in ['DELTA', 'GAUGE'] and value_type in ['DOUBLE', 'INT64']:
                        gc_metric_info = {
                            'key': key,
                            'name': gc_metric.get('displayName', ''),
                            'unit': self._get_metric_unit(gc_metric.get('unit')),
                            'metric_query': {
                                'name': query['name'],
                                'resource_id': query['resource_id'],
                                'filter': {
                                    'metric_type': key,
                                    'labels': metric_filter.get('labels')
                                }
                            }
                        }

                        metrics_info.append(gc_metric_info)

        return {'metrics': metrics_info}

    def get_metric_data(self, metric_query, metric, start, end, period, stat):
        start = self.date_time_to_iso(start)
        end = self.date_time_to_iso(end)
        response_data = self.list_metrics_time_series(metric_query, metric, start, end, period, stat)
        multiply = True if response_data.get('unit') in PERCENT_METRIC else False

        metric_data_info = {
            'labels': [],
            'values': {}
        }

        metric_data_set = response_data.get('metric_data', [])

        for metric_data in metric_data_set:
            cloud_service_id = metric_data['cloud_service_id']
            time_series = metric_data['time_series']

            labels, values = self.set_metric_data_labels_values(cloud_service_id, time_series, multiply)

            if not metric_data_info['labels']:
                metric_data_info['labels'] = labels

            metric_data_info['values'].update(values)

        return metric_data_info

    def list_metric_descriptors(self, query):
        response = self.client.projects().metricDescriptors().list(**query).execute()
        return response.get('metricDescriptors', [])

    def set_metric_data_labels_values(self, cloud_service_id, time_series, multiply):
        metric_points = time_series.get('points', [])

        time_stamps = []
        values = []
        sorted_metric_points = sorted(metric_points, key=lambda point: (point['interval']['startTime']))
        for metric_point in sorted_metric_points:
            interval = metric_point.get('interval', {})
            value = metric_point.get('value', {})
            time_stamps.append(self._get_time_stamps(interval))
            values.append(self._get_value(value, multiply))

        labels = list(map(self._convert_timestamp, time_stamps))
        values = {cloud_service_id: values}

        return labels, values

    def list_metrics_time_series(self, metric_query, metric, start, end, period, stat):
        response_data = {}
        metric_data = []
        for cloud_service_id, _query in metric_query.items():
            try:
                name = _query['name']
                _filter = _query['filter']
                _merge_filter = f"metric.type = starts_with(\"{_filter['metric_type']}\")"

                or_filter_list = []
                for _label in _filter.get('labels', []):
                    or_filter_list.append(f"{_label['key']} = {_label['value']}")

                or_merge_filter = ' OR '.join(or_filter_list)
                _metric_filter = ' AND '.join([_merge_filter, or_merge_filter])

                query = self.get_metric_data_query(name, _metric_filter, metric, start, end, period, stat)
                _LOGGER.debug(f'[list_metrics_time_series] query: {query}')

                response = self.client.projects().timeSeries().list(**query).execute()
                response_data.update({'unit': response.get('unit')})
                metric_data.append({
                    'cloud_service_id': cloud_service_id,
                    'resource_id': _query['resource_id'],
                    'time_series': response.get('timeSeries', []),
                })

            except Exception as e:
                print(f'==== Error to get Metric =====')
                print(e)

        response_data.update({'metric_data': metric_data})
        return response_data

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

    @staticmethod
    def get_metric_data_query(name, metric_filter, metric, start, end, period, stat):
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
        metric_query = {
            'name': name,
            'filter': metric_filter,
            'aggregation_alignmentPeriod': period,
            'aggregation_crossSeriesReducer': 'REDUCE_NONE',
            'aggregation_perSeriesAligner': stat,
            'interval_endTime': end,
            'interval_startTime': start,
            'view': 'FULL'
        }
        return metric_query

    @staticmethod
    def set_metric_filter(metric_filter):
        _metric_filter = f"metric.type = starts_with(\"{metric_filter['metric_type']}\")"

        filter_list = []
        for _label in metric_filter.get('labels', []):
            filter_list.append(f"{_label['key']} = {_label['value']}")

        or_merge_filter = ' OR '.join(filter_list)
        _metric_filter = ' AND '.join([_metric_filter, or_merge_filter])

        return _metric_filter

    @staticmethod
    def _convert_timestamp(metric_datetime):
        return utils.datetime_to_iso8601(metric_datetime)

    @staticmethod
    def _get_name(project_id):
        return f'projects/{project_id}'

    @staticmethod
    def _get_list_metric_filter(resource):
        if 'data' in resource:
            data = resource.get('data', {})
            resource = data.get('stackdriver') if 'stackdriver' in data else resource

        filtering_list = []

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

        #  metric.type = starts_with("cloudsql.googleapis.com")
        #  AND metric.type = starts_with("cloudsql.googleapis.com")

        all_metrics_list = ' AND '.join(filtering_list) + f' AND metric.type = starts_with("{resource_type}")' \
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

        if 'data' in resource:
            data = resource.get('data', {})
            resource = data.get('stackdriver') if 'stackdriver' in data else resource

        try:
            metric_filter = f'metric.type="{metric}"'

            resource_type = resource.get('type', None)     # VM_instance, gce_instance
            resource_filters = resource.get('filters', [])       # resource.labels.instance_id

            # if resource_type is not None:
            #     metric_filter = metric_filter + f' AND resource.type = "{resource_type}"'

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