__all__ = ["GoogleCloudStackDriverConnector"]

import logging
import os
import google.oauth2.service_account
import googleapiclient
import googleapiclient.discovery
from spaceone.core.connector import BaseConnector

_LOGGER = logging.getLogger(__name__)

class GoogleCloudStackDriverConnector(BaseConnector):

    def __init__(self, transaction=None, config=None):
        self.client = None
        self.project_id = None

    def verify(self, options, secret_data):
        self.get_connect(secret_data)
        return "ACTIVE"

    def get_connect(self, secret_data):
        """
        cred(dict)
            - type: ..
            - project_id: ...
            - token_uri: ...
            - ...
        """
        try:
            self.project_id = secret_data.get('project_id')
            credentials = google.oauth2.service_account.Credentials.from_service_account_info(secret_data)
            self.client = googleapiclient.discovery.build('compute', 'v1', credentials=credentials)
        except Exception as e:
            print(e)
            raise self.client(message='connection failed. Please check your authentication information.')

    def list_metric_descriptions(self, name):
        result = self.client.regions().list(project=self.project_id).execute()
        return result.get('items', [])

    def _get_filter_to_params(self, **query):
        filtering_list = []
        filters = query.get('filter', None)
        if filters and isinstance(filters, list):
            for single_filter in filters:
                filter_key = single_filter.get('key', '')
                filter_values = single_filter.get('values', [])
                filter_str = self._get_full_filter_string(filter_key, filter_values)
                if filter_str != '':
                    filtering_list.append(filter_str)

            return ' AND '.join(filtering_list)

    def generate_query(self, **query):
        query.update({
            'project': self.project_id,
        })
        return query

    def generate_key_query(self, key, value, delete, is_default=False, **query):
        if is_default:
            if delete != '':
                query.pop(delete, None)

            query.update({
                key: value,
                'project': self.project_id
            })

        return query

    @staticmethod
    def get_region(zone):
        index = zone.find('-')
        region = zone[0:index] if index > -1 else ''
        return region

    @staticmethod
    def _get_full_filter_string(filter_key, filter_values):
        filter_string = ''
        if filter_key != '' and filter_values != [] and isinstance(filter_values, list):
            single_filter_list = [f'{filter_key}={x}' for x in filter_values]
            join_string = ' OR '.join(single_filter_list)
            filter_string = f'({join_string})'
        elif filter_key != '' and filter_values != [] and not isinstance(filter_values, dict):
            filter_string = f'({filter_key}={filter_values})'
        return filter_string
