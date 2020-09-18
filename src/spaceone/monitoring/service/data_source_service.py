import logging

from spaceone.core.service import *

from spaceone.monitoring.error import *
from spaceone.monitoring.manager.google_cloud_manager import GoogleCloudManager
from spaceone.monitoring.manager.data_source_manager import DataSourceManager

_LOGGER = logging.getLogger(__name__)
DEFAULT_SCHEMA = 'google_oauth2_credentials'

@authentication_handler
@authorization_handler
@event_handler
class DataSourceService(BaseService):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.google_mgr: GoogleCloudManager = self.locator.get_manager('GoogleCloudManager')
        self.data_source_mgr: DataSourceManager = self.locator.get_manager('DataSourceManager')

    @check_required(['options'])
    def init(self, params):
        """ init plugin by options
        """
        return self.data_source_mgr.init_response()

    @transaction
    @check_required(['options', 'secret_data'])
    def verify(self, params):
        """ Verifying data source plugin

        Args:
            params (dict): {
                'schema': 'str',
                'options': 'dict',
                'secret_data': 'dict'
            }

        Returns:
            plugin_verify_response (dict)
        """

        self.aws_mgr.verify(params.get('schema', DEFAULT_SCHEMA), params['options'], params['secret_data'])
