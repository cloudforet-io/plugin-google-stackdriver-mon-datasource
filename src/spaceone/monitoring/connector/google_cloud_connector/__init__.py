import logging
import os

import google.oauth2.service_account
import googleapiclient
import googleapiclient.discovery
import httplib2
import socks
from google_auth_httplib2 import AuthorizedHttp

from spaceone.core.connector import BaseConnector
from spaceone.monitoring.connector.google_cloud_connector.google_cloud_monitoring import (
    GoogleCloudMonitoring,
)

__all__ = ["GoogleCloudConnector"]
_LOGGER = logging.getLogger(__name__)


class GoogleCloudConnector(BaseConnector):
    def __init__(self, *args, **kwargs):
        self.client = None
        self.project_id = None
        super().__init__(*args, **kwargs)

    def set_connect(self, schema, options: dict, secret_data: dict):
        """
        cred(dict)
            - type: ..
            - project_id: ...
            - token_uri: ...
            - ...
        """
        try:
            google_client_service = "monitoring"
            version = "v3"

            self.project_id = secret_data.get("project_id")
            credentials = (
                google.oauth2.service_account.Credentials.from_service_account_info(
                    secret_data
                )
            )
            proxy_http = self._create_http_client()
            if proxy_http:
                _LOGGER.info(
                    f"** Using proxy in environment variable HTTPS_PROXY/https_proxy: {proxy_http}"
                )
                self.client = googleapiclient.discovery.build(
                    google_client_service,
                    version,
                    http=AuthorizedHttp(
                        credentials.with_scopes(
                            [
                                "https://www.googleapis.com/auth/cloud-platform"
                            ]  # FOR PROXY SCOPE SUPPORT
                        ),
                        http=proxy_http,
                    ),
                )
            else:
                self.client = googleapiclient.discovery.build(
                    google_client_service,
                    version,
                    credentials=credentials,
                )
        except Exception as e:
            print(e)
            raise self.client(
                message="connection failed. Please check your authentication information."
            )

    def list_metrics(self, *args, **kwargs):
        monitoring = GoogleCloudMonitoring(self.client, self.project_id)
        return monitoring.list_metrics(*args, **kwargs)

    def get_metric_data(self, *args, **kwargs):
        monitoring = GoogleCloudMonitoring(self.client, self.project_id)
        return monitoring.get_metric_data(*args, **kwargs)

    def _create_http_client(self):
        https_proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy")

        if https_proxy:
            _LOGGER.info(
                f"** Using proxy in environment variable HTTPS_PROXY/https_proxy: {https_proxy}"
            )
            try:
                proxy_url = https_proxy.replace("http://", "").replace("https://", "")
                if ":" in proxy_url:
                    proxy_host, proxy_port = proxy_url.split(":", 1)
                    proxy_port = int(proxy_port)

                proxy_info = httplib2.ProxyInfo(
                    proxy_host=proxy_host,
                    proxy_port=proxy_port,
                    proxy_type=socks.PROXY_TYPE_HTTP,
                )

                return httplib2.Http(
                    proxy_info=proxy_info, disable_ssl_certificate_validation=True
                )
            except Exception as e:
                _LOGGER.warning(
                    f"Failed to configure proxy. Using direct connection.: {e}. "
                )
                return None
        else:
            return None
