from spaceone.core.error import *

class ERROR_INVALID_CREDENTIALS(ERROR_INVALID_ARGUMENT):
    _message = 'Google Cloud credentials is invalid.'


class ERROR_INVALID_RESOURCE_FORMAT(ERROR_INVALID_ARGUMENT):
    _message = 'Resource format is invalid. (format = ARN).'


class ERROR_NOT_SUPPORT_RESOURCE(ERROR_INVALID_ARGUMENT):
    _message = 'This Resource is not supported by Google Cloud Stackdriver. (resource = {resource})'


class ERROR_NOT_SUPPORT_ALIGN(ERROR_INVALID_ARGUMENT):
    _message = 'Aligner is invalid with given Metric type (metric_type = {type})'


class ERROR_NOT_SUPPORT_STAT(ERROR_INVALID_ARGUMENT):
    _message = 'Statistics option is invalid. (supported_stat = {supported_stat})'

