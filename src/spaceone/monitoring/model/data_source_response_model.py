from schematics import Model
from schematics.types import ListType, StringType
from schematics.types.compound import ModelType

__all__ = ['PluginInitResponse']

_SUPPORTED_STAT = [
    'MEAN',
    'MAX',
    'MIN',
    'SUM'
]

_REQUIRED_KEYS = ['data.google_cloud_monitoring']
_SUPPORTED_PROVIDERS = ['google_cloud']


class PluginMetadata(Model):
    supported_stat = ListType(StringType, default=_SUPPORTED_STAT)
    required_keys = ListType(StringType, default=_REQUIRED_KEYS)
    supported_providers = ListType(StringType, default=_SUPPORTED_PROVIDERS)


class PluginInitResponse(Model):
    _metadata = ModelType(PluginMetadata, default=PluginMetadata, serialized_name='metadata')
