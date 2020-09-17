from spaceone.api.monitoring.plugin import data_source_pb2
from spaceone.api.core.v1 import plugin_pb2
from spaceone.core.pygrpc.message_type import *


__all__ = ['PluginInfo', 'PluginAction', 'PluginVerifyInfo', 'PluginVerifyResponse']

def PluginInfo(result):
    result['metadata'] = change_struct_type(result['metadata'])
    return plugin_pb2.PluginInfo(**result)

def PluginAction(action):
    info = {
        'method': action['method'],
    }

    if 'options' in action:
        info['options'] = change_struct_type(action['options'])

    return plugin_pb2.PluginAction(**info)


def PluginVerifyInfo(result):
    info = {
        'options': change_struct_type(result['options'])
    }

    return data_source_pb2.PluginVerifyInfo(**info)


def PluginVerifyResponse(response):
    info = {
        'resource_type': response['resource_type'],
        'result': PluginVerifyInfo(response['result'])
    }

    if response.get('actions'):
        info['actions']: list(map(PluginAction, response.get('actions', [])))

    return data_source_pb2.PluginVerifyResponse(**info)
