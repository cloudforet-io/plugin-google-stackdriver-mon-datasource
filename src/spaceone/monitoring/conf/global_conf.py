CONNECTORS = {
    'GoogleCloudConnector': {}
}

LOG = {
    'filters': {
        'masking': {
            'rules': {
                'Metric.list': [
                    'secret_data'
                ],
                'Metric.get_data': [
                    'secret_data'
                ]
            }
        }
    }
}