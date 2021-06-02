import datetime
import decimal
import json
import sys

import numpy as np
import pandas as pd

class EnhancedJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            ARGS = ('year', 'month', 'day', 'hour', 'minute',
                    'second', 'microsecond')
            return {'__type__': 'datetime.datetime',
                    'args': [getattr(obj, a) for a in ARGS]}
        elif isinstance(obj, datetime.date):
            ARGS = ('year', 'month', 'day')
            return {'__type__': 'datetime.date',
                    'args': [getattr(obj, a) for a in ARGS]}
        elif isinstance(obj, datetime.time):
            ARGS = ('hour', 'minute', 'second', 'microsecond')
            return {'__type__': 'datetime.time',
                    'args': [getattr(obj, a) for a in ARGS]}
        elif isinstance(obj, datetime.timedelta):
            ARGS = ('days', 'seconds', 'microseconds')
            return {'__type__': 'datetime.timedelta',
                    'args': [getattr(obj, a) for a in ARGS]}
        elif isinstance(obj, decimal.Decimal):
            return {'__type__': 'decimal.Decimal',
                    'args': [str(obj), ]}
        else:
            return super().default(obj)


class EnhancedJSONDecoder(json.JSONDecoder):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, object_hook=self.object_hook,
                         **kwargs)

    def object_hook(self, d):
        if '__type__' not in d:
            return d
        o = sys.modules[__name__]
        for e in d['__type__'].split('.'):
            o = getattr(o, e)
        args, kwargs = d.get('args', ()), d.get('kwargs', {})
        return o(*args, **kwargs)


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, pd.Timedelta):
            return obj.isoformat()
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(NpEncoder, self).default(obj)