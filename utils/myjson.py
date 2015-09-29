import json, datetime

__author__ = 'lbenitez'


class JSONEncoderPlus(json.JSONEncoder):
    """Custom JSON encoder supporting datetime serialization"""
    def default(self, o):
        if isinstance(o, datetime.datetime) or isinstance(o, datetime.date):
            return o.isoformat()

        return super(JSONEncoderPlus, self).default(o)

    def encode(self, o):
        o_encoded = super(JSONEncoderPlus, self).encode(o);

        return o_encoded.replace('{}', 'null')

