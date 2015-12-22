import json, datetime

__author__ = 'lbenitez'


class JSONEncoderPlus(json.JSONEncoder):
    """Custom JSON encoder with added support for datetime serialization and empty objects conversion to null"""
    def default(self, o):
        """Extends the default parser to support serialization of date and datetime objects
        :param o: The object to be serialized
        :return: The serialized object
        """
        if isinstance(o, datetime.datetime) or isinstance(o, datetime.date):
            return o.isoformat()

        return super(JSONEncoderPlus, self).default(o)

    def encode(self, o):
        """Extends the default encoder to replace empty objects with null values

        :param o: The object to be encoded
        :return: The ASCII encoded string
        """
        o_encoded = super(JSONEncoderPlus, self).encode(o)

        return o_encoded.replace('{}', 'null')

