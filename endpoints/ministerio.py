import json

import falcon

from playhouse.shortcuts import model_to_dict

from utils.myjson import JSONEncoderPlus

from models.models_stats import *


class MinisterioStatsItem(object):

    @database.atomic()
    def on_get(self, req, resp, ministerio_id, categoria_id):

        try:
            stats = MinisterioProductoStats.get(MinisterioProductoStats.ministerio == ministerio_id, MinisterioProductoStats.categoria == categoria_id)
        except MinisterioProductoStats.DoesNotExist:
            raise falcon.HTTPNotFound()

        resp.body = json.dumps(model_to_dict(stats), cls=JSONEncoderPlus, sort_keys=True)
