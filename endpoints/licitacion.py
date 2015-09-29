import json

import falcon

from playhouse.shortcuts import model_to_dict

from models.models_bkn import *
from utils.myjson import JSONEncoderPlus


class LicitacionItem(object):

    @database.atomic()
    def on_get(self, req, resp, licitacion_id=None):

        # Get the licitacion
        try:
            if '-' in licitacion_id:
                licitacion = Licitacion.get(Licitacion.codigo == licitacion_id)
            elif licitacion_id.isdigit():
                licitacion = Licitacion.get(Licitacion.id == licitacion_id)
            else:
                raise Licitacion.DoesNotExist()

        except Licitacion.DoesNotExist:
            raise falcon.HTTPNotFound()

        response = json.dumps(model_to_dict(licitacion, backrefs=True), cls=JSONEncoderPlus)

        callback = req.params.get('callback', None)
        if callback:
            response = "%s(%s)" % (callback, response)

        resp.body = response
