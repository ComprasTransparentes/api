import json

import falcon

from playhouse.shortcuts import model_to_dict

from models.models_bkn import *
from models import models_stats
from utils.myjson import JSONEncoderPlus


class ProveedorItem(object):

    @database.atomic()
    def on_get(self, req, resp, proveedor_id=None):

        # Get the proveedor
        try:
            proveedor = Proveedor.get(Proveedor.id == proveedor_id)
        except Proveedor.DoesNotExist:
            raise falcon.HTTPNotFound()

        response = model_to_dict(proveedor, backrefs=False)

        top_licitaciones = models_stats.LicitacionItemAdjudicadas.select(
            models_stats.LicitacionItemAdjudicadas.licitacion.alias('id'),
            Licitacion.nombre,
            Licitacion.codigo,
            fn.sum(models_stats.LicitacionItemAdjudicadas.monto).alias('monto')
        ).where(
            models_stats.LicitacionItemAdjudicadas.proveedor == proveedor.id,
        ).join(
            Licitacion,
            on=(models_stats.LicitacionItemAdjudicadas.licitacion == Licitacion.id)
        ).group_by(
            models_stats.LicitacionItemAdjudicadas.licitacion,
            Licitacion.nombre,
            Licitacion.codigo,
        ).order_by(
            SQL('monto').desc()
        ).limit(10)

        response['extra'] = {
            'top_licitaciones': [licitacion for licitacion in top_licitaciones.dicts()]
        }

        resp.body = json.dumps(response, cls=JSONEncoderPlus)
