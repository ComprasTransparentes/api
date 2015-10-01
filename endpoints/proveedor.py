import json

import falcon

from playhouse.shortcuts import model_to_dict

from models.models_bkn import *
from models import models_stats
from utils.myjson import JSONEncoderPlus
from utils.mypeewee import ts_match


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

        response = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)

        callback = req.params.get('callback', None)
        if callback:
            response = "%s(%s)" % (callback, response)

        resp.body = response


class ProveedorList(object):

    ALLOWED_PARAMS = ['q']
    MAX_RESULTS = 10

    @database.atomic()
    def on_get(self, req, resp):

        # Get all proveedores
        proveedores = Proveedor.select().order_by(Proveedor.nombre.desc(), Proveedor.rut.desc())

        # Get page
        q_page = req.params.get('pagina', '1')
        q_page = max(int(q_page) if q_page.isdigit() else 1, 1)

        q_q = req.params.get('q', None)
        if q_q:
            # TODO Try to make just one query over one index instead of two or more ORed queries
            proveedores = proveedores.where(ts_match(Proveedor.nombre, q_q) | ts_match(Proveedor.rut, q_q))

        response = {
            'n_proveedores': proveedores.count(),
            'proveedores': [proveedor for proveedor in proveedores.paginate(q_page, ProveedorList.MAX_RESULTS).dicts()]
        }

        resp.body = json.dumps(response, cls=JSONEncoderPlus)
