import json

import falcon

from playhouse.shortcuts import model_to_dict

from models.models_bkn import *
from models import models_stats
from utils.myjson import JSONEncoderPlus
from utils.mypeewee import ts_match, ts_rank


class LicitacionItem(object):

    @database.atomic()
    def on_get(self, req, resp, licitacion_id=None):

        p_items = req.params.get('p_items', '1')
        p_items = max(int(p_items) if p_items.isdigit() else 1, 1)

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

        items = licitacion.items.order_by(SQL('id'))

        response = model_to_dict(licitacion, backrefs=True)
        response['comprador']['id'] = response['comprador']['jerarquia_id']
        response['items'] = [model_to_dict(item, backrefs=True) for item in items.paginate(p_items, 10).iterator()]
        response['n_items'] = items.count()
        response = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)

        callback = req.params.get('callback', None)
        if callback:
            response = "%s(%s)" % (callback, response)

        resp.body = response


class LicitacionList(object):

    MAX_RESULTS = 10

    @database.atomic()
    def on_get(self, req, resp):

        # Get all licitaciones
        licitaciones = models_stats.MasterPlop.select(
            models_stats.MasterPlop.licitacion.alias('id'),
            models_stats.MasterPlop.licitacion_codigo.alias('codigo'),
            models_stats.MasterPlop.licitacion_nombre.alias('nombre'),
            models_stats.MasterPlop.licitacion_descripcion.alias('descripcion'),
            models_stats.MasterPlop.fecha_creacion.alias('fecha_creacion'),
            models_stats.MasterPlop.organismo.alias('organismo_id'),
            models_stats.MasterPlop.nombre_organismo.alias('organismo_nombre'),
            fn.sum(models_stats.MasterPlop.monto).alias('monto')
        )

        # Get page
        q_page = req.params.get('pagina', '1')
        q_page = max(int(q_page) if q_page.isdigit() else 1, 1)

        filters = []

        q_q = req.params.get('q', None)
        if q_q:
            # TODO Try to make just one query over one index instead of two or more ORed queries

            filters.append(ts_match(models_stats.MasterPlop.licitacion_nombre, q_q) | ts_match(models_stats.MasterPlop.licitacion_descripcion, q_q))

        q_producto = req.params.get('producto', None)
        if q_producto:
            if not q_producto.isdigit():
                raise falcon.HTTPBadRequest("Bad product code", "product code must be an integer")
            q_producto = int(q_producto)
            filters.append(models_stats.MasterPlop.categoria == q_producto)

        if filters:
            licitaciones = licitaciones.where(*filters)

        licitaciones = licitaciones.group_by(
            models_stats.MasterPlop.licitacion,
            models_stats.MasterPlop.licitacion_codigo,
            models_stats.MasterPlop.licitacion_nombre,
            models_stats.MasterPlop.licitacion_descripcion,
            models_stats.MasterPlop.fecha_creacion,
            models_stats.MasterPlop.organismo,
            models_stats.MasterPlop.nombre_organismo,
        )

        response = {
            'n_licitaciones': licitaciones.count(),
            'licitaciones': [
                {
                    'id': licitacion['id'],
                    'codigo': licitacion['codigo'],
                    'nombre': licitacion['nombre'],
                    'descripcion': licitacion['descripcion'],
                    'fecha_creacion': licitacion['fecha_creacion'],
                    'organismo': {
                        'id': licitacion['organismo_id'],
                        'nombre': licitacion['organismo_nombre'],
                        'rut': "0.000.000-0"
                    },
                    'monto_adjudicado': licitacion['monto']
                }
                for licitacion in licitaciones.paginate(q_page, LicitacionList.MAX_RESULTS).dicts()
            ]
        }

        resp.body = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)
