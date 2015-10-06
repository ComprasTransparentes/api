import json

import falcon
import dateutil.parser

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
        ).group_by(
            models_stats.MasterPlop.licitacion,
            models_stats.MasterPlop.licitacion_codigo,
            models_stats.MasterPlop.licitacion_nombre,
            models_stats.MasterPlop.licitacion_descripcion,
            models_stats.MasterPlop.fecha_creacion,
            models_stats.MasterPlop.organismo,
            models_stats.MasterPlop.nombre_organismo
        )

        filters = []

        q_q = req.params.get('q', None)
        if q_q:
            # TODO Try to make just one query over one index instead of two or more ORed queries

            filters.append(ts_match(models_stats.MasterPlop.licitacion_nombre, q_q) | ts_match(models_stats.MasterPlop.licitacion_descripcion, q_q))

        q_producto = req.params.get('producto', None)
        if q_producto:
            if not q_producto.isdigit():
                raise falcon.HTTPBadRequest("Wrong product code", "product code must be an integer")
            q_producto = int(q_producto)

            # licitacion_id with items of type q_product
            licitaciones_producto = models_stats.MasterPlop.select(
                models_stats.MasterPlop.licitacion
            ).where(
                models_stats.MasterPlop.categoria == q_producto
            ).distinct()

            # Add filter
            filters.append(models_stats.MasterPlop.licitacion << licitaciones_producto)

        q_estado = req.params.get('estado', None)
        if q_estado:
            if not q_estado.isdigit():
                raise falcon.HTTPBadRequest("Wrong product code", "state must be an integer")
            q_estado = int(q_estado)

            estados_recientes = LicitacionEstado.select(
                LicitacionEstado.licitacion,
                LicitacionEstado.estado,
                fn.max(LicitacionEstado.fecha)
            ).group_by(
                LicitacionEstado.licitacion,
                LicitacionEstado.estado
            ).alias('estados_recientes')

            licitacion_estados = LicitacionEstado.select(
                LicitacionEstado.licitacion,
            ).join(
                estados_recientes,
                on=(LicitacionEstado.licitacion == estados_recientes.c.licitacion_id)
            ).where(
                estados_recientes.c.estado == q_estado
            ).distinct()

            # Add filter
            filters.append(models_stats.MasterPlop.licitacion << licitacion_estados)

        q_fecha_creacion = req.params.get('fecha_creacion', None)
        if q_fecha_creacion:
            q_fecha_creacion = q_fecha_creacion.split('|')
            try:
                fecha_creacion_min = dateutil.parser.parse(q_fecha_creacion[0]) if q_fecha_creacion[0] else None
                fecha_creacion_max = dateutil.parser.parse(q_fecha_creacion[1]) if q_fecha_creacion[1] else None
            except IndexError:
                raise falcon.HTTPBadRequest("Wrong creation date", "dates must be separated by a pipe [|]")
            except ValueError:
                raise falcon.HTTPBadRequest("Wrong creation date", "must be a datetime in ISO8601 format")

            if fecha_creacion_min:
                filters.append(models_stats.MasterPlop.fecha_creacion >= fecha_creacion_min)
            if fecha_creacion_max:
                filters.append(models_stats.MasterPlop.fecha_creacion <= fecha_creacion_max)

        q_monto = req.params.get('monto', None)
        if q_monto:
            q_monto = q_monto.split('|')
            try:
                monto_min = int(q_monto[0]) if q_monto[0] else None
                monto_max = int(q_monto[1]) if q_monto[1] else None
            except IndexError:
                raise falcon.HTTPBadRequest("Wrong amount", "amounts must be separated by a pipe [|]")
            except ValueError:
                raise falcon.HTTPBadRequest("Wrong amount", "amounts must be integers")

            if monto_min:
                filters.append(SQL('monto') >= monto_min)
            if monto_max:
                filters.append(SQL('monto') <= monto_max)

        if filters:
            licitaciones = licitaciones.where(*filters)

        # Get page
        q_page = req.params.get('pagina', '1')
        q_page = max(int(q_page) if q_page.isdigit() else 1, 1)

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
                    },
                    'monto_adjudicado': licitacion['monto']
                }
                for licitacion in licitaciones.paginate(q_page, LicitacionList.MAX_RESULTS).dicts()
            ]
        }

        resp.body = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)
