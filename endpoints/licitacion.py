import json

import falcon
import peewee
import dateutil.parser

from peewee import fn, SQL
from playhouse.shortcuts import model_to_dict

from models import models_bkn
from models import models_stats
from utils.myjson import JSONEncoderPlus
from utils.mypeewee import ts_match, ts_rank


class LicitacionItem(object):

    @models_bkn.database.atomic()
    def on_get(self, req, resp, licitacion_id=None):

        p_items = req.params.get('p_items', '1')
        p_items = max(int(p_items) if p_items.isdigit() else 1, 1)

        # Get the licitacion
        try:
            if '-' in licitacion_id:
                licitacion = models_bkn.Licitacion.get(models_bkn.Licitacion.codigo == licitacion_id)
            elif licitacion_id.isdigit():
                licitacion = models_bkn.Licitacion.get(models_bkn.Licitacion.id == licitacion_id)
            else:
                raise models_bkn.Licitacion.DoesNotExist()

        except models_bkn.Licitacion.DoesNotExist:
            raise falcon.HTTPNotFound()

        items = licitacion.items.order_by(SQL('id'))

        response = model_to_dict(licitacion, backrefs=True)
        response['comprador']['id'] = response['comprador']['jerarquia_id']
        response['items'] = [model_to_dict(item, exclude=[models_bkn.LicitacionItem.licitacion], backrefs=True) for item in items.paginate(p_items, 10).iterator()]

        monto_adjudicado = 0
        n_items_adjudicados = 0
        for item in items:
            if item.adjudicacion:
                n_items_adjudicados += 1
                monto_adjudicado += int((item.adjudicacion.cantidad or 0) * (item.adjudicacion.monto_unitario or 0))
        response['n_items_adjudicados'] = n_items_adjudicados
        response['monto_adjudicado'] = monto_adjudicado

        response['n_items'] = items.count()
        response = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)

        callback = req.params.get('callback', None)
        if callback:
            response = "%s(%s)" % (callback, response)

        resp.body = response


class LicitacionItemItem(object):

    @models_bkn.database.atomic()
    def on_get(self, req, resp, licitacion_id):

        # Get the licitacion
        try:
            if '-' in licitacion_id:
                licitacion = models_bkn.Licitacion.get(models_bkn.Licitacion.codigo == licitacion_id)
            elif licitacion_id.isdigit():
                licitacion = models_bkn.Licitacion.get(models_bkn.Licitacion.id == licitacion_id)
            else:
                raise models_bkn.Licitacion.DoesNotExist()

        except models_bkn.Licitacion.DoesNotExist:
            raise falcon.HTTPNotFound()

        items = licitacion.items.order_by(SQL('id'))

        response = {
            'items': [model_to_dict(item, exclude=[models_bkn.LicitacionItem.licitacion],backrefs=True) for item in items],
            'n_items': items.count(),
        }

        response = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)

        callback = req.params.get('callback', None)
        if callback:
            response = "%s(%s)" % (callback, response)

        resp.body = response


class LicitacionList(object):

    MAX_RESULTS = 10

    @models_stats.database.atomic()
    def on_get(self, req, resp):

        # Get all licitaciones
        licitaciones = models_stats.MasterPlopAll.select(
            models_stats.MasterPlopAll.licitacion.alias('id'),
            models_stats.MasterPlopAll.licitacion_codigo.alias('codigo'),
            models_stats.MasterPlopAll.licitacion_nombre.alias('nombre'),
            models_stats.MasterPlopAll.licitacion_descripcion.alias('descripcion'),
            models_stats.MasterPlopAll.fecha_creacion.alias('fecha_creacion'),
            models_stats.MasterPlopAll.organismo.alias('organismo_id'),
            models_stats.MasterPlopAll.nombre_organismo.alias('organismo_nombre'),
            fn.sum(models_stats.MasterPlopAll.monto).alias('monto')
        ).group_by(
            models_stats.MasterPlopAll.licitacion,
            models_stats.MasterPlopAll.licitacion_codigo,
            models_stats.MasterPlopAll.licitacion_nombre,
            models_stats.MasterPlopAll.licitacion_descripcion,
            models_stats.MasterPlopAll.fecha_creacion,
            models_stats.MasterPlopAll.organismo,
            models_stats.MasterPlopAll.nombre_organismo
        )

        filters = []

        q_q = req.params.get('q', None)
        if q_q:
            # TODO Try to make just one query over one index instead of two or more ORed queries
            filters.append(ts_match(models_stats.MasterPlopAll.licitacion_nombre, q_q) | ts_match(models_stats.MasterPlopAll.licitacion_descripcion, q_q))

        q_producto = req.params.get('producto', None)
        if q_producto:
            if not q_producto.isdigit():
                raise falcon.HTTPBadRequest("Wrong product code", "product code must be an integer")
            q_producto = int(q_producto)

            # licitacion_id with items of type q_product
            licitaciones_producto = models_stats.MasterPlopAll.select(
                models_stats.MasterPlopAll.licitacion
            ).where(
                models_stats.MasterPlopAll.categoria == q_producto
            ).distinct()

            # Add filter
            filters.append(models_stats.MasterPlopAll.licitacion << licitaciones_producto)

        q_estado = req.params.get('estado', None)
        if q_estado:
            if not q_estado.isdigit():
                raise falcon.HTTPBadRequest("Wrong product code", "state must be an integer")
            q_estado = int(q_estado)

            # Add filter
            filters.append(models_stats.MasterPlopAll.estado == q_estado)

        q_fecha_creacion = req.params.get('fecha_creacion', None)
        if q_fecha_creacion:
            q_fecha_creacion = q_fecha_creacion.split('|')
            try:
                fecha_creacion_min = dateutil.parser.parse(q_fecha_creacion[0], dayfirst=True, yearfirst=True) if q_fecha_creacion[0] else None
                fecha_creacion_max = dateutil.parser.parse(q_fecha_creacion[1], dayfirst=True, yearfirst=True) if q_fecha_creacion[1] else None
            except IndexError:
                raise falcon.HTTPBadRequest("Wrong creation date", "dates must be separated by a pipe [|]")
            except ValueError:
                raise falcon.HTTPBadRequest("Wrong creation date", "must be a datetime in ISO8601 format")

            if fecha_creacion_min:
                filters.append(models_stats.MasterPlopAll.fecha_creacion >= fecha_creacion_min)
            if fecha_creacion_max:
                filters.append(models_stats.MasterPlopAll.fecha_creacion <= fecha_creacion_max)

        q_fecha_publicacion = req.params.get('fecha_publicacion', None)
        if q_fecha_publicacion:
            q_fecha_publicacion = q_fecha_publicacion.split('|')
            try:
                fecha_publicacion_min = dateutil.parser.parse(q_fecha_publicacion[0], dayfirst=True, yearfirst=True) if q_fecha_publicacion[0] else None
                fecha_publicacion_max = dateutil.parser.parse(q_fecha_publicacion[1], dayfirst=True, yearfirst=True) if q_fecha_publicacion[1] else None
            except IndexError:
                raise falcon.HTTPBadRequest("Wrong creation date", "dates must be separated by a pipe [|]")
            except ValueError:
                raise falcon.HTTPBadRequest("Wrong creation date", "must be a datetime in ISO8601 format")

            if fecha_publicacion_min:
                filters.append(models_stats.MasterPlopAll.fecha_publicacion >= fecha_publicacion_min)
            if fecha_publicacion_max:
                filters.append(models_stats.MasterPlopAll.fecha_publicacion <= fecha_publicacion_max)

        q_fecha_adjudicacion = req.params.get('fecha_adjudicacion', None)
        if q_fecha_adjudicacion:
            q_fecha_adjudicacion = q_fecha_adjudicacion.split('|')
            try:
                fecha_adjudicacion_min = dateutil.parser.parse(q_fecha_adjudicacion[0], dayfirst=True, yearfirst=True) if q_fecha_adjudicacion[0] else None
                fecha_adjudicacion_max = dateutil.parser.parse(q_fecha_adjudicacion[1], dayfirst=True, yearfirst=True) if q_fecha_adjudicacion[1] else None
            except IndexError:
                raise falcon.HTTPBadRequest("Wrong creation date", "dates must be separated by a pipe [|]")
            except ValueError:
                raise falcon.HTTPBadRequest("Wrong creation date", "must be a datetime in ISO8601 format")

            if fecha_adjudicacion_min:
                filters.append(models_stats.MasterPlopAll.fecha_adjudicacion >= fecha_adjudicacion_min)
            if fecha_adjudicacion_max:
                filters.append(models_stats.MasterPlopAll.fecha_adjudicacion <= fecha_adjudicacion_max)

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
