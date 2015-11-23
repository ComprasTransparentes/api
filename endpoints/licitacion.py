import json

import falcon
import peewee
import dateutil.parser

from peewee import fn, SQL
from playhouse.shortcuts import model_to_dict

from models import models_bkn
from models import models_stats
from models import models_api
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

    """
    q
    categoria_producto
    """

    @models_bkn.database.atomic()
    def on_get(self, req, resp):

        # Get all licitaciones
        licitaciones = models_api.Licitacion.select()

        filters = []

        # Search by text
        q_q = req.params.get('q', None)
        if q_q:
            # TODO Try to make just one query over one index instead of two or more ORed queries

            filters.append(ts_match(models_api.Licitacion.nombre_licitacion, q_q) | ts_match(models_api.Licitacion.descripcion_licitacion, q_q))

        # Search by categoria_producto
        # TODO Support multiple params of the same type
        q_categoria_producto = req.params.get('categoria_producto', None)
        if q_categoria_producto:
            # Check param is digit
            if not q_categoria_producto.isdigit():
                raise falcon.HTTPBadRequest("Wrong categoria_producto code", "categoria_producto must be an integer")
            q_producto = int(q_categoria_producto)

            filters.append(models_api.Licitacion.id_categoria_nivel1.contains(q_categoria_producto))

        # Search by estado
        q_estado = req.params.get('estado', None)
        if q_estado:
            # Check param is digit
            if not q_estado.isdigit():
                raise falcon.HTTPBadRequest("Wrong estado code", "estado must be an integer")
            q_estado = int(q_estado)

            filters.append(models_api.Licitacion.estado == q_estado)

        # Search by monto
        q_monto = req.params.get('monto', None)
        if q_monto:
            q_monto = q_monto.split('|')
            try:
                monto_min = int(q_monto[0]) if q_monto[0] else None
                monto_max = int(q_monto[1]) if q_monto[1] else None
            except IndexError:
                raise falcon.HTTPBadRequest("Wrong monto", "montos must be separated by a pipe [|]")
            except ValueError:
                raise falcon.HTTPBadRequest("Wrong monto", "montos must be integers")

            if monto_min:
                filters.append(models_api.Licitacion.monto_total >= monto_min)
            if monto_max:
                filters.append(models_api.Licitacion.monto_total <= monto_max)

        # Search by fecha_publicacion
        q_fecha_publicacion = req.params.get('fecha_publicacion', None)
        if q_fecha_publicacion:
            q_fecha_publicacion = q_fecha_publicacion.split('|')
            try:
                fecha_publicacion_min = dateutil.parser.parse(q_fecha_publicacion[0], dayfirst=True, yearfirst=True) if q_fecha_publicacion[0] else None
                fecha_publicacion_max = dateutil.parser.parse(q_fecha_publicacion[1], dayfirst=True, yearfirst=True) if q_fecha_publicacion[1] else None
            except IndexError:
                raise falcon.HTTPBadRequest("Wrong fecha_publicacion", "Dates must be separated by a pipe [|]")
            except ValueError:
                raise falcon.HTTPBadRequest("Wrong fecha_publicacion", "Date must be a datetime in ISO8601 format")

            if fecha_publicacion_min:
                filters.append(models_api.Licitacion.fecha_publicacion >= fecha_publicacion_min)
            if fecha_publicacion_max:
                filters.append(models_api.Licitacion.fecha_publicacion <= fecha_publicacion_max)

        # Search by fecha_cierre
        q_fecha_cierre = req.params.get('fecha_cierre', None)
        if q_fecha_cierre:
            q_fecha_cierre = q_fecha_cierre.split('|')
            try:
                fecha_cierre_min = dateutil.parser.parse(q_fecha_cierre[0], dayfirst=True, yearfirst=True) if q_fecha_cierre[0] else None
                fecha_cierre_max = dateutil.parser.parse(q_fecha_cierre[1], dayfirst=True, yearfirst=True) if q_fecha_cierre[1] else None
            except IndexError:
                raise falcon.HTTPBadRequest("Wrong fecha_cierre", "Dates must be separated by a pipe [|]")
            except ValueError:
                raise falcon.HTTPBadRequest("Wrong fecha_cierre", "Dates must be a datetime in ISO8601 format")

            if fecha_cierre_min:
                filters.append(models_api.Licitacion.fecha_cierre >= fecha_cierre_min)
            if fecha_cierre_max:
                filters.append(models_api.Licitacion.fecha_cierre <= fecha_cierre_max)

        # Search by fecha_adjudicacion
        q_fecha_adjudicacion = req.params.get('fecha_adjudicacion', None)
        if q_fecha_adjudicacion:
            q_fecha_adjudicacion = q_fecha_adjudicacion.split('|')
            try:
                fecha_adjudicacion_min = dateutil.parser.parse(q_fecha_adjudicacion[0], dayfirst=True, yearfirst=True) if q_fecha_adjudicacion[0] else None
                fecha_adjudicacion_max = dateutil.parser.parse(q_fecha_adjudicacion[1], dayfirst=True, yearfirst=True) if q_fecha_adjudicacion[1] else None
            except IndexError:
                raise falcon.HTTPBadRequest("Wrong fecha_adjudicacion", "Dates must be separated by a pipe [|]")
            except ValueError:
                raise falcon.HTTPBadRequest("Wrong fecha_adjudicacion", "Dates must be a datetime in ISO8601 format")

            if fecha_adjudicacion_min:
                filters.append(models_api.Licitacion.fecha_adjudicacion >= fecha_adjudicacion_min)
            if fecha_adjudicacion_max:
                filters.append(models_api.Licitacion.fecha_adjudicacion <= fecha_adjudicacion_max)


        if filters:
            licitaciones = licitaciones.where(*filters)

        # Get page
        q_page = req.params.get('pagina', '1')
        q_page = max(int(q_page) if q_page.isdigit() else 1, 1)

        response = {
            'n_licitaciones': licitaciones.count(),
            'licitaciones': [
                {
                    'id': licitacion['id_licitacion'],
                    'codigo': licitacion['codigo_licitacion'],
                    'nombre': licitacion['nombre_licitacion'],
                    'descripcion': licitacion['descripcion_licitacion'],

                    'organismo': {
                        'id': licitacion['id_organismo'],

                        'categoria': licitacion['nombre_ministerio'],
                        'nombre': licitacion['nombre_organismo'],
                    },

                    'fecha_publicacion': licitacion['fecha_publicacion'],
                    'fecha_cierre': licitacion['fecha_cierre'],
                    'fecha_adjudicacion': licitacion['fecha_adjudicacion'],

                    'estado': licitacion['estado'],
                    'fecha_cambio_estado': licitacion['fecha_cambio_estado'],

                    'n_items': licitacion['items_totales'],

                    'adjudicacion': {
                        'n_items': licitacion['items_adjudicados'],
                        'monto': int(licitacion['monto_total']) if licitacion['monto_total'] else None,
                        'acta': licitacion['url_acta'],
                    } if licitacion['url_acta'] else None, # Only if there is an acta

                    'categorias': [
                        {
                            'id': licitacion['id_categoria_nivel1'][i],
                            'nombre': licitacion['categoria_nivel1'][i],
                        }
                    for i in range(len(licitacion['id_categoria_nivel1']))]
                }
                for licitacion in licitaciones.paginate(q_page, LicitacionList.MAX_RESULTS).dicts()
            ]
        }

        resp.body = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)
