import json
import operator

import dateutil.parser
import falcon

from models import models_api
from utils.myjson import JSONEncoderPlus
from utils.mypeewee import ts_match


class LicitacionId(object):

    @models_api.database.atomic()
    def on_get(self, req, resp, licitacion_id=None):

        # Obtener la licitacion
        try:
            if '-' in licitacion_id:
                licitacion = models_api.Licitacion.get(models_api.Licitacion.codigo_licitacion == licitacion_id)
            elif licitacion_id.isdigit():
                licitacion_id = int(licitacion_id)
                licitacion = models_api.Licitacion.get(models_api.Licitacion.id_licitacion == licitacion_id)
            else:
                raise models_api.Licitacion.DoesNotExist()
        except models_api.Licitacion.DoesNotExist:
            raise falcon.HTTPNotFound()

        response = {
            'id': licitacion.id_licitacion,
            'codigo': licitacion.codigo_licitacion,

            'nombre': licitacion.nombre_licitacion,
            'descripcion': licitacion.descripcion_licitacion,

            'organismo': {
                'id': licitacion.id_organismo,

                'categoria': licitacion.nombre_ministerio,
                'nombre': licitacion.nombre_organismo
            },

            'unidad': {
                'nombre': licitacion.nombre_unidad,
                'rut': licitacion.rut_unidad,
                'region': licitacion.region_unidad,
                'comuna': licitacion.comuna_unidad,
                'direccion': licitacion.direccion_unidad
            },

            'usuario': {
                'cargo': licitacion.cargo_usuario_organismo,
                'nombre': licitacion.nombre_usuario_organismo,
                'rut': licitacion.rut_usuario_organismo
            },

            'responsable_contrato': {
                'nombre': licitacion.nombre_responsable_contrato,
                'telefono': licitacion.fono_responsable_contrato,
                'email': licitacion.email_responsable_contrato
            },

            'responsable_pago': {
                'nombre': licitacion.nombre_responsable_pago,
                'email': licitacion.email_responsable_pago
            },

            'estado': licitacion.estado,
            'fecha_cambio_estado': licitacion.fecha_cambio_estado,

            'fecha_creacion': licitacion.fecha_creacion,
            'fecha_publicacion': licitacion.fecha_publicacion,
            'fecha_inicio': licitacion.fecha_inicio,
            'fecha_final': licitacion.fecha_final,
            'fecha_cierre': licitacion.fecha_cierre,
            'fecha_adjudicacion': licitacion.fecha_adjudicacion,

            'n_items': licitacion.items_totales,

            'adjudicacion': {
                'n_items': licitacion.items_adjudicados,
                'monto': int(licitacion.monto_total) if licitacion.monto_total else None,
                'acta': licitacion.url_acta,
            } if licitacion.monto_total else None, # Only if there is an monto_total

            'categorias': [
                {
                    'id': licitacion.id_categoria_nivel1[i],
                    'nombre': licitacion.categoria_nivel1[i],
                }
            for i in range(len(licitacion.id_categoria_nivel1))]
        }

        response = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)

        callback = req.params.get('callback', None)
        if callback:
            response = "%s(%s)" % (callback, response)

        resp.body = response


# TODO Definir orden de resultados
class Licitacion(object):

    MAX_RESULTS = 10

    """
    q
    categoria_producto
    producto
    estado
    monto
    fecha_publicacion
    fecha_cierre
    fecha_adjudicacion
    organismo
    proveedor

    orden
    """

    @models_api.database.atomic()
    def on_get(self, req, resp):

        # Obtener todas las licitacion
        licitaciones = models_api.Licitacion.select()

        wheres = []
        order_bys = []

        # Busqueda de texto
        q_q = req.params.get('q', None)
        if q_q:
            # TODO Try to make just one query over one index instead of two or more ORed queries
            wheres.append(ts_match(models_api.Licitacion.nombre_licitacion, q_q) | ts_match(models_api.Licitacion.descripcion_licitacion, q_q))

        # Busqueda por categoria de producto
        q_categoria_producto = req.params.get('categoria_producto', None)
        if q_categoria_producto:
            if isinstance(q_categoria_producto, basestring):
                q_categoria_producto = [q_categoria_producto]

            try:
                q_categoria_producto = map(lambda x: int(x), q_categoria_producto)
            except ValueError:
                raise falcon.HTTPBadRequest("Parametro incorrecto", "categoria_producto debe ser un entero")

            wheres.append(models_api.Licitacion.id_categoria_nivel1.contains_any(q_categoria_producto))

        # Busqueda por  producto
        q_producto = req.params.get('producto', None)
        if q_producto:
            if isinstance(q_producto, basestring):
                q_producto = [q_producto]

            try:
                q_producto = map(lambda x: int(x), q_producto)
            except ValueError:
                raise falcon.HTTPBadRequest("Parametro incorrecto", "producto debe ser un entero")

            wheres.append(models_api.Licitacion.id_categoria_nivel3.contains_any(q_producto))

        # Busqueda por estado
        q_estado = req.params.get('estado', None)
        if q_estado:
            if isinstance(q_estado, basestring):
                q_estado = [q_estado]

            try:
                q_estado = map(lambda x: int(x), q_estado)
            except ValueError:
                raise falcon.HTTPBadRequest("Parametro incorrecto", "estado debe ser un entero")

            wheres.append(models_api.Licitacion.estado << q_estado)

        # Busqueda por organismo
        q_organismo = req.params.get('organismo', None)
        if q_organismo:
            if isinstance(q_organismo, basestring):
                q_organismo = [q_organismo]

            try:
                q_organismo = map(lambda x: int(x), q_organismo)
            except ValueError:
                raise falcon.HTTPBadRequest("Parametro incorrecto", "organismo debe ser un entero")

            wheres.append(models_api.Licitacion.id_organismo << q_organismo)

        # Busqueda por proveedor
        q_proveedor = req.params.get('proveedor', None)
        if q_proveedor:
            if isinstance(q_proveedor, basestring):
                q_proveedor = [q_proveedor]

            try:
                q_proveedor = map(lambda x: int(x), q_proveedor)
            except ValueError:
                raise falcon.HTTPBadRequest("Parametro incorrecto", "proveedor debe ser un entero")

            wheres.append(models_api.Licitacion.empresas_ganadoras.contains_any(q_proveedor))

        # Busqueda por monto
        q_monto = req.params.get('monto', None)
        if q_monto:
            if isinstance(q_monto, basestring):
                q_monto = [q_monto]

            filter_monto = []
            for montos in q_monto:
                montos = montos.split('|')
                try:
                    monto_min = int(montos[0]) if montos[0] else None
                    monto_max = int(montos[1]) if montos[1] else None
                except IndexError:
                    raise falcon.HTTPBadRequest("Parametro incorrecto", "Los valores en monto deben estar separados por un pipe [|]")
                except ValueError:
                    raise falcon.HTTPBadRequest("Parametro incorrecto", "Los valores en monto deben ser enteros")

                if monto_min and monto_max:
                    filter_monto.append((models_api.Licitacion.monto_total >= monto_min) & (models_api.Licitacion.monto_total <= monto_max))
                elif monto_min:
                    filter_monto.append(models_api.Licitacion.monto_total >= monto_min)
                elif monto_max:
                    filter_monto.append(models_api.Licitacion.monto_total <= monto_max)

            if filter_monto:
                wheres.append(reduce(operator.or_, filter_monto))

        # Busqueda por fecha de publicacion
        q_fecha_publicacion = req.params.get('fecha_publicacion', None)
        if q_fecha_publicacion:
            if isinstance(q_fecha_publicacion, basestring):
                q_fecha_publicacion = [q_fecha_publicacion]

            filter_fecha_publicacion = []
            for fechas in q_fecha_publicacion:
                fechas = fechas.split('|')
                try:
                    fecha_publicacion_min = dateutil.parser.parse(fechas[0], dayfirst=True, yearfirst=True).date() if fechas[0] else None
                    fecha_publicacion_max = dateutil.parser.parse(fechas[1], dayfirst=True, yearfirst=True).date() if fechas[1] else None
                except IndexError:
                    raise falcon.HTTPBadRequest("Parametro incorrecto", "Los valores en fecha_publicacion deben estar separados por un pipe [|]")
                except ValueError:
                    raise falcon.HTTPBadRequest("Parametro incorrecto", "El formato de la fecha en fecha_publicacion no es correcto")

                if fecha_publicacion_min and fecha_publicacion_max:
                    filter_fecha_publicacion.append((models_api.Licitacion.fecha_publicacion >= fecha_publicacion_min) & (models_api.Licitacion.fecha_publicacion <= fecha_publicacion_max))
                elif fecha_publicacion_min:
                    filter_fecha_publicacion.append(models_api.Licitacion.fecha_publicacion >= fecha_publicacion_min)
                elif fecha_publicacion_max:
                    filter_fecha_publicacion.append(models_api.Licitacion.fecha_publicacion <= fecha_publicacion_max)

            if filter_fecha_publicacion:
                wheres.append(reduce(operator.or_, filter_fecha_publicacion))

        # Busqueda por fecha de cierre
        q_fecha_cierre = req.params.get('fecha_cierre', None)
        if q_fecha_cierre:
            if isinstance(q_fecha_cierre, basestring):
                q_fecha_cierre = [q_fecha_cierre]

            filter_fecha_cierre = []
            for fechas in q_fecha_cierre:
                fechas = fechas.split('|')
                try:
                    fecha_cierre_min = dateutil.parser.parse(fechas[0], dayfirst=True, yearfirst=True).date() if fechas[0] else None
                    fecha_cierre_max = dateutil.parser.parse(fechas[1], dayfirst=True, yearfirst=True).date() if fechas[1] else None
                except IndexError:
                    raise falcon.HTTPBadRequest("Parametro incorrecto", "Los valores en fecha_cierre deben estar separados por un pipe [|]")
                except ValueError:
                    raise falcon.HTTPBadRequest("Parametro incorrecto", "El formato de la fecha en fecha_cierre no es correcto")

                if fecha_cierre_min and fecha_cierre_max:
                    filter_fecha_cierre.append((models_api.Licitacion.fecha_cierre >= fecha_cierre_min) & (models_api.Licitacion.fecha_cierre <= fecha_cierre_max))
                elif fecha_cierre_min:
                    filter_fecha_cierre.append(models_api.Licitacion.fecha_cierre >= fecha_cierre_min)
                elif fecha_cierre_max:
                    filter_fecha_cierre.append(models_api.Licitacion.fecha_cierre <= fecha_cierre_max)

            if filter_fecha_cierre:
                wheres.append(reduce(operator.or_, filter_fecha_cierre))

        # Busqueda por fecha de adjudicacion
        q_fecha_adjudicacion = req.params.get('fecha_adjudicacion', None)
        if q_fecha_adjudicacion:
            if isinstance(q_fecha_adjudicacion, basestring):
                q_fecha_adjudicacion = [q_fecha_adjudicacion]

            filter_fecha_adjudicacion = []
            for fechas in q_fecha_adjudicacion:
                fechas = fechas.split('|')
                try:
                    fecha_adjudicacion_min = dateutil.parser.parse(fechas[0], dayfirst=True, yearfirst=True).date() if fechas[0] else None
                    fecha_adjudicacion_max = dateutil.parser.parse(fechas[1], dayfirst=True, yearfirst=True).date() if fechas[1] else None
                except IndexError:
                    raise falcon.HTTPBadRequest("Parametro incorrecto", "Los valores en fecha_adjudicacion deben estar separados por un pipe [|]")
                except ValueError:
                    raise falcon.HTTPBadRequest("Parametro incorrecto", "El formato de la fecha en fecha_adjudicacion no es correcto")

                if fecha_adjudicacion_min and fecha_adjudicacion_max:
                    filter_fecha_adjudicacion.append((models_api.Licitacion.fecha_adjudicacion >= fecha_adjudicacion_min) & (models_api.Licitacion.fecha_adjudicacion <= fecha_adjudicacion_max))
                elif fecha_adjudicacion_min:
                    filter_fecha_adjudicacion.append(models_api.Licitacion.fecha_adjudicacion >= fecha_adjudicacion_min)
                elif fecha_adjudicacion_max:
                    filter_fecha_adjudicacion.append(models_api.Licitacion.fecha_adjudicacion <= fecha_adjudicacion_max)

            if filter_fecha_adjudicacion:
                wheres.append(reduce(operator.or_, filter_fecha_adjudicacion))

        q_orden = req.params.get('orden', None)
        if q_orden:
            if q_orden == 'monto':
                wheres.append(models_api.Licitacion.monto_total.is_null(False))
                order_bys.append(models_api.Licitacion.monto_total.asc())
            elif q_orden == '-monto':
                wheres.append(models_api.Licitacion.monto_total.is_null(False))
                order_bys.append(models_api.Licitacion.monto_total.desc())
            elif q_orden == 'fecha_publicacion':
                wheres.append(models_api.Licitacion.fecha_publicacion.is_null(False))
                order_bys.append(models_api.Licitacion.fecha_publicacion.asc())
            elif q_orden == '-fecha_publicacion':
                wheres.append(models_api.Licitacion.fecha_publicacion.is_null(False))
                order_bys.append(models_api.Licitacion.fecha_publicacion.desc())

        if wheres:
            licitaciones = licitaciones.where(*wheres)
        if order_bys:
            licitaciones = licitaciones.order_by(*order_bys)

        # Get page
        q_pagina = req.params.get('pagina', '1')
        q_pagina = max(int(q_pagina) if q_pagina.isdigit() else 1, 1)

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
                    } if licitacion['monto_total'] else None, # Only if there is monto_total

                }
                for licitacion in licitaciones.paginate(q_pagina, Licitacion.MAX_RESULTS).dicts()
            ]
        }

        resp.body = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)


class LicitacionIdItem(object):

    @models_api.database.atomic()
    def on_get(self, req, resp, licitacion_id):

        # Get the licitacion
        try:
            if licitacion_id.isdigit():
                licitacion_id = int(licitacion_id)
                items = models_api.LicitacionIdItem.select().filter(models_api.LicitacionIdItem.licitacion == licitacion_id)
            else:
                raise models_api.LicitacionIdItem.DoesNotExist()
        except models_api.Licitacion.DoesNotExist:
            raise falcon.HTTPNotFound()

        n_items = items.count()

        # Get page
        q_page = req.params.get('pagina', None)
        if q_page:
            q_page = max(int(q_page) if q_page.isdigit() else 1, 1)
            items = items.paginate(q_page, 10)

        response = {
            'n_items': n_items,
            'items': [
                {
                    'adjudicacion': {
                        'cantidad': float(item['cantidad_adjudicada']),
                        'monto_unitario': int(item['monto_pesos_adjudicado']) if item['monto_pesos_adjudicado'] else None,
                        'monto_total': int(item['monto_total']) if item['monto_total'] else None,

                        'fecha': item['fecha_adjudicacion'],

                        'proveedor': {
                            'id': item['id_empresa'],
                            'nombre': item['nombre_empresa'],
                            'rut': item['rut_sucursal']
                        }
                    } if item['id_empresa'] else None,

                    'codigo_categoria': item['codigo_categoria'],
                    'nombre_categoria': item['categoria_global'],

                    'codigo_producto': item['codigo_producto'],
                    'nombre_producto': item['nombre_producto'],

                    'descripcion': item['descripcion'],

                    'unidad': item['unidad_medida'],
                    'cantidad': item['cantidad']
                }
            for item in items.dicts().iterator()]
        }

        response = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)

        callback = req.params.get('callback', None)
        if callback:
            response = "%s(%s)" % (callback, response)

        resp.body = response
