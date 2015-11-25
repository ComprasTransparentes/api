import operator
import copy
import json

import falcon
import peewee
import dateutil

from models import models_api
from utils.myjson import JSONEncoderPlus
from utils.mypeewee import ts_match


class ProveedorId(object):

    @models_api.database.atomic()
    def on_get(self, req, resp, proveedor_id):

        try:
            proveedor_id = int(proveedor_id)
        except ValueError:
            raise falcon.HTTPNotFound()

        # Get the proveedor
        try:
            proveedor = models_api.ProveedorStats.get(
                models_api.ProveedorStats.empresa == proveedor_id
            )
        except models_api.ProveedorStats.DoesNotExist:
            raise falcon.HTTPNotFound()

        response = {
            'id': proveedor.empresa,

            'nombre': proveedor.nombre_empresa,
            'rut': proveedor.rut_sucursal
        }

        resp.body = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)


class Proveedor(object):

    MAX_RESULTS = 10

    """
    q
    proveedor
    fecha_adjudicacion
    organismo_adjudicador
    n_licitaciones_adjudicadas  (group by)
    monto_adjudicado            (group by)
    """

    @models_api.database.atomic()
    def on_get(self, req, resp):

        # Get all proveedores
        proveedores = models_api.ProveedorOrganismoCruce.select(
            models_api.ProveedorOrganismoCruce.empresa,
            models_api.ProveedorOrganismoCruce.nombre_empresa,
            models_api.ProveedorOrganismoCruce.rut_sucursal
        )

        filters = []

        # Busqueda de texto
        q_q = req.params.get('q', None)
        if q_q:
            # TODO Try to make just one query over one index instead of two or more ORed queries
            filters.append(ts_match(models_api.ProveedorOrganismoCruce.nombre_empresa, q_q) | ts_match(models_api.ProveedorOrganismoCruce.rut_sucursal, q_q))

        q_proveedor = req.params.get('organismo', None)
        if q_proveedor:
            if isinstance(q_proveedor, basestring):
                q_proveedor = [q_proveedor]

            try:
                q_proveedor = map(lambda x: int(x), q_proveedor)
            except ValueError:
                raise falcon.HTTPBadRequest("Parametro incorrecto", "organismo debe ser un entero")

            filters.append(models_api.ProveedorOrganismoCruce.empresa << q_proveedor)

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
                    filter_fecha_adjudicacion.append((models_api.ProveedorOrganismoCruce.fecha_adjudicacion >= fecha_adjudicacion_min) & (models_api.ProveedorOrganismoCruce.fecha_adjudicacion <= fecha_adjudicacion_max))
                elif fecha_adjudicacion_min:
                    filter_fecha_adjudicacion.append(models_api.ProveedorOrganismoCruce.fecha_adjudicacion >= fecha_adjudicacion_min)
                elif fecha_adjudicacion_max:
                    filter_fecha_adjudicacion.append(models_api.ProveedorOrganismoCruce.fecha_adjudicacion <= fecha_adjudicacion_max)

            if filter_fecha_adjudicacion:
                filters.append(reduce(operator.or_, filter_fecha_adjudicacion))

        # Search by organismo_adjudicador
        q_organismo_adjudicador = req.params.get('organismo_adjudicador', None)
        if q_organismo_adjudicador:
            if isinstance(q_organismo_adjudicador, basestring):
                q_organismo_adjudicador = [q_organismo_adjudicador]

            try:
                q_organismo_adjudicador = map(lambda x: int(x), q_organismo_adjudicador)
            except ValueError:
                raise falcon.HTTPBadRequest("Parametro incorrecto", "organismo_adjudicador debe ser un entero")

            filters.append(models_api.ProveedorOrganismoCruce.organismo << q_organismo_adjudicador)

        # Search by n_licitaciones_adjudicadas
        q_n_licitaciones_adjudicadas = req.params.get('n_licitaciones_adjudicadas')
        if q_n_licitaciones_adjudicadas:
            if isinstance(q_n_licitaciones_adjudicadas, basestring):
                q_n_licitaciones_adjudicadas = [q_n_licitaciones_adjudicadas]

            filter_n_licitaciones_adjudicadas = []
            for n_licitaciones in q_n_licitaciones_adjudicadas:
                n_licitaciones = n_licitaciones.split('|')
                try:
                    n_licitaciones_adjudicadas_min = int(n_licitaciones[0]) if n_licitaciones[0] else None
                    n_licitaciones_adjudicadas_max = int(n_licitaciones[1]) if n_licitaciones[1] else None
                except IndexError:
                        raise falcon.HTTPBadRequest("Parametro incorrecto", "Los valores en n_licitaciones_adjudicadas deben estar separados por un pipe [|]")
                except ValueError:
                    raise falcon.HTTPBadRequest("Parametro incorrecto", "n_licitaciones_adjudicadas debe ser un entero")

                if n_licitaciones_adjudicadas_min and n_licitaciones_adjudicadas_max:
                    filter_n_licitaciones_adjudicadas.append(
                        (peewee.fn.count(peewee.SQL('DISTINCT licitacion_id')) >= n_licitaciones_adjudicadas_min) &
                        (peewee.fn.count(peewee.SQL('DISTINCT licitacion_id')) <= n_licitaciones_adjudicadas_max)
                    )
                elif n_licitaciones_adjudicadas_min:
                    filter_n_licitaciones_adjudicadas.append(
                        peewee.fn.count(peewee.SQL('DISTINCT licitacion_id')) >= n_licitaciones_adjudicadas_min
                    )
                elif n_licitaciones_adjudicadas_max:
                    filter_n_licitaciones_adjudicadas.append(
                        peewee.fn.count(peewee.SQL('DISTINCT licitacion_id')) <= n_licitaciones_adjudicadas_max
                    )

            if filter_n_licitaciones_adjudicadas:
                filter_n_licitaciones_adjudicadas = reduce(operator.or_, filter_n_licitaciones_adjudicadas)

                proveedores_n_licitaciones = models_api.ProveedorOrganismoCruce.select(
                    models_api.ProveedorOrganismoCruce.empresa,
                    peewee.fn.count(peewee.SQL('DISTINCT licitacion_id'))
                )

                if filters:
                    proveedores_n_licitaciones = proveedores_n_licitaciones.where(*filters)

                proveedores_n_licitaciones = proveedores_n_licitaciones.group_by(
                    models_api.ProveedorOrganismoCruce.empresa
                ).having(
                    filter_n_licitaciones_adjudicadas
                )

                proveedores_ids = [proveedor_n_licitaciones['empresa'] for proveedor_n_licitaciones in proveedores_n_licitaciones.dicts().iterator()]

                filters.append(models_api.ProveedorOrganismoCruce.empresa << proveedores_ids if proveedores_ids else False)

        # Search by monto_adjudicado
        q_monto_adjudicado = req.params.get('monto_adjudicado')
        if q_monto_adjudicado:
            if isinstance(q_monto_adjudicado, basestring):
                q_monto_adjudicado = [q_monto_adjudicado]

            filter_monto_adjudicado = []
            for monto_adjudicado in q_monto_adjudicado:
                monto_adjudicado = monto_adjudicado.split('|')
                try:
                    monto_adjudicado_min = int(monto_adjudicado[0]) if monto_adjudicado[0] else None
                    monto_adjudicado_max = int(monto_adjudicado[1]) if monto_adjudicado[1] else None
                except IndexError:
                        raise falcon.HTTPBadRequest("Parametro incorrecto", "Los valores en monto_adjudicado deben estar separados por un pipe [|]")
                except ValueError:
                    raise falcon.HTTPBadRequest("Parametro incorrecto", "monto_adjudicado debe ser un entero")

                if monto_adjudicado_min and monto_adjudicado_max:
                    filter_monto_adjudicado.append(
                        (peewee.fn.sum(models_api.ProveedorOrganismoCruce.monto_total) >= monto_adjudicado_min) &
                        (peewee.fn.sum(models_api.ProveedorOrganismoCruce.monto_total) <= monto_adjudicado_max)
                    )
                elif monto_adjudicado_min:
                    filter_monto_adjudicado.append(
                        peewee.fn.sum(models_api.ProveedorOrganismoCruce.monto_total) >= monto_adjudicado_min
                    )
                elif monto_adjudicado_max:
                    filter_monto_adjudicado.append(
                        peewee.fn.sum(models_api.ProveedorOrganismoCruce.monto_total) <= monto_adjudicado_max
                    )

            if filter_monto_adjudicado:
                filter_monto_adjudicado = reduce(operator.or_, filter_monto_adjudicado)

                proveedores_montos = models_api.ProveedorOrganismoCruce.select(
                    models_api.ProveedorOrganismoCruce.empresa,
                    peewee.fn.sum(models_api.ProveedorOrganismoCruce.monto_total)
                )

                if filters:
                    proveedores_montos = proveedores_montos.where(*filters)

                proveedores_montos = proveedores_montos.group_by(
                    models_api.ProveedorOrganismoCruce.empresa
                ).having(
                    filter_monto_adjudicado
                )

                proveedores_ids = [proveedor_monto['empresa'] for proveedor_monto in proveedores_montos.dicts().iterator()]

                filters.append(models_api.ProveedorOrganismoCruce.empresa << proveedores_ids if proveedores_ids else False)

        if filters:
            proveedores = proveedores.where(*filters)

        proveedores = proveedores.distinct()

        # Get page
        q_page = req.params.get('pagina', None)
        if q_page:
            q_page = max(int(q_page) if q_page.isdigit() else 1, 1)
            proveedores.paginate(q_page, 10)

        response = {
            'n_proveedores': proveedores.count(),
            'proveedores': [
                {
                    'id': proveedor['empresa'],
                    'nombre': proveedor['nombre_empresa'],
                    'rut': proveedor['rut_sucursal'],
                }
            for proveedor in proveedores.dicts()]
        }

        resp.body = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)

    @models_api.database.atomic()
    def on_post(self, req, resp):

        filtros = req.context['payload'].get('filtros', [])

        if filtros:

            req.params.clear()
            for index, filtro in enumerate(filtros):

                if index > 0:
                    pre_organismos = [organismo['id'] for organismo in json.loads(resp.body)['proveedores']]
                    if not pre_organismos:
                        break
                    else:
                        req.params['proveedor'] = pre_organismos

                # for k, v in filtro.iteritems():
                #     if isinstance(v, int):
                #         filtro[k] = str(v)
                #     elif isinstance(v, list):
                #         filtro[k] = [str(vv) for vv in v]

                req.params.update(filtro)

                self.on_get(req, resp)

        else:
            raise falcon.HTTPBadRequest("Parametros incorrectos", "El atributo filtros no esta presente")


class ProveedorEmbed(object):

    @models_api.database.atomic()
    def on_get(self, req, resp, proveedor_id):

        try:
            proveedor_id = int(proveedor_id)
        except ValueError:
            raise falcon.HTTPNotFound()

        # Get the proveedor
        try:
            proveedor = models_api.ProveedorStats.get(
                models_api.ProveedorStats.empresa == proveedor_id
            )
        except Proveedor.DoesNotExist:
            raise falcon.HTTPNotFound()

        response = {
            'id': proveedor.empresa,

            'nombre': proveedor.nombre_empresa,
            'rut': proveedor.rut_sucursal,

            'stats': {
                'n_licitaciones_adjudicadas': proveedor.licitaciones_adjudicadas,
                'monto_adjudicado': int(proveedor.monto_adjudicado),
            }
        }

        response = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)

        callback = req.params.get('callback', None)
        if callback:
            response = "%s(%s)" % (callback, response)

        resp.body = response


# TODO 404 si organismo no existe
class ProveedorIdLicitacion(object):

    @models_api.database.atomic()
    def on_get(self, req, resp, proveedor_id):

        try:
            proveedor_id = int(proveedor_id)
        except ValueError:
            raise falcon.HTTPNotFound()

        licitaciones = models_api.Licitacion.filter(
            models_api.Licitacion.empresas_ganadoras.contains(proveedor_id)
        ).order_by(
            models_api.Licitacion.fecha_adjudicacion.desc()
        )

        q_pagina = req.params.get('pagina', None)
        if q_pagina:
            q_pagina = max(int(q_pagina) if q_pagina.isdigit() else 1, 1)
            licitaciones = licitaciones.paginate(q_pagina, 10)

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
                }
                for licitacion in licitaciones.dicts().iterator()
            ]
        }

        resp.body = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)
