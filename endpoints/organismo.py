import operator
import json

import falcon
import peewee
import dateutil

from models import models_api
from utils.myjson import JSONEncoderPlus
from utils.mypeewee import ts_match


class OrganismoId(object):

    @models_api.database.atomic()
    def on_get(self, req, resp, organismo_id):

        try:
            organismo_id = int(organismo_id)
        except ValueError:
            raise falcon.HTTPNotFound()

        # Get the organismo
        try:
            organismo = models_api.OrganismoStats.get(
                models_api.OrganismoStats.id_organismo == organismo_id
            )
        except models_api.OrganismoStats.DoesNotExist:
            raise falcon.HTTPNotFound()

        response = {
            'id': organismo.id_organismo,

            'categoria': organismo.nombre_ministerio,
            'nombre': organismo.nombre_organismo
        }

        resp.body = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)


class Organismo(object):

    MAX_RESULTS = 10

    """
    q
    organismo
    fecha_adjudicacion
    proveedor_adjudicado
    n_licitaciones_adjudicadas  (group by)
    monto_adjudicado            (group by)

    orden
    """

    @models_api.database.atomic()
    def on_get(self, req, resp):

        selects = [
            models_api.ProveedorOrganismoCruce.organismo,
            models_api.ProveedorOrganismoCruce.nombre_ministerio,
            models_api.ProveedorOrganismoCruce.nombre_organismo
        ]
        wheres = []
        joins = []
        order_bys = []

        # Busqueda de texto
        q_q = req.params.get('q', None)
        if q_q:
            # TODO Try to make just one query over one index instead of two or more ORed queries
            wheres.append(ts_match(models_api.ProveedorOrganismoCruce.nombre_ministerio, q_q) | ts_match(models_api.ProveedorOrganismoCruce.nombre_organismo, q_q))

        q_organismo = req.params.get('organismo', None)
        if q_organismo:
            if isinstance(q_organismo, basestring):
                q_organismo = [q_organismo]

            try:
                q_organismo = map(lambda x: int(x), q_organismo)
            except ValueError:
                raise falcon.HTTPBadRequest("Parametro incorrecto", "organismo debe ser un entero")

            wheres.append(models_api.ProveedorOrganismoCruce.organismo << q_organismo)


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
                wheres.append(reduce(operator.or_, filter_fecha_adjudicacion))

        # Search by proveedor_adjudicado
        q_proveedor_adjudicado = req.params.get('proveedor_adjudicado', None)
        if q_proveedor_adjudicado:
            if isinstance(q_proveedor_adjudicado, basestring):
                q_proveedor_adjudicado = [q_proveedor_adjudicado]

            try:
                q_proveedor_adjudicado = map(lambda x: int(x), q_proveedor_adjudicado)
            except ValueError:
                raise falcon.HTTPBadRequest("Parametro incorrecto", "proveedor_adjudicado debe ser un entero")

            wheres.append(models_api.ProveedorOrganismoCruce.empresa << q_proveedor_adjudicado)

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

                if n_licitaciones_adjudicadas_min is not None and n_licitaciones_adjudicadas_max is not None:
                    filter_n_licitaciones_adjudicadas.append(
                        (peewee.fn.count(peewee.SQL('DISTINCT licitacion_id')) >= n_licitaciones_adjudicadas_min) &
                        (peewee.fn.count(peewee.SQL('DISTINCT licitacion_id')) <= n_licitaciones_adjudicadas_max)
                    )
                elif n_licitaciones_adjudicadas_min is not None:
                    filter_n_licitaciones_adjudicadas.append(
                        peewee.fn.count(peewee.SQL('DISTINCT licitacion_id')) >= n_licitaciones_adjudicadas_min
                    )
                elif n_licitaciones_adjudicadas_max is not None:
                    filter_n_licitaciones_adjudicadas.append(
                        peewee.fn.count(peewee.SQL('DISTINCT licitacion_id')) <= n_licitaciones_adjudicadas_max
                    )

            if filter_n_licitaciones_adjudicadas:
                filter_n_licitaciones_adjudicadas = reduce(operator.or_, filter_n_licitaciones_adjudicadas)

                organismos_n_licitaciones = models_api.ProveedorOrganismoCruce.select(
                    models_api.ProveedorOrganismoCruce.organismo,
                    peewee.fn.count(peewee.SQL('DISTINCT licitacion_id'))
                )

                if wheres:
                    organismos_n_licitaciones = organismos_n_licitaciones.where(*wheres)

                organismos_n_licitaciones = organismos_n_licitaciones.group_by(
                    models_api.ProveedorOrganismoCruce.organismo
                ).having(
                    filter_n_licitaciones_adjudicadas
                )

                organismos_ids = [organismo_n_licitaciones['organismo'] for organismo_n_licitaciones in organismos_n_licitaciones.dicts().iterator()]

                wheres.append(models_api.ProveedorOrganismoCruce.organismo << organismos_ids if organismos_ids else False)

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

                if monto_adjudicado_min is not None and monto_adjudicado_max is not None:
                    filter_monto_adjudicado.append(
                        (peewee.fn.sum(models_api.ProveedorOrganismoCruce.monto_total) >= monto_adjudicado_min) &
                        (peewee.fn.sum(models_api.ProveedorOrganismoCruce.monto_total) <= monto_adjudicado_max)
                    )
                elif monto_adjudicado_min is not None:
                    filter_monto_adjudicado.append(
                        peewee.fn.sum(models_api.ProveedorOrganismoCruce.monto_total) >= monto_adjudicado_min
                    )
                elif monto_adjudicado_max is not None:
                    filter_monto_adjudicado.append(
                        peewee.fn.sum(models_api.ProveedorOrganismoCruce.monto_total) <= monto_adjudicado_max
                    )

            if filter_monto_adjudicado:
                filter_monto_adjudicado = reduce(operator.or_, filter_monto_adjudicado)

                organismos_montos = models_api.ProveedorOrganismoCruce.select(
                    models_api.ProveedorOrganismoCruce.organismo,
                    peewee.fn.sum(models_api.ProveedorOrganismoCruce.monto_total).alias('monto_adjudicado')
                )

                if wheres:
                    organismos_montos = organismos_montos.where(*wheres)

                organismos_montos = organismos_montos.group_by(
                    models_api.ProveedorOrganismoCruce.organismo
                ).having(
                    filter_monto_adjudicado
                ).alias('organismos_montos')

                selects.append(organismos_montos.c.monto_adjudicado)

                joins.append([
                    organismos_montos,
                    peewee.JOIN_INNER,
                    (models_api.ProveedorOrganismoCruce.organismo == organismos_montos.c.organismo_id)
                ])

                q_orden = req.params.get('orden', None)
                if q_orden == 'monto_adjudicado':
                    order_bys.append(organismos_montos.c.monto_adjudicado.asc())
                elif q_orden == '-monto_adjudicado':
                    order_bys.append(organismos_montos.c.monto_adjudicado.desc())

        # Build query
        organismos = models_api.ProveedorOrganismoCruce.select(*selects)

        if wheres:
            organismos = organismos.where(*wheres)
        if joins:
            for join in joins:
                organismos = organismos.join(*join)
        if order_bys:
            organismos = organismos.order_by(*order_bys)

        organismos = organismos.distinct()

        n_organismos = organismos.count()

        # Get page
        q_page = req.params.get('pagina', None)
        if q_page:
            q_page = max(int(q_page) if q_page.isdigit() else 1, 1)
            organismos = organismos.paginate(q_page, 10)

        response = {
            'n_organismos': n_organismos,
            'organismos': [
                {
                    'id': organismo['organismo'],
                    'categoria': organismo['nombre_ministerio'],
                    'nombre': organismo['nombre_organismo'],

                    'monto_adjudicado': int(organismo['monto_adjudicado']) if 'monto_adjudicado' in organismo else None
                }
            for organismo in organismos.dicts().iterator()]
        }

        resp.body = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)


    @models_api.database.atomic()
    def on_post(self, req, resp):

        filtros = req.context['payload'].get('filtros', [])

        if filtros:

            if len(filtros) > 5:
                raise falcon.HTTPBadRequest("Filtros incorrectos", "Demasiados filtros. Maximo 5.")

            for index, filtro in enumerate(filtros):

                if index > 0:
                    pre_organismos = [organismo['id'] for organismo in json.loads(resp.body)['organismos']]
                    if not pre_organismos:
                        break
                    else:
                        req.params['organismo'] = pre_organismos

                # for k, v in filtro.iteritems():
                #     if isinstance(v, int):
                #         filtro[k] = str(v)
                #     elif isinstance(v, list):
                #         filtro[k] = [str(vv) for vv in v]

                req.params.update(filtro)

                self.on_get(req, resp)

        else:
            self.on_get(req, resp)


class OrganismoEmbed(object):

    @models_api.database.atomic()
    def on_get(self, req, resp, organismo_id):

        try:
            organismo_id = int(organismo_id)
        except ValueError:
            raise falcon.HTTPNotFound()

        # Get the organismo
        try:
            organismo = models_api.OrganismoStats.get(
                models_api.OrganismoStats.id_organismo == organismo_id
            )
        except models_api.OrganismoStats.DoesNotExist:
            raise falcon.HTTPNotFound()

        response = {
            'id': organismo.id_organismo,

            'categoria': organismo.nombre_ministerio,
            'nombre': organismo.nombre_organismo,

            'stats': {
                'n_licitaciones_publicadas': organismo.licitaciones_publicadas,
                'n_licitaciones_adjudicadas': organismo.licitaciones_adjudicadas,
                'monto_adjudicado': int(organismo.monto_adjudicado),
            }
        }

        response = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)

        callback = req.params.get('callback', None)
        if callback:
            response = "%s(%s)" % (callback, response)

        resp.body = response


# TODO 404 si organismo no existe
class OrganismoIdLicitacion(object):

    @models_api.database.atomic()
    def on_get(self, req, resp, organismo_id):

        try:
            organismo_id = int(organismo_id)
        except ValueError:
            raise falcon.HTTPNotFound()

        licitaciones = models_api.Licitacion.filter(
            models_api.Licitacion.id_organismo == organismo_id
        ).order_by(
            models_api.Licitacion.fecha_publicacion.desc()
        )

        n_licitaciones = licitaciones.count()

        q_pagina = req.params.get('pagina', None)
        if q_pagina:
            q_pagina = max(int(q_pagina) if q_pagina.isdigit() else 1, 1)
            licitaciones = licitaciones.paginate(q_pagina, 10)

        response = {
            'n_licitaciones': n_licitaciones,
            'licitaciones': [
                {
                    'id': licitacion['id_licitacion'],
                    'codigo': licitacion['codigo_licitacion'],

                    'nombre': licitacion['nombre_licitacion'],
                    'descripcion': licitacion['descripcion_licitacion'],

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

