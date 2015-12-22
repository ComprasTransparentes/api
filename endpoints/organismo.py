# coding=utf-8
import operator
import json

import falcon
import peewee
import dateutil

from models import models_api
from utils.myjson import JSONEncoderPlus
from utils.mypeewee import ts_match


class OrganismoId(object):
    """Endpoint para un organismo en particular, identificado por ID"""

    @models_api.database.atomic()
    def on_get(self, req, resp, organismo_id):
        """Obtiene la informacion de un organismo en particular

        ================   =======     ===============
        Parámetro de URL   Ejemplo     Descripción
        ================   =======     ===============
        ``organismo_id``   5           ID de organismo
        ================   =======     ===============
        """

        # Validar que organismo_id es int
        # TODO Delegar la validacion a la BD o raise HTTPBadRequest
        try:
            organismo_id = int(organismo_id)
        except ValueError:
            raise falcon.HTTPNotFound()

        # Obtener un organismo
        try:
            organismo = models_api.OrganismoStats.get(
                models_api.OrganismoStats.id_organismo == organismo_id
            )
        except models_api.OrganismoStats.DoesNotExist:
            raise falcon.HTTPNotFound()

        # Construir la respuesta
        response = {
            'id': organismo.id_organismo,

            'categoria': organismo.nombre_ministerio,
            'nombre': organismo.nombre_organismo
        }

        # Codificar la respuesta en JSON
        resp.body = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)


class Organismo(object):
    """Endpoint para todos los organismos"""

    MAX_RESULTS = 10

    @models_api.database.atomic()
    def on_get(self, req, resp):
        """Obtiene informacion de todos los organismos.

        Permite filtrar y paginar los resultados.

        El paginamiento es de 10 elementos por pagina y es opcional.

            **Nota**: Para usar un mismo filtro con diferentes valores, se debe usar el parametro tantas veces como
            sea necesario. e.g.: `?organismo=6&organismo=8`. El filtro se aplicara usando la disyuncion de los
            valores. i.e: ... `organismo = 6 OR organismo = 8`. El filtro ``q`` no puede ser usado de esta forma.

            **Nota**: El campo `monto_adjudicado` de la respuesta solo tiene un valor si se ha usado el filtro
            ``monto_adjudicado`` en el request, si no, es ``null``.

        Los parametros aceptados son:

        Filtros

        ==============================  ==================  ==================================================================
        Parámetro                       Ejemplo             Descripción
        ==============================  ==================  ==================================================================
        ``q``                           clavos y martillos  Busqueda de texto
        ``organismo``                   1                   Por ID de organismo
        ``fecha_adjudicacion``          20140101|20141231   Por fecha de adjudicacion de licitaciones
        ``proveedor_adjudicado``        1                   Por ID de proveedores que han sido adjudicados en sus licitaciones
        ``n_licitaciones_adjudicadas``  10|20               Por cantidad de licitaciones adjudicadas
        ``monto_adjudicado``            10000|1000000       Por monto adjudicado en sus licitaciones
        ==============================  ==================  ==================================================================

        Modificadores

        ==============================  ================    ============================================================
        Parámetro                       Ejemplo             Descripción
        ==============================  ================    ============================================================
        ``orden``                       monto_adjudicado    Ordenar los resultados
        ``pagina``                      1                   Paginar y entregar la pagina solicitada
        ==============================  ================    ============================================================
        """

        # Preparar filtros y operaciones variables
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
            # TODO Hacer esta consulta sobre un solo indice combinado en lugar de usar dos filtros separados por OR
            wheres.append(ts_match(models_api.ProveedorOrganismoCruce.nombre_ministerio, q_q) | ts_match(models_api.ProveedorOrganismoCruce.nombre_organismo, q_q))

        # Filtrar por organismo
        q_organismo = req.params.get('organismo', None)
        if q_organismo:
            if isinstance(q_organismo, basestring):
                q_organismo = [q_organismo]
            try:
                q_organismo = map(lambda x: int(x), q_organismo)
            except ValueError:
                raise falcon.HTTPBadRequest("Parametro incorrecto", "organismo debe ser un entero")

            wheres.append(models_api.ProveedorOrganismoCruce.organismo << q_organismo)

        # Filtrar por fecha de adjudicacion
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

        # Filtrar por proveedor_adjudicado
        q_proveedor_adjudicado = req.params.get('proveedor_adjudicado', None)
        if q_proveedor_adjudicado:
            if isinstance(q_proveedor_adjudicado, basestring):
                q_proveedor_adjudicado = [q_proveedor_adjudicado]

            try:
                q_proveedor_adjudicado = map(lambda x: int(x), q_proveedor_adjudicado)
            except ValueError:
                raise falcon.HTTPBadRequest("Parametro incorrecto", "proveedor_adjudicado debe ser un entero")

            wheres.append(models_api.ProveedorOrganismoCruce.empresa << q_proveedor_adjudicado)

        # Filtrar por n_licitaciones_adjudicadas
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

        # Filtrar por monto_adjudicado
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

        # Construir query
        organismos = models_api.ProveedorOrganismoCruce.select(*selects)

        # Aplicar filtros
        if wheres:
            organismos = organismos.where(*wheres)
        if joins:
            for join in joins:
                organismos = organismos.join(*join)
        if order_bys:
            organismos = organismos.order_by(*order_bys)

        organismos = organismos.distinct()

        n_organismos = organismos.count()

        # Aplicar paginamiento
        q_page = req.params.get('pagina', None)
        if q_page:
            q_page = max(int(q_page) if q_page.isdigit() else 1, 1)
            organismos = organismos.paginate(q_page, 10)

        # Construir la respuesta
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

        # Codificar la respuesta en JSON
        resp.body = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)


    @models_api.database.atomic()
    def on_post(self, req, resp):
        """Obtiene informacion de todos los organismos usando la llamada a GET.

        Este endpoint POST permite realizar consultas igual que el endpoint GET y ademas:
        - No se ve afectado por el limite de tamano del querystring del enpoint GET
        - Permite encadenar consultas. i.e.: aplicar fltros sobre el resultado de la consulta anterior.

        :param req: Falcon request object
        :param resp: Falcon response object
        :return:
        """

        # Obtener contenido del request
        filtros = req.context['payload'].get('filtros', [])

        # Validar contenido
        if not isinstance(filtros, list):
            raise falcon.HTTPBadRequest("Filtros incorrectos", "El campo filtros no esta presente")

        # Delegar a on_get
        if len(filtros) == 0:
            self.on_get(req, resp)
        if len(filtros) <= 5:
            # Eliminar params de pagina. Usarlos solo al final.
            q_pagina = req.params.pop('pagina', None)

            for index, filtro in enumerate(filtros):

                if index > 0:
                    pre_organismos = [organismo['id'] for organismo in json.loads(resp.body)['organismos']]
                    if not pre_organismos:
                        break
                    else:
                        req.params['organismo'] = pre_organismos

                # TODO Aceptar filtros con valores primitivos (int, floats, ...).
                # for k, v in filtro.iteritems():
                #     if isinstance(v, int):
                #         filtro[k] = str(v)
                #     elif isinstance(v, list):
                #         filtro[k] = [str(vv) for vv in v]

                # Eliminar parametro de pagina enviado en el JSON
                filtro.pop('pagina', None)
                req.params.update(filtro)

                # Volver a agregar el parametro de pagina en la ultima iteracion
                if index == (len(filtros) - 1) and q_pagina:
                    req.params['pagina'] = q_pagina

                self.on_get(req, resp)
        else:
            # Limitar el numero de iteraciones posibles
            raise falcon.HTTPBadRequest("Filtros incorrectos", "Demasiados filtros. Maximo 5.")


class OrganismoEmbed(object):
    """Endpoint para la informacion resumida y estadisticas de un organismo"""

    @models_api.database.atomic()
    def on_get(self, req, resp, organismo_id):
        """Obtiene la informacion resumida y estadisticas de un organismo
        """

        # Validar organismo_id
        try:
            organismo_id = int(organismo_id)
        except ValueError:
            raise falcon.HTTPNotFound()

        # Obtener organismo
        try:
            organismo = models_api.OrganismoStats.get(
                models_api.OrganismoStats.id_organismo == organismo_id
            )
        except models_api.OrganismoStats.DoesNotExist:
            raise falcon.HTTPNotFound()

        # Construir la respuesta
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

        # Codificar la respuesta en JSON
        response = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)

        # Codificar la respuesta a JOSNP
        callback = req.params.get('callback', None)
        if callback:
            response = "%s(%s)" % (callback, response)

        resp.body = response


# TODO 404 si organismo no existe
class OrganismoIdLicitacion(object):
    """Endpoint para las licitaciones de un organismo"""

    @models_api.database.atomic()
    def on_get(self, req, resp, organismo_id):
        """Obtiene la lista de licitaciones de un organismo en particular.

        Permite paginar el resultado.

        Modificadores

        ==============================  ================    ============================================================
        Parámetro                       Ejemplo             Descripción
        ==============================  ================    ============================================================
        ``pagina``                      1                   Paginar y entregar la pagina solicitada
        ==============================  ================    ============================================================
        """

        # Validar organismo_id
        # TODO Delegar la validacion a la BD o raise HTTPBadRequest
        try:
            organismo_id = int(organismo_id)
        except ValueError:
            raise falcon.HTTPNotFound()

        # Construir la query
        licitaciones = models_api.Licitacion.filter(
            models_api.Licitacion.id_organismo == organismo_id
        ).order_by(
            models_api.Licitacion.fecha_publicacion.desc()
        )

        # Contar
        # TODO Contar en python si es mas rapido que en SQL. Mantener paginacion.
        n_licitaciones = licitaciones.count()

        # Aplicar paginacion
        q_pagina = req.params.get('pagina', None)
        if q_pagina:
            q_pagina = max(int(q_pagina) if q_pagina.isdigit() else 1, 1)
            licitaciones = licitaciones.paginate(q_pagina, 10)

        # Construir la respuesta
        response = {
            'n_licitaciones': n_licitaciones,
            'licitaciones': [
                {
                    # Identificadores
                    'id': licitacion['id_licitacion'],
                    'codigo': licitacion['codigo_licitacion'],
                    # Descriptores
                    'nombre': licitacion['nombre_licitacion'],
                    'descripcion': licitacion['descripcion_licitacion'],
                    # Fechas
                    'fecha_publicacion': licitacion['fecha_publicacion'],
                    'fecha_cierre': licitacion['fecha_cierre'],
                    'fecha_adjudicacion': licitacion['fecha_adjudicacion'],
                    # Estado
                    'estado': licitacion['estado'],
                    'fecha_cambio_estado': licitacion['fecha_cambio_estado'],
                    # Items
                    # El detalle de los items se encuentra en otro endpoint
                    'n_items': licitacion['items_totales'],
                    # Adjudicacion
                    'adjudicacion': {
                        'n_items': licitacion['items_adjudicados'],
                        'monto': int(licitacion['monto_total']) if licitacion['monto_total'] else None,
                        'acta': licitacion['url_acta'],
                    } if licitacion['url_acta'] else None, # Only if there is an acta
                }
                for licitacion in licitaciones.dicts().iterator()
            ]
        }

        # Codificar la respuesta en JSON
        resp.body = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)
