# coding=utf-8
import operator
import json

import falcon
import peewee
import dateutil

from models import models_api
from utils.myjson import JSONEncoderPlus
from utils.mypeewee import ts_match


class ProveedorId(object):
    """Endpoint para un proveedor en particular, identificado por ID"""

    @models_api.database.atomic()
    def on_get(self, req, resp, proveedor_id):
        """Obtiene la informacion de un proveedor en particular

        ================   =======     ===============
        Parámetro de URL   Ejemplo     Descripción
        ================   =======     ===============
        ``proveedor_id``   5           ID de proveedor
        ================   =======     ===============
        """

        # Validar que proveedor_id es int
        # TODO Delegar la validacion a la BD o raise HTTPBadRequest
        try:
            proveedor_id = int(proveedor_id)
        except ValueError:
            raise falcon.HTTPNotFound()

        # Obtener un proveedor
        try:
            proveedor = models_api.ProveedorStats.get(
                models_api.ProveedorStats.empresa == proveedor_id
            )
        except models_api.ProveedorStats.DoesNotExist:
            raise falcon.HTTPNotFound()

        # Construir la respuesta
        response = {
            'id': proveedor.empresa,

            'nombre': proveedor.nombre_empresa,
            'rut': proveedor.rut_sucursal
        }

        # Codificar la respuesta en JSON
        resp.body = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)


class Proveedor(object):
    """Endpoint para todos los proveedores"""

    MAX_RESULTS = 10

    @models_api.database.atomic()
    def on_get(self, req, resp):
        """Obtiene informacion de todos los proveedores.

        Permite filtrar y paginar los resultados.

        El paginamiento es de 10 elementos por pagina y es opcional.

            **Nota**: Para usar un mismo filtro con diferentes valores, se debe usar el parametro tantas veces como
            sea necesario. e.g.: `?proveedor=6&proveedor=8`. El filtro se aplicara usando la disyuncion de los
            valores. i.e: ... `proveedor = 6 OR proveedor = 8`. El filtro ``q`` no puede ser usado de esta forma.

            **Nota**: El campo `monto_adjudicado` de la respuesta solo tiene un valor si se ha usado el filtro
            ``monto_adjudicado`` en el request, si no, es ``null``.

        Los parametros aceptados son:

        Filtros

        ==============================  ==================  ============================================================
        Parámetro                       Ejemplo             Descripción
        ==============================  ==================  ============================================================
        ``q``                           clavos y martillos  Busqueda de texto
        ``proveedor``                   1                   Por ID de proveedor
        ``fecha_adjudicacion``          20140101|20141231   Por fecha de adjudicacion de licitaciones
        ``organismo_adjudicador``       1                   Por ID de organismos que han les concedido licitaciones
        ``n_licitaciones_adjudicadas``  10|20               Por cantidad de licitaciones adjudicadas
        ``monto_adjudicado``            10000|1000000       Por monto adjudicado en licitaciones
        ==============================  ==================  ============================================================

        Modificadores

        ==============================  ================    ============================================================
        Parámetro                       Ejemplo             Descripción
        ==============================  ================    ============================================================
        ``orden``                       monto_adjudicado    Ordenar los resultados
        ``pagina``                      1                   Paginar y entregar la pagina solicitada
        ==============================  ================    ============================================================
        """

        # Preparar los filtros y operaciones variables
        selects = [
            models_api.ProveedorOrganismoCruce.empresa,
            models_api.ProveedorOrganismoCruce.nombre_empresa,
            models_api.ProveedorOrganismoCruce.rut_sucursal
        ]
        wheres = []
        joins = []
        order_bys = []

        # Busqueda de texto
        q_q = req.params.get('q', None)
        if q_q:
            # TODO Hacer esta consulta sobre un solo indice combinado en lugar de usar dos filtros separados por OR
            wheres.append(ts_match(models_api.ProveedorOrganismoCruce.nombre_empresa, q_q) | ts_match(models_api.ProveedorOrganismoCruce.rut_sucursal, q_q))

        # Filtrar por proveedor
        q_proveedor = req.params.get('proveedor', None)
        if q_proveedor:
            if isinstance(q_proveedor, basestring):
                q_proveedor = [q_proveedor]
            try:
                q_proveedor = map(lambda x: int(x), q_proveedor)
            except ValueError:
                raise falcon.HTTPBadRequest("Parametro incorrecto", "proveedor debe ser un entero")

            wheres.append(models_api.ProveedorOrganismoCruce.empresa << q_proveedor)

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

        # Filtrar por organismo_adjudicador
        q_organismo_adjudicador = req.params.get('organismo_adjudicador', None)
        if q_organismo_adjudicador:
            if isinstance(q_organismo_adjudicador, basestring):
                q_organismo_adjudicador = [q_organismo_adjudicador]

            try:
                q_organismo_adjudicador = map(lambda x: int(x), q_organismo_adjudicador)
            except ValueError:
                raise falcon.HTTPBadRequest("Parametro incorrecto", "organismo_adjudicador debe ser un entero")

            wheres.append(models_api.ProveedorOrganismoCruce.organismo << q_organismo_adjudicador)

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

                proveedores_n_licitaciones = models_api.ProveedorOrganismoCruce.select(
                    models_api.ProveedorOrganismoCruce.empresa,
                    peewee.fn.count(peewee.SQL('DISTINCT licitacion_id'))
                )

                if wheres:
                    proveedores_n_licitaciones = proveedores_n_licitaciones.where(*wheres)

                proveedores_n_licitaciones = proveedores_n_licitaciones.group_by(
                    models_api.ProveedorOrganismoCruce.empresa
                ).having(
                    filter_n_licitaciones_adjudicadas
                )

                proveedores_ids = [proveedor_n_licitaciones['empresa'] for proveedor_n_licitaciones in proveedores_n_licitaciones.dicts().iterator()]

                wheres.append(models_api.ProveedorOrganismoCruce.empresa << proveedores_ids if proveedores_ids else False)

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

                if monto_adjudicado_min is not None and monto_adjudicado_max:
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

                proveedores_montos = models_api.ProveedorOrganismoCruce.select(
                    models_api.ProveedorOrganismoCruce.empresa,
                    peewee.fn.sum(models_api.ProveedorOrganismoCruce.monto_total).alias('monto_adjudicado')
                )

                if wheres:
                    proveedores_montos = proveedores_montos.where(*wheres)

                proveedores_montos = proveedores_montos.group_by(
                    models_api.ProveedorOrganismoCruce.empresa
                ).having(
                    filter_monto_adjudicado
                ).alias('proveedores_montos')

                selects.append(proveedores_montos.c.monto_adjudicado)

                joins.append([
                    proveedores_montos,
                    peewee.JOIN_INNER,
                    (models_api.ProveedorOrganismoCruce.empresa == proveedores_montos.c.empresa_id)
                ])

                q_orden = req.params.get('orden', None)
                if q_orden == 'monto_adjudicado':
                    order_bys.append(proveedores_montos.c.monto_adjudicado.asc())
                elif q_orden == '-monto_adjudicado':
                    order_bys.append(proveedores_montos.c.monto_adjudicado.desc())

        # Construir query
        proveedores = models_api.ProveedorOrganismoCruce.select(*selects)

        # Aplicar filtros
        if wheres:
            proveedores = proveedores.where(*wheres)
        if joins:
            for join in joins:
                proveedores = proveedores.join(*join)
        if order_bys:
            proveedores = proveedores.order_by(*order_bys)

        proveedores = proveedores.distinct()

        n_proveedores = proveedores.count()

        # Aplicar paginamiento
        q_page = req.params.get('pagina', None)
        if q_page:
            q_page = max(int(q_page) if q_page.isdigit() else 1, 1)
            proveedores = proveedores.paginate(q_page, 10)

        # Construir la respuesta
        response = {
            'n_proveedores': n_proveedores,
            'proveedores': [
                {
                    'id': proveedor['empresa'],
                    'nombre': proveedor['nombre_empresa'],
                    'rut': proveedor['rut_sucursal'],

                    'monto_adjudicado': int(proveedor['monto_adjudicado']) if 'monto_adjudicado' in proveedor else None
                }
            for proveedor in proveedores.dicts()]
        }

        # Codificar la respuesta en JSON
        resp.body = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)


    @models_api.database.atomic()
    def on_post(self, req, resp):
        """Obtiene informacion de todos los proveedores usando la llamada a GET.

        Este endpoint POST permite realizar consultas igual que el endpoint GET y ademas:
        - No se ve afectado por el limite de tamano del querystring del enpoint GET
        - Permite encadenar consultas. i.e.: aplicar fltros sobre el resultado de la consulta anterior.

        :param req: Falcon request object
        :param resp: Falcon response object
        :return:
        """

        # Obtener contenido del request
        filtros = req.context['payload'].get('filtros', [])

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
                    pre_proveedores = [organismo['id'] for organismo in json.loads(resp.body)['proveedores']]
                    if not pre_proveedores:
                        break
                    else:
                        req.params['proveedor'] = pre_proveedores

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
            raise falcon.HTTPBadRequest("Filtros incorrectos", "Demasiados filtros. Maximo 5.")


class ProveedorEmbed(object):
    """Endpoint para la informacion resumida y estadisticas de un proveedor"""

    @models_api.database.atomic()
    def on_get(self, req, resp, proveedor_id):
        """Obtiene la informacion resumida y estadisticas de un proveedor

        :param req: Falcon request object
        :param resp: Falcon response object
        :param proveedor_id: ID del proveedor
        :return:
        """

        # Validar organismo_id
        try:
            proveedor_id = int(proveedor_id)
        except ValueError:
            raise falcon.HTTPNotFound()

        # Obtener proveedor
        try:
            proveedor = models_api.ProveedorStats.get(
                models_api.ProveedorStats.empresa == proveedor_id
            )
        except Proveedor.DoesNotExist:
            raise falcon.HTTPNotFound()

        # Construir la respuesta
        response = {
            'id': proveedor.empresa,

            'nombre': proveedor.nombre_empresa,
            'rut': proveedor.rut_sucursal,

            'stats': {
                'n_licitaciones_adjudicadas': proveedor.licitaciones_adjudicadas,
                'monto_adjudicado': int(proveedor.monto_adjudicado),
            }
        }

        # Codificar la respuesta en JSON
        response = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)

        # Codificar la respuesta en JSONP
        callback = req.params.get('callback', None)
        if callback:
            response = "%s(%s)" % (callback, response)

        resp.body = response


# TODO 404 si organismo no existe
class ProveedorIdLicitacion(object):
    """Endpoint para las licitaciones de un proveedor"""

    @models_api.database.atomic()
    def on_get(self, req, resp, proveedor_id):
        """Obtiene la lista de licitaciones de un proveedor en particular.

        Permite paginar el resultado.

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
            proveedor_id = int(proveedor_id)
        except ValueError:
            raise falcon.HTTPNotFound()

        # Construir la query
        licitaciones = models_api.Licitacion.filter(
            models_api.Licitacion.empresas_ganadoras.contains(proveedor_id)
        ).order_by(
            models_api.Licitacion.fecha_adjudicacion.desc()
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
                    # Organismo emisor
                    'organismo': {
                        'id': licitacion['id_organismo'],

                        'categoria': licitacion['nombre_ministerio'],
                        'nombre': licitacion['nombre_organismo'],
                    },
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
