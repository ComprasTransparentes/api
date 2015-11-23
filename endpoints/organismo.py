import sys
import json

import falcon
import peewee
import dateutil

from datetime import date

from models import models as models_old
from models import models_api
from models.models_bkn import *
from models import models_stats
from utils.myjson import JSONEncoderPlus
from utils.mypeewee import ts_match


class OrganismoItem(object):

    ALLOWED_FILTERS = ['producto']

    @database.atomic()
    def on_get(self, req, resp, organismo_id):

        # Get the organismo
        try:

            organismo = models_old.Jerarquia.get(models_old.Jerarquia.catalogo_organismo == organismo_id)

        except models_old.Jerarquia.DoesNotExist:
            raise falcon.HTTPNotFound()

        response = {
            'id': organismo.catalogo_organismo,
            'categoria': organismo.ministerio_nombre,
            'codigo_comprador': organismo.organismo_codigo,
            'nombre_comprador': organismo.organismo_nombre
        }

        top_licitaciones_organismo_global = models_stats.MasterPlop.select(
            models_stats.MasterPlop.licitacion.alias('id'),
            fn.sum(models_stats.MasterPlop.monto).alias('monto_global'),
        ).group_by(
            models_stats.MasterPlop.licitacion
        ).where(
            models_stats.MasterPlop.organismo == organismo_id
        ).alias('licitaciones_organismo_global')

        top_licitaciones_organismo = models_stats.MasterPlop.select(
            models_stats.MasterPlop.licitacion.alias('id'),
            models_stats.MasterPlop.licitacion_codigo.alias('codigo'),
            models_stats.MasterPlop.licitacion_nombre.alias('nombre'),
            models_stats.MasterPlop.licitacion_descripcion.alias('descripcion'),
            models_stats.MasterPlop.fecha_creacion.alias('fecha_creacion'),
            models_stats.MasterPlop.fecha_adjudicacion.alias('fecha_adjudicacion'),
            top_licitaciones_organismo_global.c.monto_global.alias('monto')
        ).join(
            top_licitaciones_organismo_global,
            on=(models_stats.MasterPlop.licitacion == top_licitaciones_organismo_global.c.id)
        ).distinct()

        # Top proveedores
        top_proveedores = models_stats.MasterPlop.select(
            models_stats.MasterPlop.company.alias('id'),
            models_stats.MasterPlop.nombre,
            models_stats.MasterPlop.rut_sucursal.alias('rut'),
            fn.sum(models_stats.MasterPlop.monto).alias('monto')
        ).group_by(
            models_stats.MasterPlop.company,
            models_stats.MasterPlop.nombre,
            models_stats.MasterPlop.rut_sucursal
        ).where(
            models_stats.MasterPlop.organismo == organismo_id
        ).order_by(
            SQL('monto').desc()
        ).limit(10)

        # Licitaciones
        estados_recientes = LicitacionEstado.select(
            LicitacionEstado.licitacion,
            fn.max(LicitacionEstado.fecha)
        ).group_by(
            LicitacionEstado.licitacion
        ).alias('estados_recientes')

        licitacion_estados = LicitacionEstado.select(
            LicitacionEstado.licitacion,
            LicitacionEstado.estado,
            LicitacionEstado.fecha
        ).join(
            estados_recientes,
            on=(LicitacionEstado.licitacion == estados_recientes.c.licitacion_id)
        ).alias('licitacion_estados')

        licitaciones = models_stats.LicitacionMaster.select(
            models_stats.LicitacionMaster.licitacion.alias('id'),
            models_stats.LicitacionMaster.licitacion_codigo.alias('codigo'),
            models_stats.LicitacionMaster.nombre.alias('nombre'),
            models_stats.LicitacionMaster.fecha_creacion,
            licitacion_estados.c.estado.alias('estado')
        ).join(
            licitacion_estados,
            on=(models_stats.LicitacionMaster.licitacion == licitacion_estados.c.licitacion_id)
        ).where(
            models_stats.LicitacionMaster.catalogo_organismo == organismo_id
        ).order_by(
            models_stats.LicitacionMaster.fecha_creacion.desc()
        )

        p_licitaciones = req.params.get('p_licitaciones', '1')
        p_licitaciones = max(int(p_licitaciones) if p_licitaciones.isdigit() else 1, 1)

        response['extra'] = {
            'top_licitaciones':             [licitacion for licitacion in top_licitaciones_organismo.order_by(SQL('monto').desc()).limit(10).dicts()],
            'top_proveedores':              [proveedor for proveedor in top_proveedores.dicts()],
            'licitaciones':                 [licitacion for licitacion in licitaciones.order_by(SQL('fecha_creacion').desc()).paginate(p_licitaciones, 10).dicts()],
            'n_licitaciones':               licitaciones.count(),
            'n_licitaciones_adjudicadas':   top_licitaciones_organismo.count(),
            'monto_adjudicado':             int(top_licitaciones_organismo.select(fn.sum(SQL('monto')).alias('monto_adjudicado')).first().monto_adjudicado),


            # 'cantidad_licitaciones': licitacion_items_producto.count(),
            # 'cantidad_licitaciones_adjudicadas': 123,
            # 'monto_total': 123,
            # 'monto_promedio': 123,
            # 'cantidad_proveedores': 123
        }

        response = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)

        callback = req.params.get('callback', None)
        if callback:
            response = "%s(%s)" % (callback, response)

        resp.body = response


class OrganismoEmbed(object):

    @database.atomic()
    def on_get(self, req, resp, organismo_id):

        # Get the organismo
        try:

            organismo = models_old.Jerarquia.get(models_old.Jerarquia.catalogo_organismo == organismo_id)

        except models_old.Jerarquia.DoesNotExist:
            raise falcon.HTTPNotFound()

        response = {
            'id': organismo.catalogo_organismo,
            'categoria': organismo.ministerio_nombre,
            'codigo_comprador': organismo.organismo_codigo,
            'nombre_comprador': organismo.organismo_nombre
        }

        licitaciones_organismo_global = models_stats.MasterPlop.select(
            models_stats.MasterPlop.licitacion.alias('id'),
            fn.sum(models_stats.MasterPlop.monto).alias('monto_global'),
        ).group_by(
            models_stats.MasterPlop.licitacion
        ).where(
            models_stats.MasterPlop.organismo == organismo_id
        ).alias('licitaciones_organismo_global')

        licitaciones_organismo = models_stats.MasterPlop.select(
            models_stats.MasterPlop.licitacion.alias('id'),
            models_stats.MasterPlop.licitacion_codigo.alias('codigo'),
            models_stats.MasterPlop.licitacion_nombre.alias('nombre'),
            models_stats.MasterPlop.licitacion_descripcion.alias('descripcion'),
            models_stats.MasterPlop.fecha_creacion.alias('fecha_creacion'),
            licitaciones_organismo_global.c.monto_global.alias('monto')
        ).join(
            licitaciones_organismo_global,
            on=(models_stats.MasterPlop.licitacion == licitaciones_organismo_global.c.id)
        )

        # Licitaciones

        licitaciones = models_stats.LicitacionMaster.select(
            models_stats.LicitacionMaster.licitacion
        ).where(
            models_stats.LicitacionMaster.catalogo_organismo == organismo_id
        )

        response['extra'] = {
            'n_licitaciones':               licitaciones.count(),
            'n_licitaciones_adjudicadas':   licitaciones_organismo.count(),
            'monto_adjudicado':             int(licitaciones_organismo.select(fn.sum(SQL('monto')).alias('monto_adjudicado')).first().monto_adjudicado),
        }

        response = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)

        callback = req.params.get('callback', None)
        if callback:
            response = "%s(%s)" % (callback, response)

        resp.body = response


class OrganismoLicitacion(object):

    @database.atomic()
    def on_get(self, req, resp, organismo_id):

        estados_recientes = LicitacionEstado.select(
            LicitacionEstado.licitacion,
            fn.max(LicitacionEstado.fecha)
        ).group_by(
            LicitacionEstado.licitacion
        ).alias('estados_recientes')

        licitacion_estados = LicitacionEstado.select(
            LicitacionEstado.licitacion,
            LicitacionEstado.estado,
            LicitacionEstado.fecha
        ).join(
            estados_recientes,
            on=(LicitacionEstado.licitacion == estados_recientes.c.licitacion_id)
        ).alias('licitacion_estados')

        licitaciones = models_stats.LicitacionMaster.select(
            models_stats.LicitacionMaster.licitacion.alias('id'),
            models_stats.LicitacionMaster.licitacion_codigo.alias('codigo'),
            models_stats.LicitacionMaster.nombre.alias('nombre'),
            models_stats.LicitacionMaster.fecha_creacion,
            models_stats.LicitacionMaster.fecha_adjudicacion,
            licitacion_estados.c.estado.alias('estado')
        ).join(
            licitacion_estados,
            on=(models_stats.LicitacionMaster.licitacion == licitacion_estados.c.licitacion_id)
        ).where(
            models_stats.LicitacionMaster.catalogo_organismo == organismo_id
        ).order_by(
            models_stats.LicitacionMaster.fecha_creacion.desc()
        )

        p_licitaciones = req.params.get('pagina', None)
        if p_licitaciones:
            p_licitaciones = max(int(p_licitaciones) if p_licitaciones.isdigit() else 1, 1)
            licitaciones = licitaciones.paginate(p_licitaciones, 10)

        response = {
            'licitaciones' : [licitacion for licitacion in licitaciones.dicts()],
            'n_licitaciones': licitaciones.count()
        }

        resp.body = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)


class OrganismoList(object):

    ALLOWED_PARAMS = ['q']
    MAX_RESULTS = 10

    @database.atomic()
    def on_get(self, req, resp):

        # Get all organismos
        organismos = models_api.ProveedorOrganismoCruce.select(
            models_api.ProveedorOrganismoCruce.organismo,
            models_api.ProveedorOrganismoCruce.nombre_ministerio,
            models_api.ProveedorOrganismoCruce.nombre_organismo
        )

        filters = []

        q_q = req.params.get('q', None)
        if q_q:
            filters.append(ts_match(models_api.ProveedorOrganismoCruce.nombre_ministerio, q_q) | ts_match(models_api.ProveedorOrganismoCruce.nombre_organismo, q_q))

        # Search by fecha_adjudicacion
        q_fecha_adjudicacion = req.params.get('fecha_adjudicacion', None)
        if q_fecha_adjudicacion:
            q_fecha_adjudicacion = q_fecha_adjudicacion.split('|')
            try:
                fecha_adjudicacion_min = dateutil.parser.parse(q_fecha_adjudicacion[0], dayfirst=True, yearfirst=True) if q_fecha_adjudicacion[0] else date(0, 1, 1)
                fecha_adjudicacion_max = dateutil.parser.parse(q_fecha_adjudicacion[1], dayfirst=True, yearfirst=True) if q_fecha_adjudicacion[1] else date(3000, 12, 31)
            except IndexError:
                raise falcon.HTTPBadRequest("Wrong fecha_adjudicacion", "Dates must be separated by a pipe [|]")
            except ValueError:
                raise falcon.HTTPBadRequest("Wrong fecha_adjudicacion", "Dates must be a datetime in ISO8601 format")

            if fecha_adjudicacion_min:
                filters.append(models_api.ProveedorOrganismoCruce.fecha_adjudicacion >= fecha_adjudicacion_min)
            if fecha_adjudicacion_max:
                filters.append(models_api.ProveedorOrganismoCruce.fecha_adjudicacion <= fecha_adjudicacion_max)

        # Search by proveedor_adjudicado
        q_proveedor_adjudicado = req.params.get('proveedor_adjudicado', None)
        if q_proveedor_adjudicado:
            if not q_proveedor_adjudicado.isdigit():
                raise falcon.HTTPBadRequest("Wrong proveedor_adjudicado", "proveedor_adjudicado must be an integer")
            q_proveedor_adjudicado = int(q_proveedor_adjudicado)

            filters.append(models_api.ProveedorOrganismoCruce.empresa == q_proveedor_adjudicado)

        # Search by n_licitaciones_adjudicadas
        q_n_licitaciones_adjudicadas = req.params.get('n_licitaciones_adjudicadas')
        if q_n_licitaciones_adjudicadas:
            # Sanitize parameters
            q_n_licitaciones_adjudicadas = q_n_licitaciones_adjudicadas.split('|')
            try:
                n_licitaciones_adjudicadas_min = int(q_n_licitaciones_adjudicadas[0]) if q_n_licitaciones_adjudicadas[0] and q_n_licitaciones_adjudicadas[0].isdigit() else 0
                n_licitaciones_adjudicadas_max = int(q_n_licitaciones_adjudicadas[1]) if q_n_licitaciones_adjudicadas[1] and q_n_licitaciones_adjudicadas[1].isdigit() else sys.maxint
            except IndexError:
                raise falcon.HTTPBadRequest("Wrong n_licitaciones_adjudicadas", "Numbers must be separated by a pipe [|]")

            organismos_n_licitaciones = models_api.ProveedorOrganismoCruce.select(
                models_api.ProveedorOrganismoCruce.organismo,
                peewee.fn.count(peewee.SQL('DISTINCT licitacion_id'))
            ).where(
                models_api.ProveedorOrganismoCruce.fecha_adjudicacion >= fecha_adjudicacion_min if 'fecha_adjudicacion_min' in locals() else True,
                models_api.ProveedorOrganismoCruce.fecha_adjudicacion <= fecha_adjudicacion_max if 'fecha_adjudicacion_max' in locals() else True,
                models_api.ProveedorOrganismoCruce.empresa == q_proveedor_adjudicado if q_proveedor_adjudicado else True
            ).group_by(
                models_api.ProveedorOrganismoCruce.organismo
            ).having(
                peewee.fn.count(peewee.SQL('DISTINCT licitacion_id')) >= n_licitaciones_adjudicadas_min,
                peewee.fn.count(peewee.SQL('DISTINCT licitacion_id')) <= n_licitaciones_adjudicadas_max
            )

            organismos_ids = [organismo_n_licitaciones['organismo'] for organismo_n_licitaciones in organismos_n_licitaciones.dicts()]


            filters.append(models_api.ProveedorOrganismoCruce.organismo << organismos_ids if organismos_ids else False)


        # Search by monto_adjudicado
        q_monto_adjudicado = req.params.get('monto_adjudicado')
        if q_monto_adjudicado:
            # Sanitize parameters
            q_monto_adjudicado = q_monto_adjudicado.split('|')
            try:
                monto_adjudicado_min = int(q_monto_adjudicado[0]) if q_monto_adjudicado[0] and q_monto_adjudicado[0].isdigit() else 0
                monto_adjudicado_max = int(q_monto_adjudicado[1]) if q_monto_adjudicado[1] and q_monto_adjudicado[1].isdigit() else sys.maxint
            except IndexError:
                raise falcon.HTTPBadRequest("Wrong monto_adjudicado", "Numbers must be separated by a pipe [|]")

            organismos_montos = models_api.ProveedorOrganismoCruce.select(
                models_api.ProveedorOrganismoCruce.organismo,
                peewee.fn.sum(models_api.ProveedorOrganismoCruce.monto_total)
            ).where(
                models_api.ProveedorOrganismoCruce.fecha_adjudicacion >= fecha_adjudicacion_min if 'fecha_adjudicacion_min' in locals() else True,
                models_api.ProveedorOrganismoCruce.fecha_adjudicacion <= fecha_adjudicacion_max if 'fecha_adjudicacion_max' in locals() else True,
                models_api.ProveedorOrganismoCruce.empresa == q_proveedor_adjudicado if q_proveedor_adjudicado else True
            ).group_by(
                models_api.ProveedorOrganismoCruce.organismo
            ).having(
                peewee.fn.sum(models_api.ProveedorOrganismoCruce.monto_total) >= monto_adjudicado_min,
                peewee.fn.sum(models_api.ProveedorOrganismoCruce.monto_total) <= monto_adjudicado_max
            )

            organismos_ids = [organismo_monto['organismo'] for organismo_monto in organismos_montos.dicts()]


            filters.append(models_api.ProveedorOrganismoCruce.organismo << organismos_ids if organismos_ids else False)



        if filters:
            organismos = organismos.where(*filters)

        organismos = organismos.distinct()

        # Get page
        q_page = req.params.get('pagina', None)
        if q_page:
            q_page = max(int(q_page) if q_page.isdigit() else 1, 1)
            organismos.paginate(q_page, 10)

        response = {
            'n_organismos': organismos.count(),
            'organismos': [
                {
                    'id': organismo['organismo'],
                    'categoria': organismo['nombre_ministerio'],
                    'nombre': organismo['nombre_organismo']
                }
            for organismo in organismos.dicts()]
        }

        resp.body = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)


class OrganismoCategoriaList(object):

    @database.atomic()
    def on_get(self, req, resp, organismo_id):

        # Get all categories

        categorias = models_old.CategoriaProucto1.select()

        # Get page
        q_page = req.params.get('pagina', None)
        if q_page:
            q_page = max(int(q_page) if q_page.isdigit() else 1, 1)
            categorias.paginate(q_page, 10)

        response = {
            'n_organismos': categorias.count(),
            'organismos': [organismo for organismo in categorias.dicts()]
        }

        resp.body = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)
