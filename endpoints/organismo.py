import json

import falcon

from peewee import fn, SQL
from playhouse.shortcuts import model_to_dict

from models import models as models_old
from models.models_bkn import *
from models import models_stats
from utils.myjson import JSONEncoderPlus
from utils.mypeewee import ts_match


class OrganismoItem(object):

    ALLOWED_FILTERS = ['producto']

    @database.atomic()
    def on_get(self, req, resp, organismo_id=None):

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

        # Estados
        estados_recientes = LicitacionEstado.select(
            LicitacionEstado.licitacion,
            fn.max(LicitacionEstado.fecha).alias('fecha')
        ).group_by(
            LicitacionEstado.licitacion
        ).alias('estados_recientes')

        licitacion_estados = LicitacionEstado.select(
            LicitacionEstado.licitacion,
            LicitacionEstado.estado,
            estados_recientes.c.fecha
        ).join(
            estados_recientes,
            on=((LicitacionEstado.licitacion == estados_recientes.c.licitacion_id) & (LicitacionEstado.fecha == estados_recientes.c.fecha))
        ).alias('licitacion_estados')

        top_licitaciones_organismo_global = models_stats.MasterPlop.select(
            models_stats.MasterPlop.licitacion.alias('id'),
            fn.sum(models_stats.MasterPlop.monto).alias('monto_global'),
        ).group_by(
            models_stats.MasterPlop.licitacion
        ).where(
            models_stats.MasterPlop.organismo == organismo_id
        ).alias('licitaciones_organismo_')

        top_licitaciones_organismo = models_stats.MasterPlop.select(
            models_stats.MasterPlop.licitacion.alias('id'),
            models_stats.MasterPlop.licitacion_codigo.alias('codigo'),
            models_stats.MasterPlop.licitacion_nombre.alias('nombre'),
            models_stats.MasterPlop.licitacion_descripcion.alias('descripcion'),
            models_stats.MasterPlop.fecha_creacion.alias('fecha_creacion'),
            top_licitaciones_organismo_global.c.monto_global.alias('monto')
        ).join(
            top_licitaciones_organismo_global,
            on=(models_stats.MasterPlop.licitacion == top_licitaciones_organismo_global.c.id)
        )

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
            licitacion_estados.c.estado.alias('estado')
        ).join(
            licitacion_estados,
            on=(models_stats.LicitacionMaster.licitacion == licitacion_estados.c.licitacion_id)
        ).where(
            models_stats.LicitacionMaster.catalogo_organismo == organismo_id
        ).order_by(
            models_stats.LicitacionMaster.fecha_creacion.desc()
        )

        p_licitaciones = req.params.get('pagina', '1')
        p_licitaciones = max(int(p_licitaciones) if p_licitaciones.isdigit() else 1, 1)

        response = {
            'licitaciones' : [licitacion for licitacion in licitaciones.paginate(p_licitaciones, 10).dicts()],
            'n_licitaciones': licitaciones.count()
        }

        resp.body = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)


class OrganismoList(object):

    ALLOWED_PARAMS = ['q']
    MAX_RESULTS = 10

    @database.atomic()
    def on_get(self, req, resp):

        # Get all organismos
        organismos = models_old.Jerarquia.select(
            models_old.Jerarquia.id,
            models_old.Jerarquia.organismo_codigo.alias('codigo'),
            models_old.Jerarquia.ministerio_nombre.alias('categoria'),
            models_old.Jerarquia.organismo_codigo.alias('nombre'),
        ).distinct()

        # Get page
        q_page = req.params.get('pagina', '1')
        q_page = max(int(q_page) if q_page.isdigit() else 1, 1)

        q_q = req.params.get('q', None)
        if q_q:
            organismos = organismos.where(ts_match(models_old.Jerarquia.organismo_codigo, q_q) | ts_match(models_old.Jerarquia.ministerio_nombre, q_q))


        response = {
            'n_organismos': organismos.count(),
            'organismos': [organismo for organismo in organismos.order_by(models_old.Jerarquia.organismo_codigo).paginate(q_page, OrganismoList.MAX_RESULTS).dicts()]
        }

        resp.body = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)
