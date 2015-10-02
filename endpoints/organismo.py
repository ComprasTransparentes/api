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

            organismo = models_old.Jerarquia.select(
                models_old.Jerarquia.id,
                models_old.Jerarquia.nombre_ministerio.alias('categoria'),
                models_old.Jerarquia.codigo_organismo.alias('codigo_comprador'),
                models_old.Jerarquia.nombre_organismo.alias('nombre_comprador'),
            ).where(models_old.Jerarquia.id == organismo_id).first()

        except Comprador.DoesNotExist:
            raise falcon.HTTPNotFound()

        response = {
            'id': organismo.id,
            'categoria': organismo.categoria,
            'codigo_comprador': organismo.codigo_comprador,
            'nombre_comprador': organismo.nombre_comprador
        }

        # Top licitaciones adjudicadas
        top_licitaciones_global = models_stats.MasterPlop.select(
            models_stats.MasterPlop.licitacion.alias('id'),
            fn.sum(models_stats.MasterPlop.monto).alias('monto'),
        ).group_by(
            models_stats.MasterPlop.licitacion
        ).order_by(
            SQL('monto').desc()
        ).alias('top_licitaciones_global')

        top_licitaciones = models_stats.LicitacionMaster.select(
            top_licitaciones_global.c.id,
            models_stats.LicitacionMaster.licitacion_codigo.alias('codigo'),
            models_stats.LicitacionMaster.nombre,
            top_licitaciones_global.c.monto
        ).join(
            top_licitaciones_global,
            on=(models_stats.LicitacionMaster.licitacion == top_licitaciones_global.c.id)
        ).where(
            models_stats.LicitacionMaster.idorganismo == organismo_id
        ).order_by(
            SQL('monto').desc()
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
            models_stats.LicitacionMaster.idorganismo == organismo_id
        ).order_by(
            models_stats.LicitacionMaster.fecha_creacion.desc()
        )

        licitaciones_adjudicadas = licitaciones.where(
            SQL('estado') == 8
        )

        p_licitaciones = req.params.get('p_items', '1')
        p_licitaciones = max(int(p_licitaciones) if p_licitaciones.isdigit() else 1, 1)

        response['extra'] = {
            'top_licitaciones': [licitacion for licitacion in top_licitaciones.paginate(1, 10).dicts()],
            'top_proveedores': [proveedor for proveedor in top_proveedores.dicts()],
            'licitaciones': [licitacion for licitacion in licitaciones.paginate(p_licitaciones, 10).dicts()],
            'n_licitaciones': licitaciones.count(),
            'n_licitaciones_adjudicadas': licitaciones_adjudicadas.count(),
            'monto_adjudicado': int(top_licitaciones.select(fn.sum(SQL('monto')).alias('monto')).first().monto),


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


class OrganismoList(object):

    ALLOWED_PARAMS = ['q']
    MAX_RESULTS = 10

    @database.atomic()
    def on_get(self, req, resp):

        # Get all organismos
        organismos = models_old.Jerarquia.select(
            models_old.Jerarquia.id,
            models_old.Jerarquia.codigo_organismo.alias('codigo'),
            models_old.Jerarquia.nombre_ministerio.alias('categoria'),
            models_old.Jerarquia.nombre_organismo.alias('nombre'),
        ).distinct()

        # Get page
        q_page = req.params.get('pagina', '1')
        q_page = max(int(q_page) if q_page.isdigit() else 1, 1)

        q_q = req.params.get('q', None)
        if q_q:
            organismos = organismos.where(ts_match(models_old.Jerarquia.nombre_organismo, q_q) | ts_match(models_old.Jerarquia.nombre_ministerio, q_q))


        response = {
            'n_organismos': organismos.count(),
            'organismos': [organismo for organismo in organismos.order_by(models_old.Jerarquia.nombre_organismo).paginate(q_page, OrganismoList.MAX_RESULTS).dicts()]
        }

        resp.body = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)
