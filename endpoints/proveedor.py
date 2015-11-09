import json

import falcon

from playhouse.shortcuts import model_to_dict

from models.models_bkn import *
from models import models_stats
from utils.myjson import JSONEncoderPlus
from utils.mypeewee import ts_match


class ProveedorItem(object):

    @database.atomic()
    def on_get(self, req, resp, proveedor_id):

        # Get the proveedor
        try:
            proveedor = Proveedor.get(Proveedor.id == proveedor_id)
        except Proveedor.DoesNotExist:
            raise falcon.HTTPNotFound()

        response = model_to_dict(proveedor, backrefs=False)

        monto_adjudicado = models_stats.MasterPlop.select(
            models_stats.MasterPlop.company,
            fn.sum(models_stats.MasterPlop.monto).alias('monto')
        ).group_by(
            models_stats.MasterPlop.company
        ).where(
            models_stats.MasterPlop.company == proveedor_id
        ).first().monto

        licitacion_monto_adjudicado = models_stats.MasterPlop.select(
            models_stats.MasterPlop.licitacion.alias('id'),
            fn.sum(models_stats.MasterPlop.monto).alias('monto_adjudicado')
        ).group_by(
            SQL('id')
        ).where(
            models_stats.MasterPlop.company == proveedor_id
        ).alias('monto_adjudicado')

        licitacion_monto_total = models_stats.MasterPlop.select(
            models_stats.MasterPlop.licitacion.alias('id'),
            fn.sum(models_stats.MasterPlop.monto).alias('monto_total')
        ).group_by(
            SQL('id')
        ).alias('monto_total')

        licitaciones = models_stats.LicitacionMaster.select(
            models_stats.LicitacionMaster.licitacion.alias('id'),
            models_stats.LicitacionMaster.nombre,
            models_stats.LicitacionMaster.licitacion_codigo.alias('codigo'),
            licitacion_monto_adjudicado.c.monto_adjudicado,
            licitacion_monto_total.c.monto_total
        ).join(
            licitacion_monto_adjudicado,
            on=(models_stats.LicitacionMaster.licitacion == licitacion_monto_adjudicado.c.id)
        ).join(
            licitacion_monto_total,
            on=(models_stats.LicitacionMaster.licitacion == licitacion_monto_total.c.id)
        )

        top_licitaciones = licitaciones.order_by(
            licitacion_monto_adjudicado.c.monto_adjudicado.desc()
        ).limit(10)

        licitaciones = licitaciones.order_by(
            models_stats.LicitacionMaster.fecha_creacion.desc()
        )

        p_licitaciones = req.params.get('p_licitaciones', '1')
        p_licitaciones = max(int(p_licitaciones) if p_licitaciones.isdigit() else 1, 1)

        response['extra'] = {
            'monto_adjudicado': monto_adjudicado,
            'top_licitaciones': [licitacion for licitacion in top_licitaciones.dicts()],
            'n_licitaciones': licitaciones.count(),
            'licitaciones': [licitacion for licitacion in licitaciones.paginate(p_licitaciones, 10).dicts()]
        }

        response = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)

        callback = req.params.get('callback', None)
        if callback:
            response = "%s(%s)" % (callback, response)

        resp.body = response


class ProveedorEmbed(object):

    @database.atomic()
    def on_get(self, req, resp, proveedor_id=None):

        # Get the proveedor
        try:
            proveedor = Proveedor.get(Proveedor.id == proveedor_id)
        except Proveedor.DoesNotExist:
            raise falcon.HTTPNotFound()

        response = model_to_dict(proveedor, backrefs=False)

        monto_adjudicado = models_stats.MasterPlop.select(
            models_stats.MasterPlop.company,
            fn.sum(models_stats.MasterPlop.monto).alias('monto')
        ).group_by(
            models_stats.MasterPlop.company
        ).where(
            models_stats.MasterPlop.company == proveedor_id
        ).first().monto

        licitaciones = models_stats.MasterPlop.select(
            models_stats.MasterPlop.licitacion
        ).where(
            models_stats.MasterPlop.company == proveedor_id
        ).distinct()

        response['extra'] = {
            'monto_adjudicado': monto_adjudicado,
            'n_licitaciones': licitaciones.count(),
        }

        response = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)

        callback = req.params.get('callback', None)
        if callback:
            response = "%s(%s)" % (callback, response)

        resp.body = response


class ProveedorLicitacion(object):

    @database.atomic()
    def on_get(self, req, resp, proveedor_id):

        licitacion_monto_adjudicado = models_stats.MasterPlop.select(
            models_stats.MasterPlop.licitacion.alias('id'),
            fn.sum(models_stats.MasterPlop.monto).alias('monto_adjudicado')
        ).group_by(
            SQL('id')
        ).where(
            models_stats.MasterPlop.company == proveedor_id
        ).alias('monto_adjudicado')

        licitacion_monto_total = models_stats.MasterPlop.select(
            models_stats.MasterPlop.licitacion.alias('id'),
            fn.sum(models_stats.MasterPlop.monto).alias('monto_total')
        ).group_by(
            SQL('id')
        ).alias('monto_total')

        licitaciones = models_stats.LicitacionMaster.select(
            models_stats.LicitacionMaster.licitacion.alias('id'),
            models_stats.LicitacionMaster.nombre,
            models_stats.LicitacionMaster.licitacion_codigo.alias('codigo'),
            licitacion_monto_adjudicado.c.monto_adjudicado,
            licitacion_monto_total.c.monto_total
        ).join(
            licitacion_monto_adjudicado,
            on=(models_stats.LicitacionMaster.licitacion == licitacion_monto_adjudicado.c.id)
        ).join(
            licitacion_monto_total,
            on=(models_stats.LicitacionMaster.licitacion == licitacion_monto_total.c.id)
        )

        licitaciones = licitaciones.order_by(
            models_stats.LicitacionMaster.fecha_creacion.desc()
        )


        p_licitaciones = req.params.get('pagina', None)

        if p_licitaciones:
            p_licitaciones = max(int(p_licitaciones) if p_licitaciones.isdigit() else 1, 1)

            licitaciones = licitaciones.paginate(p_licitaciones, 10)

        response = {
            'licitaciones': [licitacion for licitacion in licitaciones.dicts()],
            'n_licitaciones': licitaciones.count(),
        }

        resp.body = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)


class ProveedorList(object):

    ALLOWED_PARAMS = ['q']
    MAX_RESULTS = 10

    @database.atomic()
    def on_get(self, req, resp):

        # Get all proveedores
        proveedores = Proveedor.select().order_by(Proveedor.nombre.desc(), Proveedor.rut.desc())

        # Get page
        q_page = req.params.get('pagina', '1')
        q_page = max(int(q_page) if q_page.isdigit() else 1, 1)

        q_q = req.params.get('q', None)
        if q_q:
            # TODO Try to make just one query over one index instead of two or more ORed queries
            proveedores = proveedores.where(ts_match(Proveedor.nombre, q_q) | ts_match(Proveedor.rut, q_q))

        response = {
            'n_proveedores': proveedores.count(),
            'proveedores': [proveedor for proveedor in proveedores.paginate(q_page, ProveedorList.MAX_RESULTS).dicts()]
        }

        resp.body = json.dumps(response, cls=JSONEncoderPlus)
