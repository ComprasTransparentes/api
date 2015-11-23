import sys
import json

import falcon
import peewee
import dateutil

from datetime import date

from playhouse.shortcuts import model_to_dict

from models.models_bkn import *
from models import models_api
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
            'n_licitaciones': licitaciones.count()
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
        proveedores = models_api.ProveedorOrganismoCruce.select(
            models_api.ProveedorOrganismoCruce.empresa,
            models_api.ProveedorOrganismoCruce.nombre_empresa,
            models_api.ProveedorOrganismoCruce.rut_sucursal
        )

        filters = []


        # Search by text
        q_q = req.params.get('q', None)
        if q_q:
            # TODO Try to make just one query over one index instead of two or more ORed queries
            filters.append(ts_match(models_api.ProveedorOrganismoCruce.nombre_empresa, q_q) | ts_match(models_api.ProveedorOrganismoCruce.rut_sucursal, q_q))

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

        # Search by organismo_adjudicador
        q_organismo_adjudicador = req.params.get('organismo_adjudicador', None)
        if q_organismo_adjudicador:
            if not q_organismo_adjudicador.isdigit():
                raise falcon.HTTPBadRequest("Wrong organismo_adjudicador", "organismo_adjudicador must be an integer")
            q_organismo_adjudicador = int(q_organismo_adjudicador)

            filters.append(models_api.ProveedorOrganismoCruce.organismo == q_organismo_adjudicador)

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

            proveedores_n_licitaciones = models_api.ProveedorOrganismoCruce.select(
                models_api.ProveedorOrganismoCruce.empresa,
                peewee.fn.count(peewee.SQL('DISTINCT licitacion_id'))
            ).where(
                models_api.ProveedorOrganismoCruce.fecha_adjudicacion >= fecha_adjudicacion_min if 'fecha_adjudicacion_min' in locals() else True,
                models_api.ProveedorOrganismoCruce.fecha_adjudicacion <= fecha_adjudicacion_max if 'fecha_adjudicacion_max' in locals() else True,
                models_api.ProveedorOrganismoCruce.organismo == q_organismo_adjudicador if q_organismo_adjudicador else True
            ).group_by(
                models_api.ProveedorOrganismoCruce.empresa
            ).having(
                peewee.fn.count(peewee.SQL('DISTINCT licitacion_id')) >= n_licitaciones_adjudicadas_min,
                peewee.fn.count(peewee.SQL('DISTINCT licitacion_id')) <= n_licitaciones_adjudicadas_max
            )

            empresas = [proveedor_n_licitaciones['empresa'] for proveedor_n_licitaciones in proveedores_n_licitaciones.dicts()]


            filters.append(models_api.ProveedorOrganismoCruce.empresa << empresas if empresas else False)


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

            proveedores_monto = models_api.ProveedorOrganismoCruce.select(
                models_api.ProveedorOrganismoCruce.empresa,
                peewee.fn.sum(models_api.ProveedorOrganismoCruce.monto_total)
            ).where(
                models_api.ProveedorOrganismoCruce.fecha_adjudicacion >= fecha_adjudicacion_min if 'fecha_adjudicacion_min' in locals() else True,
                models_api.ProveedorOrganismoCruce.fecha_adjudicacion <= fecha_adjudicacion_max if 'fecha_adjudicacion_max' in locals() else True,
                models_api.ProveedorOrganismoCruce.organismo == q_organismo_adjudicador if q_organismo_adjudicador else True
            ).group_by(
                models_api.ProveedorOrganismoCruce.empresa
            ).having(
                peewee.fn.sum(models_api.ProveedorOrganismoCruce.monto_total) >= monto_adjudicado_min,
                peewee.fn.sum(models_api.ProveedorOrganismoCruce.monto_total) <= monto_adjudicado_max
            )

            empresas = [proveedor_monto['empresa'] for proveedor_monto in proveedores_monto.dicts()]


            filters.append(models_api.ProveedorOrganismoCruce.empresa << empresas if empresas else False)


        if filters:
            proveedores = proveedores.where(*filters)

        proveedores = proveedores.distinct()

        # Get page
        q_page = req.params.get('pagina', None)
        if q_page:
            q_page = max(int(q_page) if q_page.isdigit() else 1, 1)
            proveedores.paginate(q_page, 10)

        print proveedores.sql()

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

        resp.body = json.dumps(response, cls=JSONEncoderPlus)
