from io import BytesIO
import json

import falcon
import unicodecsv as csv

from playhouse.shortcuts import model_to_dict, cast

from models import models_api
from models.models_stats import *
from models import models_bkn
from utils.myjson import JSONEncoderPlus


class StatsItem(object):

    @database.atomic()
    def on_get(self, req, resp, datatype=None):

        if datatype == '0':

            stats = Sumario.select().first()

            response = model_to_dict(stats)

            resp.body = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)

        elif datatype == '1':

            gasto_organismos = MinisterioOrganismoMonto.select(
                MinisterioOrganismoMonto.nombre_ministerio.concat('-').concat(MinisterioOrganismoMonto.nombre_organismo).alias('nombre'),
                cast(MinisterioOrganismoMonto.monto, 'bigint').alias('monto')
            ).order_by(
                SQL('nombre')
            )

            output = BytesIO()
            csvwriter = csv.writer(output, encoding='utf-8')

            for go in gasto_organismos.tuples():
                csvwriter.writerow(go if len(go) == 4 else go+('null', 'null'))

            resp.content_type = 'text/csv'
            output.seek(0)
            resp.stream = output

        else:
            raise falcon.HTTPNotFound()


class StatsTop(object):

    @database.atomic()
    def on_get(self, req, resp, datatype=None):



        if datatype in ['licitacion', 'licitaciones']:

            top_licitaciones = LicitacionMonto.select(
                models_bkn.Licitacion.id,
                models_bkn.Licitacion.codigo,
                models_bkn.Licitacion.nombre,
                cast(LicitacionMonto.monto, 'bigint')
            ).join(
                models_bkn.Licitacion,
                on=(LicitacionMonto.licitacion_codigo == models_bkn.Licitacion.codigo)
            ).order_by(
                LicitacionMonto.monto.desc()
            ).limit(5)

            response = {
                'top_licitaciones': [licitacion for licitacion in top_licitaciones.dicts()]
            }

            resp.body = json.dumps(response, cls=JSONEncoderPlus)

        elif datatype in ['organismo', 'organismos']:

            organismos = models_api.RankingOrganismos.select().order_by(models_api.RankingOrganismos.monto.desc())

            response = {
                'organismos': [
                    {
                        'id': organismo['organismo'],
                        'nombre': organismo['nombre_organismo'],
                        'monto': int(organismo['monto'])
                    }
                for organismo in organismos.dicts().iterator()]
            }

            resp.body = json.dumps(response, cls=JSONEncoderPlus)

        elif datatype in ['proveedor', 'proveedores']:

            proveedores = models_api.RankingProveedores.select().order_by(models_api.RankingProveedores.monto.desc())

            response = {
                'proveedores': [
                    {
                        'id': proveedor['empresa'],
                        'nombre': 'Proveedor',
                        'rut': proveedor['rut_sucursal'],
                        'monto': int(proveedor['monto'])
                    }
                for proveedor in proveedores.dicts().iterator()]
            }

            resp.body = json.dumps(response, cls=JSONEncoderPlus)

        elif datatype in ['categoria', 'categorias']:

            categorias = models_api.RankingCategorias.select().order_by(models_api.RankingCategorias.monto)

            response = {
                'categorias': [
                    {
                        'id': categoria['id_categoria_nivel3'],
                        'nombre': categoria['categoria_nivel3'],
                        'monto': int(categoria['monto'])
                    }
                for categoria in categorias.dicts().iterator()]
            }

            resp.body = json.dumps(response, cls=JSONEncoderPlus)
