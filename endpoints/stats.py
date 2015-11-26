from io import BytesIO
import json

import falcon
import peewee
import unicodecsv as csv

from playhouse.shortcuts import model_to_dict, cast

from models import models_api
from models import models_stats
from utils.myjson import JSONEncoderPlus


class StatsItem(object):

    @models_stats.database.atomic()
    def on_get(self, req, resp, datatype=None):

        if datatype == '0':

            stats = models_stats.Sumario.select().first()

            response = model_to_dict(stats)

            resp.body = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)

        elif datatype == '1':

            gasto_organismos = models_stats.MinisterioOrganismoMonto.select(
                models_stats.MinisterioOrganismoMonto.nombre_ministerio.concat('-').concat(models_stats.MinisterioOrganismoMonto.nombre_organismo).alias('nombre'),
                cast(models_stats.MinisterioOrganismoMonto.monto, 'bigint').alias('monto')
            ).order_by(
                peewee.SQL('nombre')
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

    @models_api.database.atomic()
    def on_get(self, req, resp, datatype=None):

        if datatype in ['licitacion', 'licitaciones']:

            licitaciones = models_api.LicitacionesCategorias.select().order_by(models_api.LicitacionesCategorias.monto.desc())

            response = {
                'licitaciones': [
                    {
                        'id': licitacion['licitacion'],
                        'codigo': licitacion['codigo_licitacion'],
                        'nombre': licitacion['nombre_licitacion'],
                        'monto': int(licitacion['monto'])
                    }
                for licitacion in licitaciones.dicts().iterator()]
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
                        'nombre': proveedor['nombre_empresa'],
                        'rut': proveedor['rut_sucursal'],
                        'monto': int(proveedor['monto'])
                    }
                for proveedor in proveedores.dicts().iterator()]
            }

            resp.body = json.dumps(response, cls=JSONEncoderPlus)

        elif datatype in ['categoria', 'categorias']:

            categorias = models_api.RankingCategorias.select().order_by(models_api.RankingCategorias.monto.desc())

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
