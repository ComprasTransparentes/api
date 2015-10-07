from io import BytesIO
import json

import falcon
import unicodecsv as csv

from playhouse.shortcuts import model_to_dict, cast

from models.models_stats import *
from models import models as models_old
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
            # for go in gasto_organismo_region_anos.tuples():
            #     csvwriter.writerow(go)

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

        elif datatype in ['proveedor', 'proveedores']:

            top_proveedores = ProveedorMonto.select(
                models_bkn.Proveedor.id,
                models_bkn.Proveedor.nombre,
                models_bkn.Proveedor.rut,
                cast(ProveedorMonto.monto, 'bigint').alias('monto')
            ).join(
                models_bkn.Proveedor,
                on=(ProveedorMonto.company == models_bkn.Proveedor.id)
            ).order_by(
                ProveedorMonto.monto.desc()
            ).limit(10)

            response = {
                'top_proveedores': [proveedor for proveedor in top_proveedores.dicts()]
            }

            resp.body = json.dumps(response, cls=JSONEncoderPlus)

        elif datatype in ['categoria', 'categorias']:

            top_categorias = CategoriaMonto.select(
                CategoriaMonto.categoria_tercer_nivel.alias('categoria'),
                cast(CategoriaMonto.monto, 'bigint')
            ).order_by(
                CategoriaMonto.monto.desc()
            ).limit(5)

            response = {
                'top_categorias': [categoria for categoria in top_categorias.dicts()]
            }

            resp.body = json.dumps(response, cls=JSONEncoderPlus)