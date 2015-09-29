from io import BytesIO
import json

import falcon
import unicodecsv as csv

from playhouse.shortcuts import model_to_dict, cast

from models.models_stats import *
from models import models_bkn
from utils.myjson import JSONEncoderPlus


class StatsItem(object):

    @database.atomic()
    def on_get(self, req, resp, datatype=None):

        if datatype == '0':

            sumario = Sumario.select().first()

            resp.body = json.dumps(model_to_dict(sumario), cls=JSONEncoderPlus)

        if datatype == '1':

            gasto_organismos = GastoOrganismo.select(
                GastoOrganismo.nombre_categoria.concat('-').concat(GastoOrganismo.nombre_organismo).alias('nombre'),
                fn.sum(cast(GastoOrganismo.monto, 'bigint')).alias('monto')
            ).group_by(
                SQL('nombre')
            ).order_by(
                SQL('nombre')
            )

            #gasto_organismo_region_anos = GastoOrganismo.select().order_by(GastoOrganismo.nombre, GastoOrganismo.region, GastoOrganismo.ano)


            output = BytesIO()
            csvwriter = csv.writer(output, encoding='utf-8')

            for go in gasto_organismos.tuples():
                csvwriter.writerow(go if len(go) == 4 else go+('null', 'null'))
            # for go in gasto_organismo_region_anos.tuples():
            #     csvwriter.writerow(go)

            resp.content_type = 'text/csv'
            output.seek(0)
            resp.stream = output

        elif datatype == '2':

            organismos = GastoOrganismo.select(
                GastoOrganismo.nombre_categoria
            ).distinct().order_by(
                GastoOrganismo.nombre_categoria
            )

            anos = GastoOrganismo.select(
                GastoOrganismo.ano
            ).where(
                GastoOrganismo.ano != None
            ).distinct().order_by(
                GastoOrganismo.ano
            )

            gasto_organismos = GastoOrganismo.select(
                GastoOrganismo.nombre_categoria,
                GastoOrganismo.ano,
                fn.sum(cast(GastoOrganismo.monto, 'bigint')).alias('monto')
            ).where(
                GastoOrganismo.ano != None
            ).group_by(
                GastoOrganismo.nombre_categoria,
                GastoOrganismo.ano
            ).order_by(
                GastoOrganismo.nombre_categoria,
                GastoOrganismo.ano
            )

            d_anos = {}
            for index, ano in enumerate(anos.tuples()):
                d_anos[ano[0]] = index+1

            d_nombres = {}
            for o in organismos.tuples():
                d_nombres[o[0]] = [o[0]]+[0]*(len(d_anos))
            for go in gasto_organismos.tuples():
                d_nombres[go[0]][d_anos[go[1]]] = go[2]

            response = {
                'data': {
                    'columns': [v for k, v in iter(sorted(d_nombres.iteritems()))],
                    'type': 'bar',
                },
                'grid': {
                    'y': {
                        'lines': [{'value': 0}]
                    }
                },
                'bindto': '#graph2'
            }

            resp.body = json.dumps(response, cls=JSONEncoderPlus)


class StatsTop(object):

    @database.atomic()
    def on_get(self, req, resp, datatype=None):

        if datatype in ['licitacion', 'licitaciones']:
            top_licitaciones = TopLicitaciones.select(
                models_bkn.Licitacion.id,
                TopLicitaciones.codigo_licitacion.alias('codigo'),
                models_bkn.Licitacion.nombre,
                TopLicitaciones.monto,
            ).join(
                models_bkn.Licitacion,
                on=(TopLicitaciones.codigo_licitacion == models_bkn.Licitacion.codigo)
            ).order_by(
                TopLicitaciones.monto.desc()
            ).limit(5)

            response = {
                'top_licitaciones': [licitacion for licitacion in top_licitaciones.dicts()]
            }

            resp.body = json.dumps(response, cls=JSONEncoderPlus)

        elif datatype in ['proveedor', 'proveedores']:
            top_proveedores = VentasProveedor.select(
                models_bkn.Proveedor.id,
                models_bkn.Proveedor.nombre,
                models_bkn.Proveedor.rut,
                VentasProveedor.monto,
            ).join(
                models_bkn.Proveedor,
                on=(VentasProveedor.id_proveedor == models_bkn.Proveedor.id)
            ).order_by(
                VentasProveedor.monto.desc()
            ).limit(5)

            response = {
                'top_proveedores': [proveedor for proveedor in top_proveedores.dicts()]
            }

            resp.body = json.dumps(response, cls=JSONEncoderPlus)

        elif datatype in ['categoria', 'categorias']:
            top_categorias = TopCategorias.select(
                TopCategorias.categoria,
                TopCategorias.monto
            ).order_by(
                TopCategorias.monto.desc()
            ).limit(5)

            response = {
                'top_categorias': [categoria for categoria in top_categorias.dicts()]
            }

            resp.body = json.dumps(response, cls=JSONEncoderPlus)