from io import BytesIO
import json

import falcon
import peewee
import unicodecsv as csv

from playhouse.shortcuts import model_to_dict, cast

from models import models_api
from utils.myjson import JSONEncoderPlus


class StatsItem(object):
    """Endpoint de estadisticas generales.
    Hay dos tipos de estadisticas que se usan para el sitio de comprastransparentes.cl

    /stats/0:
        Estadisticas generales del sitio
    /stats/1:
        Gastos por ministerio y organismos
    """

    @models_api.database.atomic()
    def on_get(self, req, resp, datatype=None):
        """Obtiene estadisticas

        :param req: Falcon request object
        :param resp: Falcon response object
        :param datatype: Tipo de estadisticas solicitadas
        :return:
        """

        if datatype == '0':

            stats = models_api.Sumario.select().first()

            response = model_to_dict(stats)

            resp.body = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)

        elif datatype == '1':

            gasto_organismos = models_api.MinisterioOrganismoMonto.select(
                models_api.MinisterioOrganismoMonto.nombre_ministerio.concat('-').concat(models_api.MinisterioOrganismoMonto.nombre_organismo).alias('nombre'),
                cast(models_api.MinisterioOrganismoMonto.monto, 'bigint').alias('monto')
            ).order_by(
                peewee.SQL('nombre')
            )

            # Presentar los resultados como un archivo CSV
            # Simular archivo con un archivo en memoria
            output = BytesIO()
            csvwriter = csv.writer(output, encoding='utf-8')
            # Escribir  archivo
            for go in gasto_organismos.tuples():
                csvwriter.writerow(go if len(go) == 4 else go+('null', 'null'))
            # Preparar archivo para descarga
            resp.content_type = 'text/csv'
            output.seek(0)
            resp.stream = output

        else:
            raise falcon.HTTPNotFound()


class StatsTop(object):
    """Endpoint para los top elementos del sistema"""

    @models_api.database.atomic()
    def on_get(self, req, resp, datatype=None):
        """Obtiene un ranking de objetos de cada tipo segun diferentes medidas

        licitaciones
            Licitaciones mas cuantiosas

        organismos
            Organismos que mas dinero han adjudicado

        proveedores
            Proveedores que mas dinero se han adjudicado

        categorias
            Productos con mayor monto adjudicado

        :param req: Falcon request object
        :param resp: Falcon response object
        :param datatype: Tipo de estadisticas solicitadas
        :return:
        """

        if datatype in ['licitacion', 'licitaciones']:
            # Obtener licitaciones
            licitaciones = models_api.LicitacionesCategorias.select().order_by(models_api.LicitacionesCategorias.monto.desc())
            # Construir la respuesta
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
            # Codificar la respuesta en JSON
            resp.body = json.dumps(response, cls=JSONEncoderPlus)

        elif datatype in ['organismo', 'organismos']:
            # Obtener organismos
            organismos = models_api.RankingOrganismos.select().order_by(models_api.RankingOrganismos.monto.desc())
            # Construir la respuesta
            response = {
                'organismos': [
                    {
                        'id': organismo['organismo'],
                        'nombre': organismo['nombre_organismo'],
                        'monto': int(organismo['monto'])
                    }
                for organismo in organismos.dicts().iterator()]
            }
            # Codificar la respuesta en JSON
            resp.body = json.dumps(response, cls=JSONEncoderPlus)

        elif datatype in ['proveedor', 'proveedores']:
            # Obtener proveedores
            proveedores = models_api.RankingProveedores.select().order_by(models_api.RankingProveedores.monto.desc())
            # Construir la respuesta
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
            # Codificar la respuesta en JSON
            resp.body = json.dumps(response, cls=JSONEncoderPlus)

        elif datatype in ['categoria', 'categorias']:
            # Obtener categorias
            categorias = models_api.RankingCategorias.select().order_by(models_api.RankingCategorias.monto.desc())
            # Construir la respuesta
            response = {
                'categorias': [
                    {
                        'id': categoria['id_categoria_nivel3'],
                        'nombre': categoria['categoria_nivel3'],
                        'monto': int(categoria['monto'])
                    }
                for categoria in categorias.dicts().iterator()]
            }
            # Codificar la respuesta en JSON
            resp.body = json.dumps(response, cls=JSONEncoderPlus)
