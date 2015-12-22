import json

import falcon

from models import models_api
from utils.myjson import JSONEncoderPlus


class Producto(object):
    """Endpoint para productos"""

    @models_api.database.atomic()
    def on_get(self, req, resp):
        """Obtiene los productos existentes

        :param req: Falcon request object
        :param resp: Falcon response object
        :return:
        """

        # Obtener productos
        productos = models_api.Catnivel3.select().where(
            models_api.Catnivel3.categoria_nivel3.is_null(False)
        )

        # Construir la respuesta
        response = {
            'n_productos': productos.count(),
            'productos': [
                {
                    'id': producto['id_categoria_nivel3'],
                    'nombre': producto['categoria_nivel3']
                }
            for producto in productos.dicts().iterator()]
        }

        # Codificar la respuesta en JSON
        resp.body = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)


class ProductoCategoria(object):
    """Endpoint para categorias de productos"""

    @models_api.database.atomic()
    def on_get(self, req, resp):
        """Obtiene las categorias de los productos existentes

        :param req: Falcon request object
        :param resp: Falcon response object
        :return:
        """

        # Obtener categorias
        categorias = models_api.Catnivel1.select().where(
            models_api.Catnivel1.categoria_nivel1.is_null(False)
        )

        # Construir la respuesta
        response = {
            'n_categorias': categorias.count(),
            'categorias': [
                {
                    'id': categoria['id_categoria_nivel1'],
                    'nombre': categoria['categoria_nivel1']
                }
            for categoria in categorias.dicts().iterator()]
        }

        # Codificar la respuesta en JSON
        resp.body = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)
