import json

import falcon

from models import models_api
from utils.myjson import JSONEncoderPlus


class Producto(object):

    @models_api.database.atomic()
    def on_get(self, req, resp):

        productos = models_api.Catnivel3.select().where(
            models_api.Catnivel3.categoria_nivel3.is_null(False)
        )

        response = {
            'n_productos': productos.count(),
            'productos': [
                {
                    'id': producto['id_categoria_nivel3'],
                    'nombre': producto['categoria_nivel3']
                }
            for producto in productos.dicts().iterator()]
        }

        resp.body = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)


class ProductoCategoria(object):

    @models_api.database.atomic()
    def on_get(self, req, resp):

        categorias = models_api.Catnivel1.select().where(
            models_api.Catnivel1.categoria_nivel1.is_null(False)
        )

        response = {
            'n_categorias': categorias.count(),
            'categorias': [
                {
                    'id': categoria['id_categoria_nivel1'],
                    'nombre': categoria['categoria_nivel1']
                }
            for categoria in categorias.dicts().iterator()]
        }

        resp.body = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)
