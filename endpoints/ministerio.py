import json

import falcon
import peewee

from models import models_api
from utils.myjson import JSONEncoderPlus


class MinisterioId(object):
    """Endpoint para un ministerio en particular, identificado por id"""

    @models_api.database.atomic()
    def on_get(self, req, resp, ministerio_id):
        """Obtiene la informacion sobre un ministerio en particular

        :param req: Falcon request object
        :param resp: Falcon response object
        :param ministerio_id: ID de ministerio
        :return:
        """

        # Convertir ministerio_id a int
        try:
            ministerio_id = int(ministerio_id)
        except ValueError:
            raise falcon.HTTPNotFound()

        # Obtener el ministerio
        try:
            ministerio = models_api.Comparador.select(
                models_api.Comparador.id_ministerio,
                models_api.Comparador.nombre_ministerio
            ).where(
                models_api.Comparador.id_ministerio == ministerio_id
            ).get()
        except models_api.Comparador.DoesNotExist:
            raise falcon.HTTPNotFound()

        # Construir la respuesta
        response = {
            'id': ministerio.id_ministerio,
            'nombre': ministerio.nombre_ministerio
        }

        # Codificar la respuesta en JSON
        resp.body = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)


class Ministerio(object):
    """Endpoint para todos los ministerios"""

    @models_api.database.atomic()
    def on_get(self, req, resp):
        """Obtiene informacion de todos los ministerios.

        :param req: Falcon request object
        :param resp: Falcon response object
        """

        # Obtener todos los ministerios
        ministerios = models_api.Comparador.select(
            models_api.Comparador.id_ministerio,
            models_api.Comparador.nombre_ministerio
        ).distinct().order_by(
            models_api.Comparador.id_ministerio
        )

        # Construir respuesta
        response = {
            'n_ministerios': ministerios.count(),
            'ministerios': [
                {
                    'id': ministerio['id_ministerio'],
                    'nombre': ministerio['nombre_ministerio']
                }
            for ministerio in ministerios.dicts().iterator()]
        }

        # Codificar la respuesta en JSON
        resp.body = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)


class MinisterioCategoria(object):
    """Endpoint para las categorias de productos licitados por los ministerios"""

    @models_api.database.atomic()
    def on_get(self, req, resp):
        """Obtiene las categorias de productos licitados por los ministerios.
        Puede ser filtrado por ministerios con el parametro **minsterio**. Para filtrar por varios ministerios a la
        vez, se debe incluir el parametro **ministerio** varias veces.

        ***ministerio**     ID de ministerio oara filtrar

        :param req: Falcon request object
        :param resp: Falcon response object
        :return:
        """

        # Preparar la lista de filtros que se van a aplicar
        filters = []

        # Filtrar por ministerio
        q_ministerio = req.params.get('ministerio', [])
        if q_ministerio:
            if isinstance(q_ministerio, basestring):
                q_ministerio = [q_ministerio]
            try:
                q_ministerio = map(lambda x: int(x), q_ministerio)
            except ValueError:
                raise falcon.HTTPBadRequest("Parametro incorrecto", "ministerio debe ser un entero")

            filters.extend([models_api.Comparador.id_ministerio << q_ministerio])

        # Obtener las categorias
        categorias = models_api.Comparador.select(
            models_api.Comparador.id_categoria_nivel1,
            models_api.Comparador.categoria_nivel1,
            peewee.fn.count(models_api.Comparador.id_categoria_nivel1)
        ).where(
            models_api.Comparador.categoria_nivel1.is_null(False),
            *filters
        ).group_by(
            models_api.Comparador.id_categoria_nivel1,
            models_api.Comparador.categoria_nivel1
        ).having(
            peewee.fn.count(models_api.Comparador.id_categoria_nivel1) >= len(q_ministerio)
        ).order_by(
            models_api.Comparador.id_categoria_nivel1
        ).distinct()

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


class MinisterioIdCategoria(object):
    """Endpoint para las categorias de productos de un ministerio"""

    @models_api.database.atomic()
    def on_get(self, req, resp, ministerio_id):
        """Obtiene las categorias licitadas por un ministerio en particular.

        :param req: Falcon request object
        :param resp: Falcon response object
        :param ministerio_id: ID de ministerio
        :return:
        """

        # Obtener categorias del ministerio
        categorias = models_api.Comparador.select(
            models_api.Comparador.id_categoria_nivel1,
            models_api.Comparador.categoria_nivel1
        ).where(
            models_api.Comparador.id_ministerio == ministerio_id,
            models_api.Comparador.categoria_nivel1.is_null(False)
        ).order_by(
            models_api.Comparador.id_categoria_nivel1
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


class MinisterioIdCategoriaIdStats(object):
    """Endpoint de las estadisticas de las licitaciones emitidas por un ministerio en cierta categoria de producto"""

    @models_api.database.atomic()
    def on_get(self, req, resp, ministerio_id, categoria_id):
        """Obtiene las estadisticas de las licitaciones emitidas por el ministerio **ministerio_id** en la categoria
        de producto **categoria_id**

        :param req: Falcon request object
        :param resp: Falcon response object
        :param ministerio_id:   ID de minsiterio
        :param categoria_id:    ID de categoria de producto
        :return:
        """

        # Validar que ministerio_id y categoria_id son ints
        try:
            ministerio_id = int(ministerio_id)
            categoria_id = int(categoria_id)
        except ValueError:
            raise falcon.HTTPNotFound()

        # Obtener las estadisticas
        try:
            stats = models_api.Comparador.get(
                models_api.Comparador.id_ministerio == ministerio_id,
                models_api.Comparador.id_categoria_nivel1 == categoria_id
            )
        except  models_api.Comparador.DoesNotExist:
            stats = None

        # Constrir la respuesta
        if stats:
            # Si se obtuvo un resultado de la BD, rellenar la respuesta con esa informacion
            response = {
                'categoria': {
                    "id": stats.id_categoria_nivel1,
                    "nombre": stats.categoria_nivel1,
                },

                'ministerio': {
                    'id': stats.id_ministerio,
                    'nombre': stats.nombre_ministerio,
                },

                'monto_promedio': int(stats.monto_promedio),
                'monto_total': int(stats.monto),
                'n_licitaciones_adjudicadas': stats.licit_adjudicadas,
                'n_proveedores': stats.proveed_favorecidos
            }
        else:
            # Si no se obtuvo un resultado de la base de datos, rellenar la respuesta con valores neutros
            response = {
                'ministerio': {
                    'id': ministerio_id,
                    'nombre': models_api.MinisterioMr.get(models_api.MinisterioMr.id_ministerio == ministerio_id).nombre_ministerio,
                },

                'categoria': {
                    "id": categoria_id,
                    "nombre": models_api.Catnivel1.get(models_api.Catnivel1.id_categoria_nivel1 == categoria_id).categoria_nivel1,
                },

                'monto_promedio': 0,
                'monto_total': 0,
                'n_licitaciones_adjudicadas': 0,
                'n_proveedores': 0
            }

        # Codificar la respuesta en JSON
        resp.body = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)
