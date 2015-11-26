import json

import falcon
import peewee

from models import models_api
from utils.myjson import JSONEncoderPlus


class MinisterioId(object):

    @models_api.database.atomic()
    def on_get(self, req, resp, ministerio_id):

        try:
            ministerio_id = int(ministerio_id)
        except ValueError:
            raise falcon.HTTPNotFound()

        try:
            ministerio = models_api.Comparador.select(
                models_api.Comparador.id_ministerio,
                models_api.Comparador.nombre_ministerio
            ).where(
                models_api.Comparador.id_ministerio == ministerio_id
            ).get()
        except models_api.Comparador.DoesNotExist:
            raise falcon.HTTPNotFound()

        response = {
            'id': ministerio.id_ministerio,
            'nombre': ministerio.nombre_ministerio
        }

        resp.body = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)


class Ministerio(object):

    @models_api.database.atomic()
    def on_get(self, req, resp):

        ministerios = models_api.Comparador.select(
            models_api.Comparador.id_ministerio,
            models_api.Comparador.nombre_ministerio
        ).distinct().order_by(
            models_api.Comparador.id_ministerio
        )

        response = {
            'n_ministerios': ministerios.count(),
            'ministerios': [
                {
                    'id': ministerio['id_ministerio'],
                    'nombre': ministerio['nombre_ministerio']
                }
            for ministerio in ministerios.dicts().iterator()]
        }

        resp.body = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)


class MinisterioCategoria(object):

    @models_api.database.atomic()
    def on_get(self, req, resp):

        filters = []

        q_ministerio = req.params.get('ministerio', [])
        if q_ministerio:
            if isinstance(q_ministerio, basestring):
                q_ministerio = [q_ministerio]
            try:
                q_ministerio = map(lambda x: int(x), q_ministerio)
            except ValueError:
                raise falcon.HTTPBadRequest("Parametro incorrecto", "ministerio debe ser un entero")

            filters.extend([models_api.Comparador.id_ministerio << q_ministerio])

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

        print categorias.sql()[0] % tuple(categorias.sql()[1])

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


class MinisterioIdCategoria(object):

    @models_api.database.atomic()
    def on_get(self, req, resp, ministerio_id):

        categorias = models_api.Comparador.select(
            models_api.Comparador.id_categoria_nivel1,
            models_api.Comparador.categoria_nivel1
        ).where(
            models_api.Comparador.id_ministerio == ministerio_id,
            models_api.Comparador.categoria_nivel1.is_null(False)
        ).order_by(
            models_api.Comparador.id_categoria_nivel1
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


class MinisterioIdCategoriaIdStats(object):

    @models_api.database.atomic()
    def on_get(self, req, resp, ministerio_id, categoria_id):

        try:
            ministerio_id = int(ministerio_id)
            categoria_id = int(categoria_id)
        except ValueError:
            raise falcon.HTTPNotFound()

        try:
            stats = models_api.Comparador.get(
                models_api.Comparador.id_ministerio == ministerio_id,
                models_api.Comparador.id_categoria_nivel1 == categoria_id
            )
        except  models_api.Comparador.DoesNotExist:
            stats = None

        if stats:
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
            # try:
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
            # except models_api.Comparador.DoesNotExist as e:
            #     raise falcon.HTTPNotFound()
            # except:
            #     raise falcon.HTTPBadRequest("", "")

        resp.body = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)
