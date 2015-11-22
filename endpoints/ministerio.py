import json

import falcon

from playhouse.shortcuts import model_to_dict

from utils.myjson import JSONEncoderPlus

from models import models as models_old
from models.models_stats import *



class MinisterioStatsItem(object):

    @database.atomic()
    def on_get(self, req, resp, ministerio_id, categoria_id):

        if not ministerio_id.isdigit() or not categoria_id.isdigit():
            raise falcon.HTTPBadRequest("", "")

        try:
            stats = MinisterioProductoStats.get(MinisterioProductoStats.ministerio == ministerio_id, MinisterioProductoStats.categoria == categoria_id)
        except MinisterioProductoStats.DoesNotExist:
            stats = None

        if stats:
            response = model_to_dict(stats)
        else:
            try:
                response = {
                    "categoria": int(categoria_id),
                    "categoria_nombre": models_old.CategoriaProucto.get(models_old.CategoriaProucto.id == categoria_id).categoria_3,
                    "ministerio": int(ministerio_id),
                    "ministerio_nombre": models_old.Ministerio.get(models_old.Ministerio.id == ministerio_id).nombre,
                    "monto_promedio": 0,
                    "monto_total": 0,
                    "n_licitaciones_adjudicadas": 0,
                    "n_proveedores": 0
                }
            except models_old.CategoriaProucto.DoesNotExist as e:
                raise falcon.HTTPNotFound()
            except models_old.Ministerio.DoesNotExist as e:
                raise falcon.HTTPNotFound()
            except:
                raise falcon.HTTPBadRequest("", "")

        resp.body = json.dumps(response, cls=JSONEncoderPlus, sort_keys=True)
