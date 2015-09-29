import json

import falcon

from playhouse.shortcuts import model_to_dict

from models.models_bkn import *
from utils.myjson import JSONEncoderPlus
from utils.mypeewee import ts_match, ts_rank


class Search(object):

    ALLOWED_PARAMS = ['q']
    MAX_RESULTS = 10

    @database.atomic()
    def on_get(self, req, resp):

        # Get all licitaciones, compradores, proveedores
        licitaciones = Licitacion.select().order_by(-Licitacion.id)
        compradores = Comprador.select().order_by(-Comprador.id)
        proveedores = Proveedor.select().order_by(-Proveedor.id)

        # Get page
        q_page = req.params.get('pagina', '1')
        q_page = max(int(q_page), 1) if q_page.isdigit() else 1
        start = (q_page-1)*Search.MAX_RESULTS

        if any([q_param in Search.ALLOWED_PARAMS for q_param in req.params]):

            q_q = req.params.get('q', None)
            if q_q:
                # TODO Try to make just one query over one index instead of two or more ORed queries
                licitaciones = licitaciones.where(ts_match(Licitacion.nombre, q_q) | ts_match(Licitacion.descripcion, q_q))
                compradores = compradores.where(ts_match(Comprador.nombre_comprador, q_q) | ts_match(Comprador.nombre_unidad, q_q))
                proveedores = proveedores.where(ts_match(Proveedor.nombre, q_q) | ts_match(Proveedor.rut, q_q))

        else:
            title = "Missing query parameter"
            description = "Query parameter options are %s" % Search.ALLOWED_PARAMS
            raise falcon.HTTPBadRequest(title, description)

        response = {
            'licitaciones': [
                model_to_dict(licitacion, backrefs=False) for licitacion in licitaciones[start:start+10]
            ],
            'compradores': [
                model_to_dict(comprador, backrefs=False) for comprador in compradores[start:start+10]
            ],
            'proveedores': [
                model_to_dict(proveedor, backrefs=False) for proveedor in proveedores[start:start+10]
            ]
        }

        resp.body = json.dumps(response, cls=JSONEncoderPlus)

class SearchBeta(object):

    ALLOWED_PARAMS = ['q']
    MAX_RESULTS = 10

    @database.atomic()
    def on_get(self, req, resp):

        # Get all licitaciones, compradores, proveedores
        licitaciones = Licitacion.select().order_by(-Licitacion.id)
        compradores = Comprador.select().order_by(-Comprador.id)
        proveedores = Proveedor.select().order_by(-Proveedor.id)

        # Get page
        q_page = req.params.get('pagina', '1')
        q_page = max(int(q_page), 1) if q_page.isdigit() else 1
        start = (q_page-1)*Search.MAX_RESULTS

        if any([q_param in Search.ALLOWED_PARAMS for q_param in req.params]):

            q_q = req.params.get('q', None)
            if q_q:
                # TODO Try to make just one query over one index instead of two or more ORed queries
                licitaciones = licitaciones.where(ts_match(Licitacion.nombre, q_q) | ts_match(Licitacion.descripcion, q_q))
                compradores = compradores.where(ts_match(Comprador.nombre_comprador, q_q) | ts_match(Comprador.nombre_unidad, q_q))
                proveedores = proveedores.where(ts_match(Proveedor.nombre, q_q) | ts_match(Proveedor.rut, q_q))

        else:
            title = "Missing query parameter"
            description = "Query parameter options are %s" % Search.ALLOWED_PARAMS
            raise falcon.HTTPBadRequest(title, description)

        response = {
            'licitaciones': [
                model_to_dict(licitacion, backrefs=False) for licitacion in licitaciones[start:start+10]
            ],
            'compradores': [
                model_to_dict(comprador, backrefs=False) for comprador in compradores[start:start+10]
            ],
            'proveedores': [
                model_to_dict(proveedor, backrefs=False) for proveedor in proveedores[start:start+10]
            ]
        }

        resp.body = json.dumps(response, cls=JSONEncoderPlus)