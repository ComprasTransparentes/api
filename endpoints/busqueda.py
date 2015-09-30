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
        if q_page.isdigit():
            q_page = [int(q_page) for _ in range(3)]
        elif ',' in q_page and all([p.isdigit() for p in q_page.split(',')]) and len(q_page.split(',')) == 3:
            q_page = q_page.split(',')
        else:
            q_page = [1]*3

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
            'licitaciones': [licitacion for licitacion in licitaciones.paginate(q_page[0], Search.MAX_RESULTS).dicts()],
            'compradores': [comprador for comprador in compradores.paginate(q_page[1], Search.MAX_RESULTS).dicts()],
            'proveedores': [proveedor for proveedor in proveedores.paginate(q_page[2], Search.MAX_RESULTS).dicts()]
        }

        resp.body = json.dumps(response, cls=JSONEncoderPlus)
