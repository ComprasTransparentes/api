"""Compras Transparentes API
"""

import falcon

from middlewares.cors import CorsMiddleware
from middlewares.payload import PayloadParserMiddleware

from endpoints.licitacion import LicitacionId, LicitacionIdItem, Licitacion
from endpoints.organismo import OrganismoId, Organismo, OrganismoIdLicitacion, OrganismoEmbed
from endpoints.proveedor import ProveedorId, Proveedor, ProveedorIdLicitacion, ProveedorEmbed

from endpoints.ministerio import MinisterioId, Ministerio, MinisterioCategoria, MinisterioIdCategoria, MinisterioIdCategoriaIdStats
from endpoints.producto import Producto, ProductoCategoria
from endpoints.stats import StatsItem, StatsTop

# Crear la app (WSGI) de Falcon
app = falcon.API(middleware=[CorsMiddleware(), PayloadParserMiddleware()])

# Agregar las rutas
app.add_route('/licitacion/', Licitacion())
app.add_route('/licitacion/{licitacion_id}', LicitacionId())
app.add_route('/licitacion/{licitacion_id}/item', LicitacionIdItem())

app.add_route('/organismo/', Organismo())
app.add_route('/organismo/{organismo_id}', OrganismoId())
app.add_route('/organismo/{organismo_id}/licitacion', OrganismoIdLicitacion())
app.add_route('/organismo/{organismo_id}/embed', OrganismoEmbed())

app.add_route('/proveedor/', Proveedor())
app.add_route('/proveedor/{proveedor_id}', ProveedorId())
app.add_route('/proveedor/{proveedor_id}/licitacion', ProveedorIdLicitacion())
app.add_route('/proveedor/{proveedor_id}/embed', ProveedorEmbed())

# Las siguientes rutas no deben ser publicas, solo para el uso de comprastransparentes.cl
# TODO Agregar prefijo CCTT

app.add_route('/ministerio', Ministerio())
app.add_route('/ministerio/{ministerio_id}', MinisterioId())
app.add_route('/ministerio/categoria', MinisterioCategoria())
app.add_route('/ministerio/{ministerio_id}/categoria', MinisterioIdCategoria())
app.add_route('/ministerio/{ministerio_id}/categoria/{categoria_id}/stats', MinisterioIdCategoriaIdStats())

app.add_route('/producto', Producto())
app.add_route('/producto/categoria', ProductoCategoria())

app.add_route('/stats/top/{datatype}', StatsTop())
app.add_route('/stats/{datatype}', StatsItem())

