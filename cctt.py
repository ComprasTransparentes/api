import falcon

from middlewares.cors import CorsMiddleware
from middlewares.payload import PayloadParserMiddleware

from endpoints.licitacion import LicitacionId, LicitacionIdItem, Licitacion, LicitacionCategoria
from endpoints.ministerio import MinisterioStatsItem
from endpoints.organismo import OrganismoId, Organismo, OrganismoIdLicitacion, OrganismoEmbed
from endpoints.proveedor import ProveedorId, Proveedor, ProveedorIdLicitacion, ProveedorEmbed
from endpoints.stats import StatsItem, StatsTop

# Create the falcon WSGI app
app = falcon.API(middleware=[CorsMiddleware(), PayloadParserMiddleware()])

# Add routes
app.add_route('/licitacion/', Licitacion())
app.add_route('/licitacion/{licitacion_id}', LicitacionId())
app.add_route('/licitacion/{licitacion_id}/item', LicitacionIdItem())

app.add_route('/licitacion/categoria', LicitacionCategoria())

app.add_route('/ministerio/{ministerio_id}/categoria/{categoria_id}/stats', MinisterioStatsItem())

app.add_route('/organismo/', Organismo())
app.add_route('/organismo/{organismo_id}', OrganismoId())
app.add_route('/organismo/{organismo_id}/licitacion', OrganismoIdLicitacion())
app.add_route('/organismo/{organismo_id}/embed', OrganismoEmbed())

app.add_route('/proveedor/', Proveedor())
app.add_route('/proveedor/{proveedor_id}', ProveedorId())
app.add_route('/proveedor/{proveedor_id}/licitacion', ProveedorIdLicitacion())
app.add_route('/proveedor/{proveedor_id}/embed', ProveedorEmbed())

app.add_route('/stats/top/{datatype}', StatsTop())
app.add_route('/stats/{datatype}', StatsItem())

