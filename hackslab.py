import falcon

from middlewares.cors import CorsMiddleware

from endpoints.licitacion import LicitacionItem, LicitacionList
from endpoints.ministerio import MinisterioStatsItem
from endpoints.organismo import OrganismoItem, OrganismoList, OrganismoLicitacion, OrganismoEmbed
from endpoints.proveedor import ProveedorItem, ProveedorList, ProveedorLicitacion, ProveedorEmbed
from endpoints.stats import StatsItem, StatsTop

# Create the falcon WSGI app
app = falcon.API(middleware=[CorsMiddleware()])

# Add routes
app.add_route('/licitacion/', LicitacionList())
app.add_route('/licitacion/{licitacion_id}', LicitacionItem())

app.add_route('/ministerio/{ministerio_id}/categoria/{categoria_id}/stats', MinisterioStatsItem())

app.add_route('/organismo/', OrganismoList())
app.add_route('/organismo/{organismo_id}', OrganismoItem())
app.add_route('/organismo/{organismo_id}/licitacion', OrganismoLicitacion())
app.add_route('/organismo/{organismo_id}/embed', OrganismoEmbed())

app.add_route('/proveedor/', ProveedorList())
app.add_route('/proveedor/{proveedor_id}', ProveedorItem())
app.add_route('/proveedor/{proveedor_id}/licitacion', ProveedorLicitacion())
app.add_route('/proveedor/{proveedor_id}/embed', ProveedorEmbed())

app.add_route('/stats/top/{datatype}', StatsTop())
app.add_route('/stats/{datatype}', StatsItem())

