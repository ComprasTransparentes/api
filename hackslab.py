import falcon

from middlewares.cors import CorsMiddleware

from endpoints.licitacion import LicitacionItem
from endpoints.organismo import OrganismoItem
from endpoints.proveedor import ProveedorItem
from endpoints.busqueda import Search, SearchBeta
from endpoints.stats import StatsItem, StatsTop

# Create the falcon WSGI app
app = falcon.API(middleware=[CorsMiddleware()])

# Add routes
app.add_route('/licitacion/{licitacion_id}', LicitacionItem())
app.add_route('/organismo/{organismo_id}', OrganismoItem())
app.add_route('/proveedor/{proveedor_id}', ProveedorItem())

app.add_route('/buscar/', Search())
app.add_route('/buscarbeta/', SearchBeta())

app.add_route('/stats/top/{datatype}', StatsTop())
app.add_route('/stats/{datatype}', StatsItem())

