import os

from peewee import *
from playhouse.postgres_ext import PostgresqlExtDatabase

db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASS')
db_name = os.getenv('DB_NAME')
db_schema_bkn = os.getenv('DB_SCHEMA_BKN', 'bkn')
db_schema_stats = os.getenv('DB_SCHEMA_STATS', 'stats')

assert db_schema_bkn != db_schema_stats != 'public'

database = PostgresqlExtDatabase(database=db_name, host=db_host, user=db_user, password=db_pass)


class BaseModel(Model):
    class Meta:
        database = database
        schema = db_schema_stats


class GastoOrganismo(BaseModel):
    nombre_categoria = CharField(null=True)
    nombre_organismo = CharField(null=True)
    monto = IntegerField(null=True)
    region = CharField(null=True)
    ano = IntegerField(null=True)

    class Meta:
        db_table = 'gasto_organismo'
        primary_key = False


class TopCategorias(BaseModel):
    categoria = CharField(null=True)
    monto = IntegerField(null=True)

    class Meta:
        db_table = 'top_categorias'


class TopCategoriasRegionSemestre(BaseModel):
    categoria = CharField(null=True)
    monto = IntegerField(null=True)
    region = CharField(null=True)
    semestre = CharField(null=True)

    class Meta:
        db_table = 'top_categorias_region_semestre'


class TopLicitaciones(BaseModel):
    codigo_licitacion = CharField(null=True)
    monto = IntegerField(null=True)

    class Meta:
        db_table = 'top_licitaciones'


class TopLicitacionesRegion(BaseModel):
    codigo_licitacion = CharField(null=True)
    monto = IntegerField(null=True)
    region = CharField(null=True)
    semestre = CharField(null=True)
    codigo_semestre = CharField()

    class Meta:
        db_table = 'top_licitaciones_region'


class VentasProveedor(BaseModel):
    id_proveedor = IntegerField()
    monto = IntegerField()

    class Meta:
        db_table = 'ventas_proveedor'


class VentasProveedorSemestre(BaseModel):
    id_proveedor = CharField(null=True)
    monto = IntegerField(null=True)
    semestre = CharField(null=True)

    class Meta:
        db_table = 'ventas_proveedor_semestre'


class LicitacionItemAdjudicadas(BaseModel):
    licitacion = IntegerField(db_column='licitacion_id')
    licitacion_item = IntegerField(db_column='licitacion_item_id')

    jerarquia_distinct = IntegerField(db_column='jerarquia_distinct_id')

    codigo_categoria = IntegerField()
    nombre_categoria = CharField()
    codigo_producto = IntegerField()
    nombre_producto = CharField()

    proveedor = IntegerField(db_column='proveedor_id')
    nombre_proveedor = CharField(null=True)
    rut_proveedor = CharField(null=True)

    monto = BigIntegerField()

    class Meta:
        db_table = 'licitacion_item_adjudicadas'
        primary_key = False


class Sumario(BaseModel):
    monto_transado = IntegerField()
    n_licitaciones = IntegerField()
    n_organismo = IntegerField()
    n_proveedores = IntegerField()

    class Meta:
        db_table = 'sumario'
        primary_key = False
