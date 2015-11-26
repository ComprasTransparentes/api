import os

from peewee import *
from playhouse.postgres_ext import PostgresqlExtDatabase, ArrayField

db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASS')
db_name = os.getenv('DB_NAME')
db_schema_bkn = os.getenv('DB_SCHEMA_BKN', 'bkn')
db_schema_stats = os.getenv('DB_SCHEMA_STATS', 'stats')
db_schema_api = os.getenv('DB_SCHEMA_API', 'api')

assert db_schema_bkn != db_schema_stats != 'public'

database = PostgresqlExtDatabase(database=db_name, host=db_host, user=db_user, password=db_pass)


class UnknownField(object):
    pass


class BaseModelPublic(Model):
    class Meta:
        database = database

class BaseModelAPI(Model):
    class Meta:
        database = database
        schema = db_schema_api


class Licitacion(BaseModelAPI):
    cargo_usuario_organismo = CharField(null=True)
    categoria_nivel1 = ArrayField(null=True)
    categoria_nivel3 = ArrayField(null=True)
    codigo_licitacion = CharField(null=True)
    comuna_unidad = CharField(null=True)
    descripcion_licitacion = TextField(null=True)
    direccion_unidad = CharField(null=True)
    email_responsable_contrato = CharField(null=True)
    email_responsable_pago = CharField(null=True)
    empresas_ganadoras = ArrayField(null=True)
    estado = IntegerField(null=True)
    fecha_adjudicacion = CharField(null=True)
    fecha_cambio_estado = DateField(null=True)
    fecha_cierre = CharField(null=True)
    fecha_creacion = CharField(null=True)
    fecha_estimada_adjudicacion = CharField(null=True)
    fecha_final = CharField(null=True)
    fecha_inicio = CharField(null=True)
    fecha_publicacion = CharField(null=True)
    fono_responsable_contrato = CharField(null=True)
    id_categoria_nivel1 = ArrayField(null=True)
    id_categoria_nivel3 = ArrayField(null=True)
    id_licitacion = PrimaryKeyField(index=True)
    id_organismo = IntegerField(null=True)
    items_adjudicados = BigIntegerField(null=True)
    items_totales = BigIntegerField(null=True)
    monto_total = DecimalField(null=True)
    nombre_licitacion = TextField(null=True)
    nombre_ministerio = CharField(null=True)
    nombre_moneda = CharField(null=True)
    nombre_organismo = CharField(null=True)
    nombre_responsable_contrato = CharField(null=True)
    nombre_responsable_pago = CharField(null=True)
    nombre_unidad = CharField(null=True)
    nombre_usuario_organismo = CharField(null=True)
    region_unidad = CharField(null=True)
    rut_unidad = CharField(null=True)
    rut_usuario_organismo = CharField(null=True)
    url_acta = CharField(null=True)

    class Meta:
        db_table = 'licitacion_id'


class LicitacionIdItem(BaseModelAPI):
    cantidad = FloatField(null=True)
    cantidad_adjudicada = CharField(null=True)
    categoria_global = CharField(null=True)
    codigo_categoria = CharField(null=True)
    codigo_empresa_cc = CharField(null=True)
    codigo_producto = IntegerField(null=True)
    correlativo = IntegerField(null=True)
    descripcion = CharField(null=True)
    fecha_adjudicacion = CharField(null=True)
    id_empresa = IntegerField(null=True)
    licitacion = ForeignKeyField(Licitacion, db_column='licitacion_id', related_name='items')
    monto_pesos_adjudicado = DecimalField(null=True)
    monto_total = DecimalField(null=True)
    nombre_empresa = CharField(null=True)
    nombre_producto = CharField(null=True)
    rut_sucursal = CharField(null=True)
    unidad_medida = CharField(null=True)

    class Meta:
        db_table = 'licitacion_id_item'
        primary_key = False

class OrganismoStats(BaseModelAPI):
    id_organismo = PrimaryKeyField()
    licitaciones_adjudicadas = BigIntegerField(null=True)
    licitaciones_publicadas = BigIntegerField(null=True)
    monto_adjudicado = DecimalField(null=True)
    nombre_ministerio = CharField(null=True)
    nombre_organismo = CharField(null=True)
    nombre_resumen = CharField(null=True)

    class Meta:
        db_table = 'organismo_stats'

class ProveedorOrganismoCruce(BaseModelAPI):
    codigo_organismo_cc = IntegerField(null=True)
    empresa = IntegerField(db_column='empresa_id', null=True)
    fecha_adjudicacion = CharField(null=True)
    id_item = IntegerField(null=True)
    licitacion = ForeignKeyField(Licitacion, db_column='licitacion_id', related_name='proveedores_organismos')
    monto_total = DecimalField(null=True)
    nombre_empresa = CharField(null=True)
    nombre_ministerio = CharField(null=True)
    nombre_organismo = CharField(null=True)
    nombre_resumen = CharField(null=True)
    organismo = IntegerField(db_column='organismo_id', null=True)
    rut_sucursal = CharField(null=True)

    class Meta:
        db_table = 'proveedor_organismo_cruce'


class ProveedorStats(BaseModelAPI):
    empresa = PrimaryKeyField(db_column='empresa_id', null=True)
    licitaciones_adjudicadas = BigIntegerField(null=True)
    monto_adjudicado = DecimalField(null=True)
    nombre_empresa = CharField(null=True)
    rut_sucursal = CharField(null=True)

    class Meta:
        db_table = 'proveedor_stats'


class Comparador(BaseModelAPI):
    categoria_nivel1 = CharField(null=True)
    id_categoria_nivel1 = IntegerField(null=True)
    id_ministerio = IntegerField(null=True)
    licit_adjudicadas = BigIntegerField(null=True)
    monto = DecimalField(null=True)
    monto_promedio = DecimalField(null=True)
    nombre_ministerio = CharField(null=True)
    proveed_favorecidos = BigIntegerField(null=True)

    class Meta:
        db_table = 'comparador'
        primary_key = False


class Catnivel1(BaseModelPublic):
    id_categoria_nivel1 = PrimaryKeyField()
    categoria_nivel1 = CharField()

    class Meta:
        db_table = 'catnivel1'

class Catnivel3(BaseModelPublic):
    id_categoria_nivel3 = PrimaryKeyField()
    categoria_nivel3 = CharField()

    class Meta:
        db_table = 'catnivel3'

class RankingCategorias(BaseModelAPI):
    categoria_nivel3 = CharField(null=True)
    id_categoria_nivel3 = IntegerField(null=True)
    monto = DecimalField(null=True)

    class Meta:
        db_table = 'ranking_categorias'
        primary_key = False

class RankingOrganismos(BaseModelAPI):
    monto = DecimalField(null=True)
    nombre_organismo = CharField(null=True)
    organismo = IntegerField(db_column='organismo_id', null=True)

    class Meta:
        db_table = 'ranking_organismos'
        primary_key = False

class RankingProveedores(BaseModelAPI):
    empresa = IntegerField(db_column='empresa_id', null=True)
    monto = DecimalField(null=True)
    rut_sucursal = CharField(null=True)

    class Meta:
        db_table = 'ranking_proveedores'
        primary_key = False


