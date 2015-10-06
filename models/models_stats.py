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


class CategoriaMonto(BaseModel):
    categoria_tercer_nivel = CharField(null=True)
    monto = FloatField(null=True)

    class Meta:
        db_table = 'categoria_monto'


class CategoriaRegionSemestreMonto(BaseModel):
    categoria = CharField(null=True)
    monto = FloatField(null=True)
    region = CharField(null=True)
    semestre = TextField(null=True)

    class Meta:
        db_table = 'categoria_region_semestre_monto'


class LicitacionMaster(BaseModel):
    adjudicacion = IntegerField(db_column='adjudicacion_id', null=True)
    cargo_usuario = CharField(null=True)
    codigo_unidad = CharField(null=True)
    codigo_usuario = CharField(null=True)
    comuna_unidad = CharField(null=True)
    descripcion = TextField(null=True)
    direccion_unidad = CharField(null=True)
    email_responsable_contrato = CharField(null=True)
    email_responsable_pago = CharField(null=True)
    fecha_acto_apertura_economica = DateTimeField(null=True)
    fecha_acto_apertura_tecnica = DateTimeField(null=True)
    fecha_adjudicacion = DateTimeField(null=True)
    fecha_cierre = DateTimeField(null=True)
    fecha_creacion = DateTimeField(null=True)
    fecha_entrega_antecedentes = DateTimeField(null=True)
    fecha_estimada_adjudicacion = DateTimeField(null=True)
    fecha_estimada_firma = DateTimeField(null=True)
    fecha_final = DateTimeField(null=True)
    fecha_inicio = DateTimeField(null=True)
    fecha_pub_respuestas = DateTimeField(null=True)
    fecha_publicacion = DateTimeField(null=True)
    fecha_soporte_fisico = DateTimeField(null=True)
    fecha_tiempo_evaluacion = DateTimeField(null=True)
    fecha_visita_terreno = DateTimeField(null=True)
    fechas_usuario = DateTimeField(null=True)
    fono_responsable_contrato = CharField(null=True)
    ministerio_id = IntegerField(null=True)
    organismo_id = IntegerField(null=True)
    licitacion_codigo = CharField(null=True)
    licitacion = IntegerField(db_column='licitacion_id', null=True)
    moneda = CharField(null=True)
    nombre = TextField(null=True)
    ministerio_nombre = CharField(null=True)
    organismo_nombre = CharField(null=True)
    organismo_nombre_corto = CharField(null=True)
    nombre_responsable_contrato = CharField(null=True)
    nombre_responsable_pago = CharField(null=True)
    nombre_unidad = CharField(null=True)
    nombre_usuario = CharField(null=True)
    organismo = IntegerField(db_column='organismo_id', null=True)
    catalogo_organismo = IntegerField(db_column='catalogo_organismo_id')
    region_unidad = CharField(null=True)
    rut_usuario = CharField(null=True)
    tipo = CharField(null=True)

    class Meta:
        db_table = 'licitacion_master'


class LicitacionMonto(BaseModel):
    licitacion_codigo = CharField(null=True)
    monto = FloatField(null=True)

    class Meta:
        db_table = 'licitacion_monto'


class LicitacionRegionSemestreMonto(BaseModel):
    licitacion_codigo = CharField(null=True)
    monto = FloatField(null=True)
    region = CharField(null=True)
    semestre = TextField(null=True)

    class Meta:
        db_table = 'licitacion_region_semestre_monto'


class MasterPlop(BaseModel):
    ano = IntegerField(null=True)
    categoria = CharField(db_column='categoria_id', null=True)
    categoria_primer_nivel = CharField(null=True)
    categoria_segundo_nivel = CharField(null=True)
    categoria_tercer_nivel = CharField(null=True)
    codigo_categoria = IntegerField(null=True)
    codigo_producto = IntegerField(null=True)
    company = IntegerField(db_column='company_id', null=True)
    fecha_creacion = DateTimeField(null=True)
    licitacion_codigo = CharField(null=True)
    licitacion = IntegerField(db_column='licitacion_id', null=True)
    licitacion_item = IntegerField(db_column='licitacion_item_id', null=True)
    licitacion_descripcion = CharField(null=True)
    licitacion_nombre = CharField(null=True)
    mes = IntegerField(null=True)
    ministerio = IntegerField(db_column='ministerio_id', null=True)
    monto = FloatField(null=True)
    nombre = CharField(null=True)
    nombre_ministerio = CharField(null=True)
    nombre_organismo = CharField(null=True)
    nombre_organismo_corto = CharField()
    nombre_producto = CharField(null=True)
    organismo = IntegerField(db_column='organismo_id', null=True)
    region = CharField(null=True)
    rut_sucursal = CharField(null=True)

    class Meta:
        db_table = 'master_plop'


class MinisterioOrganismoMonto(BaseModel):
    monto = FloatField(null=True)
    nombre_ministerio = CharField(null=True)
    nombre_organismo = CharField(null=True)

    class Meta:
        db_table = 'ministerio_organismo_monto'


class MinisterioOrganismoRegionSemestreMonto(BaseModel):
    monto = FloatField(null=True)
    nombre_ministerio = CharField(null=True)
    nombre_organismo = CharField(null=True)
    region = CharField(null=True)
    semestre = TextField(null=True)

    class Meta:
        db_table = 'ministerio_organismo_region_semestre_monto'


class ProveedorMonto(BaseModel):
    company = IntegerField(db_column='company_id', null=True)
    monto = FloatField(null=True)

    class Meta:
        db_table = 'proveedor_monto'


class ProveedorRegionSemestreMonto(BaseModel):
    company = IntegerField(db_column='company_id', null=True)
    monto = FloatField(null=True)
    semestre = TextField(null=True)

    class Meta:
        db_table = 'proveedor_region_semestre_monto'


class MinisterioProductoStats(BaseModel):
    categoria = IntegerField(db_column='categoria_id')
    categoria_nombre = CharField()
    ministerio = IntegerField(db_column='ministerio_id')
    ministerio_nombre = CharField()
    monto_total = IntegerField()
    monto_promedio = IntegerField()
    n_proveedores = IntegerField()
    n_licitaciones_adjudicadas = IntegerField()

    class Meta:
        db_table = 'ministerio_producto_stats'
        primary_key = False
