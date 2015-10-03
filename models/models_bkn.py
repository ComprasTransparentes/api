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
        schema = db_schema_bkn


class Adjudicacion(BaseModel):
    fecha = DateTimeField(null=True)
    numero = CharField(null=True)
    numero_oferentes = IntegerField(null=True)
    tipo = IntegerField(null=True)

    class Meta:
        db_table = 'adjudicacion'


class Proveedor(BaseModel):
    nombre = CharField(null=True)
    rut = CharField(null=True)

    class Meta:
        db_table = 'proveedor'


class AdjudicacionItem(BaseModel):
    cantidad = FloatField()
    monto_unitario = FloatField()
    proveedor = ForeignKeyField(Proveedor, related_name='adjudicaciones')

    class Meta:
        db_table = 'adjudicacion_item'


class Comprador(BaseModel):
    jerarquia_id = IntegerField()

    categoria = CharField()

    codigo_comprador = IntegerField()
    nombre_comprador = CharField()

    codigo_unidad = IntegerField()
    nombre_unidad = CharField()
    rut_unidad = CharField()
    direccion_unidad = CharField()
    comuna_unidad = CharField()
    region_unidad = CharField()

    codigo_usuario = IntegerField()
    nombre_usuario = CharField()
    cargo_usuario = CharField()
    rut_usuario = CharField()


    class Meta:
        db_table = 'comprador'


class Licitacion(BaseModel):
    codigo = CharField()
    nombre = TextField()
    descripcion = TextField()

    tipo = CharField(null=True)

    nombre_responsable_contrato = CharField()
    email_responsable_contrato = CharField()
    telefono_responsable_contrato = CharField()

    nombre_responsable_pago = CharField()
    email_responsable_pago = CharField()

    fecha_creacion = DateTimeField(null=True)
    fecha_cierre = DateTimeField(null=True)
    fecha_inicio = DateTimeField(null=True)
    fecha_final = DateTimeField(null=True)
    fecha_publicacion = DateTimeField(null=True)
    fecha_publicacion_respuesta = DateTimeField(null=True)
    fecha_acto_apertura_tecnica = DateTimeField(null=True)
    fecha_acto_apertura_economica = DateTimeField(null=True)
    fecha_estimada_adjudicacion = DateTimeField(null=True)
    fecha_adjudicacion = DateTimeField(null=True)
    fecha_soporte_fisico = DateTimeField(null=True)
    fecha_tiempo_evaluacion = DateTimeField(null=True)
    fecha_estimada_firma = DateTimeField(null=True)
    fecha_usuario = DateTimeField(null=True)
    fecha_visita_terreno = DateTimeField(null=True)
    fecha_entrega_antecedentes = DateTimeField(null=True)

    adjudicacion = ForeignKeyField(Adjudicacion, null=True, on_delete='CASCADE', related_name='licitacion')
    comprador = ForeignKeyField(Comprador, on_delete='CASCADE',related_name='licitaciones')
    jerarquia = IntegerField()

    class Meta:
        db_table = 'licitacion'


class LicitacionEstado(BaseModel):
    licitacion = ForeignKeyField(Licitacion, on_delete='CASCADE',related_name='estados')

    estado = IntegerField()

    fecha = DateField()

    class Meta:
        db_table = 'licitacion_estado'



class LicitacionItem(BaseModel):
    adjudicacion = ForeignKeyField(AdjudicacionItem, on_delete='CASCADE',null=True, related_name='item')
    cantidad = IntegerField()
    codigo_categoria = IntegerField()
    codigo_producto = IntegerField()
    descripcion = TextField()
    licitacion = ForeignKeyField(Licitacion, on_delete='CASCADE',related_name='items')
    nombre_categoria = TextField()
    nombre_producto = TextField()
    unidad = CharField(null=True)

    class Meta:
        db_table = 'licitacion_item'


class CategoriaOrganismo(BaseModel):
    nombre = CharField(unique=True)

    class Meta:
        db_table = 'categoria_organismo'


class Organismo(BaseModel):
    codigo = IntegerField(unique=True)
    nombre = CharField()

    class Meta:
        db_table = 'organismo'


class OrganismoUnidad(BaseModel):
    organismo = ForeignKeyField(Organismo, on_delete='CASCADE',related_name='unidades')

    codigo = IntegerField()
    nombre = CharField()
    rut = CharField()
    direccion = CharField()
    comuna = CharField()
    region = CharField()

    class Meta:
        db_table = 'organismo_unidad'


class OrganismoUnidadUsuario(BaseModel):
    unidad = ForeignKeyField(OrganismoUnidad, on_delete='CASCADE',related_name='usuarios')

    codigo = IntegerField()
    nombre = CharField()
    rut = CharField()
    cargo = CharField()

    class Meta:
        db_table = 'organismo_unidad_usuario'