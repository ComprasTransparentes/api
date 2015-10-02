import os

from peewee import *
from playhouse.postgres_ext import PostgresqlExtDatabase


db_name = os.getenv('DB_NAME')
db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASS')

database = PostgresqlExtDatabase(database=db_name, host=db_host, user=db_user, password=db_pass)


class BaseModel(Model):
    class Meta:
        database = database
        schema = 'public'


class AdjudicationItems(BaseModel):
    cantidad = CharField(null=True)
    company = IntegerField(db_column='company_id', null=True)
    created_at = DateTimeField()
    monto_unitario = CharField(null=True)
    tender_item = IntegerField(db_column='tender_item_id', null=True)
    updated_at = DateTimeField()

    class Meta:
        db_table = 'adjudication_items'


class Adjudications(BaseModel):
    created_at = DateTimeField()
    fecha = CharField(null=True)
    numero = CharField(null=True)
    numero_oferentes = IntegerField(null=True)
    state = IntegerField(null=True)
    tender = IntegerField(db_column='tender_id', null=True)
    tipo = IntegerField(null=True)
    updated_at = DateTimeField()
    url_acta = CharField(null=True)

    class Meta:
        db_table = 'adjudications'


class Companies(BaseModel):
    actividad = CharField(null=True)
    cargo_contacto = CharField(null=True)
    code = CharField(null=True)
    codigo_sucursal = CharField(null=True)
    comuna = CharField(null=True)
    created_at = DateTimeField()
    direccion = CharField(null=True)
    fono_contacto = CharField(null=True)
    mail_contacto = CharField(null=True)
    nombre = CharField(null=True)
    nombre_contacto = CharField(null=True)
    nombre_sucursal = CharField(null=True)
    pais = CharField(null=True)
    region = CharField(null=True)
    rut_sucursal = CharField(null=True)
    updated_at = DateTimeField()

    class Meta:
        db_table = 'companies'


class Currency(BaseModel):
    moneda = CharField(null=True)
    tipo_cambio = DecimalField(null=True)

    class Meta:
        db_table = 'currency'


class Ministerio(BaseModel):
    nombre = CharField(db_column='name')

    class Meta:
        db_table = 'ministerios'


class Jerarquia(BaseModel):
    id = PrimaryKeyField(db_column='idorganismo')
    ministerio_id = ForeignKeyField(Ministerio, db_column='idministerio')
    nombre_ministerio = CharField(db_column='name')
    codigo_organismo = CharField(db_column='company_code', unique=True)
    nombre_organismo = CharField(db_column='sub_name')
    nombre_organismo_corto = CharField(db_column='sub_name_plot')

    class Meta:
        db_table = 'jerarquia_final'


class OrderDates(BaseModel):
    created_at = DateTimeField()
    fecha_aceptacion = CharField(null=True)
    fecha_cancelacion = CharField(null=True)
    fecha_creacion = CharField(null=True)
    fecha_envio = CharField(null=True)
    fecha_ultima_modificacion = CharField(null=True)
    updated_at = DateTimeField()

    class Meta:
        db_table = 'order_dates'


class OrderItems(BaseModel):
    cantidad = IntegerField(null=True)
    categoria = CharField(null=True)
    codigo_categoria = IntegerField(null=True)
    codigo_producto = IntegerField(null=True)
    correlativo = IntegerField(null=True)
    created_at = DateTimeField()
    especificacion_comprador = CharField(null=True)
    especificacion_proveedor = CharField(null=True)
    moneda = CharField(null=True)
    precio_neto = IntegerField(null=True)
    producto = CharField(null=True)
    total = IntegerField(null=True)
    total_cargos = IntegerField(null=True)
    total_descuentos = IntegerField(null=True)
    total_impuestos = IntegerField(null=True)
    updated_at = DateTimeField()

    class Meta:
        db_table = 'order_items'


class Orders(BaseModel):
    buyer = IntegerField(db_column='buyer_id', null=True)
    cantidad_evaluacion = IntegerField(null=True)
    cargos = IntegerField(null=True)
    codigo = CharField(null=True)
    codigo_estado = IntegerField(null=True)
    codigo_estado_proveedor = IntegerField(null=True)
    codigo_licitacion = CharField(null=True)
    codigo_tipo = CharField(null=True)
    created_at = DateTimeField()
    descripcion = CharField(null=True)
    descuentos = IntegerField(null=True)
    estado = CharField(null=True)
    estado_proveedor = CharField(null=True)
    financiamiento = CharField(null=True)
    forma_pago = CharField(null=True)
    impuestos = IntegerField(null=True)
    nombre = CharField(null=True)
    pais = CharField(null=True)
    porcentaje_iva = IntegerField(null=True)
    promedio_calificacion = IntegerField(null=True)
    provider = IntegerField(db_column='provider_id', null=True)
    tiene_items = CharField(null=True)
    tipo = CharField(null=True)
    tipo_despacho = CharField(null=True)
    tipo_moneda = CharField(null=True)
    total = IntegerField(null=True)
    total_neto = IntegerField(null=True)
    updated_at = DateTimeField()

    class Meta:
        db_table = 'orders'


class PublicCompanies(BaseModel):
    cargo_usuario = CharField(null=True)
    code = CharField(null=True)
    codigo_unidad = CharField(null=True)
    codigo_usuario = CharField(null=True)
    comuna_unidad = CharField(null=True)
    created_at = DateTimeField()
    direccion_unidad = CharField(null=True)
    nombre = CharField(null=True)
    nombre_unidad = CharField(null=True)
    nombre_usuario = CharField(null=True)
    region_unidad = CharField(null=True)
    rut_unidad = CharField(null=True)
    rut_usuario = CharField(null=True)
    updated_at = DateTimeField()

    class Meta:
        db_table = 'public_companies'


class TenderDates(BaseModel):
    created_at = DateTimeField()
    fecha_acto_apertura_economica = CharField(null=True)
    fecha_acto_apertura_tecnica = CharField(null=True)
    fecha_adjudicacion = CharField(null=True)
    fecha_cierre = CharField(null=True)
    fecha_creacion = CharField(null=True)
    fecha_entrega_antecedentes = CharField(null=True)
    fecha_estimada_adjudicacion = CharField(null=True)
    fecha_estimada_firma = CharField(null=True)
    fecha_final = CharField(null=True)
    fecha_inicio = CharField(null=True)
    fecha_pub_respuestas = CharField(null=True)
    fecha_publicacion = CharField(null=True)
    fecha_soporte_fisico = CharField(null=True)
    fecha_tiempo_evaluacion = CharField(null=True)
    fecha_visita_terreno = CharField(null=True)
    fechas_usuario = CharField(null=True)
    tender = IntegerField(db_column='tender_id', null=True)
    updated_at = DateTimeField()

    class Meta:
        db_table = 'tender_dates'


class Tenders(BaseModel):
    adjudication = IntegerField(db_column='adjudication_id', null=True)
    buyer = IntegerField(db_column='buyer_id', null=True)
    cantidad_reclamos = IntegerField(null=True)
    code = CharField(index=True, null=True)
    codigo_tipo = IntegerField(null=True)
    contrato = CharField(null=True)
    created_at = DateTimeField()
    descripcion = TextField(null=True)
    dias_cierre_licitacion = CharField(null=True)
    direccion_entrega = CharField(null=True)
    direccion_visita = CharField(null=True)
    email_responsable_contrato = CharField(null=True)
    email_responsable_pago = CharField(null=True)
    es_base_tipo = IntegerField(null=True)
    es_renovable = IntegerField(null=True)
    estado_etapas = CharField(null=True)
    estado_publicidad_ofertas = IntegerField(null=True)
    estimacion = IntegerField(null=True)
    etapas = IntegerField(null=True)
    extension_plazo = IntegerField(null=True)
    fono_responsable_contrato = CharField(null=True)
    fuente_financiamiento = CharField(null=True)
    informada = IntegerField(null=True)
    justificacion_monto_estimado = CharField(null=True)
    justificacion_publicidad = CharField(null=True)
    modalidad = IntegerField(null=True)
    moneda = CharField(null=True)
    monto_estimado = CharField(null=True)
    name = TextField(null=True)
    nombre_responsable_contrato = CharField(null=True)
    nombre_responsable_pago = CharField(null=True)
    obras = CharField(null=True)
    observacion_contract = CharField(null=True)
    periodo_tiempo_renovacion = CharField(null=True)
    prohibicion_contratacion = CharField(null=True)
    sub_contratacion = CharField(null=True)
    tiempo = CharField(null=True)
    tiempo_duracion_contrato = CharField(null=True)
    tipo = CharField(null=True)
    tipo_convocatoria = CharField(null=True)
    tipo_duracion_contrato = CharField(null=True)
    tipo_pago = CharField(null=True)
    toma_razon = CharField(null=True)
    unidad_tiempo = CharField(null=True)
    unidad_tiempo_contrato_licitacion = CharField(null=True)
    unidad_tiempo_duracion_contrato = CharField(null=True)
    unidad_tiempo_evaluacion = IntegerField(null=True)
    updated_at = DateTimeField()
    valor_tiempo_renovacion = CharField(null=True)
    visibilidad_monto = IntegerField(null=True)

    class Meta:
        db_table = 'tenders'


class TenderItems(BaseModel):
    adjudicacion = CharField(null=True)
    cantidad = FloatField(null=True)
    categoria = CharField(null=True)
    codigo_categoria = CharField(null=True)
    codigo_producto = IntegerField(null=True)
    correlativo = IntegerField(null=True)
    created_at = DateTimeField()
    descripcion = CharField(null=True)
    nombre_producto = CharField(null=True)
    tender = ForeignKeyField(db_column='tender_id', null=True, rel_model=Tenders, to_field='id')
    unidad_medida = CharField(null=True)
    updated_at = DateTimeField()

    class Meta:
        db_table = 'tender_items'


class TenderJerarquiaDistinct(BaseModel):
    jerarquia_distinct = IntegerField(db_column='jerarquia_distinct_id')
    tender = IntegerField(db_column='tender_id')

    class Meta:
        db_table = 'tender_jerarquia_distinct'


class TenderParticipants(BaseModel):
    company = IntegerField(db_column='company_id', null=True)
    created_at = DateTimeField()
    is_winner = BooleanField(null=True)
    part_cant = CharField(null=True)
    part_valor = CharField(null=True)
    price = CharField(null=True)
    tender = IntegerField(db_column='tender_id', null=True)
    tender_item = IntegerField(db_column='tender_item_id', null=True)
    updated_at = DateTimeField()

    class Meta:
        db_table = 'tender_participants'


class TenderStates(BaseModel):
    created_at = DateTimeField()
    date = CharField(null=True)
    state = IntegerField(null=True)
    tender = IntegerField(db_column='tender_id', null=True)
    updated_at = DateTimeField()

    class Meta:
        db_table = 'tender_states'


class TokenUsages(BaseModel):
    active = BooleanField(null=True)
    amount = IntegerField(null=True)
    created_at = DateTimeField()
    last_used = DateTimeField(null=True)
    token = IntegerField(db_column='token_id', null=True)
    updated_at = DateTimeField()

    class Meta:
        db_table = 'token_usages'


class Tokens(BaseModel):
    created_at = DateTimeField()
    token = CharField(null=True)
    updated_at = DateTimeField()

    class Meta:
        db_table = 'tokens'


class TransactionalOffers(BaseModel):
    company = IntegerField(db_column='company_id', null=True)
    created_at = DateTimeField()
    offer_amount = IntegerField(null=True)
    tender_item = IntegerField(db_column='tender_item_id', null=True)
    updated_at = DateTimeField()

    class Meta:
        db_table = 'transactional_offers'
