import os
import time

import psycopg2

import peewee
from playhouse.shortcuts import cast

import models as models_old
import models_bkn

db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASS')
db_name = os.getenv('DB_NAME')
db_schema_bkn = os.getenv('DB_SCHEMA_BKN', 'bkn')
db_schema_stats = os.getenv('DB_SCHEMA_STATS', 'stats')

assert db_schema_bkn != db_schema_stats != 'public'


def sql_to_str(path):
    text = ""
    with open(path, 'r') as f:
        for line in f:
            if not line.strip().startswith(('--', '\xef\xbb\xbf')):
                text += line
    return text


t = time.time()

# Prepare shchema
print "CONFIGURATION"
print "Connecting to database %s..." % db_name
with psycopg2.connect(database=db_name, host=db_host, user=db_user, password=db_pass) as connection:
    connection.autocommit = True
    cursor = connection.cursor()
    print "Dropping schema %s..." % db_schema_bkn
    cursor.execute("DROP SCHEMA IF EXISTS %s CASCADE" % db_schema_bkn)
    print "Creating schema %s..." % db_schema_bkn
    cursor.execute("CREATE SCHEMA %s" % db_schema_bkn)
    print "Installing extension hstore..."
    # cursor.execute("CREATE EXTENSION IF NOT EXISTS hstore")
    psycopg2.extras.register_hstore(cursor, globally=True, unicode=True)
    connection.autocommit = False
    print "Building Jerarquia..."
    cursor.execute(sql_to_str("sql/10-jerarquia.sql"))

print "Time: %d seconds" % (time.time()-t)

# with models_bkn.database.atomic():
# Create tables
all_models = [
    models_bkn.Licitacion,
    models_bkn.LicitacionEstado,
    models_bkn.LicitacionItem,
    models_bkn.Adjudicacion,
    models_bkn.AdjudicacionItem,
    models_bkn.Proveedor,
    models_bkn.Comprador,
]
print "Creating tables..."
models_bkn.database.create_tables(all_models)

# Migrate

###############################################
### PublicCompanies -> Comprador ###
###############################################

print "\nMIGRATE PublicCompanies -> %s.Comprador" % db_schema_bkn

fields_from = [
    models_old.PublicCompanies.id,

    models_old.JerarquiaDistinct.nombre_categoria,
    models_old.JerarquiaDistinct.id,

    models_old.JerarquiaDistinct.codigo_organismo,
    models_old.JerarquiaDistinct.nombre_organismo,

    cast(models_old.PublicCompanies.codigo_unidad, 'integer'),
    models_old.PublicCompanies.nombre_unidad,
    models_old.PublicCompanies.rut_unidad,
    models_old.PublicCompanies.direccion_unidad,
    models_old.PublicCompanies.comuna_unidad,
    models_old.PublicCompanies.region_unidad,

    cast(models_old.PublicCompanies.codigo_usuario, 'integer'),
    models_old.PublicCompanies.nombre_usuario,
    models_old.PublicCompanies.rut_usuario,
    models_old.PublicCompanies.cargo_usuario,
]

query_from = models_old.PublicCompanies.select(*fields_from).join(models_old.JerarquiaDistinct, peewee.JOIN_LEFT_OUTER, on=(cast(models_old.PublicCompanies.code, 'integer') == models_old.JerarquiaDistinct.codigo_organismo))

fields_to = [
    models_bkn.Comprador.id,

    models_bkn.Comprador.categoria,
    models_bkn.Comprador.jerarquia_id,

    models_bkn.Comprador.codigo_comprador,
    models_bkn.Comprador.nombre_comprador,

    models_bkn.Comprador.codigo_unidad,
    models_bkn.Comprador.nombre_unidad,
    models_bkn.Comprador.rut_unidad,
    models_bkn.Comprador.direccion_unidad,
    models_bkn.Comprador.comuna_unidad,
    models_bkn.Comprador.region_unidad,

    models_bkn.Comprador.codigo_usuario,
    models_bkn.Comprador.nombre_usuario,
    models_bkn.Comprador.rut_usuario,
    models_bkn.Comprador.cargo_usuario,
]

print "Inserting %d rows..." % query_from.count()
models_bkn.Comprador.insert_from(fields_to, query_from).execute()

print "Checking integrity... ",
assert models_old.PublicCompanies.select().count() == models_bkn.Comprador.select().count()
assert models_old.PublicCompanies.select().order_by(models_old.PublicCompanies.id)[0].id == models_bkn.Comprador.select().order_by(models_bkn.Comprador.id)[0].id
assert models_old.PublicCompanies.select().order_by(-models_old.PublicCompanies.id)[0].id == models_bkn.Comprador.select().order_by(-models_bkn.Comprador.id)[0].id
print "OK"
print "Inserted %d rows" % models_bkn.Comprador.select().count()

print "Time: %d seconds" % (time.time()-t)

#####################################
### Adjudications -> Adjudicacion ###
#####################################
print "\nMIGRATE Adjudications -> %s.Adjudicacion" % db_schema_bkn

fields_from = [
    models_old.Adjudications.id,
    models_old.Adjudications.tipo,
    models_old.Adjudications.numero,
    models_old.Adjudications.numero_oferentes,
    cast(models_old.Adjudications.fecha, 'timestamp'),
]

query_from = models_old.Adjudications.select(*fields_from).order_by(models_old.Adjudications.id)

fields_to = [
    models_bkn.Adjudicacion.id,
    models_bkn.Adjudicacion.tipo,
    models_bkn.Adjudicacion.numero,
    models_bkn.Adjudicacion.numero_oferentes,
    models_bkn.Adjudicacion.fecha,
]

print "Inserting %d rows..." % query_from.count()
models_bkn.Adjudicacion.insert_from(fields_to, query_from).execute()

print "Checking integrity... ",
assert models_old.Adjudications.select().count() == models_bkn.Adjudicacion.select().count()
assert models_old.Adjudications.select().order_by(models_old.Adjudications.id)[0].id == models_bkn.Adjudicacion.select().order_by(models_bkn.Adjudicacion.id)[0].id
assert models_old.Adjudications.select().order_by(-models_old.Adjudications.id)[0].id == models_bkn.Adjudicacion.select().order_by(-models_bkn.Adjudicacion.id)[0].id
print "OK"
print "Inserted %d rows" % models_bkn.Adjudicacion.select().count()

print "Time: %d seconds" % (time.time()-t)

##############################
### Companies -> Proveedor ###
##############################
print "\nMIGRATE Companies -> %s.Proveedor" % db_schema_bkn

fields_from = [
    models_old.Companies.id,
    models_old.Companies.rut_sucursal,
    models_old.Companies.nombre,
]

query_from = models_old.Companies.select(*fields_from).order_by(models_old.Companies.id)

fields_to = [
    models_bkn.Proveedor.id,
    models_bkn.Proveedor.rut,
    models_bkn.Proveedor.nombre,
]

print "Inserting %d rows..." % query_from.count()
models_bkn.Proveedor.insert_from(fields_to, query_from).execute()

print "Checking integrity... ",
assert models_old.Companies.select().count() == models_bkn.Proveedor.select().count()
assert models_old.Companies.select().order_by(models_old.Companies.id)[0].id == models_bkn.Proveedor.select().order_by(models_bkn.Proveedor.id)[0].id
assert models_old.Companies.select().order_by(-models_old.Companies.id)[0].id == models_bkn.Proveedor.select().order_by(-models_bkn.Proveedor.id)[0].id
print "OK"
print "Inserted %d rows" % models_bkn.Proveedor.select().count()

print "Time: %d seconds" % (time.time()-t)

#####################################
### AdjudicationItems -> AdjudicacionItem ###
#####################################
print "\nMIGRATE AdjudicationItems -> %s.AdjudicacionItem" % db_schema_bkn

fields_from = [
    models_old.AdjudicationItems.id,
    cast(models_old.AdjudicationItems.cantidad, 'float'),
    cast(models_old.AdjudicationItems.monto_unitario, 'float'),
    models_old.AdjudicationItems.company,
]

query_from = models_old.AdjudicationItems.select(*fields_from).order_by(models_old.AdjudicationItems.id)

fields_to = [
    models_bkn.AdjudicacionItem.id,
    models_bkn.AdjudicacionItem.cantidad,
    models_bkn.AdjudicacionItem.monto_unitario,
    models_bkn.AdjudicacionItem.proveedor,
]

print "Inserting %d rows..." % query_from.count()
models_bkn.AdjudicacionItem.insert_from(fields_to, query_from).execute()

print "Checking integrity... ",
assert models_old.AdjudicationItems.select().count() == models_bkn.AdjudicacionItem.select().count()
assert models_old.AdjudicationItems.select().order_by(models_old.AdjudicationItems.id)[0].id == models_bkn.AdjudicacionItem.select().order_by(models_bkn.AdjudicacionItem.id)[0].id
assert models_old.AdjudicationItems.select().order_by(-models_old.AdjudicationItems.id)[0].id == models_bkn.AdjudicacionItem.select().order_by(-models_bkn.AdjudicacionItem.id)[0].id
print "OK"
print "Inserted %d rows" % models_bkn.AdjudicacionItem.select().count()

print "Time: %d seconds" % (time.time()-t)

#############################
### Tenders -> Licitacion ###
#############################
print "\nMIGRATE Tenders -> %s.Licitacion" % db_schema_bkn

fields_from = [
    models_old.Tenders.id,
    models_old.Tenders.code,
    models_old.Tenders.name,
    models_old.Tenders.descripcion,

    models_old.Tenders.tipo,

    models_old.Tenders.nombre_responsable_contrato,
    models_old.Tenders.email_responsable_contrato,
    models_old.Tenders.fono_responsable_contrato,

    models_old.Tenders.nombre_responsable_pago,
    models_old.Tenders.email_responsable_pago,

    models_old.Tenders.buyer,
    models_old.Adjudications.id,

    cast(models_old.TenderDates.fecha_creacion, 'timestamp'),
    cast(models_old.TenderDates.fecha_cierre, 'timestamp'),
    cast(models_old.TenderDates.fecha_inicio, 'timestamp'),
    cast(models_old.TenderDates.fecha_final, 'timestamp'),
    cast(models_old.TenderDates.fecha_publicacion, 'timestamp'),
    cast(models_old.TenderDates.fecha_pub_respuestas, 'timestamp'),
    cast(models_old.TenderDates.fecha_acto_apertura_tecnica, 'timestamp'),
    cast(models_old.TenderDates.fecha_acto_apertura_economica, 'timestamp'),
    cast(models_old.TenderDates.fecha_estimada_adjudicacion, 'timestamp'),
    cast(models_old.TenderDates.fecha_estimada_firma, 'timestamp'),
    cast(models_old.TenderDates.fecha_adjudicacion, 'timestamp'),
    cast(models_old.TenderDates.fecha_soporte_fisico, 'timestamp'),
    cast(models_old.TenderDates.fecha_tiempo_evaluacion, 'timestamp'),
    cast(models_old.TenderDates.fechas_usuario, 'timestamp'),
    cast(models_old.TenderDates.fecha_visita_terreno, 'timestamp'),
    cast(models_old.TenderDates.fecha_entrega_antecedentes, 'timestamp'),
]

query_from = models_old.Tenders.select(*fields_from).join(models_old.Adjudications, peewee.JOIN_LEFT_OUTER, on=(models_old.Tenders.id == models_old.Adjudications.tender)).join(models_old.TenderDates, peewee.JOIN_LEFT_OUTER, on=(models_old.Tenders.id == models_old.TenderDates.id)).order_by(models_old.Tenders.id)

fields_to = [
    models_bkn.Licitacion.id,
    models_bkn.Licitacion.codigo,
    models_bkn.Licitacion.nombre,
    models_bkn.Licitacion.descripcion,

    models_bkn.Licitacion.tipo,

    models_bkn.Licitacion.nombre_responsable_contrato,
    models_bkn.Licitacion.email_responsable_contrato,
    models_bkn.Licitacion.telefono_responsable_contrato,

    models_bkn.Licitacion.nombre_responsable_pago,
    models_bkn.Licitacion.email_responsable_pago,

    models_bkn.Licitacion.comprador,
    models_bkn.Licitacion.adjudicacion,

    models_bkn.Licitacion.fecha_creacion,
    models_bkn.Licitacion.fecha_cierre,
    models_bkn.Licitacion.fecha_inicio,
    models_bkn.Licitacion.fecha_final,
    models_bkn.Licitacion.fecha_publicacion,
    models_bkn.Licitacion.fecha_publicacion_respuesta,
    models_bkn.Licitacion.fecha_acto_apertura_tecnica,
    models_bkn.Licitacion.fecha_acto_apertura_economica,
    models_bkn.Licitacion.fecha_estimada_adjudicacion,
    models_bkn.Licitacion.fecha_estimada_firma,
    models_bkn.Licitacion.fecha_adjudicacion,
    models_bkn.Licitacion.fecha_soporte_fisico,
    models_bkn.Licitacion.fecha_tiempo_evaluacion,
    models_bkn.Licitacion.fecha_usuario,
    models_bkn.Licitacion.fecha_visita_terreno,
    models_bkn.Licitacion.fecha_entrega_antecedentes,
]

print "Inserting %d rows..." % query_from.count()
models_bkn.Licitacion.insert_from(fields_to, query_from).execute()

print "Checking integrity... ",
assert models_old.Tenders.select().count() == models_bkn.Licitacion.select().count()
assert models_old.Tenders.select().order_by(models_old.Tenders.id)[0].id == models_bkn.Licitacion.select().order_by(models_bkn.Licitacion.id)[0].id
assert models_old.Tenders.select().order_by(-models_old.Tenders.id)[0].id == models_bkn.Licitacion.select().order_by(-models_bkn.Licitacion.id)[0].id
print "OK"
print "Inserted %d rows" % models_bkn.Licitacion.select().count()

print "Time: %d seconds" % (time.time()-t)

########################################
### TenderStates -> LicitacionEstado ###
########################################
print "\nMIGRATE TenderStates -> %s.LicitacionEstado" % db_schema_bkn

fields_from = [
    models_old.TenderStates.tender,
    models_old.TenderStates.state,
    peewee.fn.to_date(models_old.TenderStates.date, 'DDMMYYYY'),
]

query_from = models_old.TenderStates.select(*fields_from).order_by(models_old.TenderStates.id)

fields_to = [
    models_bkn.LicitacionEstado.licitacion,
    models_bkn.LicitacionEstado.estado,
    models_bkn.LicitacionEstado.fecha,
]

print "Inserting %d rows..." % query_from.count()
models_bkn.LicitacionEstado.insert_from(fields_to, query_from).execute()

print "Checking integrity... ",
assert models_old.TenderStates.select().count() == models_bkn.LicitacionEstado.select().count()
assert models_old.TenderStates.select().order_by(models_old.TenderStates.id)[0].id == models_bkn.LicitacionEstado.select().order_by(models_bkn.LicitacionEstado.id)[0].id
assert models_old.TenderStates.select().order_by(-models_old.TenderStates.id)[0].id == models_bkn.LicitacionEstado.select().order_by(-models_bkn.LicitacionEstado.id)[0].id
print "OK"
print "Inserted %d rows" % models_bkn.LicitacionEstado.select().count()

print "Time: %d seconds" % (time.time()-t)

#####################################
### TenderItems -> LicitacionItem ###
#####################################
print "\nMIGRATE TenderItems -> %s.LicitacionItem" % db_schema_bkn

fields_from = [
    models_old.TenderItems.id,
    models_old.TenderItems.tender,
    models_old.AdjudicationItems.id,
    cast(models_old.TenderItems.codigo_categoria, 'int'),
    models_old.TenderItems.categoria,
    cast(models_old.TenderItems.codigo_producto, 'int'),
    models_old.TenderItems.nombre_producto,
    models_old.TenderItems.descripcion,
    models_old.TenderItems.unidad_medida,
    cast(models_old.TenderItems.cantidad, 'int'),
]

query_from = models_old.TenderItems.select(*fields_from).join(models_old.AdjudicationItems, peewee.JOIN_LEFT_OUTER, on=(models_old.TenderItems.id == models_old.AdjudicationItems.tender_item)).order_by(models_old.TenderItems.id)

fields_to = [
    models_bkn.LicitacionItem.id,
    models_bkn.LicitacionItem.licitacion,
    models_bkn.LicitacionItem.adjudicacion,
    models_bkn.LicitacionItem.codigo_categoria,
    models_bkn.LicitacionItem.nombre_categoria,
    models_bkn.LicitacionItem.codigo_producto,
    models_bkn.LicitacionItem.nombre_producto,
    models_bkn.LicitacionItem.descripcion,
    models_bkn.LicitacionItem.unidad,
    models_bkn.LicitacionItem.cantidad,
]

print "Inserting %d rows..." % query_from.count()
models_bkn.LicitacionItem.insert_from(fields_to, query_from).execute()

print "Checking integrity... ",
assert models_old.TenderItems.select().count() == models_bkn.LicitacionItem.select().count()
assert models_old.TenderItems.select().order_by(models_old.TenderItems.id)[0].id == models_bkn.LicitacionItem.select().order_by(models_bkn.LicitacionItem.id)[0].id
assert models_old.TenderItems.select().order_by(-models_old.TenderItems.id)[0].id == models_bkn.LicitacionItem.select().order_by(-models_bkn.LicitacionItem.id)[0].id
print "OK"
print "Inserted %d rows" % models_bkn.LicitacionItem.select().count()

print "Time: %d seconds" % (time.time()-t)

print "\nCreating indexes for TS..."
with psycopg2.connect(database=db_name, host=db_host, user=db_user, password=db_pass) as connection:
    cursor = connection.cursor()
    cursor.execute(sql_to_str("sql/20-ts_index.sql"))

print "Time: %d seconds" % (time.time()-t)

print "\nBuilding stats..."
with psycopg2.connect(database=db_name, host=db_host, user=db_user, password=db_pass) as connection:
    cursor = connection.cursor()
    cursor.execute(sql_to_str("sql/30-stats.sql"))

print "Time: %d seconds" % (time.time()-t)

print "DONE"
