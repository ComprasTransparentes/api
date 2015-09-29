-- ############################################################################
-- TS INDEX TS INDEX TS INDEX TS INDEX TS INDEX TS INDEX TS INDEX TS INDEX TS I
-- ############################################################################

DROP INDEX IF EXISTS bkn.ts_licitacion;
DROP INDEX IF EXISTS bkn.ts_comprador;

DROP INDEX IF EXISTS bkn.ts_licitacion_nombre;
DROP INDEX IF EXISTS bkn.ts_licitacion_descripcion;
DROP INDEX IF EXISTS bkn.ts_comprador_nombre_comprador;
DROP INDEX IF EXISTS bkn.ts_comprador_nombre_unidad;
DROP INDEX IF EXISTS bkn.ts_proveedor_nombre;
DROP INDEX IF EXISTS bkn.ts_proveedor_rut;

-- CREATE INDEX ts_licitacion ON bkn.licitacion USING gin(to_tsvector('spanish', COALESCE(nombre, '') || ' ' ||  COALESCE(descripcion, '')));
-- CREATE INDEX ts_comprador ON bkn.comprador USING gin(to_tsvector('spanish', COALESCE(nombre_comprador, '') || ' ' || COALESCE(nombre_unidad, '')));

-- CREATE INDEX ts_licitacion_nombre ON bkn.licitacion USING gin(to_tsvector('spanish', COALESCE(nombre, '')));
-- CREATE INDEX ts_licitacion_descripcion ON bkn.licitacion USING gin(to_tsvector('spanish', COALESCE(descripcion, '')));
-- CREATE INDEX ts_comprador_nombre_comprador ON bkn.comprador USING gin(to_tsvector('spanish', COALESCE(nombre_comprador, '')));
-- CREATE INDEX ts_comprador_nombre_unidad ON bkn.comprador USING gin(to_tsvector('spanish', COALESCE(nombre_unidad, '')));

CREATE INDEX ts_licitacion_nombre ON bkn.licitacion USING GIN (to_tsvector('spanish', nombre));
CREATE INDEX ts_licitacion_descripcion ON bkn.licitacion USING GIN (to_tsvector('spanish', descripcion));

CREATE INDEX ts_comprador_nombre_comprador ON bkn.comprador USING GIN (to_tsvector('spanish', nombre_comprador));
CREATE INDEX ts_comprador_nombre_unidad ON bkn.comprador USING GIN (to_tsvector('spanish', nombre_unidad));

CREATE INDEX ts_proveedor_nombre ON bkn.proveedor USING GIN (to_tsvector('spanish', nombre));
CREATE INDEX ts_proveedor_rut ON bkn.proveedor USING GIN (to_tsvector('spanish', rut));

