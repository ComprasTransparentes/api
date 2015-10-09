-- ##############
-- ### CREATE ###
-- ##############

-- licitacion

CREATE TABLE bkn.licitacion (
    id                            SERIAL       NOT NULL PRIMARY KEY,
    codigo                        VARCHAR(255) NOT NULL,
    nombre                        TEXT         NOT NULL,
    descripcion                   TEXT         NOT NULL,
    tipo                          VARCHAR(255),
    nombre_responsable_contrato   VARCHAR(255) NOT NULL,
    email_responsable_contrato    VARCHAR(255) NOT NULL,
    telefono_responsable_contrato VARCHAR(255) NOT NULL,
    nombre_responsable_pago       VARCHAR(255) NOT NULL,
    email_responsable_pago        VARCHAR(255) NOT NULL,
    fecha_creacion                TIMESTAMP,
    fecha_cierre                  TIMESTAMP,
    fecha_inicio                  TIMESTAMP,
    fecha_final                   TIMESTAMP,
    fecha_publicacion             TIMESTAMP,
    fecha_publicacion_respuesta   TIMESTAMP,
    fecha_acto_apertura_tecnica   TIMESTAMP,
    fecha_acto_apertura_economica TIMESTAMP,
    fecha_estimada_adjudicacion   TIMESTAMP,
    fecha_adjudicacion            TIMESTAMP,
    fecha_soporte_fisico          TIMESTAMP,
    fecha_tiempo_evaluacion       TIMESTAMP,
    fecha_estimada_firma          TIMESTAMP,
    fecha_usuario                 TIMESTAMP,
    fecha_visita_terreno          TIMESTAMP,
    fecha_entrega_antecedentes    TIMESTAMP,
    adjudicacion_id               INTEGER,
    comprador_id                  INTEGER      NOT NULL,
    jerarquia                     INTEGER      NOT NULL,
    FOREIGN KEY (adjudicacion_id) REFERENCES bkn.adjudicacion (id) ON DELETE CASCADE,
    FOREIGN KEY (comprador_id) REFERENCES bkn.comprador (id) ON DELETE CASCADE;

CREATE INDEX licitacion_adjudicacion_id ON bkn.licitacion (adjudicacion_id);
CREATE INDEX licitacion_comprador_id ON bkn.licitacion (comprador_id);

-- licitacion_estado

CREATE TABLE bkn.licitacion_estado (
    id            SERIAL  NOT NULL PRIMARY KEY,
    licitacion_id INTEGER NOT NULL,
    estado        INTEGER NOT NULL,
    fecha         DATE    NOT NULL,
    FOREIGN KEY (licitacion_id) REFERENCES bkn.licitacion (id) ON DELETE CASCADE
);
CREATE INDEX licitacion_estado_licitacion_id ON bkn.licitacion_estado (licitacion_id);
CREATE INDEX licitacion_estado_fecha ON bkn.licitacion_estado (fecha);
CREATE INDEX licitacion_estado_estado ON bkn.licitacion_estado (estado);

-- licitacion_estado

CREATE TABLE bkn.licitacion_item (
    id               SERIAL  NOT NULL PRIMARY KEY,
    adjudicacion_id  INTEGER,
    cantidad         INTEGER NOT NULL,
    codigo_categoria INTEGER NOT NULL,
    codigo_producto  INTEGER NOT NULL,
    descripcion      TEXT    NOT NULL,
    licitacion_id    INTEGER NOT NULL,
    nombre_categoria TEXT    NOT NULL,
    nombre_producto  TEXT    NOT NULL,
    unidad           VARCHAR(255),
    FOREIGN KEY (adjudicacion_id) REFERENCES bkn.adjudicacion_item (id) ON DELETE CASCADE,
    FOREIGN KEY (licitacion_id) REFERENCES bkn.licitacion (id) ON DELETE CASCADE
);
CREATE INDEX licitacion_item_adjudicacion_id ON bkn.licitacion_item (adjudicacion_id);
CREATE INDEX licitacion_item_licitacion_id ON bkn.licitacion_item (licitacion_id);

-- adjudicacion

CREATE TABLE bkn.adjudicacion (
    id               SERIAL NOT NULL PRIMARY KEY,
    fecha            TIMESTAMP,
    numero           VARCHAR(255),
    numero_oferentes INTEGER,
    tipo             INTEGER
);

-- adjudicacion_item

CREATE TABLE bkn.adjudicacion_item (
    id             SERIAL  NOT NULL PRIMARY KEY,
    cantidad       REAL    NOT NULL,
    monto_unitario REAL    NOT NULL,
    proveedor_id   INTEGER NOT NULL,
    FOREIGN KEY (proveedor_id) REFERENCES bkn.proveedor (id)
);
CREATE INDEX adjudicacion_item_proveedor_id ON bkn.adjudicacion_item (proveedor_id);

-- proveedor

CREATE TABLE bkn.proveedor (
    id     SERIAL NOT NULL PRIMARY KEY,
    nombre VARCHAR(255),
    rut    VARCHAR(255)
);

-- comprador

CREATE TABLE bkn.comprador (
    id               SERIAL       NOT NULL PRIMARY KEY,
    jerarquia_id     INTEGER      NOT NULL,
    categoria        VARCHAR(255) NOT NULL,
    codigo_comprador INTEGER      NOT NULL,
    nombre_comprador VARCHAR(255) NOT NULL,
    codigo_unidad    INTEGER      NOT NULL,
    nombre_unidad    VARCHAR(255) NOT NULL,
    rut_unidad       VARCHAR(255) NOT NULL,
    direccion_unidad VARCHAR(255) NOT NULL,
    comuna_unidad    VARCHAR(255) NOT NULL,
    region_unidad    VARCHAR(255) NOT NULL,
    codigo_usuario   INTEGER      NOT NULL,
    nombre_usuario   VARCHAR(255) NOT NULL,
    cargo_usuario    VARCHAR(255) NOT NULL,
    rut_usuario      VARCHAR(255) NOT NULL
);

-- ##############
-- ### INSERT ###
-- ##############

INSERT INTO bkn.comprador (id, categoria, jerarquia_id, codigo_comprador, nombre_comprador, codigo_unidad, nombre_unidad, rut_unidad, direccion_unidad, comuna_unidad, region_unidad, codigo_usuario, nombre_usuario, rut_usuario, cargo_usuario)
    SELECT
        t2.id,
        t3.ministerio_nombre,
        t3.catalogo_organismo_id,
        CAST(t3.organismo_codigo AS INTEGER),
        t3.organismo_nombre,
        CAST(t2.codigo_unidad AS INTEGER),
        t2.nombre_unidad,
        t2.rut_unidad,
        t2.direccion_unidad,
        t2.comuna_unidad,
        t2.region_unidad,
        CAST(t2.codigo_usuario AS INTEGER),
        t2.nombre_usuario,
        t2.rut_usuario,
        t2.cargo_usuario
    FROM
        public.public_companies AS t2
        INNER JOIN public._jerarquia AS t3
            ON (t2.code = t3.organismo_codigo);


INSERT INTO bkn.adjudicacion (id, tipo, numero, numero_oferentes, fecha)
    SELECT
        t2.id,
        t2.tipo,
        t2.numero,
        t2.numero_oferentes,
        CAST(t2.fecha AS TIMESTAMP)
    FROM public.adjudications AS t2
    ORDER BY t2.id;

INSERT INTO bkn.proveedor (id, rut, nombre)
    SELECT
        t2.id,
        t2.rut_sucursal,
        t2.nombre
    FROM public.companies AS t2
    WHERE (t2.id != 11917)
    ORDER BY t2.id;

INSERT INTO bkn.adjudicacion_item (id, cantidad, monto_unitario, proveedor_id)
    SELECT
        t2.id,
        CAST(t2.cantidad AS FLOAT),
        CAST(t2.monto_unitario AS FLOAT),
        t2.company_id
    FROM
        public.adjudication_items AS t2
        INNER JOIN bkn.proveedor AS t3
            ON (t3.id = t2.company_id)
    ORDER BY t2.id;

INSERT INTO bkn.licitacion (id, codigo, nombre, descripcion, tipo, nombre_responsable_contrato, email_responsable_contrato, telefono_responsable_contrato, nombre_responsable_pago, email_responsable_pago, comprador_id, jerarquia, adjudicacion_id, fecha_creacion, fecha_cierre, fecha_inicio, fecha_final, fecha_publicacion, fecha_publicacion_respuesta, fecha_acto_apertura_tecnica, fecha_acto_apertura_economica, fecha_estimada_adjudicacion, fecha_estimada_firma, fecha_adjudicacion, fecha_soporte_fisico, fecha_tiempo_evaluacion, fecha_usuario, fecha_visita_terreno, fecha_entrega_antecedentes)
    SELECT
        t2.id,
        t2.code,
        t2.name,
        t2.descripcion,
        t2.tipo,
        t2.nombre_responsable_contrato,
        t2.email_responsable_contrato,
        t2.fono_responsable_contrato,
        t2.nombre_responsable_pago,
        t2.email_responsable_pago,
        t2.buyer_id,
        t4.catalogo_organismo_id,
        t5.id,
        CAST(t6.fecha_creacion AS TIMESTAMP),
        CAST(t6.fecha_cierre AS TIMESTAMP),
        CAST(t6.fecha_inicio AS TIMESTAMP),
        CAST(t6.fecha_final AS TIMESTAMP),
        CAST(t6.fecha_publicacion AS TIMESTAMP),
        CAST(t6.fecha_pub_respuestas AS TIMESTAMP),
        CAST(t6.fecha_acto_apertura_tecnica AS TIMESTAMP),
        CAST(t6.fecha_acto_apertura_economica AS TIMESTAMP),
        CAST(t6.fecha_estimada_adjudicacion AS TIMESTAMP),
        CAST(t6.fecha_estimada_firma AS TIMESTAMP),
        CAST(t6.fecha_adjudicacion AS TIMESTAMP),
        CAST(t6.fecha_soporte_fisico AS TIMESTAMP),
        CAST(t6.fecha_tiempo_evaluacion AS TIMESTAMP),
        CAST(t6.fechas_usuario AS TIMESTAMP),
        CAST(t6.fecha_visita_terreno AS TIMESTAMP),
        CAST(t6.fecha_entrega_antecedentes AS TIMESTAMP)
    FROM
        public.tenders AS t2
        INNER JOIN bkn.comprador AS t3
            ON (t3.id = t2.buyer_id)
        INNER JOIN public._jerarquia AS t4
            ON (t3.codigo_comprador = CAST(t4.organismo_codigo AS INTEGER))
        LEFT OUTER JOIN public.adjudications AS t5
            ON (t2.id = t5.tender_id)
        LEFT OUTER JOIN public.tender_dates AS t6
            ON (t2.id = t6.id)
    ORDER BY
        t2.id;

INSERT INTO bkn.licitacion_estado (id, licitacion_id, estado, fecha)
    SELECT
        t2.id,
        t2.tender_id,
        t2.state,
        to_date(t2.date, 'DDMMYYY')
    FROM
        public.tender_states AS t2
        INNER JOIN bkn.licitacion AS t3
            ON (t3.id = t2.tender_id)
    ORDER BY t2.id;

INSERT INTO bkn.licitacion_item (id, licitacion_id, adjudicacion_id, codigo_categoria, nombre_categoria, codigo_producto, nombre_producto, descripcion, unidad, cantidad)
    SELECT
        t2.id,
        t2.tender_id,
        t4.id,
        CAST(t2.codigo_categoria AS INT),
        t2.categoria,
        CAST(t2.codigo_producto AS INT),
        t2.nombre_producto,
        t2.descripcion,
        t2.unidad_medida,
        CAST(t2.cantidad AS INT)
    FROM
        public.tender_items AS t2
        INNER JOIN bkn.licitacion AS t3
            ON (t2.tender_id = t3.id)
        LEFT OUTER JOIN public.adjudication_items AS t4
            ON (t2.id = t4.tender_item_id)
    ORDER BY t2.id;
