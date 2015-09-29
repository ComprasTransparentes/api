-- ############################################################################
-- STATS STATS STATS STATS STATS STATS STATS STATS STATS STATS STATS STATS STAT
-- ############################################################################

DROP SCHEMA IF EXISTS stats CASCADE;
CREATE SCHEMA stats;

-- Currency

DROP TABLE IF EXISTS currency;

CREATE TABLE currency
(
    id          INTEGER NOT NULL,
    moneda      CHARACTER VARYING,
    tipo_cambio INTEGER,
    CONSTRAINT currency_pkey PRIMARY KEY (id)
);

INSERT INTO currency (id, moneda, tipo_cambio)
VALUES
    (1, 'CLP', 1),
    (2, 'USD', 650),
    (3, 'CLF', 1),
    (4, 'UTM', 43000),
    (5, 'EUR', 750);

-- Gastos por ministerio, por ano y por region

DROP TABLE IF EXISTS stats.gasto_organismo;

CREATE TABLE stats.gasto_organismo
(
    nombre_categoria CHARACTER VARYING,
    nombre_organismo CHARACTER VARYING,
    monto            BIGINT,
    region           CHARACTER VARYING,
    ano              INTEGER
);

INSERT INTO stats.gasto_organismo
    SELECT
        XY.nombre_categoria                              AS nombre_categoria,
        XY.nombre_organismo                              AS nombre_organismo,
        sum(Q.itemQuant * QQ.tipo_cambio)                AS totalMonto,
        MM.region_unidad                                 AS region,
        cast(substring(S.fecha FROM 5 FOR 4) AS INTEGER) AS year
    FROM
        (
            SELECT
                sum(CASE WHEN cast(B.cantidad AS FLOAT) < cast(A.cantidad AS FLOAT)
                    THEN cast(B.cantidad AS FLOAT)
                    ELSE cast(A.cantidad AS FLOAT) END * cast(A.monto_unitario AS FLOAT)) AS itemQuant,
                B.tender_id                                                               AS tenderId
            FROM adjudication_items A
                LEFT JOIN tender_items B
                    ON A.tender_item_id = B.id
            GROUP BY B.tender_id
        ) Q
        LEFT JOIN tenders R
            ON R.id = Q.tenderId
        INNER JOIN
        (
            SELECT
                tender_id AS tenderId,
                date      AS fecha
            FROM tender_states
            WHERE state = '8'
        ) S
            ON Q.tenderId = S.tenderId
        LEFT JOIN public_companies MM
            ON MM.id = R.buyer_id
        LEFT JOIN currency QQ
            ON QQ.moneda = R.moneda
        INNER JOIN jerarquia XY
            ON MM.code :: INTEGER = XY.codigo_organismo
    GROUP BY XY.nombre_categoria, XY.nombre_organismo, MM.region_unidad,
        cast(substring(S.fecha FROM 5 FOR 4) AS INTEGER)
    ORDER BY XY.nombre_categoria,
        MM.region_unidad, cast(substring(S.fecha FROM 5 FOR 4) AS INTEGER);

-- RANKING DE EMPRESAS QUE MAS DINERO HAN CONSEGUIDO

DROP TABLE IF EXISTS stats.ventas_proveedor;

CREATE TABLE stats.ventas_proveedor
(
    id_proveedor INTEGER,
    monto        BIGINT
);

INSERT INTO stats.ventas_proveedor
    SELECT
        AA.companyId       AS companyId,
        sum(AA.totalMonto) AS monto
    FROM
        (
            SELECT
                Q.tenderId                                       AS tenderId,
                Q.itemId                                         AS itemId,
                Q.companyId                                      AS companyId,
                Q.itemQuant * Q.itemMontoUnit * QQ.tipo_cambio   AS totalMonto,
                cast(substring(S.fecha FROM 1 FOR 2) AS INTEGER) AS day,
                cast(substring(S.fecha FROM 3 FOR 2) AS INTEGER) AS month,
                cast(substring(S.fecha FROM 5 FOR 4) AS INTEGER) AS year
            FROM
                (
                    SELECT
                        A.id                               AS itemId,
                        CASE WHEN cast(B.cantidad AS FLOAT) < cast(A.cantidad AS FLOAT)
                            THEN cast(B.cantidad AS FLOAT)
                        ELSE cast(A.cantidad AS FLOAT) END AS itemQuant,
                        cast(A.monto_unitario AS FLOAT)    AS itemMontoUnit,
                        A.company_id                       AS companyId,
                        B.tender_id                        AS tenderId
                    FROM adjudication_items A
                        LEFT JOIN tender_items B
                            ON A.tender_item_id = B.id
                ) Q
                LEFT JOIN tenders R
                    ON R.id = Q.tenderId
                LEFT JOIN
                (
                    SELECT
                        tender_id AS tenderId,
                        date      AS fecha
                    FROM tender_states
                    WHERE state = '8'
                ) S
                    ON Q.tenderId = S.tenderId
                LEFT JOIN currency QQ
                    ON QQ.moneda = R.moneda
        ) AA
    GROUP BY AA.companyId
    ORDER BY sum(AA.totalMonto) DESC
    LIMIT 15;

-- RANKING DE EMPRESAS QUE MAS DINERO HAN CONSEGUIDO EN CADA SEMESTRE

-- nota: cambiar la asignacion de semestres al pasar a produccion

DROP TABLE IF EXISTS temp1;
DROP TABLE IF EXISTS stats.ventas_proveedor_semestre;

CREATE TABLE stats.ventas_proveedor_semestre
(
    id_proveedor CHARACTER VARYING,
    monto        BIGINT,
    semestre     CHARACTER VARYING
);

CREATE TEMP TABLE temp1 AS
    SELECT
        AA.companyId       AS companyId,
        sum(AA.totalMonto) AS monto,
        CASE WHEN day = '10'
            THEN 'S1'
        WHEN day = '11'
            THEN 'S2'
        ELSE 'S3' END      AS Semestre
    FROM
        (
            SELECT
                Q.tenderId                                       AS tenderId,
                Q.itemId                                         AS itemId,
                Q.companyId                                      AS companyId,
                Q.itemQuant * Q.itemMontoUnit * QQ.tipo_cambio   AS totalMonto,
                cast(substring(S.fecha FROM 1 FOR 2) AS INTEGER) AS day,
                cast(substring(S.fecha FROM 3 FOR 2) AS INTEGER) AS month,
                cast(substring(S.fecha FROM 5 FOR 4) AS INTEGER) AS year
            FROM
                (
                    SELECT
                        A.id                               AS itemId,
                        CASE WHEN cast(B.cantidad AS FLOAT) < cast(A.cantidad AS FLOAT)
                            THEN cast(B.cantidad AS FLOAT)
                        ELSE cast(A.cantidad AS FLOAT) END AS itemQuant,
                        cast(A.monto_unitario AS FLOAT)    AS itemMontoUnit,
                        A.company_id                       AS companyId,
                        B.tender_id                        AS tenderId
                    FROM adjudication_items A
                        LEFT JOIN tender_items B
                            ON A.tender_item_id = B.id
                ) Q
                LEFT JOIN tenders R
                    ON R.id = Q.tenderId
                LEFT JOIN (
                              SELECT
                                  tender_id AS tenderId,
                                  date      AS fecha
                              FROM tender_states
                              WHERE state = '8'
                          ) S
                    ON Q.tenderId = S.tenderId
                LEFT JOIN currency QQ
                    ON QQ.moneda = R.moneda
        ) AA
    GROUP BY AA.companyId, Semestre;


INSERT INTO stats.ventas_proveedor_semestre
    (
        SELECT *
        FROM temp1
        WHERE semestre = 'S1'
        ORDER BY monto DESC
        LIMIT 15
    )
    UNION ALL
    (
        SELECT *
        FROM temp1
        WHERE semestre = 'S2'
        ORDER BY monto DESC
        LIMIT 15
    )
    UNION ALL
    (
        SELECT *
        FROM temp1
        WHERE semestre = 'S3'
        ORDER BY monto DESC
        LIMIT 15
    );


-- RANKING NACIONAL DE LICITACIONES

-- para obtener los resultados a nivel de ventana temporal, es necesario filtrar mediante un WHERE 

DROP TABLE IF EXISTS stats.top_licitaciones;

CREATE TABLE stats.top_licitaciones
(
    codigo_licitacion CHARACTER VARYING,
    monto             BIGINT
);

INSERT INTO stats.top_licitaciones
    SELECT
        MA.tenderCode,
        MA.totalMonto
    FROM
        (
            SELECT
                Q.tenderId                                       AS tenderId,
                Q.itemQuant * QQ.tipo_cambio                     AS totalMonto,
                R.code                                           AS tenderCode,
                cast(substring(S.fecha FROM 1 FOR 2) AS INTEGER) AS day,
                cast(substring(S.fecha FROM 3 FOR 2) AS INTEGER) AS month,
                cast(substring(S.fecha FROM 5 FOR 4) AS INTEGER) AS year,
                MM.code,
                MM.nombre,
                MM.codigo_unidad,
                MM.nombre_unidad,
                MM.comuna_unidad,
                MM.region_unidad
            FROM
                (
                    SELECT
                        sum(CASE WHEN cast(B.cantidad AS FLOAT) < cast(A.cantidad AS FLOAT)
                            THEN cast(B.cantidad AS FLOAT)
                            ELSE cast(A.cantidad AS FLOAT) END * cast(A.monto_unitario AS FLOAT)) AS itemQuant,
                        B.tender_id                                                               AS tenderId
                    FROM adjudication_items A
                        LEFT JOIN tender_items B
                            ON A.tender_item_id = B.id
                    GROUP BY B.tender_id
                ) Q
                LEFT JOIN tenders R
                    ON R.id = Q.tenderId
                LEFT JOIN
                (
                    SELECT
                        tender_id AS tenderId,
                        date      AS fecha
                    FROM tender_states
                    WHERE state = '8'
                ) S
                    ON Q.tenderId = S.tenderId
                LEFT JOIN public_companies MM
                    ON MM.id = Q.tenderId
                LEFT JOIN currency QQ
                    ON QQ.moneda = R.moneda
        ) MA
    ORDER BY MA.totalMonto DESC
    LIMIT 15;
-- WHERE dia = '10'

-- RANKING DE LICITACIONES A NIVEL DE REGION

-- para obtener los resultados a nivel de ventana temporal, es necesario filtrar
-- mediante un WHERE 

DROP TABLE IF EXISTS temp2;
DROP TABLE IF EXISTS stats.top_licitaciones_region;

CREATE TABLE stats.top_licitaciones_region
(
    codigo_licitacion CHARACTER VARYING,
    semestre          CHARACTER VARYING,
    monto             BIGINT,
    region            CHARACTER VARYING,
    codigo_region          CHARACTER VARYING
);

CREATE TEMP TABLE temp2 AS
    SELECT
        MA.tenderCode,
        CASE WHEN month <= '6' AND year = '2013'
            THEN 'S1'
        WHEN month > '6' AND year = '2013'
            THEN 'S2'
        WHEN month <= '6' AND year = '2014'
            THEN 'S3'
        WHEN month > '6' AND year = '2014'
            THEN 'S4'
        WHEN month <= '6' AND year = '2015'
            THEN 'S5'
        ELSE 'S6' END AS Semestre,
        MA.totalMonto,
        MA.region_unidad
    FROM (
             SELECT
                 Q.tenderId                                       AS tenderId,
                 Q.itemQuant * QQ.tipo_cambio                     AS totalMonto,
                 R.code                                           AS tenderCode,
                 cast(substring(S.fecha FROM 1 FOR 2) AS INTEGER) AS day,
                 cast(substring(S.fecha FROM 3 FOR 2) AS INTEGER) AS month,
                 cast(substring(S.fecha FROM 5 FOR 4) AS INTEGER) AS year,
                 MM.code,
                 MM.nombre,
                 MM.codigo_unidad,
                 MM.nombre_unidad,
                 MM.comuna_unidad,
                 MM.region_unidad
             FROM (
                      SELECT
                          sum(CASE WHEN cast(B.cantidad AS FLOAT) < cast(A.cantidad AS FLOAT)
                              THEN cast(B.cantidad AS FLOAT)
                              ELSE cast(A.cantidad AS FLOAT) END * cast(A.monto_unitario AS FLOAT)) AS itemQuant,
                          B.tender_id                                                               AS tenderId
                      FROM adjudication_items A
                          LEFT JOIN tender_items B
                              ON A.tender_item_id = B.id
                      GROUP BY B.tender_id
                  ) Q
                 LEFT JOIN tenders R
                     ON R.id = Q.tenderId
                 LEFT JOIN (
                               SELECT
                                   tender_id AS tenderId,
                                   date      AS fecha
                               FROM tender_states
                               WHERE state = '8'
                           ) S
                     ON Q.tenderId = S.tenderId
                 LEFT JOIN public_companies MM
                     ON MM.id = R.buyer_id
                 LEFT JOIN currency QQ
                     ON QQ.moneda = R.moneda
         ) MA
    ORDER BY MA.region_unidad, MA.totalMonto DESC;

INSERT INTO stats.top_licitaciones_region (codigo_licitacion, semestre, monto, region, codigo_region)
    (SELECT *, '3
'     FROM temp2
     WHERE semestre = 'S1' AND region_unidad = 'Región de Atacama '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '3
'     FROM temp2
     WHERE semestre = 'S2' AND region_unidad = 'Región de Atacama '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '3
'     FROM temp2
     WHERE semestre = 'S3' AND region_unidad = 'Región de Atacama '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '3
'     FROM temp2
     WHERE semestre = 'S4' AND region_unidad = 'Región de Atacama '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '3
'     FROM temp2
     WHERE semestre = 'S5' AND region_unidad = 'Región de Atacama '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '3
'     FROM temp2
     WHERE semestre = 'S6' AND region_unidad = 'Región de Atacama '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '2
'     FROM temp2
     WHERE semestre = 'S1' AND region_unidad = 'Región de Antofagasta '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '2
'     FROM temp2
     WHERE semestre = 'S2' AND region_unidad = 'Región de Antofagasta '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '2
'     FROM temp2
     WHERE semestre = 'S3' AND region_unidad = 'Región de Antofagasta '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '2
'     FROM temp2
     WHERE semestre = 'S4' AND region_unidad = 'Región de Antofagasta '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '2
'     FROM temp2
     WHERE semestre = 'S5' AND region_unidad = 'Región de Antofagasta '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '2
'     FROM temp2
     WHERE semestre = 'S6' AND region_unidad = 'Región de Antofagasta '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '4
'     FROM temp2
     WHERE semestre = 'S1' AND region_unidad = 'Región de Coquimbo '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '4
'     FROM temp2
     WHERE semestre = 'S2' AND region_unidad = 'Región de Coquimbo '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '4
'     FROM temp2
     WHERE semestre = 'S3' AND region_unidad = 'Región de Coquimbo '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '4
'     FROM temp2
     WHERE semestre = 'S4' AND region_unidad = 'Región de Coquimbo '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '4
'     FROM temp2
     WHERE semestre = 'S5' AND region_unidad = 'Región de Coquimbo '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '4
'     FROM temp2
     WHERE semestre = 'S6' AND region_unidad = 'Región de Coquimbo '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '8
'     FROM temp2
     WHERE semestre = 'S1' AND region_unidad = 'Región del Biobío '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '8
'     FROM temp2
     WHERE semestre = 'S2' AND region_unidad = 'Región del Biobío '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '8
'     FROM temp2
     WHERE semestre = 'S3' AND region_unidad = 'Región del Biobío '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '8
'     FROM temp2
     WHERE semestre = 'S4' AND region_unidad = 'Región del Biobío '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '8
'     FROM temp2
     WHERE semestre = 'S5' AND region_unidad = 'Región del Biobío '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '8
'     FROM temp2
     WHERE semestre = 'S6' AND region_unidad = 'Región del Biobío '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '10'
     FROM temp2
     WHERE semestre = 'S1' AND region_unidad = 'Región de los Lagos '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '10'
     FROM temp2
     WHERE semestre = 'S2' AND region_unidad = 'Región de los Lagos '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '10'
     FROM temp2
     WHERE semestre = 'S3' AND region_unidad = 'Región de los Lagos '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '10'
     FROM temp2
     WHERE semestre = 'S4' AND region_unidad = 'Región de los Lagos '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '10'
     FROM temp2
     WHERE semestre = 'S5' AND region_unidad = 'Región de los Lagos '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '10'
     FROM temp2
     WHERE semestre = 'S6' AND region_unidad = 'Región de los Lagos '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '1
'     FROM temp2
     WHERE semestre = 'S1' AND region_unidad = 'Región de Tarapacá  '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '1
'     FROM temp2
     WHERE semestre = 'S2' AND region_unidad = 'Región de Tarapacá  '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '1
'     FROM temp2
     WHERE semestre = 'S3' AND region_unidad = 'Región de Tarapacá  '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '1
'     FROM temp2
     WHERE semestre = 'S4' AND region_unidad = 'Región de Tarapacá  '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '1
'     FROM temp2
     WHERE semestre = 'S5' AND region_unidad = 'Región de Tarapacá  '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '1
'     FROM temp2
     WHERE semestre = 'S6' AND region_unidad = 'Región de Tarapacá  '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '12'
     FROM temp2
     WHERE semestre = 'S1' AND region_unidad = 'Región de Magallanes y de la Antártica'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '12'
     FROM temp2
     WHERE semestre = 'S2' AND region_unidad = 'Región de Magallanes y de la Antártica'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '12'
     FROM temp2
     WHERE semestre = 'S3' AND region_unidad = 'Región de Magallanes y de la Antártica'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '12'
     FROM temp2
     WHERE semestre = 'S4' AND region_unidad = 'Región de Magallanes y de la Antártica'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '12'
     FROM temp2
     WHERE semestre = 'S5' AND region_unidad = 'Región de Magallanes y de la Antártica'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '12'
     FROM temp2
     WHERE semestre = 'S6' AND region_unidad = 'Región de Magallanes y de la Antártica'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, 'RM'
     FROM temp2
     WHERE semestre = 'S1' AND region_unidad = 'Región Metropolitana de Santiago'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, 'RM'
     FROM temp2
     WHERE semestre = 'S2' AND region_unidad = 'Región Metropolitana de Santiago'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, 'RM'
     FROM temp2
     WHERE semestre = 'S3' AND region_unidad = 'Región Metropolitana de Santiago'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, 'RM'
     FROM temp2
     WHERE semestre = 'S4' AND region_unidad = 'Región Metropolitana de Santiago'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, 'RM'
     FROM temp2
     WHERE semestre = 'S5' AND region_unidad = 'Región Metropolitana de Santiago'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, 'RM'
     FROM temp2
     WHERE semestre = 'S6' AND region_unidad = 'Región Metropolitana de Santiago'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '11'
     FROM temp2
     WHERE semestre = 'S1' AND region_unidad = 'Región Aysén del General Carlos Ibáñez del Campo'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '11'
     FROM temp2
     WHERE semestre = 'S2' AND region_unidad = 'Región Aysén del General Carlos Ibáñez del Campo'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '11'
     FROM temp2
     WHERE semestre = 'S3' AND region_unidad = 'Región Aysén del General Carlos Ibáñez del Campo'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '11'
     FROM temp2
     WHERE semestre = 'S4' AND region_unidad = 'Región Aysén del General Carlos Ibáñez del Campo'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '11'
     FROM temp2
     WHERE semestre = 'S5' AND region_unidad = 'Región Aysén del General Carlos Ibáñez del Campo'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '11'
     FROM temp2
     WHERE semestre = 'S6' AND region_unidad = 'Región Aysén del General Carlos Ibáñez del Campo'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '14'
     FROM temp2
     WHERE semestre = 'S1' AND region_unidad = 'Región de Los Ríos'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '14'
     FROM temp2
     WHERE semestre = 'S2' AND region_unidad = 'Región de Los Ríos'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '14'
     FROM temp2
     WHERE semestre = 'S3' AND region_unidad = 'Región de Los Ríos'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '14'
     FROM temp2
     WHERE semestre = 'S4' AND region_unidad = 'Región de Los Ríos'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '14'
     FROM temp2
     WHERE semestre = 'S5' AND region_unidad = 'Región de Los Ríos'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '14'
     FROM temp2
     WHERE semestre = 'S6' AND region_unidad = 'Región de Los Ríos'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '9'
     FROM temp2
     WHERE semestre = 'S1' AND region_unidad = 'Región de la Araucanía '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '9'
     FROM temp2
     WHERE semestre = 'S2' AND region_unidad = 'Región de la Araucanía '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '9'
     FROM temp2
     WHERE semestre = 'S3' AND region_unidad = 'Región de la Araucanía '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '9'
     FROM temp2
     WHERE semestre = 'S4' AND region_unidad = 'Región de la Araucanía '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '9'
     FROM temp2
     WHERE semestre = 'S5' AND region_unidad = 'Región de la Araucanía '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '9'
     FROM temp2
     WHERE semestre = 'S6' AND region_unidad = 'Región de la Araucanía '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '15'
     FROM temp2
     WHERE semestre = 'S1' AND region_unidad = 'Región de Arica y Parinacota'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '15'
     FROM temp2
     WHERE semestre = 'S2' AND region_unidad = 'Región de Arica y Parinacota'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '15'
     FROM temp2
     WHERE semestre = 'S3' AND region_unidad = 'Región de Arica y Parinacota'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '15'
     FROM temp2
     WHERE semestre = 'S4' AND region_unidad = 'Región de Arica y Parinacota'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '15'
     FROM temp2
     WHERE semestre = 'S5' AND region_unidad = 'Región de Arica y Parinacota'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '15'
     FROM temp2
     WHERE semestre = 'S6' AND region_unidad = 'Región de Arica y Parinacota'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '7'
     FROM temp2
     WHERE semestre = 'S1' AND region_unidad = 'Región del Maule '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '7'
     FROM temp2
     WHERE semestre = 'S2' AND region_unidad = 'Región del Maule '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '7'
     FROM temp2
     WHERE semestre = 'S3' AND region_unidad = 'Región del Maule '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '7'
     FROM temp2
     WHERE semestre = 'S4' AND region_unidad = 'Región del Maule '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '7'
     FROM temp2
     WHERE semestre = 'S5' AND region_unidad = 'Región del Maule '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '7'
     FROM temp2
     WHERE semestre = 'S6' AND region_unidad = 'Región del Maule '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '6'
     FROM temp2
     WHERE semestre = 'S1' AND region_unidad = 'Región del Libertador General Bernardo O´Higgins'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '6'
     FROM temp2
     WHERE semestre = 'S2' AND region_unidad = 'Región del Libertador General Bernardo O´Higgins'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '6'
     FROM temp2
     WHERE semestre = 'S3' AND region_unidad = 'Región del Libertador General Bernardo O´Higgins'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '6'
     FROM temp2
     WHERE semestre = 'S4' AND region_unidad = 'Región del Libertador General Bernardo O´Higgins'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '6'
     FROM temp2
     WHERE semestre = 'S5' AND region_unidad = 'Región del Libertador General Bernardo O´Higgins'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '6'
     FROM temp2
     WHERE semestre = 'S6' AND region_unidad = 'Región del Libertador General Bernardo O´Higgins'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '5'
     FROM temp2
     WHERE semestre = 'S1' AND region_unidad = 'Región de Valparaíso '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '5'
     FROM temp2
     WHERE semestre = 'S2' AND region_unidad = 'Región de Valparaíso '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '5'
     FROM temp2
     WHERE semestre = 'S3' AND region_unidad = 'Región de Valparaíso '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '5'
     FROM temp2
     WHERE semestre = 'S4' AND region_unidad = 'Región de Valparaíso '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '5'
     FROM temp2
     WHERE semestre = 'S5' AND region_unidad = 'Región de Valparaíso '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *, '5'
     FROM temp2
     WHERE semestre = 'S6' AND region_unidad = 'Región de Valparaíso '
     ORDER BY totalMonto DESC
     LIMIT 15);


-- RANKING DE CATEGORIAS EN LAS QUE SE GASTA MAS DINERO
-- la cantidad lleva un case debido a errores en la fuente

DROP TABLE IF EXISTS stats.top_categorias;

CREATE TABLE stats.top_categorias
(
    categoria CHARACTER VARYING,
    monto     BIGINT
);

INSERT INTO stats.top_categorias
    SELECT
        XYZ.categoria,
        sum(XYZ.totalMonto)
    FROM
        (
            SELECT
                Q.categoria,
                Q.itemQuant * Q.itemMontoUnit * QQ.tipo_cambio   AS totalMonto,
                cast(substring(S.fecha FROM 3 FOR 2) AS INTEGER) AS month,
                cast(substring(S.fecha FROM 5 FOR 4) AS INTEGER) AS year,
                MM.region_unidad                                 AS region
            FROM
                (
                    SELECT
                        A.id                               AS itemId,
                        CASE WHEN cast(B.cantidad AS FLOAT) < cast(A.cantidad AS FLOAT)
                            THEN cast(B.cantidad AS FLOAT)
                        ELSE cast(A.cantidad AS FLOAT) END AS itemQuant,
                        cast(A.monto_unitario AS FLOAT)    AS itemMontoUnit,
                        A.company_id                       AS companyId,
                        B.tender_id                        AS tenderId,
                        B.categoria                        AS categoria
                    FROM adjudication_items A
                        LEFT JOIN tender_items B
                            ON A.tender_item_id = B.id
                ) Q
                JOIN
                (
                    SELECT
                        tender_id AS tenderId,
                        date      AS fecha
                    FROM tender_states
                    WHERE state = '8'
                ) S
                    ON Q.tenderId = S.tenderId
                LEFT JOIN tenders R
                    ON R.id = Q.tenderId
                LEFT JOIN currency QQ
                    ON QQ.moneda = R.moneda
                LEFT JOIN public_companies MM
                    ON MM.id = Q.tenderId
        ) XYZ
    GROUP BY categoria
    ORDER BY sum(XYZ.totalMonto) DESC
    LIMIT 15;


-- RANKING DE CATEGORIAS EN LAS QUE SE GASTA MAS DINERO POR REGION Y SEMESTRE
-- la cantidad lleva un case debido a errores en la fuente

DROP TABLE IF EXISTS temp3;
DROP TABLE IF EXISTS stats.top_categorias_region_semestre;

CREATE TABLE stats.top_categorias_region_semestre
(
    categoria CHARACTER VARYING,
    monto     BIGINT,
    semestre  CHARACTER VARYING,
    region    CHARACTER VARYING
);

CREATE TEMP TABLE temp3 AS
    SELECT
        XYZ.categoria,
        sum(XYZ.totalMonto) AS totalMonto,
        CASE WHEN month <= '6' AND year = '2013'
            THEN 'S1'
        WHEN month > '6' AND year = '2013'
            THEN 'S2'
        WHEN month <= '6' AND year = '2014'
            THEN 'S3'
        WHEN month > '6' AND year = '2014'
            THEN 'S4'
        WHEN month <= '6' AND year = '2015'
            THEN 'S5'
        ELSE 'S6' END       AS Semestre,
        region_unidad
    FROM
        (
            SELECT
                Q.categoria,
                Q.itemQuant * Q.itemMontoUnit * QQ.tipo_cambio   AS totalMonto,
                cast(substring(S.fecha FROM 3 FOR 2) AS INTEGER) AS month,
                cast(substring(S.fecha FROM 5 FOR 4) AS INTEGER) AS year,
                MM.region_unidad                                 AS region_unidad
            FROM
                (
                    SELECT
                        A.id                               AS itemId,
                        CASE WHEN cast(B.cantidad AS FLOAT) < cast(A.cantidad AS FLOAT)
                            THEN cast(B.cantidad AS FLOAT)
                        ELSE cast(A.cantidad AS FLOAT) END AS itemQuant,
                        cast(A.monto_unitario AS FLOAT)    AS itemMontoUnit,
                        A.company_id                       AS companyId,
                        B.tender_id                        AS tenderId,
                        B.categoria                        AS categoria
                    FROM adjudication_items A
                        LEFT JOIN tender_items B
                            ON A.tender_item_id = B.id
                ) Q
                JOIN
                (
                    SELECT
                        tender_id AS tenderId,
                        date      AS fecha
                    FROM tender_states
                    WHERE state = '8'
                ) S
                    ON Q.tenderId = S.tenderId
                LEFT JOIN tenders R
                    ON R.id = Q.tenderId
                LEFT JOIN currency QQ
                    ON QQ.moneda = R.moneda
                LEFT JOIN public_companies MM
                    ON MM.id = Q.tenderId
        ) XYZ
    GROUP BY categoria, Semestre, region_unidad;


INSERT INTO stats.top_categorias_region_semestre
    (SELECT *
     FROM temp3
     WHERE semestre = 'S1' AND region_unidad = 'Región de Atacama '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S2' AND region_unidad = 'Región de Atacama '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S3' AND region_unidad = 'Región de Atacama '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S4' AND region_unidad = 'Región de Atacama '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S5' AND region_unidad = 'Región de Atacama '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S6' AND region_unidad = 'Región de Atacama '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S1' AND region_unidad = 'Región de Antofagasta '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S2' AND region_unidad = 'Región de Antofagasta '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S3' AND region_unidad = 'Región de Antofagasta '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S4' AND region_unidad = 'Región de Antofagasta '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S5' AND region_unidad = 'Región de Antofagasta '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S6' AND region_unidad = 'Región de Antofagasta '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S1' AND region_unidad = 'Región de Coquimbo '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S2' AND region_unidad = 'Región de Coquimbo '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S3' AND region_unidad = 'Región de Coquimbo '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S4' AND region_unidad = 'Región de Coquimbo '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S5' AND region_unidad = 'Región de Coquimbo '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S6' AND region_unidad = 'Región de Coquimbo '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S1' AND region_unidad = 'Región del Biobío '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S2' AND region_unidad = 'Región del Biobío '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S3' AND region_unidad = 'Región del Biobío '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S4' AND region_unidad = 'Región del Biobío '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S5' AND region_unidad = 'Región del Biobío '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S6' AND region_unidad = 'Región del Biobío '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S1' AND region_unidad = 'Región de los Lagos '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S2' AND region_unidad = 'Región de los Lagos '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S3' AND region_unidad = 'Región de los Lagos '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S4' AND region_unidad = 'Región de los Lagos '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S5' AND region_unidad = 'Región de los Lagos '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S6' AND region_unidad = 'Región de los Lagos '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S1' AND region_unidad = 'Región de Tarapacá  '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S2' AND region_unidad = 'Región de Tarapacá  '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S3' AND region_unidad = 'Región de Tarapacá  '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S4' AND region_unidad = 'Región de Tarapacá  '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S5' AND region_unidad = 'Región de Tarapacá  '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S6' AND region_unidad = 'Región de Tarapacá  '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S1' AND region_unidad = 'Región de Magallanes y de la Antártica'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S2' AND region_unidad = 'Región de Magallanes y de la Antártica'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S3' AND region_unidad = 'Región de Magallanes y de la Antártica'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S4' AND region_unidad = 'Región de Magallanes y de la Antártica'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S5' AND region_unidad = 'Región de Magallanes y de la Antártica'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S6' AND region_unidad = 'Región de Magallanes y de la Antártica'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S1' AND region_unidad = 'Región Metropolitana de Santiago'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S2' AND region_unidad = 'Región Metropolitana de Santiago'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S3' AND region_unidad = 'Región Metropolitana de Santiago'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S4' AND region_unidad = 'Región Metropolitana de Santiago'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S5' AND region_unidad = 'Región Metropolitana de Santiago'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S6' AND region_unidad = 'Región Metropolitana de Santiago'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S1' AND region_unidad = 'Región Aysén del General Carlos Ibáñez del Campo'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S2' AND region_unidad = 'Región Aysén del General Carlos Ibáñez del Campo'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S3' AND region_unidad = 'Región Aysén del General Carlos Ibáñez del Campo'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S4' AND region_unidad = 'Región Aysén del General Carlos Ibáñez del Campo'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S5' AND region_unidad = 'Región Aysén del General Carlos Ibáñez del Campo'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S6' AND region_unidad = 'Región Aysén del General Carlos Ibáñez del Campo'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S1' AND region_unidad = 'Región de Los Ríos'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S2' AND region_unidad = 'Región de Los Ríos'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S3' AND region_unidad = 'Región de Los Ríos'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S4' AND region_unidad = 'Región de Los Ríos'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S5' AND region_unidad = 'Región de Los Ríos'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S6' AND region_unidad = 'Región de Los Ríos'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S1' AND region_unidad = 'Región de la Araucanía '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S2' AND region_unidad = 'Región de la Araucanía '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S3' AND region_unidad = 'Región de la Araucanía '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S4' AND region_unidad = 'Región de la Araucanía '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S5' AND region_unidad = 'Región de la Araucanía '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S6' AND region_unidad = 'Región de la Araucanía '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S1' AND region_unidad = 'Región de Arica y Parinacota'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S2' AND region_unidad = 'Región de Arica y Parinacota'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S3' AND region_unidad = 'Región de Arica y Parinacota'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S4' AND region_unidad = 'Región de Arica y Parinacota'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S5' AND region_unidad = 'Región de Arica y Parinacota'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S6' AND region_unidad = 'Región de Arica y Parinacota'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S1' AND region_unidad = 'Región del Maule '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S2' AND region_unidad = 'Región del Maule '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S3' AND region_unidad = 'Región del Maule '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S4' AND region_unidad = 'Región del Maule '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S5' AND region_unidad = 'Región del Maule '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S6' AND region_unidad = 'Región del Maule '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S1' AND region_unidad = 'Región del Libertador General Bernardo O´Higgins'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S2' AND region_unidad = 'Región del Libertador General Bernardo O´Higgins'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S3' AND region_unidad = 'Región del Libertador General Bernardo O´Higgins'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S4' AND region_unidad = 'Región del Libertador General Bernardo O´Higgins'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S5' AND region_unidad = 'Región del Libertador General Bernardo O´Higgins'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S6' AND region_unidad = 'Región del Libertador General Bernardo O´Higgins'
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S1' AND region_unidad = 'Región de Valparaíso '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S2' AND region_unidad = 'Región de Valparaíso '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S3' AND region_unidad = 'Región de Valparaíso '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S4' AND region_unidad = 'Región de Valparaíso '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S5' AND region_unidad = 'Región de Valparaíso '
     ORDER BY totalMonto DESC
     LIMIT 15)
    UNION ALL
    (SELECT *
     FROM temp3
     WHERE semestre = 'S6' AND region_unidad = 'Región de Valparaíso '
     ORDER BY totalMonto DESC
     LIMIT 15);

-- LICITACIONES ADJUDICADAS CON : LICITACION, LICITACION_ITEM, COMPRADOR, PROVEEDOR, PROVEEDOR.NOMBRE, PROVEEDOR.RUT, MONTO

DROP TABLE IF EXISTS stats.licitacion_item_adjudicadas;

CREATE TABLE stats.licitacion_item_adjudicadas
(
    licitacion_item_id    INTEGER NOT NULL,
    licitacion_id         INTEGER NOT NULL,
    jerarquia_distinct_id INTEGER NOT NULL,
    codigo_categoria      INTEGER NOT NULL,
    nombre_categoria      CHARACTER VARYING,
    codigo_producto       INTEGER NOT NULL,
    nombre_producto       CHARACTER VARYING,
    proveedor_id          INTEGER NOT NULL,
    nombre_proveedor      CHARACTER VARYING,
    rut_proveedor         CHARACTER VARYING,
    monto                 BIGINT  NOT NULL
);

INSERT INTO stats.licitacion_item_adjudicadas (licitacion_item_id, licitacion_id, jerarquia_distinct_id, codigo_categoria, nombre_categoria, codigo_producto, nombre_producto, proveedor_id, nombre_proveedor, rut_proveedor, monto)
    SELECT DISTINCT
        B.id                                                                                  AS licitacion_item_id,
        R.id                                                                                  AS licitacion_id,
        FH.id                                                                                 AS jerarquia_distinct_id,
        B.codigo_categoria :: INTEGER                                                         AS codigo_categoria,
        B.categoria                                                                           AS nombre_categoria,
        B.codigo_producto                                                                     AS codigo_producto,
        B.nombre_producto                                                                     AS nombre_producto,
        A.company_id                                                                          AS proveedor_id,
        CC.nombre                                                                             AS nombre_proveedor,
        CC.rut_sucursal                                                                       AS rut_proveedor,
        CASE WHEN cast(B.cantidad AS FLOAT) < cast(A.cantidad AS FLOAT)
            THEN cast(B.cantidad AS FLOAT)
        ELSE cast(A.cantidad AS FLOAT) END * cast(A.monto_unitario AS FLOAT) * QQ.tipo_cambio AS monto
    FROM adjudication_items A
        LEFT JOIN tender_items B
            ON A.tender_item_id = B.id
        INNER JOIN (
                       SELECT
                           tender_id AS tenderId,
                           date      AS fecha
                       FROM tender_states
                       WHERE state = '8'
                   ) S
            ON B.tender_id = S.tenderId
        LEFT JOIN tenders R
            ON R.id = B.tender_id
        LEFT JOIN currency QQ
            ON QQ.moneda = R.moneda
        LEFT JOIN public_companies PC
            ON R.buyer_id = PC.id
        INNER JOIN jerarquia_distinct FH
            ON FH.codigo_organismo = PC.code :: INTEGER
        LEFT JOIN companies CC
            ON A.company_id = CC.id
    ORDER BY licitacion_id, licitacion_item_id;

-- Index: stats.index_lia_licitacion_item_id
CREATE UNIQUE INDEX index_lia_licitacion_item_id
  ON stats.licitacion_item_adjudicadas
  USING btree
  (licitacion_item_id);

-- Index: stats.index_lia_licitacion_id

CREATE INDEX index_lia_licitacion_id
  ON stats.licitacion_item_adjudicadas
  USING btree
  (licitacion_id);

-- Index: stats.index_lia_jerarquia_distinct_id

CREATE INDEX index_lia_jerarquia_distinct_id
  ON stats.licitacion_item_adjudicadas
  USING btree
  (jerarquia_distinct_id);

-- Index: stats.index_lia_proveedor_id

CREATE INDEX index_lia_proveedor_id
  ON stats.licitacion_item_adjudicadas
  USING btree
  (proveedor_id);

-- Index: stats.index_lia_monto

CREATE INDEX index_lia_monto
  ON stats.licitacion_item_adjudicadas
  USING btree
  (monto);



-- ESTADISTICAS PARA EL HOME

DROP TABLE IF EXISTS stats.sumario;

CREATE TABLE stats.sumario
(
    monto_transado BIGINT NOT NULL,
    n_licitaciones BIGINT NOT NULL,
    n_organismo    BIGINT NOT NULL,
    n_proveedores  BIGINT NOT NULL
);

INSERT INTO stats.sumario (monto_transado, n_licitaciones, n_organismo, n_proveedores)
    SELECT
        (SELECT SUM(A.monto)
         FROM
             (SELECT monto
              FROM stats.licitacion_item_adjudicadas) A) AS monto_transado,
        (SELECT COUNT(B)
         FROM
             (SELECT DISTINCT licitacion_id
              FROM stats.licitacion_item_adjudicadas) B) AS n_licitaciones,
        (SELECT COUNT(C)
         FROM
             (SELECT DISTINCT jerarquia_distinct_id
              FROM stats.licitacion_item_adjudicadas) C) AS n_organismo,
        COUNT(*)
    FROM
        (SELECT DISTINCT proveedor_id
         FROM stats.licitacion_item_adjudicadas) n_proveedores;



-- LICITACIONES ADJUDICADAS CON PRECIO, COMPRADOR, PROVEEDOR

-- DROP TABLE IF EXISTS stats.licitaciones_adjudicadas;
--
-- CREATE TABLE stats.licitaciones_adjudicadas
-- (
--     jerarquia_distinct_id INTEGER NOT NULL,
--     proveedor_id          INTEGER NOT NULL,
--     nombre_proveedor      CHARACTER VARYING,
--     rut_proveedor         CHARACTER VARYING,
--     n_licitaciones        BIGINT  NOT NULL,
--     monto                 BIGINT  NOT NULL
-- );
--
-- INSERT INTO stats.licitaciones_adjudicadas (jerarquia_distinct_id, proveedor_id, nombre_proveedor, rut_proveedor, n_licitaciones, monto)
--     SELECT
--         AAA.jerarquiaId     AS jerarquia_distinct_id,
--         AAA.company_id      AS proveedor_id,
--         BBB.nombre          AS nombre_proveedor,
--         BBB.rut_sucursal    AS rut_proveedor,
--         AAA.count           AS n_licitaciones,
--         BBB.monto :: BIGINT AS monto
--     FROM (
--              SELECT DISTINCT
--                  company_id,
--                  JerarquiaId,
--                  count(*)
--              FROM (
--                       SELECT
--                           A.company_id,
--                           CC.nombre,
--                           CC.rut_sucursal,
--                           FH.id                                                                                 AS JerarquiaId,
--                           CASE WHEN cast(B.cantidad AS FLOAT) < cast(A.cantidad AS FLOAT)
--                               THEN cast(B.cantidad AS FLOAT)
--                           ELSE cast(A.cantidad AS FLOAT) END * cast(A.monto_unitario AS FLOAT) * QQ.tipo_cambio AS monto
--                       FROM adjudication_items A
--                           LEFT JOIN tender_items B
--                               ON A.tender_item_id = B.id
--                           INNER JOIN (
--                                          SELECT
--                                              tender_id AS tenderId,
--                                              date      AS fecha
--                                          FROM tender_states
--                                          WHERE state = '8'
--                                      ) S
--                               ON B.tender_id = S.tenderId
--                           LEFT JOIN tenders R
--                               ON R.id = B.tender_id
--                           LEFT JOIN currency QQ
--                               ON QQ.moneda = R.moneda
--                           LEFT JOIN public_companies PC
--                               ON R.buyer_id = PC.id
--                           INNER JOIN jerarquia_distinct FH
--                               ON FH.codigo_organismo = PC.code :: INTEGER
--                           LEFT JOIN companies CC
--                               ON A.company_id = CC.id
--                       -- where PC.id = 520
--                       ORDER BY jerarquiaid, monto) ABC
--              GROUP BY company_id, JerarquiaId) AAA
--         LEFT JOIN (
--                       SELECT
--                           A.company_id,
--                           CC.nombre,
--                           CC.rut_sucursal,
--                           FH.id               AS JerarquiaId,
--                           sum(CASE WHEN cast(B.cantidad AS FLOAT) < cast(A.cantidad AS FLOAT)
--                               THEN cast(B.cantidad AS FLOAT)
--                               ELSE cast(A.cantidad AS FLOAT) END * cast(A.monto_unitario AS FLOAT) *
--                               QQ.tipo_cambio) AS monto
--                       FROM adjudication_items A
--                           LEFT JOIN tender_items B
--                               ON A.tender_item_id = B.id
--                           INNER JOIN (
--                                          SELECT
--                                              tender_id AS tenderId,
--                                              date      AS fecha
--                                          FROM tender_states
--                                          WHERE state = '8'
--                                      ) S
--                               ON B.tender_id = S.tenderId
--                           LEFT JOIN tenders R
--                               ON R.id = B.tender_id
--                           LEFT JOIN currency QQ
--                               ON QQ.moneda = R.moneda
--                           LEFT JOIN public_companies PC
--                               ON R.buyer_id = PC.id :: INTEGER
--                           INNER JOIN jerarquia_distinct FH
--                               ON FH.codigo_organismo = PC.code :: INTEGER
--                           LEFT JOIN companies CC
--                               ON A.company_id = CC.id
--                       --where FH.id = 664
--                       GROUP BY A.company_id, CC.nombre, CC.rut_sucursal, FH.id
--                       ORDER BY company_id) BBB
--             ON AAA.company_id = BBB.company_id AND AAA.JerarquiaId = BBB.JerarquiaId
