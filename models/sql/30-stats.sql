-- ############################################################################
-- STATS STATS STATS STATS STATS STATS STATS STATS STATS STATS STATS STATS STAT
-- ############################################################################

DROP SCHEMA IF EXISTS stats CASCADE;
CREATE SCHEMA stats;

-- CREO TABLA MASTER de LICITACIONES
-- tiene muchos campos! :O

DROP TABLE IF EXISTS stats.licitacion_master;

CREATE TABLE stats.licitacion_master AS
    SELECT
        t2.id            AS licitacion_id,
        t2.code          AS licitacion_codigo,
        t2.name          AS nombre,
        t2.descripcion,
        t2.tipo,
        t2.moneda,
        t2.nombre_responsable_contrato,
        t2.email_responsable_contrato,
        t2.fono_responsable_contrato,
        t2.nombre_responsable_pago,
        t2.email_responsable_pago,
        t2.buyer_id      AS organismo_id,
        t3.id            AS adjudicacion_id,
        t4.fecha_creacion :: TIMESTAMP,
        t4.fecha_cierre :: TIMESTAMP,
        t4.fecha_inicio :: TIMESTAMP,
        t4.fecha_final :: TIMESTAMP,
        t4.fecha_publicacion :: TIMESTAMP,
        t4.fecha_pub_respuestas :: TIMESTAMP,
        t4.fecha_acto_apertura_tecnica :: TIMESTAMP,
        t4.fecha_acto_apertura_economica :: TIMESTAMP,
        t4.fecha_estimada_adjudicacion :: TIMESTAMP,
        t4.fecha_estimada_firma :: TIMESTAMP,
        t4.fecha_adjudicacion :: TIMESTAMP,
        t4.fecha_soporte_fisico :: TIMESTAMP,
        t4.fecha_tiempo_evaluacion :: TIMESTAMP,
        t4.fechas_usuario :: TIMESTAMP,
        t4.fecha_visita_terreno :: TIMESTAMP,
        t4.fecha_entrega_antecedentes :: TIMESTAMP,
        t5.codigo_unidad,
        t5.nombre_unidad,
        t5.direccion_unidad,
        t5.comuna_unidad,
        t5.region_unidad,
        t5.rut_usuario,
        t5.codigo_usuario,
        t5.nombre_usuario,
        t5.cargo_usuario,
        t6.catalogo_organismo_id AS catalogo_organismo_id,
        t6.organismo_nombre      AS nombre_organismo,
        t6.organismo_nombre_corto AS nombre_organismo_plot,
        t6.ministerio_id,
        t6.ministerio_nombre          AS nombre_ministerio
    FROM public.tenders AS t2
        LEFT JOIN public.adjudications AS t3
            ON (t2.id = t3.tender_id)
        LEFT JOIN public.tender_dates AS t4
            ON (t2.id = t4.id)
        LEFT JOIN public_companies AS t5
            ON (t2.buyer_id = t5.id)
        INNER JOIN _jerarquia AS t6
            ON (t5.code = t6.organismo_codigo)
    ORDER BY t2.id;

-- CREO TABLA DE ITEM_LICITACION_ORGANISMO_EMPRESA
-- Es la mejor, por lo tanto se llama master plop!

DROP TABLE IF EXISTS stats.master_plop;

CREATE TABLE stats.master_plop AS
    SELECT DISTINCT
        B.id                                                                                  AS licitacion_item_id,
        R.licitacion_id,
        R.nombre as licitacion_nombre,
        R.descripcion as licitacion_descripcion,
        R.licitacion_codigo,
        R.fecha_creacion,
        R.catalogo_organismo_id                                                                        AS organismo_id,
        R.nombre_organismo_plot                                                               AS nombre_organismo,
        R.ministerio_id                                                                       AS ministerio_id,
        R.nombre_ministerio                                                                   AS nombre_ministerio,
        B.codigo_categoria :: INTEGER,
        XD.id                                                                                 AS categoria_id,
        XD.categoria_1                                                                        AS categoria_primer_nivel,
        XD.categoria_2                                                                        AS categoria_segundo_nivel,
        XD.categoria_3                                                                        AS categoria_tercer_nivel,
        B.codigo_producto,
        B.nombre_producto,
        A.company_id,
        CC.nombre,
        CC.rut_sucursal,
        R.region_unidad                                                                       AS region,
        CASE WHEN cast(B.cantidad AS FLOAT) < cast(A.cantidad AS FLOAT)
            THEN cast(B.cantidad AS FLOAT)
        ELSE cast(A.cantidad AS FLOAT) END * cast(A.monto_unitario AS FLOAT) * QQ.tipo_cambio AS monto,
        cast(substring(S.fecha FROM 3 FOR 2) AS INTEGER)                                      AS mes,
        cast(substring(S.fecha FROM 5 FOR 4) AS INTEGER)                                      AS ano
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
        inner JOIN stats.licitacion_master R
            ON R.licitacion_id = B.tender_id
        LEFT JOIN _currency QQ
            ON QQ.moneda = R.moneda
        LEFT JOIN companies CC
            ON A.company_id = CC.id
        INNER JOIN _categoria_producto XD
            ON B.categoria = XD.categoria
    ORDER BY licitacion_id, licitacion_item_id;

-- COMPARADOR

DROP TABLE IF EXISTS stats.ministerio_producto_stats;

CREATE TABLE stats.ministerio_producto_stats AS
    SELECT
        ministerio_id,
        categoria_id,
        categoria_tercer_nivel                                      AS categoria_nombre,
        nombre_ministerio                                           AS ministerio_nombre,
        monto_total,
        (monto_total / cantidad_licitaciones_adjudicadas) :: BIGINT AS monto_promedio,
        cantidad_proveedores                                        AS n_proveedores,
        cantidad_licitaciones_adjudicadas                           AS n_licitaciones_adjudicadas
    FROM (

             SELECT
                 categoria_id,
                 categoria_tercer_nivel,
                 ministerio_id,
                 nombre_ministerio,
                 sum(monto_total)                         AS monto_total,
                 count(cantidad_licitaciones_adjudicadas) AS cantidad_licitaciones_adjudicadas,
                 count(DISTINCT cantidad_proveedores)     AS cantidad_proveedores
             FROM
                 (
                     SELECT
                         categoria_id,
                         categoria_tercer_nivel,
                         ministerio_id,
                         nombre_ministerio,
                         monto :: BIGINT AS monto_total,
                         licitacion_id   AS cantidad_licitaciones_adjudicadas,
                         company_id      AS cantidad_proveedores
                     FROM stats.master_plop
                 ) AA
             GROUP BY categoria_id, categoria_tercer_nivel, ministerio_id, nombre_ministerio
         ) BB
    ORDER BY categoria_id, categoria_nombre, ministerio_id, ministerio_nombre;

-- CREO QUERIES PARA LAS VISUALIZACIONES DEL PRINCIPIO

-- MONTOS POR MINISTERIO, ORGANISMO

CREATE TABLE stats.ministerio_organismo_monto AS

    SELECT
        nombre_ministerio,
        nombre_organismo,
        sum(monto) AS monto
    FROM stats.master_plop
    GROUP BY nombre_ministerio, nombre_organismo
    ORDER BY monto DESC;

-- query 1 desagregada

-- MONTOS POR MINISTERIO, ORGANISMO, REGION, SEMESTRE

CREATE TABLE stats.ministerio_organismo_region_semestre_monto AS
    SELECT
        nombre_ministerio,
        nombre_organismo,
        sum(monto)    AS monto,
        CASE WHEN mes <= '6' AND ano = '2013'
            THEN 'S1'
        WHEN mes > '6' AND ano = '2013'
            THEN 'S2'
        WHEN mes <= '6' AND ano = '2014'
            THEN 'S3'
        WHEN mes > '6' AND ano = '2014'
            THEN 'S4'
        WHEN mes <= '6' AND ano = '2015'
            THEN 'S5'
        ELSE 'S6' END AS Semestre,
        region
    FROM stats.master_plop
    GROUP BY nombre_ministerio, nombre_organismo, semestre, region;

-- query 2

-- MONTOS POR PROVEEDOR

DROP TABLE IF EXISTS stats.proveedor_monto;

CREATE TABLE stats.proveedor_monto AS
    SELECT
        company_id,
        sum(coalesce(monto, 0)) AS monto
    FROM stats.master_plop
    GROUP BY company_id
    ORDER BY monto DESC
    LIMIT 5;

--query 2 desagregada

-- MONTOS POR PROVEEDOR, SEMESTRE

DROP TABLE IF EXISTS temp2;

CREATE TEMP TABLE temp2 AS
    SELECT
        company_id,
        sum(monto)    AS monto,
        CASE WHEN mes <= '6' AND ano = '2013'
            THEN 'S1'
        WHEN mes > '6' AND ano = '2013'
            THEN 'S2'
        WHEN mes <= '6' AND ano = '2014'
            THEN 'S3'
        WHEN mes > '6' AND ano = '2014'
            THEN 'S4'
        WHEN mes <= '6' AND ano = '2015'
            THEN 'S5'
        ELSE 'S6' END AS Semestre
    FROM stats.master_plop
    GROUP BY company_id, semestre;

CREATE TABLE stats.proveedor_region_semestre_monto AS
    (SELECT *
     FROM temp2
     WHERE semestre = 'S1'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp2
     WHERE semestre = 'S2'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp2
     WHERE semestre = 'S3'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp2
     WHERE semestre = 'S4'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp2
     WHERE semestre = 'S5'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp2
     WHERE semestre = 'S6'
     ORDER BY monto DESC
     LIMIT 5);

-- query 4

-- MONTOS POR LICITACION

CREATE TABLE stats.licitacion_monto AS
    SELECT
        licitacion_codigo,
        sum(monto) AS monto
    FROM stats.master_plop
    GROUP BY licitacion_codigo
    ORDER BY monto DESC
    LIMIT 5;

-- query 4 desagregada

-- MONTOS POR LICITACION, REGION, SEMESTRE

DROP TABLE IF EXISTS temp4;

CREATE TEMP TABLE temp4 AS
    SELECT
        licitacion_codigo,
        sum(monto)    AS monto,
        CASE WHEN mes <= '6' AND ano = '2013'
            THEN 'S1'
        WHEN mes > '6' AND ano = '2013'
            THEN 'S2'
        WHEN mes <= '6' AND ano = '2014'
            THEN 'S3'
        WHEN mes > '6' AND ano = '2014'
            THEN 'S4'
        WHEN mes <= '6' AND ano = '2015'
            THEN 'S5'
        ELSE 'S6' END AS Semestre,
        region
    FROM stats.master_plop
    GROUP BY licitacion_codigo, semestre, region;

CREATE TABLE stats.licitacion_region_semestre_monto AS

    (SELECT *
     FROM temp4
     WHERE semestre = 'S1' AND region = 'Región de Atacama '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S2' AND region = 'Región de Atacama '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S3' AND region = 'Región de Atacama '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S4' AND region = 'Región de Atacama '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S5' AND region = 'Región de Atacama '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S6' AND region = 'Región de Atacama '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S1' AND region = 'Región de Antofagasta '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S2' AND region = 'Región de Antofagasta '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S3' AND region = 'Región de Antofagasta '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S4' AND region = 'Región de Antofagasta '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S5' AND region = 'Región de Antofagasta '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S6' AND region = 'Región de Antofagasta '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S1' AND region = 'Región de Coquimbo '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S2' AND region = 'Región de Coquimbo '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S3' AND region = 'Región de Coquimbo '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S4' AND region = 'Región de Coquimbo '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S5' AND region = 'Región de Coquimbo '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S6' AND region = 'Región de Coquimbo '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S1' AND region = 'Región del Biobío '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S2' AND region = 'Región del Biobío '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S3' AND region = 'Región del Biobío '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S4' AND region = 'Región del Biobío '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S5' AND region = 'Región del Biobío '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S6' AND region = 'Región del Biobío '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S1' AND region = 'Región de los Lagos '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S2' AND region = 'Región de los Lagos '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S3' AND region = 'Región de los Lagos '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S4' AND region = 'Región de los Lagos '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S5' AND region = 'Región de los Lagos '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S6' AND region = 'Región de los Lagos '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S1' AND region = 'Región de Tarapacá  '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S2' AND region = 'Región de Tarapacá  '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S3' AND region = 'Región de Tarapacá  '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S4' AND region = 'Región de Tarapacá  '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S5' AND region = 'Región de Tarapacá  '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S6' AND region = 'Región de Tarapacá  '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S1' AND region = 'Región de Magallanes y de la Antártica'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S2' AND region = 'Región de Magallanes y de la Antártica'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S3' AND region = 'Región de Magallanes y de la Antártica'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S4' AND region = 'Región de Magallanes y de la Antártica'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S5' AND region = 'Región de Magallanes y de la Antártica'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S6' AND region = 'Región de Magallanes y de la Antártica'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S1' AND region = 'Región Metropolitana de Santiago'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S2' AND region = 'Región Metropolitana de Santiago'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S3' AND region = 'Región Metropolitana de Santiago'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S4' AND region = 'Región Metropolitana de Santiago'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S5' AND region = 'Región Metropolitana de Santiago'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S6' AND region = 'Región Metropolitana de Santiago'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S1' AND region = 'Región Aysén del General Carlos Ibáñez del Campo'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S2' AND region = 'Región Aysén del General Carlos Ibáñez del Campo'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S3' AND region = 'Región Aysén del General Carlos Ibáñez del Campo'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S4' AND region = 'Región Aysén del General Carlos Ibáñez del Campo'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S5' AND region = 'Región Aysén del General Carlos Ibáñez del Campo'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S6' AND region = 'Región Aysén del General Carlos Ibáñez del Campo'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S1' AND region = 'Región de Los Ríos'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S2' AND region = 'Región de Los Ríos'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S3' AND region = 'Región de Los Ríos'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S4' AND region = 'Región de Los Ríos'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S5' AND region = 'Región de Los Ríos'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S6' AND region = 'Región de Los Ríos'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S1' AND region = 'Región de la Araucanía '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S2' AND region = 'Región de la Araucanía '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S3' AND region = 'Región de la Araucanía '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S4' AND region = 'Región de la Araucanía '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S5' AND region = 'Región de la Araucanía '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S6' AND region = 'Región de la Araucanía '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S1' AND region = 'Región de Arica y Parinacota'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S2' AND region = 'Región de Arica y Parinacota'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S3' AND region = 'Región de Arica y Parinacota'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S4' AND region = 'Región de Arica y Parinacota'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S5' AND region = 'Región de Arica y Parinacota'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S6' AND region = 'Región de Arica y Parinacota'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S1' AND region = 'Región del Maule '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S2' AND region = 'Región del Maule '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S3' AND region = 'Región del Maule '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S4' AND region = 'Región del Maule '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S5' AND region = 'Región del Maule '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S6' AND region = 'Región del Maule '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S1' AND region = 'Región del Libertador General Bernardo O´Higgins'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S2' AND region = 'Región del Libertador General Bernardo O´Higgins'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S3' AND region = 'Región del Libertador General Bernardo O´Higgins'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S4' AND region = 'Región del Libertador General Bernardo O´Higgins'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S5' AND region = 'Región del Libertador General Bernardo O´Higgins'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S6' AND region = 'Región del Libertador General Bernardo O´Higgins'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S1' AND region = 'Región de Valparaíso '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S2' AND region = 'Región de Valparaíso '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S3' AND region = 'Región de Valparaíso '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S4' AND region = 'Región de Valparaíso '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S5' AND region = 'Región de Valparaíso '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp4
     WHERE semestre = 'S6' AND region = 'Región de Valparaíso '
     ORDER BY monto DESC
     LIMIT 5);

-- query 5

-- MONTO POR CATEGORIA DE PRODUCTO

CREATE TABLE stats.categoria_monto AS
    SELECT
        categoria_tercer_nivel,
        sum(monto) AS monto
    FROM stats.master_plop
    GROUP BY categoria_tercer_nivel
    ORDER BY monto DESC
    LIMIT 5;

-- query 5 desagregada

-- MONTO POR CATEGPRIA DE PRODUCTO, REGION, SEMESTRE

DROP TABLE IF EXISTS temp5;

CREATE TEMP TABLE temp5 AS
    SELECT
        categoria_tercer_nivel AS categoria,
        sum(monto)             AS monto,
        CASE WHEN mes <= '6' AND ano = '2013'
            THEN 'S1'
        WHEN mes > '6' AND ano = '2013'
            THEN 'S2'
        WHEN mes <= '6' AND ano = '2014'
            THEN 'S3'
        WHEN mes > '6' AND ano = '2014'
            THEN 'S4'
        WHEN mes <= '6' AND ano = '2015'
            THEN 'S5'
        ELSE 'S6' END          AS Semestre,
        region                 AS region
    FROM stats.master_plop
    GROUP BY categoria, semestre, region;

CREATE TABLE stats.categoria_region_semestre_monto AS
    (SELECT *
     FROM temp5
     WHERE semestre = 'S1' AND region = 'Región de Atacama '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S2' AND region = 'Región de Atacama '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S3' AND region = 'Región de Atacama '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S4' AND region = 'Región de Atacama '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S5' AND region = 'Región de Atacama '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S6' AND region = 'Región de Atacama '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S1' AND region = 'Región de Antofagasta '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S2' AND region = 'Región de Antofagasta '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S3' AND region = 'Región de Antofagasta '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S4' AND region = 'Región de Antofagasta '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S5' AND region = 'Región de Antofagasta '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S6' AND region = 'Región de Antofagasta '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S1' AND region = 'Región de Coquimbo '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S2' AND region = 'Región de Coquimbo '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S3' AND region = 'Región de Coquimbo '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S4' AND region = 'Región de Coquimbo '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S5' AND region = 'Región de Coquimbo '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S6' AND region = 'Región de Coquimbo '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S1' AND region = 'Región del Biobío '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S2' AND region = 'Región del Biobío '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S3' AND region = 'Región del Biobío '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S4' AND region = 'Región del Biobío '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S5' AND region = 'Región del Biobío '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S6' AND region = 'Región del Biobío '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S1' AND region = 'Región de los Lagos '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S2' AND region = 'Región de los Lagos '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S3' AND region = 'Región de los Lagos '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S4' AND region = 'Región de los Lagos '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S5' AND region = 'Región de los Lagos '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S6' AND region = 'Región de los Lagos '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S1' AND region = 'Región de Tarapacá  '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S2' AND region = 'Región de Tarapacá  '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S3' AND region = 'Región de Tarapacá  '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S4' AND region = 'Región de Tarapacá  '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S5' AND region = 'Región de Tarapacá  '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S6' AND region = 'Región de Tarapacá  '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S1' AND region = 'Región de Magallanes y de la Antártica'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S2' AND region = 'Región de Magallanes y de la Antártica'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S3' AND region = 'Región de Magallanes y de la Antártica'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S4' AND region = 'Región de Magallanes y de la Antártica'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S5' AND region = 'Región de Magallanes y de la Antártica'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S6' AND region = 'Región de Magallanes y de la Antártica'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S1' AND region = 'Región Metropolitana de Santiago'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S2' AND region = 'Región Metropolitana de Santiago'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S3' AND region = 'Región Metropolitana de Santiago'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S4' AND region = 'Región Metropolitana de Santiago'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S5' AND region = 'Región Metropolitana de Santiago'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S6' AND region = 'Región Metropolitana de Santiago'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S1' AND region = 'Región Aysén del General Carlos Ibáñez del Campo'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S2' AND region = 'Región Aysén del General Carlos Ibáñez del Campo'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S3' AND region = 'Región Aysén del General Carlos Ibáñez del Campo'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S4' AND region = 'Región Aysén del General Carlos Ibáñez del Campo'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S5' AND region = 'Región Aysén del General Carlos Ibáñez del Campo'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S6' AND region = 'Región Aysén del General Carlos Ibáñez del Campo'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S1' AND region = 'Región de Los Ríos'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S2' AND region = 'Región de Los Ríos'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S3' AND region = 'Región de Los Ríos'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S4' AND region = 'Región de Los Ríos'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S5' AND region = 'Región de Los Ríos'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S6' AND region = 'Región de Los Ríos'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S1' AND region = 'Región de la Araucanía '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S2' AND region = 'Región de la Araucanía '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S3' AND region = 'Región de la Araucanía '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S4' AND region = 'Región de la Araucanía '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S5' AND region = 'Región de la Araucanía '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S6' AND region = 'Región de la Araucanía '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S1' AND region = 'Región de Arica y Parinacota'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S2' AND region = 'Región de Arica y Parinacota'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S3' AND region = 'Región de Arica y Parinacota'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S4' AND region = 'Región de Arica y Parinacota'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S5' AND region = 'Región de Arica y Parinacota'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S6' AND region = 'Región de Arica y Parinacota'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S1' AND region = 'Región del Maule '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S2' AND region = 'Región del Maule '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S3' AND region = 'Región del Maule '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S4' AND region = 'Región del Maule '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S5' AND region = 'Región del Maule '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S6' AND region = 'Región del Maule '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S1' AND region = 'Región del Libertador General Bernardo O´Higgins'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S2' AND region = 'Región del Libertador General Bernardo O´Higgins'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S3' AND region = 'Región del Libertador General Bernardo O´Higgins'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S4' AND region = 'Región del Libertador General Bernardo O´Higgins'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S5' AND region = 'Región del Libertador General Bernardo O´Higgins'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S6' AND region = 'Región del Libertador General Bernardo O´Higgins'
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S1' AND region = 'Región de Valparaíso '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S2' AND region = 'Región de Valparaíso '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S3' AND region = 'Región de Valparaíso '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S4' AND region = 'Región de Valparaíso '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S5' AND region = 'Región de Valparaíso '
     ORDER BY monto DESC
     LIMIT 5)
    UNION ALL
    (SELECT *
     FROM temp5
     WHERE semestre = 'S6' AND region = 'Región de Valparaíso '
     ORDER BY monto DESC
     LIMIT 5);
