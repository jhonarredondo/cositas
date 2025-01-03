-- Última actualización: 26/10/2023

-------------------------------------------------------------------------------------------------

--describe resultados_vspc_canales.saldo_sucursal_hist;



-- VALIDAMOS DATOS EN LA TABLA QUE SE INGESTÓ CON NUEVOS DATOS DESDE PROGRAMA PYTHON
SELECT COUNT(*) FROM proceso_vspc_canales.f_actualizar_cupos;
SELECT * FROM proceso_vspc_canales.f_actualizar_cupos;

-- VALIDACION CANTIDAD DE REGISTROS
SELECT COUNT(*) FROM resultados_vspc_canales.efectivo_cupo_suc_prov;

-- SE BORRA LA TABLA DE RESPALDO EN CASO QUE EXISTA
DROP TABLE IF EXISTS proceso_vspc_canales.efectivo_cupo_suc_copia_pedro purge;

-- SE CREA COPIA EN PROCESOS DE LA TABLA EN RESULTADOS
CREATE TABLE proceso_vspc_canales.efectivo_cupo_suc_copia_pedro  stored AS parquet AS
SELECT *
FROM resultados_vspc_canales.efectivo_cupo_suc_prov;

--SELECT * FROM proceso_vspc_canales.efectivo_cupo_suc_copia_pedro;

--SE BORRAN REGISTROS DE LA TABLA EN RESULTADOS
TRUNCATE resultados_vspc_canales.efectivo_cupo_suc_prov;

--SELECT * FROM resultados_vspc_canales.efectivo_cupo_suc_prov;

--SE INSERTA INFORMACION DE LA TABLA EN PROCESO QUE ACTUALIZMOS EN PYTHON 
INSERT INTO resultados_vspc_canales.efectivo_cupo_suc_prov PARTITION (year)
select ingestion_year,
ingestion_month,
ingestion_day,
suc,
nombre_suc,
region,
estado_suc,
lider_svcio,
gte_suc,
director_svcio,
tipo_transpo,
transportadora,
cast(cupo_aseg as BIGINT),
cast(cupo_suc as BIGINT),
cupo_boveda_ml,
cupo_cuarto_boveda_ppl_ml,
cupo_tot_atm,
cupo_sugerido,
year
 from proceso_vspc_canales.f_actualizar_cupos;

--SELECT * FROM resultados_vspc_canales.efectivo_cupo_suc_prov;