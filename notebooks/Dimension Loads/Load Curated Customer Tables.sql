-- Databricks notebook source
-- MAGIC %run "./Initialize Connections and Functions" $BulkInsertTableName_Update="" $BulkInsertTableName_Insert=""

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS DataLakeCurated

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pcrundata_tbcid_DF = spark.read \
-- MAGIC        .parquet("/mnt/datalake/staging/master data/customer/DB203002/pcrundata_tbcid") \
-- MAGIC        .createOrReplaceTempView("staging_pcrundata_tbcid")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pcrundata_tbcidprf_DF = spark.read \
-- MAGIC        .parquet("/mnt/datalake/staging/master data/customer/DB203002/pcrundata_tbcidprf") \
-- MAGIC        .createOrReplaceTempView("staging_pcrundata_tbcidprf")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pcrundata_tbcidadr_DF = spark.read \
-- MAGIC        .parquet("/mnt/datalake/staging/master data/customer/DB203002/pcrundata_tbcidadr") \
-- MAGIC        .createOrReplaceTempView("staging_pcrundata_tbcidadr")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pcrundata_tbcideml_DF = spark.read \
-- MAGIC        .parquet("/mnt/datalake/staging/master data/customer/DB203002/pcrundata_tbcideml") \
-- MAGIC        .createOrReplaceTempView("staging_pcrundata_tbcideml")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pcrundata_tbcidnam_DF = spark.read \
-- MAGIC        .parquet("/mnt/datalake/staging/master data/customer/DB203002/pcrundata_tbcidnam") \
-- MAGIC        .createOrReplaceTempView("staging_pcrundata_tbcidnam")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pcrundata_tbcidphn_DF = spark.read \
-- MAGIC        .parquet("/mnt/datalake/staging/master data/customer/DB203002/pcrundata_tbcidphn") \
-- MAGIC        .createOrReplaceTempView("staging_pcrundata_tbcidphn")

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS DataLakeCurated.pcrundata_tbcid 
USING DELTA
LOCATION '/mnt/datalake/curated/master data/customer/DB203002/pcrundata_tbcid/'
AS
SELECT * FROM staging_pcrundata_tbcid WHERE 1 = 0

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS DataLakeCurated.pcrundata_tbcidprf
USING DELTA
LOCATION '/mnt/datalake/curated/master data/customer/DB203002/pcrundata_tbcidprf/'
AS
SELECT * FROM staging_pcrundata_tbcidprf WHERE 1 = 0

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS DataLakeCurated.pcrundata_tbcidadr
USING DELTA
LOCATION '/mnt/datalake/curated/master data/customer/DB203002/pcrundata_tbcidadr/'
AS
SELECT * FROM staging_pcrundata_tbcidadr WHERE 1 = 0

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS DataLakeCurated.pcrundata_tbcideml
USING DELTA
LOCATION '/mnt/datalake/curated/master data/customer/DB203002/pcrundata_tbcideml/'
AS
SELECT * FROM staging_pcrundata_tbcideml WHERE 1 = 0

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS DataLakeCurated.pcrundata_tbcidnam
USING DELTA
LOCATION '/mnt/datalake/curated/master data/customer/DB203002/pcrundata_tbcidnam/'
AS
SELECT * FROM staging_pcrundata_tbcidnam WHERE 1 = 0

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS DataLakeCurated.pcrundata_tbcidphn
USING DELTA
LOCATION '/mnt/datalake/curated/master data/customer/DB203002/pcrundata_tbcidphn/'
AS
SELECT * FROM staging_pcrundata_tbcidphn WHERE 1 = 0

-- COMMAND ----------

WITH stage AS (
SELECT
*,
ROW_NUMBER() OVER(PARTITION BY CIDNUM ORDER BY CREATED DESC) AS ROWNUM
FROM
staging_pcrundata_tbcid
)

MERGE INTO DataLakeCurated.pcrundata_tbcid AS cur
USING (SELECT * FROM stage WHERE ROWNUM=1) AS stage 
ON cur.CIDNUM = stage.CIDNUM
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *

-- COMMAND ----------

WITH stage AS (
SELECT
*,
ROW_NUMBER() OVER(PARTITION BY CIDNUM, SEQNUM ORDER BY CREATED DESC) AS ROWNUM
FROM
staging_pcrundata_tbcidprf
)

MERGE INTO DataLakeCurated.pcrundata_tbcidprf AS cur
USING (SELECT * FROM stage WHERE ROWNUM=1) AS stage 
ON cur.CIDNUM = stage.CIDNUM AND cur.SEQNUM = stage.SEQNUM
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *

-- COMMAND ----------

WITH stage AS (
SELECT
*,
ROW_NUMBER() OVER(PARTITION BY CIDNUM, SEQNUM ORDER BY CREATED DESC) AS ROWNUM
FROM
staging_pcrundata_tbcidadr
)

MERGE INTO DataLakeCurated.pcrundata_tbcidadr AS cur
USING (SELECT * FROM stage WHERE ROWNUM=1) AS stage 
ON cur.CIDNUM = stage.CIDNUM AND cur.SEQNUM = stage.SEQNUM
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *

-- COMMAND ----------

WITH stage AS (
SELECT
*,
ROW_NUMBER() OVER(PARTITION BY CIDNUM, SEQNUM ORDER BY CREATED DESC) AS ROWNUM
FROM
staging_pcrundata_tbcideml
)

MERGE INTO DataLakeCurated.pcrundata_tbcideml AS cur
USING (SELECT * FROM stage WHERE ROWNUM=1) AS stage 
ON cur.CIDNUM = stage.CIDNUM AND cur.SEQNUM = stage.SEQNUM
WHEN MATCHED THEN 
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *

-- COMMAND ----------

WITH stage AS (
SELECT
*,
ROW_NUMBER() OVER(PARTITION BY CIDNUM, SEQNUM ORDER BY CREATED DESC) AS ROWNUM
FROM
staging_pcrundata_tbcidnam
)

MERGE INTO DataLakeCurated.pcrundata_tbcidnam AS cur
USING (SELECT * FROM stage WHERE ROWNUM=1) AS stage 
ON cur.CIDNUM = stage.CIDNUM AND cur.SEQNUM = stage.SEQNUM
WHEN MATCHED THEN 
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *

-- COMMAND ----------

WITH stage AS (
SELECT
*,
ROW_NUMBER() OVER(PARTITION BY CIDNUM, SEQNUM ORDER BY CREATED DESC) AS ROWNUM
FROM
staging_pcrundata_tbcidphn
)

MERGE INTO DataLakeCurated.pcrundata_tbcidphn AS cur
USING (SELECT * FROM stage WHERE ROWNUM=1) AS stage 
ON cur.CIDNUM = stage.CIDNUM AND cur.SEQNUM = stage.SEQNUM
WHEN MATCHED THEN 
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *

-- COMMAND ----------

--OPTIMIZE DataLakeCurated.pcrundata_tbcid ZORDER BY (CIDNUM) 

-- COMMAND ----------

--OPTIMIZE DataLakeCurated.pcrundata_tbcidprf ZORDER BY (CIDNUM, SEQNUM) 

-- COMMAND ----------

--OPTIMIZE DataLakeCurated.pcrundata_tbcidadr ZORDER BY (CIDNUM, SEQNUM) 

-- COMMAND ----------

--OPTIMIZE DataLakeCurated.pcrundata_tbcideml ZORDER BY (CIDNUM, SEQNUM) 

-- COMMAND ----------

--OPTIMIZE DataLakeCurated.pcrundata_tbcidnam ZORDER BY (CIDNUM, SEQNUM) 

-- COMMAND ----------

--OPTIMIZE DataLakeCurated.pcrundata_tbcidphn ZORDER BY (CIDNUM, SEQNUM) 