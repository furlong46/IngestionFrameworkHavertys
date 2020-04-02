-- Databricks notebook source
-- MAGIC %run "./Initialize Connections and Functions" $BulkInsertTableName_Update="" $BulkInsertTableName_Insert=""

-- COMMAND ----------

SELECT COUNT(1) FROM DataLakeCurated.pcrundata_tbcid

-- COMMAND ----------

SELECT COUNT(1) FROM DataLakeCurated.pcrundata_tbcidprf

-- COMMAND ----------

SELECT COUNT(1) FROM DataLakeCurated.pcrundata_tbcidadr

-- COMMAND ----------

SELECT COUNT(1) FROM DataLakeCurated.pcrundata_tbcideml

-- COMMAND ----------

SELECT COUNT(1) FROM DataLakeCurated.pcrundata_tbcidnam

-- COMMAND ----------

SELECT COUNT(1) FROM DataLakeCurated.pcrundata_tbcidphn

-- COMMAND ----------

SELECT COUNT(1) FROM DataLakeCurated.SalesTeam

-- COMMAND ----------

SELECT COUNT(1) FROM DataLakeCurated.DesignerTeam