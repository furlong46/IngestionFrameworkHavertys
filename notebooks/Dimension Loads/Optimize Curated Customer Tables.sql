-- Databricks notebook source
-- MAGIC %run "./Initialize Connections and Functions" $BulkInsertTableName_Update="" $BulkInsertTableName_Insert=""

-- COMMAND ----------

OPTIMIZE DataLakeCurated.pcrundata_tbcid ZORDER BY (CIDNUM) 

-- COMMAND ----------

OPTIMIZE DataLakeCurated.pcrundata_tbcidprf ZORDER BY (CIDNUM, SEQNUM) 

-- COMMAND ----------

OPTIMIZE DataLakeCurated.pcrundata_tbcidadr ZORDER BY (CIDNUM, SEQNUM) 

-- COMMAND ----------

OPTIMIZE DataLakeCurated.pcrundata_tbcideml ZORDER BY (CIDNUM, SEQNUM) 

-- COMMAND ----------

OPTIMIZE DataLakeCurated.pcrundata_tbcidnam ZORDER BY (CIDNUM, SEQNUM) 

-- COMMAND ----------

OPTIMIZE DataLakeCurated.pcrundata_tbcidphn ZORDER BY (CIDNUM, SEQNUM) 

-- COMMAND ----------

OPTIMIZE DataLakeCurated.SalesTeam

-- COMMAND ----------

OPTIMIZE DataLakeCurated.DesignerTeam