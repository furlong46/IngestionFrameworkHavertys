# Databricks notebook source
#Creates and sets the widgets and variables used later on in the notebook.
dbutils.widgets.text("SchemaTable", "")
dbutils.widgets.text("PKColumnList", "")
dbutils.widgets.text("DataLakeStagingFolder", "")
dbutils.widgets.text("DatabaseRaw", "")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Load parameters to variables

# COMMAND ----------

varSchemaTable = dbutils.widgets.get("SchemaTable")
varPKColumnList = dbutils.widgets.get("PKColumnList")
varDataLakeStagingFolder = dbutils.widgets.get("DataLakeStagingFolder")
varDatabaseRaw = dbutils.widgets.get("DatabaseRaw")

print(varSchemaTable)
print(varPKColumnList)
print(varDataLakeStagingFolder)
print(varDatabaseRaw)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Setting partition sizes to impact performance.  Set the optimize flag parameter

# COMMAND ----------

partitions = 8
spark.conf.set("spark.sql.shuffle.partitions", str(partitions))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Create variables to dynamically set folder locations and table names

# COMMAND ----------

filePathStaging = "/mnt/" + varDataLakeStagingFolder + "/" + varSchemaTable.replace(".","_")
filePathRaw = "/mnt/" + varDataLakeStagingFolder.replace("datalake/staging/","datalake/raw/") + "/" + varSchemaTable.replace(".","_")
stagingTableName = "staging_" + varSchemaTable.replace(".","_")
tableName = varSchemaTable.replace(".","_")
columnList = varPKColumnList.replace(" ", "").split(",")

print(filePathStaging)
print(filePathRaw)
print(stagingTableName)
print(tableName)
print(columnList)

# COMMAND ----------

DF = spark.read \
  .parquet(filePathStaging) \
  .createOrReplaceTempView(stagingTableName)

# COMMAND ----------

rowsList = spark.sql("DESCRIBE TABLE {0}".format(stagingTableName)).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ####This command will only be used when we run for the first time.  Checking if the database exists each time should not have much of a performance penalty

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS ${DatabaseRaw}

# COMMAND ----------

sqlQuery = "CREATE TABLE IF NOT EXISTS " + varDatabaseRaw + "." + tableName + " ( \n"
for rows in rowsList:
  sqlQuery += rows["col_name"] + " " + rows["data_type"] + ", \n"
  
sqlQuery += "TS timestamp \n"
sqlQuery += ") \n"
sqlQuery += "USING PARQUET \n"
sqlQuery += "PARTITIONED BY(TS) \n" 
sqlQuery += "LOCATION '" + filePathRaw + "' \n"
sqlQuery += "OPTIONS ('compression'='snappy')"

print(sqlQuery)

spark.sql(sqlQuery)

# COMMAND ----------

partRepair = "MSCK REPAIR TABLE " + varDatabaseRaw + "." + tableName

print(partRepair)

spark.sql(partRepair)

# COMMAND ----------

parList = spark.sql("SELECT DISTINCT TS FROM " + varDatabaseRaw + "." + tableName)
display(parList)
