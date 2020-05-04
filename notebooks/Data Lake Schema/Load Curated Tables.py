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
filePathCurated = "/mnt/" + varDataLakeStagingFolder.replace("datalake/staging/","datalake/curated/") + "/" + varSchemaTable.replace(".","_")
stagingTableName = "staging_" + varSchemaTable.replace(".","_")
tableName = varSchemaTable.replace(".","_")
columnList = varPKColumnList.replace(" ", "").split(",")

print(filePathStaging)
print(filePathCurated)
print(stagingTableName)
print(tableName)
print(columnList)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Load a dataframe from the Staging area of the Data Lake.  Search for any string columns and trim them.  Create a temp view

# COMMAND ----------

from pyspark.sql.functions import *

autotrimStringsDF = spark.read \
  .parquet(filePathStaging) 

for columnName, columnType in autotrimStringsDF.dtypes:
  if columnType == "string":
    autotrimStringsDF = autotrimStringsDF.withColumn(columnName, trim(col(columnName)))
    
autotrimStringsDF.createOrReplaceTempView(stagingTableName)

# COMMAND ----------

# MAGIC %md
# MAGIC ####This command will only be used when we run for the first time.  Checking if the database exists each time should not have much of a performance penalty

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS ${DatabaseRaw}

# COMMAND ----------

# MAGIC %md
# MAGIC ####Using the schema from the parquet file, use a CTAS statement to create a curated table.  There WHERE 1 = 0 ensures we aren't loading data at this time

# COMMAND ----------

createQuery = """CREATE TABLE IF NOT EXISTS {3}.{0} 
USING DELTA
LOCATION '{1}'
AS
SELECT * FROM {2} WHERE 1 = 0""".format(tableName, filePathCurated, stagingTableName, varDatabaseRaw)

print(createQuery)

spark.sql(createQuery)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Build the merge statement join criteria using the PKColumnList variable

# COMMAND ----------

pkJoinList = ""

for i in columnList:
  pkJoinList += "stage." + i + " = " + "cur." + i + " AND "
  
pkJoinList = pkJoinList[:-5]

# COMMAND ----------

# MAGIC %md
# MAGIC ####Run the curated merge statement which loads new rows and updates old rows using the matching column names
# MAGIC Doing a ROW_NUMBER to prevent possible duplicates if multiple files ended up in the stage folder

# COMMAND ----------

mergeQuery = """
WITH stg AS (
SELECT
*,
ROW_NUMBER() OVER(PARTITION BY {0} ORDER BY {0} DESC) AS ROWNUM
FROM
{1}
)

MERGE INTO {4}.{2} AS cur
USING (SELECT * FROM stg WHERE ROWNUM=1) AS stage
ON {3}
WHEN MATCHED THEN 
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *
""".format(varPKColumnList, stagingTableName, tableName, pkJoinList, varDatabaseRaw)

print(mergeQuery)

spark.sql(mergeQuery)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Counting the # of records in the table

# COMMAND ----------

countSQL = "SELECT COUNT(1) AS Cnt FROM DataLakeCurated." + tableName
DF = spark.sql(countSQL)
display(DF)

# COMMAND ----------


