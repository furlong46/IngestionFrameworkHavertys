# Databricks notebook source
#Creates and sets the widgets and variables used later on in the notebook.
dbutils.widgets.text("MasterProcessNumber", "0")
varMasterProcessNumber = dbutils.widgets.get("MasterProcessNumber")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation:
# MAGIC Our notebook starts with setting up connection information to our Data Lake in ADLS, and our Data Warehouse in Azure SQL DB.  Much of the repetive connection info would be templatized or put in a child notebook for code reuse.  

# COMMAND ----------

# https://docs.databricks.com/user-guide/notebooks/widgets.html

# COMMAND ----------

# MAGIC %run "./Initialize Connections and Functions" $BulkInsertTableName_Update="" $BulkInsertTableName_Insert=""

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Checking what's in the Data Lake

# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/datalake/staging/master data/product/DB203002")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation:
# MAGIC Our ETL in this notebook really starts when we source data from the Data Lake.  In this section we're loading our parquet files from the Data Lake into dataframes.  We also enable the dataframes to be refereced as a hive table/view for enabling SQL queries later in the ETL.  

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load the Latest Data Lake Data
# MAGIC Loading the various directories and parquet files from the Data Lake into dataframes and temporary views

# COMMAND ----------

DB2table_DF = spark.read \
       .parquet("/mnt/datalake/.../DB2table/") \
       .createOrReplaceTempView("DB2table")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation:
# MAGIC Here we're creating our source query to represent our staging data set.  If we were comparing this to an SSIS package, this would be the source query component of our SSIS Data Flow Task.  

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Staging Query
# MAGIC Creating a source query to transform and de-normalize the product tables into a dimensional row using SQL (Common Table Expressions (CTE), joins, and formulas)
# MAGIC 
# MAGIC Adding a hash key column for downstream processing
# MAGIC 
# MAGIC Reading the data into a datafram and temporary view

# COMMAND ----------

Query = """
SELECT* 
  ,udfSHA1withPython(array(col1,col2,col3)) AS HashKey
  ,0 AS SourceSystem_fk
  ,CAST({0} AS INT) AS ETLBatchID_Insert
  ,CAST({0} AS INT) AS ETLBatchID_Update
FROM DB2table
""".format(varMasterProcessNumber)

# COMMAND ----------

Staging_DF = spark.sql(Query) \
              .createOrReplaceTempView("Staging")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load SKU dimension for comparison
# MAGIC Load the SKU dimension from the data warehouse for comparison to the source data in the Data Lake

# COMMAND ----------

Dim_Query = """
(SELECT
_sk AS _sk_Dest,
_nk AS _nk_Dest,
HashKey AS HashKey_Dest,
ETLBatchID_Insert AS ETLBatchID_Insert_Dest
FROM
DW.Dim_) Dim
"""

# COMMAND ----------

Dim_DF = spark.read \
    .jdbc(url=jdbcUrl, table=Dim_Query, properties=connectionProperties) \
    .createOrReplaceTempView("Dim")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation:
# MAGIC Here we are checking which of our Staging records are updates by checking which already exist in our dimension.  We compare our hash keys to see if the record has changed in any way.  We store the results to a dataframe called UpdateRecordsDF

# COMMAND ----------

UpdateRecordsDF = spark.sql("""
SELECT 
Staging.*, Dim.*
FROM 
Staging
INNER JOIN Dim ON Staging._nk = Dim._nk_Dest
WHERE Staging.HashKey <> Dim.HashKey_Dest 
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation:
# MAGIC Here we are checking which of our Staging records are inserts by checking which don't exist in our dimension.  We store the results to a dataframe called NewRecordsDF.  

# COMMAND ----------

NewRecordsDF = spark.sql("""
SELECT 
Staging.*, Dim.*
FROM 
Staging
LEFT OUTER JOIN Dim ON Staging._nk = Dim._nk_Dest
WHERE Dim._sk_Dest IS NULL
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation:
# MAGIC We truncate our bulk update table.  Using the Spark SQL DB Connector to run specific commands.  The JDBC driver doesn't support this.

# COMMAND ----------

# MAGIC %scala 
# MAGIC import com.microsoft.azure.sqldb.spark.config.Config
# MAGIC import com.microsoft.azure.sqldb.spark.query._
# MAGIC 
# MAGIC val storedproc = Config(Map(
# MAGIC   "url"          -> dbutils.secrets.get(scope = "key-vault-secrets", key = "HavertysDWServerName"),
# MAGIC   "databaseName" -> dbutils.secrets.get(scope = "key-vault-secrets", key = "HavertysDWDBName"),
# MAGIC   "user"         -> "ETL",
# MAGIC   "password"     -> dbutils.secrets.get(scope = "key-vault-secrets", key = "HavertysDWETLAccountPassword"),
# MAGIC   "queryCustom"  -> "TRUNCATE TABLE [Updates].[Dim_<blah>_Update]"
# MAGIC ))
# MAGIC 
# MAGIC sqlContext.sqlDBQuery(storedproc)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation:
# MAGIC Loading changed records to a bulk update table using JDBC. 

# COMMAND ----------

#Creating the table.  JDBC creates the table or overwrites it
UpdateRecordsDF.write \
  .jdbc(url=jdbcUrl, table="Updates.Dim_<blah>_Update", mode="append", properties=connectionProperties) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation:
# MAGIC Inserting directly to the dimension using JDBC.  Dropping columns that aren't needed.

# COMMAND ----------

#Creating the table.  JDBC creates the table or overwrites it
NewRecordsDF.drop('_sk_Dest', '_nk_Dest', 'HashKey_Dest', 'ETLBatchID_Insert_Dest').write \
  .jdbc(url=jdbcUrl, table="DW.Dim_<blah>", mode="append", properties=connectionProperties) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation:
# MAGIC Running a Stored Procedure to do a Bulk Update.  Using the Spark SQL DB Connector to run specific commands.  The JDBC driver doesn't support this.

# COMMAND ----------

# MAGIC %scala 
# MAGIC import com.microsoft.azure.sqldb.spark.config.Config
# MAGIC import com.microsoft.azure.sqldb.spark.query._
# MAGIC 
# MAGIC val storedproc = Config(Map(
# MAGIC   "url"          -> dbutils.secrets.get(scope = "key-vault-secrets", key = "HavertysDWServerName"),
# MAGIC   "databaseName" -> dbutils.secrets.get(scope = "key-vault-secrets", key = "HavertysDWDBName"),
# MAGIC   "user"         -> "ETL",
# MAGIC   "password"     -> dbutils.secrets.get(scope = "key-vault-secrets", key = "HavertysDWETLAccountPassword"),
# MAGIC   "queryCustom"  -> "Exec dbo.usp_Load_Dim_<blah>"
# MAGIC ))
# MAGIC 
# MAGIC sqlContext.sqlDBQuery(storedproc)

# COMMAND ----------

