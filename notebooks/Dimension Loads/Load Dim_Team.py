# Databricks notebook source
#Creates and sets the widgets and variables used later on in the notebook.
dbutils.widgets.text("MasterProcessNumber", "0")
varMasterProcessNumber = dbutils.widgets.get("MasterProcessNumber")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation:
# MAGIC Our notebook starts with setting up connection information to our Data Lake in ADLS, and our Data Warehouse in Azure SQL DB.  Much of the repetive connection info would be templatized or put in a child notebook for code reuse.  

# COMMAND ----------

# MAGIC %run "./Initialize Connections and Functions" $BulkInsertTableName_Update="" $BulkInsertTableName_Insert=""

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation:
# MAGIC Our ETL in this notebook really starts when we source data from the Data Lake.  In this section we're loading our parquet files from the Data Lake into dataframes.  We also enable the dataframes to be refereced as a hive table/view for enabling SQL queries later in the ETL.  

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load the Latest Data Lake Data
# MAGIC Loading the various directories and parquet files from the Data Lake into dataframes and temporary views

# COMMAND ----------

ipcorpdta_tbdwempDF = spark.read \
       .parquet("/mnt/datalake/staging/master data/employee/DB203002/ipcorpdta_tbdwemp") \
       .createOrReplaceTempView("ipcorpdta_tbdwemp")

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
WITH 
Sales AS (
SELECT
ST.TeamMember1 AS TeamMember1_ID_nk,
COALESCE(TRIM(EL1.FIRST_NAME) || ' ' || TRIM(EL1.LAST_NAME), '') AS TeamMember1_Name,
ST.TeamMember2 AS TeamMember2_ID_nk,
COALESCE(TRIM(EL2.FIRST_NAME) || ' ' || TRIM(EL2.LAST_NAME), '') AS TeamMember2_Name,
ST.TeamType AS TeamType_nk,
ROW_NUMBER() OVER(PARTITION BY ST.TeamMember1, ST.TeamMember2 ORDER BY ST.TeamMember1, ST.TeamMember2) AS ROWNUM,
0 AS SourceSystem_fk,
CAST({0} AS INT) AS ETLBatchID_Insert,
CAST({0} AS INT) AS ETLBatchID_Update
FROM 
DataLakeCurated.SalesTeam ST
LEFT OUTER JOIN ipcorpdta_tbdwemp EL1 ON ST.TeamMember1 = CAST(TRIM(EL1.EMPLOYEE_NUMBER) AS INT)
LEFT OUTER JOIN ipcorpdta_tbdwemp EL2 ON ST.TeamMember2 = CAST(TRIM(EL2.EMPLOYEE_NUMBER) AS INT)
)
,
Team AS (
SELECT
ST.TeamMember1 AS TeamMember1_ID_nk,
COALESCE(TRIM(EL1.FIRST_NAME) || ' ' || TRIM(EL1.LAST_NAME), '') AS TeamMember1_Name,
ST.TeamMember2 AS TeamMember2_ID_nk,
COALESCE(TRIM(EL2.FIRST_NAME) || ' ' || TRIM(EL2.LAST_NAME), '') AS TeamMember2_Name,
ST.TeamType AS TeamType_nk,
ROW_NUMBER() OVER(PARTITION BY ST.TeamMember1, ST.TeamMember2 ORDER BY ST.TeamMember1, ST.TeamMember2) AS ROWNUM,
0 AS SourceSystem_fk,
CAST({0} AS INT) AS ETLBatchID_Insert,
CAST({0} AS INT) AS ETLBatchID_Update
FROM 
DataLakeCurated.DesignerTeam ST
LEFT OUTER JOIN ipcorpdta_tbdwemp EL1 ON ST.TeamMember1 = EL1.DESIGNER_NUMBER
LEFT OUTER JOIN ipcorpdta_tbdwemp EL2 ON ST.TeamMember2 = EL2.DESIGNER_NUMBER
)

SELECT TeamMember1_ID_nk, TeamMember1_Name, TeamMember2_ID_nk, TeamMember2_Name, TeamType_nk, udfSHA1withPython(array(TeamMember1_Name,TeamMember2_Name)) AS HashKey, SourceSystem_fk, ETLBatchID_Insert, ETLBatchID_Update FROM Sales WHERE ROWNUM=1
UNION ALL
SELECT TeamMember1_ID_nk, TeamMember1_Name, TeamMember2_ID_nk, TeamMember2_Name, TeamType_nk, udfSHA1withPython(array(TeamMember1_Name,TeamMember2_Name)) AS HashKey, SourceSystem_fk, ETLBatchID_Insert, ETLBatchID_Update FROM Team WHERE ROWNUM=1
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
Team_sk AS Team_sk_Dest,
TeamMember1_ID_nk AS TeamMember1_ID_nk_Dest,
TeamMember2_ID_nk AS TeamMember2_ID_nk_Dest,
TeamType_nk AS TeamType_nk_Dest,
HashKey AS HashKey_Dest,
ETLBatchID_Insert AS ETLBatchID_Insert_Dest
FROM
DW.Dim_Team) Dim
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
INNER JOIN Dim ON Staging.TeamMember1_ID_nk = Dim.TeamMember1_ID_nk_Dest AND Staging.TeamMember2_ID_nk = Dim.TeamMember2_ID_nk_Dest AND Staging.TeamType_nk = Dim.TeamType_nk_Dest
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
LEFT OUTER JOIN Dim ON Staging.TeamMember1_ID_nk = Dim.TeamMember1_ID_nk_Dest AND Staging.TeamMember2_ID_nk = Dim.TeamMember2_ID_nk_Dest AND Staging.TeamType_nk = Dim.TeamType_nk_Dest
WHERE Dim.Team_sk_Dest IS NULL
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
# MAGIC   "queryCustom"  -> "TRUNCATE TABLE [Updates].[Dim_Team_Update]"
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
  .jdbc(url=jdbcUrl, table="Updates.Dim_Team_Update", mode="append", properties=connectionProperties) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation:
# MAGIC Inserting directly to the dimension using JDBC.  Dropping columns that aren't needed.

# COMMAND ----------

#Creating the table.  JDBC creates the table or overwrites it
NewRecordsDF.drop('Team_sk_Dest', 'TeamMember1_ID_nk_Dest', 'TeamMember2_ID_nk_Dest', 'TeamType_nk_Dest', 'HashKey_Dest', 'ETLBatchID_Insert_Dest').write \
  .jdbc(url=jdbcUrl, table="DW.Dim_Team", mode="append", properties=connectionProperties) 

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
# MAGIC   "queryCustom"  -> "Exec dbo.usp_Load_Dim_Team"
# MAGIC ))
# MAGIC 
# MAGIC sqlContext.sqlDBQuery(storedproc)

# COMMAND ----------

