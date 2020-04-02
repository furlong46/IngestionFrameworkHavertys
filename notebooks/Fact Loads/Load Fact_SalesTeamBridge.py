# Databricks notebook source
#Creates and sets the widgets and variables used later on in the notebook.
dbutils.widgets.text("MasterProcessNumber", "0")
varMasterProcessNumber = dbutils.widgets.get("MasterProcessNumber")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation:
# MAGIC Our notebook starts with setting up connection information to our Data Lake in ADLS, and our Data Warehouse in Azure SQL DB.  Much of the repetive connection info would be templatized or put in a child notebook for code reuse.  

# COMMAND ----------

# MAGIC %run "Dimension Loads/Initialize Connections and Functions" $BulkInsertTableName_Update="" $BulkInsertTableName_Insert=""

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation:
# MAGIC Our ETL in this notebook really starts when we source data from the Data Lake.  In this section we're loading our parquet files from the Data Lake into dataframes.  We also enable the dataframes to be refereced as a hive table/view for enabling SQL queries later in the ETL.  

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load the Latest Data Lake Data
# MAGIC Loading the various directories and parquet files from the Data Lake into dataframes and temporary views

# COMMAND ----------

# no data lake tables needed

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
SELECT
ST.TeamMember1 AS Employee_ID_nk,
ST.TeamMember1 AS TeamMember1_ID_nk,
ST.TeamMember2 AS TeamMember2_ID_nk,
ST.TeamType AS TeamType_nk,
0 AS SourceSystem_fk,
CAST({0} AS INT) AS ETLBatchID_Insert,
CAST({0} AS INT) AS ETLBatchID_Update
FROM 
DataLakeCurated.SalesTeam ST

UNION ALL

SELECT
ST.TeamMember2 AS Employee_ID_nk,
ST.TeamMember1 AS TeamMember1_ID_nk,
ST.TeamMember2 AS TeamMember2_ID_nk,
ST.TeamType AS TeamType_nk,
0 AS SourceSystem_fk,
CAST({0} AS INT) AS ETLBatchID_Insert,
CAST({0} AS INT) AS ETLBatchID_Update
FROM 
DataLakeCurated.SalesTeam ST
--Getting rid of the no Co-team member scenario
WHERE ST.TeamMember2 <> -1
""".format(varMasterProcessNumber)

# COMMAND ----------

Staging_DF = spark.sql(Query) \
              .createOrReplaceTempView("Staging")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load SKU dimension for comparison
# MAGIC Load the SKU dimension from the data warehouse for comparison to the source data in the Data Lake

# COMMAND ----------

Dim_TeamQuery = """
(SELECT
Team_sk,
TeamMember1_ID_nk AS TeamMember1_ID_nk_Dest,
TeamMember2_ID_nk AS TeamMember2_ID_nk_Dest,
TeamType_nk AS TeamType_nk_Dest
FROM
DW.Dim_Team) Dim
"""

# COMMAND ----------

Dim_Team_DF = spark.read \
    .jdbc(url=jdbcUrl, table=Dim_TeamQuery, properties=connectionProperties) \
    .createOrReplaceTempView("Dim_Team")

# COMMAND ----------

Dim_EmployeeQuery = """
(SELECT 
Employee_sk, 
CONVERT(int, Employee_ID_nk) AS Employee_ID_nk_Dest
FROM DW.Dim_Employee) Dim
"""

# COMMAND ----------

Dim_Employee_DF = spark.read \
    .jdbc(url=jdbcUrl, table=Dim_EmployeeQuery, properties=connectionProperties) \
    .createOrReplaceTempView("Dim_Employee")

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
# MAGIC   "queryCustom"  -> "TRUNCATE TABLE DW.Fact_SalesTeamBridge"
# MAGIC ))
# MAGIC 
# MAGIC sqlContext.sqlDBQuery(storedproc)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation:
# MAGIC Here we are checking which of our Staging records are inserts by checking which don't exist in our dimension.  We store the results to a dataframe called NewRecordsDF.  

# COMMAND ----------

NewRecordsDF = spark.sql("""
WITH Fact AS (
SELECT 
1 AS Count,
CASE WHEN S.TeamMember1_ID_nk <> -1 AND S.TeamMember2_ID_nk <> -1 THEN .5 ELSE 1 END AS Allocation,
COALESCE(T.Team_sk, 0) AS Sales_Team_fk,
COALESCE(SA.Employee_sk, 0) AS Sales_Associate_fk,
S.TeamMember1_ID_nk,
S.TeamMember2_ID_nk,
S.Employee_ID_nk,
S.SourceSystem_fk,
S.ETLBatchID_Insert,
S.ETLBatchID_Update,
ROW_NUMBER() OVER(PARTITION BY S.TeamMember1_ID_nk, S.TeamMember2_ID_nk, S.Employee_ID_nk ORDER BY S.TeamMember1_ID_nk) AS ROWNUM
FROM 
Staging S
LEFT OUTER JOIN Dim_Team T ON S.TeamMember1_ID_nk = T.TeamMember1_ID_nk_Dest AND S.TeamMember2_ID_nk = T.TeamMember2_ID_nk_Dest AND S.TeamType_nk = T.TeamType_nk_Dest
LEFT OUTER JOIN Dim_Employee SA ON S.Employee_ID_nk = SA.Employee_ID_nk_Dest
)

SELECT Count, Allocation, Sales_Team_fk, Sales_Associate_fk, TeamMember1_ID_nk, TeamMember2_ID_nk, Employee_ID_nk, SourceSystem_fk, ETLBatchID_Insert, ETLBatchID_Update FROM Fact WHERE ROWNUM=1
UNION
SELECT 1, 1, 0, 0, 0, 0, 0, 0, 0, 0
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation:
# MAGIC Inserting directly to the dimension using JDBC.  Dropping columns that aren't needed.

# COMMAND ----------

#Creating the table.  JDBC creates the table or overwrites it
NewRecordsDF.write \
  .jdbc(url=jdbcUrl, table="DW.Fact_SalesTeamBridge", mode="append", properties=connectionProperties) 

# COMMAND ----------

