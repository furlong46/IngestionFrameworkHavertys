# Databricks notebook source
#Creates and sets the widgets and variables used later on in the notebook.
dbutils.widgets.text("MasterProcessNumber", "3")
varMasterProcessNumber = dbutils.widgets.get("MasterProcessNumber")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation:
# MAGIC Our notebook starts with setting up connection information to our Data Lake in ADLS, and our Data Warehouse in Azure SQL DB.  Much of the repetive connection info would be templatized or put in a child notebook for code reuse.  

# COMMAND ----------

# MAGIC %run "/Dimension Loads/Initialize Connections and Functions" $BulkInsertTableName_Update="" $BulkInsertTableName_Insert=""

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Checking what's in the Data Lake

# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/datalake/staging/sales/DB203002")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation:
# MAGIC Our ETL in this notebook really starts when we source data from the Data Lake.  In this section we're loading our parquet files from the Data Lake into dataframes.  We also enable the dataframes to be refereced as a hive table/view for enabling SQL queries later in the ETL.  

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load the Latest Data Lake Data
# MAGIC Loading the various directories and parquet files from the Data Lake into dataframes and temporary views

# COMMAND ----------

pcrundata_tbbgtdly_DF = spark.read \
       .parquet("/mnt/datalake/staging/sales/DB203002/pcrundata_tbbgtdly") \
       .createOrReplaceTempView("pcrundata_tbbgtdly")

# COMMAND ----------

# %sql 
# SELECT * FROM pcrundata_tbbgtdly

# COMMAND ----------

# %sql 
# SELECT COUNT(*) FROM pcrundata_tbbgtdly

# COMMAND ----------

Dim_Location_DF = spark.read.jdbc(url=jdbcUrl, table="DW.Dim_Location", properties=connectionProperties).createOrReplaceTempView("Dim_Location")

# COMMAND ----------

#  %sql
# SELECT * FROM Dim_Location

# COMMAND ----------

Dim_Date_DF =  spark.read.jdbc(url=jdbcUrl, table="DW.Dim_Date", properties=connectionProperties).createOrReplaceTempView("Dim_Date")

# COMMAND ----------

#  %sql
# SELECT * FROM Dim_Date

# COMMAND ----------

# %sql
# WITH b AS (
#   SELECT 
#   division, pc, branch, bgtdate, SUM(coalesce(delivered, 0)) as delivered, SUM(coalesce(written, 0)) as written 
#   FROM pcrundata_tbbgtdly
#   group by division, pc, branch, bgtdate
#   order by bgtdate
# )
# SELECT 
# cast(b.written as decimal(9,2)) as Written_Retail_Sales,
# cast(b.delivered as decimal(9,2)) as Delivered_Retail_Sales,
# coalesce(d.Date_sk, 0) as Date_fk,
# coalesce(l.Location_sk, 0) as Location_fk,
# b.bgtdate as BudgetDate_nk,
# cast((b.division * 100000) + (b.pc * 100) + b.branch as int) as Location_Code_nk,
# 0 as SourceSystem_fk
# from b
# left outer join Dim_Location l on b.division = l.Division_ID and b.pc = l.Market_ID and b.branch = l.Location_ID
# left outer join Dim_Date d on cast(substring(replace(b.bgtdate, '-', ''), 1, 8) as INT) = d.Date_sk

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Staging Query
# MAGIC Creating a source query to join with dimensions - in this case we are joining with the location and date dimensions
# MAGIC 
# MAGIC Reading the data into a dataframe and temporary view

# COMMAND ----------

Query = """
  WITH b AS (
    SELECT 
    division, pc, branch, bgtdate, SUM(coalesce(delivered, 0)) as delivered, SUM(coalesce(written, 0)) as written 
    FROM pcrundata_tbbgtdly
    group by division, pc, branch, bgtdate
    order by bgtdate
  ),
  Sales_Budgets AS (
    SELECT 
    cast(b.written as decimal(9,2)) as Written_Retail_Sales,
    cast(b.delivered as decimal(9,2)) as Delivered_Retail_Sales,
    coalesce(d.Date_sk, 0) as Date_fk,
    coalesce(l.Location_sk, 0) as Location_fk,
    b.bgtdate as BudgetDate_nk,
    cast((b.division * 100000) + (b.pc * 100) + b.branch as int) as Location_Code_nk,
    0 as SourceSystem_fk
    from b
    left outer join Dim_Location l on b.division = l.Division_ID and b.pc = l.Market_ID and b.branch = l.Location_ID
    left outer join Dim_Date d on cast(substring(replace(b.bgtdate, '-', ''), 1, 8) as INT) = d.Date_sk
  )
  SELECT Written_Retail_Sales, Delivered_Retail_Sales, Date_fk, Location_fk, BudgetDate_nk, Location_Code_nk, SourceSystem_fk, CAST({0} AS INT) AS ETLBatchID_Insert, CAST({0} AS INT) AS ETLBatchID_Update
  FROM Sales_Budgets
""".format(varMasterProcessNumber)

# COMMAND ----------

Staging_DF = spark.sql(Query)
Staging_DF.cache()
Staging_DF.createOrReplaceTempView("Staging")

# COMMAND ----------

# %sql
# SELECT COUNT(1) AS StagingCnt FROM Staging

# COMMAND ----------

Fact_Query = """
  (SELECT
    Sales_Budget_sk as Sales_Budget_sk_Dest,
    Written_Retail_Sales as Written_Retail_Sales_Dest,
    Delivered_Retail_Sales as Delivered_Retail_Sales_Dest,
    BudgetDate_nk as BudgetDate_nk_Dest,
    Location_Code_nk as Location_Code_nk_Dest
    FROM Sales.Fact_SalesBudget
  ) Fact
"""

# COMMAND ----------

Fact_DF = spark.read.jdbc(url=jdbcUrl, table=Fact_Query, properties=connectionProperties)
Fact_DF.cache()
Fact_DF.createOrReplaceTempView("Fact")

# COMMAND ----------

# %sql
# SELECT COUNT(1) AS FactCnt FROM Fact

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation:
# MAGIC Here we are checking which of our Staging records are updates by checking which already exist in our fact table. We compare  to see if the record has changed in any way.  We store the results to a dataframe called UpdateRecordsDF

# COMMAND ----------

UpdateRecordsDF = spark.sql("""
  SELECT
  Staging.*, Fact.Sales_Budget_sk_Dest
  FROM
  Staging
  INNER JOIN Fact ON Staging.Location_Code_nk = Fact.Location_Code_nk_Dest AND Staging.BudgetDate_nk = Fact.BudgetDate_nk_Dest
  WHERE Staging.Written_Retail_Sales <> Fact.Written_Retail_Sales_Dest OR Staging.Delivered_Retail_Sales <> Fact.Delivered_Retail_Sales_Dest
""")
UpdateRecordsDF.createOrReplaceTempView("Updates")

# COMMAND ----------

# %sql
# SELECT COUNT(1) AS UpdatesCount FROM Updates

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation:
# MAGIC Here we are checking which of our Staging records are inserts by checking which don't exist in the fact table.  We store the results to a dataframe called NewRecordsDF.  

# COMMAND ----------

NewRecordsDF = spark.sql("""
  SELECT
  Staging.*
  FROM
  Staging
  LEFT OUTER JOIN Fact ON Staging.Location_Code_nk = Fact.Location_Code_nk_Dest AND Staging.BudgetDate_nk = Fact.BudgetDate_nk_Dest
  WHERE Fact.Location_Code_nk_Dest IS NULL
""")
NewRecordsDF.createOrReplaceTempView("NewRecords")

# COMMAND ----------

# %sql
# SELECT COUNT(1) AS NewCount FROM NewRecords

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
# MAGIC   "queryCustom"  -> "TRUNCATE TABLE Updates.Fact_SalesBudget_Update"
# MAGIC ))
# MAGIC 
# MAGIC sqlContext.sqlDBQuery(storedproc)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation:
# MAGIC Loading changed records to bulk update table using JDBC.

# COMMAND ----------

UpdateRecordsDF.write \
  .jdbc(url=jdbcUrl, table="Updates.Fact_SalesBudget_Update", mode="append", properties=connectionProperties) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation:
# MAGIC Inserting directly to the dimension using JDBC. 

# COMMAND ----------

NewRecordsDF.write \
  .jdbc(url=jdbcUrl, table="Sales.Fact_SalesBudget", mode="append", properties=connectionProperties)

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
# MAGIC   "queryCustom"  -> "Exec dbo.usp_Load_Fact_SalesBudgets"
# MAGIC ))
# MAGIC 
# MAGIC sqlContext.sqlDBQuery(storedproc)