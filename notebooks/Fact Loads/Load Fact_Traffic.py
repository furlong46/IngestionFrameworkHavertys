# Databricks notebook source
#Creates and sets the widgets and variables used later on in the notebook.
dbutils.widgets.text("MasterProcessNumber", "0")
varMasterProcessNumber = dbutils.widgets.get("MasterProcessNumber")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation:
# MAGIC Our notebook starts with setting up connection information to our Data Lake in ADLS, and our Data Warehouse in Azure SQL DB. Uses the notebook "Initialize Connections and Functions" to do so.

# COMMAND ----------

# https://docs.databricks.com/user-guide/notebooks/widgets.html

# COMMAND ----------

# MAGIC %run "/Dimension Loads/Initialize Connections and Functions" $BulkInsertTableName_Update="" $BulkInsertTableName_Insert=""

# COMMAND ----------

# MAGIC %md
# MAGIC ##### The next command checks what's in the data lake at the given path

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

ipcorpdta_scupbrn_DF = spark.read \
    .parquet("/mnt/datalake/staging/sales/DB203002/ipcorpdta_scupbrn") \
    .createOrReplaceTempView("ipcorpdta_scupbrn")

# COMMAND ----------

# %sql
# SELECT * FROM ipcorpdta_scupbrn

# COMMAND ----------

Dim_Time_DF = spark.read.jdbc(url=jdbcUrl, table="DW.Dim_Time", properties=connectionProperties).createOrReplaceTempView("Dim_Time")

# COMMAND ----------

# %sql 
# SELECT * FROM Dim_Time

# COMMAND ----------

Dim_Date_DF = spark.read.jdbc(url=jdbcUrl, table="DW.Dim_Date", properties=connectionProperties).createOrReplaceTempView("Dim_Date")

# COMMAND ----------

# %sql 
# SELECT * FROM Dim_Date

# COMMAND ----------

Dim_Location_DF = spark.read.jdbc(url=jdbcUrl, table="DW.Dim_Location", properties=connectionProperties).createOrReplaceTempView("Dim_Location") 

# COMMAND ----------

# %sql
# SELECT * FROM Dim_Location

# COMMAND ----------

# %sql
# SELECT
# cast(coalesce(scbstaff, 0) as int) as Staff_on_Floor,
# cast(coalesce(scbups, 0) as int) as Ups,
# cast(coalesce(scbccups, 0) as int) as Call_Customer_Ups,
# cast(coalesce(scbtrfcraw, 0) as int) as Raw_Traffic_Count,
# cast(coalesce(scbtraffic, 0) as int) as Traffic_Count,
# coalesce(d.Date_sk, 0) as Date_fk,
# coalesce(t.Time_sk, 0) as Hour_fk,
# coalesce(l.Location_sk, 0) as Location_fk,
# cast((scbdiv * 100000) + (scbpc * 100) + scbbranch as int) as Location_Code_nk,
# cast(scbhour as tinyint) as Hour_code,
# 0 as SourceSystem_fk
# from ipcorpdta_scupbrn s
# left outer join Dim_Date d on s.scbdate=d.Date_sk
# left outer join Dim_Time t on (case when s.scbhour=24 then t.Hour24_Code = 0 else t.Hour24_Code = s.scbhour end) and t.Minute_name="00" and t.Second_name = "00" 
# left outer join Dim_Location l on s.scbdiv=l.Division_ID and s.scbpc=l.Market_ID and s.scbbranch=l.Location_ID
# where scbtype='H' and scbdate >= 20100101

# COMMAND ----------

Query = """
  WITH Traffic AS (
    SELECT
    cast(coalesce(scbstaff, 0) as int) as Staff_on_Floor,
    cast(coalesce(scbups, 0) as int) as Ups,
    cast(coalesce(scbccups, 0) as int) as Call_Customer_Ups,
    cast(coalesce(scbtrfcraw, 0) as int) as Raw_Traffic_Count,
    cast(coalesce(scbtraffic, 0) as int) as Traffic_Count,
    coalesce(d.Date_sk, 0) as Date_fk,
    coalesce(t.Time_sk, 0) as Hour_fk,
    coalesce(l.Location_sk, 0) as Location_fk,
    cast((scbdiv * 100000) + (scbpc * 100) + scbbranch as int) as Location_Code_nk,
    cast(scbhour as tinyint) as Hour_code,
    0 as SourceSystem_fk
    from ipcorpdta_scupbrn s
    left outer join Dim_Date d on s.scbdate=d.Date_sk
    left outer join Dim_Time t on (case when s.scbhour=24 then t.Hour24_Code = 0 else t.Hour24_Code = s.scbhour end) and t.Minute_name="00" and t.Second_name = "00" 
    left outer join Dim_Location l on s.scbdiv=l.Division_ID and s.scbpc=l.Market_ID and s.scbbranch=l.Location_ID
    where scbtype='H' and scbdate >= 20100101
  )
  SELECT Traffic.*, CAST({0} AS INT) AS ETLBatchID_Insert, CAST({0} AS INT) AS ETLBatchID_Update
  FROM TRAFFIC
""".format(varMasterProcessNumber)

# COMMAND ----------

Staging_DF = spark.sql(Query) 
Staging_DF.cache()
Staging_DF.createOrReplaceTempView("Staging")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) AS StagingCnt FROM Staging

# COMMAND ----------

Fact_Query = """
  (SELECT 
    Traffic_sk as Traffic_sk_Dest,
    Staff_on_Floor as Staff_on_Floor_Dest,
    Ups as Ups_Dest,
    Call_Customer_Ups as Call_Customer_Ups_Dest,
    Raw_Traffic_Count as Raw_Traffic_Count_Dest,
    Traffic_Count as Traffic_Count_Dest,
    Location_Code_nk as Location_Code_nk_Dest,
    Date_fk as Date_fk_Dest,
    Hour_code as Hour_code_Dest
    FROM Sales.Fact_Traffic
  ) Fact
"""

# COMMAND ----------

Fact_DF = spark.read.jdbc(url=jdbcUrl, table=Fact_Query, properties=connectionProperties)
Fact_DF.cache()
Fact_DF.createOrReplaceTempView("Fact")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) AS FactCnt FROM Fact

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation:
# MAGIC Here we are checking which of our Staging records are updates by checking which already exist in our fact table. We compare  to see if the record has changed in any way.  We store the results to a dataframe called UpdateRecordsDF

# COMMAND ----------

UpdateRecordsDF = spark.sql("""
  SELECT
  Staging.*, Fact.Traffic_sk_Dest
  FROM 
  Staging 
  INNER JOIN Fact ON Staging.Location_Code_nk = Fact.Location_Code_nk_Dest AND Staging.Date_fk = Fact.Date_fk_Dest AND Staging.Hour_code = Fact.Hour_code_Dest
  WHERE Staging.Staff_on_Floor <> Fact.Staff_on_Floor_Dest OR Staging.Ups <> Fact.Ups_Dest OR Staging.Call_Customer_Ups <> Fact.Call_Customer_Ups_Dest 
  OR Staging.Raw_Traffic_Count <> Fact.Raw_Traffic_Count_Dest OR Staging.Traffic_Count <> Fact.Traffic_Count_Dest 
""")
UpdateRecordsDF.createOrReplaceTempView("Updates")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) AS UpdatesCount FROM Updates

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
  LEFT OUTER JOIN Fact on Staging.Location_Code_nk = Fact.Location_Code_nk_Dest AND Staging.Date_fk = Fact.Date_fk_Dest AND Staging.Hour_code = Fact.Hour_code_Dest
  WHERE Fact.Location_Code_nk_Dest IS NULL
""")
NewRecordsDF.createOrReplaceTempView("NewRecords")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) AS NewCount FROM NewRecords

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation:
# MAGIC We truncate our bulk update table. Using the Spark SQL DB Connector to run specific commands. The JDBC driver doesn't support this.

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
# MAGIC   "queryCustom"  -> "TRUNCATE TABLE Updates.Fact_Traffic_Update"
# MAGIC ))
# MAGIC 
# MAGIC sqlContext.sqlDBQuery(storedproc)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation:
# MAGIC Loading changed records to bulk update table using JDBC.

# COMMAND ----------

UpdateRecordsDF.write.jdbc(url=jdbcUrl, table="Updates.Fact_Traffic_Update", mode="append", properties=connectionProperties) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation:
# MAGIC Inserting directly to the fact table using JDBC.

# COMMAND ----------

NewRecordsDF.write \
  .jdbc(url=jdbcUrl, table="Sales.Fact_Traffic", mode="append", properties=connectionProperties)

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
# MAGIC   "queryCustom"  -> "Exec dbo.usp_Load_Fact_Traffic"
# MAGIC ))
# MAGIC 
# MAGIC sqlContext.sqlDBQuery(storedproc)