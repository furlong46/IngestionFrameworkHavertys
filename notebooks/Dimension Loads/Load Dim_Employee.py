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

# MAGIC %run "/Dimension Loads/Initialize Connections and Functions" $BulkInsertTableName_Update="" $BulkInsertTableName_Insert=""

# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/datalake/staging/master data/employee/DB203002/ipcorpdta_tbdwemp")

# COMMAND ----------

ipcorpdta_tbdwemp_DF = spark.read \
       .parquet("/mnt/datalake/staging/master data/employee/DB203002/ipcorpdta_tbdwemp") \
       .createOrReplaceTempView("ipcorpdta_tbdwemp")

# COMMAND ----------

def udfTitle(str):
    return str.title()
  
spark.udf.register("udfTitle", udfTitle)

# COMMAND ----------

StagingQuery = """
with Base as (
  select  
    process_number, 
    employee_number,
    employee_number_type,
    case when sales_number = 0
        then null
        else sales_number end as sales_number,
    case when wms_number = 0
        then null
        else wms_number end as wms_number,
    case when driver_number = 0
        then null
        else driver_number end as driver_number,
    case when designer_number = 0
        then null
        else designer_number end as designer_number,
    first_name,
    last_name,
    division,
    center,
    branch
    from ipcorpdta_tbdwemp
),
Staging as (
select
  trim(employee_number) as employee_id_nk,
  trim(employee_number_type) as employeetype_code,
  case when trim(employee_number_type) = 'E' then 'Employee'
      when trim(employee_number_type) = 'G' then 'Contractor'
  else ''
  end as employeetype_name,
  coalesce(last(sales_number, true) over (partition by trim(employee_number) order by process_number desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),0) sales_id,
  coalesce(last(wms_number, true) over (partition by trim(employee_number) order by process_number desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),0) wms_id,
  coalesce(last(driver_number, true) over (partition by trim(employee_number) order by process_number desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),0) driver_id,
  coalesce(last(designer_number, true) over (partition by trim(employee_number) order by process_number desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),0) designer_id,
  trim(first_name) as first_name,
  trim(last_name) as last_name,
  concat(trim(first_name),' ',trim(last_name)) as full_name,
  division as division_id,
  center as market_id,
  branch as branch_id,
  ROW_NUMBER() OVER(PARTITION BY trim(employee_number) ORDER BY process_number desc) AS RowNum
  from base  
  )
  
SELECT 
  employee_id_nk, 
  employeetype_code, 
  employeetype_name, 
  sales_id, 
  wms_id, 
  driver_id, 
  designer_id, 
  udfTitle(first_name) as first_name, 
  udfTitle(last_name) as last_name, 
  udfTitle(full_name) as full_name, 
  division_id, 
  market_id, 
  branch_id ,
 udfSHA1withPython(array(EmployeeType_Code,First_Name,Last_Name,Division_ID,Market_ID,Branch_ID,Sales_ID,WMS_ID,Driver_ID,Full_Name,EmployeeType_Name,Designer_ID)) AS HashKey,
  0 AS SourceSystem_fk, CAST({0} AS INT) AS ETLBatchID_Insert, CAST({0} AS INT) AS ETLBatchID_Update 
  FROM Staging
 WHERE RowNum = 1
""".format(varMasterProcessNumber)

# COMMAND ----------

StagingQuery_DF = spark.sql(StagingQuery)

# COMMAND ----------

StagingQuery_DF.cache()
StagingQuery_DF.createOrReplaceTempView("Staging")

# COMMAND ----------

#%sql
#SELECT * FROM Staging where employee_id_nk = 65239    

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Employee dimension for comparison
# MAGIC Load the Employee dimension from the data warehouse for comparison to the source data in the Data Lake

# COMMAND ----------

Dim_EmployeeQuery = """
(SELECT
Employee_sk AS Employee_sk_Dest,
Employee_id_nk as Employee_ID_nk_Dest,
HashKey AS HashKey_Dest,
ETLBatchID_Insert AS ETLBatchID_Insert_Dest
FROM
DW.Dim_Employee) Dim_Employee
"""

# COMMAND ----------

Dim_Employee_DF = (spark.read
    .jdbc(url=jdbcUrl, table=Dim_EmployeeQuery, properties=connectionProperties)
)

# COMMAND ----------

Dim_Employee_DF.cache()
Dim_Employee_DF.createOrReplaceTempView("Dim_Employee")

# COMMAND ----------

# %sql
# SELECT COUNT(1) FROM Dim_Employee

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation:
# MAGIC Here we are checking which of our Staging records are updates by checking which already exist in our dimension.  We compare our hash keys to see if the record has changed in any way.  We store the results to a dataframe called UpdateRecordsDF

# COMMAND ----------

UpdateRecordsDF = spark.sql("""
SELECT 
Staging.*, Dim_Employee.*
FROM 
staging
INNER JOIN Dim_Employee ON Staging.Employee_ID_nk = Dim_Employee.Employee_ID_nk_Dest 
WHERE Staging.HashKey <> Dim_Employee.HashKey_Dest 
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation:
# MAGIC Here we are checking which of our Staging records are inserts by checking which don't exist in our dimension.  We store the results to a dataframe called NewRecordsDF.  

# COMMAND ----------

NewRecordsDF = spark.sql("""
SELECT 
Staging.*,  Dim_Employee.*
FROM 
Staging
LEFT OUTER JOIN Dim_Employee ON Staging.Employee_ID_nk = Dim_Employee.Employee_ID_nk_Dest 
WHERE Dim_Employee.Employee_sk_dest IS NULL
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
# MAGIC   "queryCustom"  -> "TRUNCATE TABLE Updates.Dim_Employee_Update"
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
  .jdbc(url=jdbcUrl, table="Updates.Dim_Employee_Update", mode="append", properties=connectionProperties) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation:
# MAGIC Inserting directly to the dimension using JDBC.  Dropping columns that aren't needed.

# COMMAND ----------

#Creating the table.  JDBC creates the table or overwrites it
NewRecordsDF.drop('Employee_sk_Dest', 'Employee_ID_nk_Dest', 'HashKey_Dest', 'ETLBatchID_Insert_Dest').write \
  .jdbc(url=jdbcUrl, table="DW.Dim_Employee", mode="append", properties=connectionProperties) 

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
# MAGIC   "queryCustom"  -> "Exec dbo.usp_Load_Dim_Employee"
# MAGIC ))
# MAGIC 
# MAGIC sqlContext.sqlDBQuery(storedproc)