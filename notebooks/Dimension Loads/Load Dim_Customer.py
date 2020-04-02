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

# dbutils.fs.ls("dbfs:/mnt/datalake/staging/master data/product/DB203002")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation:
# MAGIC Our ETL in this notebook really starts when we source data from the Data Lake.  In this section we're loading our parquet files from the Data Lake into dataframes.  We also enable the dataframes to be refereced as a hive table/view for enabling SQL queries later in the ETL.  

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load the Latest Data Lake Data
# MAGIC Loading the various directories and parquet files from the Data Lake into dataframes and temporary views

# COMMAND ----------

# DB2table_DF = spark.read \
#        .parquet("/mnt/datalake/.../DB2table/") \
#        .createOrReplaceTempView("DB2table")

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
WITH Stage AS (
SELECT 
 CAST(p.SEQNUM AS INT) AS Customer_Sequence_nk
,CAST(c.CIDNUM AS LONG) AS Customer_ID_nk
,TRIM(COALESCE(p.STATUS, '')) AS Sequence_Status
,TRIM(COALESCE(bn.FIRST, '')) AS Bill_First_Name
,TRIM(COALESCE(bn.LAST, '')) AS Bill_Last_Name
,TRIM(COALESCE(bn.STATUS, '')) AS Bill_Name_Status
,TRIM(COALESCE(ba.ADDRESS1, '')) AS Bill_Address
,TRIM(COALESCE(ba.CITY, '')) AS Bill_City
,TRIM(COALESCE(ba.STATE, '')) AS Bill_State
,TRIM(COALESCE(RIGHT('00' || CAST(CAST(ba.ZIPCODE AS LONG) AS STRING), 5), '')) AS Bill_ZIP
,TRIM(COALESCE(CAST(CAST(ba.ZIPCODE4 AS LONG) AS STRING), '')) AS Bill_ZIP4
,COALESCE(ba.LATITUDE,0) AS Bill_Latitude
,COALESCE(ba.LONGITUDE,0) AS Bill_Longitude
,TRIM(COALESCE(ba.STATUS, '')) AS Bill_Address_Status
,TRIM(COALESCE(dn.FIRST, '')) AS Delivery_First_Name
,TRIM(COALESCE(dn.LAST, '')) AS Delivery_Last_Name
,TRIM(COALESCE(dn.STATUS, '')) AS Delivery_Name_Status
,TRIM(COALESCE(da.ADDRESS1, '')) AS Delivery_Address
,TRIM(COALESCE(da.CITY, '')) AS Delivery_City
,TRIM(COALESCE(da.STATE, '')) AS Delivery_State
,TRIM(COALESCE(RIGHT('00' || CAST(CAST(da.ZIPCODE AS LONG) AS STRING), 5), '')) AS Delivery_ZIP
,TRIM(COALESCE(CAST(CAST(da.ZIPCODE4 AS LONG) AS STRING), '')) AS Delivery_ZIP4
,COALESCE(da.LATITUDE,0) AS Delivery_Latitude
,COALESCE(da.LONGITUDE,0) AS Delivery_Longitude
,TRIM(COALESCE(da.STATUS, '')) AS Delivery_Address_Status
,COALESCE(date_format(date_trunc('DD', p.CREATED), 'YYYY-MM-dd'), '1900-01-01') AS Customer_Profile_Created_Date
,TRIM(COALESCE(pn.FIRST, '')) AS Customer_First_Name
,TRIM(COALESCE(pn.LAST, '')) AS Customer_Last_Name
,TRIM(COALESCE(pn.STATUS, '')) AS Customer_Name_Status
,TRIM(COALESCE(pa.ADDRESS1, '')) AS Customer_Address
,TRIM(COALESCE(pa.CITY, '')) AS Customer_City
,TRIM(COALESCE(pa.STATE, '')) AS Customer_State
,TRIM(COALESCE(RIGHT('00' || CAST(CAST(pa.ZIPCODE AS LONG) AS STRING), 5), '')) AS Customer_ZIP
,TRIM(COALESCE(CAST(CAST(pa.ZIPCODE4 AS LONG) AS STRING), '')) AS Customer_ZIP4
,COALESCE(pa.LATITUDE,0) AS Customer_Latitude
,COALESCE(pa.LONGITUDE,0) AS Customer_Longitude
,TRIM(COALESCE(pp.TYPE, '')) AS Customer_Phone_Type
,TRIM(COALESCE(CAST(CAST(pp.PHONE AS LONG) AS STRING), '')) AS Customer_Phone
,TRIM(COALESCE(CAST(CAST(pp.EXTENSION AS LONG) AS STRING), '')) AS Customer_Phone_Extension
,TRIM(COALESCE(pp.STATUS, '')) AS Customer_Phone_Status
,TRIM(COALESCE(pe.EMAIL, '')) AS Customer_Email
,TRIM(COALESCE(pe.STATUS, '')) AS Customer_Email_Status
,COALESCE( date_format(date_trunc('DD', c.CREATED), 'YYYY-MM-dd'), '1900-01-01') AS Customer_Created_Date
,TRIM(COALESCE(c.STATUS, '')) AS Customer_Status
,TRIM(COALESCE(c.NOEMAIL, '')) AS No_Email
,TRIM(COALESCE(c.ROOMPLAN, '')) AS Room_Plan
,TRIM(COALESCE(c.SENDADV, '')) AS Send_Advertising
,TRIM(COALESCE(c.SENDCAT, '')) AS Send_Catalog
,TRIM(COALESCE(c.SENDNEWS, '')) AS Send_News
,TRIM(COALESCE(c.SENDPMAIL, '')) AS Send_P_Mail
,0 AS SourceSystem_fk
,CAST({0} AS INT) AS ETLBatchID_Insert
,CAST({0} AS INT) AS ETLBatchID_Update
,ROW_NUMBER() OVER(PARTITION BY c.cidnum, p.seqnum ORDER BY c.cidnum, p.seqnum) AS ROWNUM
FROM 
--customer/customer profile granularity
DataLakeCurated.pcrundata_tbcid c
INNER JOIN DataLakeCurated.pcrundata_tbcidprf p on c.cidnum = p.cidnum
--profile table joins
LEFT OUTER JOIN DataLakeCurated.pcrundata_tbcidadr ba on c.cidnum = ba.cidnum and p.biladrseq = ba.seqnum
LEFT OUTER JOIN DataLakeCurated.pcrundata_tbcidadr da on c.cidnum = da.cidnum and p.dlvadrseq = da.seqnum
LEFT OUTER JOIN DataLakeCurated.pcrundata_tbcidnam bn on c.cidnum = bn.cidnum and p.bilnamseq = bn.seqnum
LEFT OUTER JOIN DataLakeCurated.pcrundata_tbcidnam dn on c.cidnum = dn.cidnum and p.dlvnamseq = dn.seqnum
--preferred table joins
LEFT OUTER JOIN DataLakeCurated.pcrundata_tbcidnam pn on c.cidnum = pn.cidnum and c.prefnam = pn.seqnum
LEFT OUTER JOIN DataLakeCurated.pcrundata_tbcideml pe on c.cidnum = pe.cidnum and c.prefeml = pe.seqnum
LEFT OUTER JOIN DataLakeCurated.pcrundata_tbcidphn pp on c.cidnum = pp.cidnum and c.prefphn = pp.seqnum
LEFT OUTER JOIN DataLakeCurated.pcrundata_tbcidadr pa on c.cidnum = pa.cidnum and c.prefadr = pa.seqnum
)

SELECT 
Customer_Sequence_nk, Customer_ID_nk, Sequence_Status, Bill_First_Name, Bill_Last_Name, Bill_Name_Status, Bill_Address, Bill_City, Bill_State, Bill_ZIP, Bill_ZIP4, Bill_Latitude, Bill_Longitude, Bill_Address_Status, Delivery_First_Name, Delivery_Last_Name, Delivery_Name_Status, Delivery_Address, Delivery_City, Delivery_State, Delivery_ZIP, Delivery_ZIP4, Delivery_Latitude, Delivery_Longitude, Delivery_Address_Status, Customer_Profile_Created_Date, Customer_First_Name, Customer_Last_Name, Customer_Name_Status, Customer_Address, Customer_City, Customer_State, Customer_ZIP, Customer_ZIP4, Customer_Latitude, Customer_Longitude, Customer_Phone_Type, Customer_Phone, Customer_Phone_Extension, Customer_Phone_Status, Customer_Email, Customer_Email_Status, Customer_Created_Date, Customer_Status, No_Email, Room_Plan, Send_Advertising, Send_Catalog, Send_News, Send_P_Mail, 
udfSHA1withPython(array(Sequence_Status, Bill_First_Name, Bill_Last_Name, Bill_Name_Status, Bill_Address, Bill_City, Bill_State, Bill_ZIP, Bill_ZIP4, Bill_Latitude, Bill_Longitude, Bill_Address_Status, Delivery_First_Name, Delivery_Last_Name, Delivery_Name_Status, Delivery_Address, Delivery_City, Delivery_State, Delivery_ZIP, Delivery_ZIP4, Delivery_Latitude, Delivery_Longitude, Delivery_Address_Status, Customer_Profile_Created_Date, Customer_First_Name, Customer_Last_Name, Customer_Name_Status, Customer_Address, Customer_City, Customer_State, Customer_ZIP, Customer_ZIP4, Customer_Latitude, Customer_Longitude, Customer_Phone_Type, Customer_Phone, Customer_Phone_Extension, Customer_Phone_Status, Customer_Email, Customer_Email_Status, Customer_Created_Date, Customer_Status, No_Email, Room_Plan, Send_Advertising, Send_Catalog, Send_News, Send_P_Mail)) AS HashKey,
SourceSystem_fk, ETLBatchID_Insert, ETLBatchID_Update
FROM Stage WHERE ROWNUM=1
""".format(varMasterProcessNumber)

# COMMAND ----------

Staging_DF = spark.sql(Query)
Staging_DF.cache()
Staging_DF.createOrReplaceTempView("Staging")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) AS StagingCnt FROM Staging

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load SKU dimension for comparison
# MAGIC Load the SKU dimension from the data warehouse for comparison to the source data in the Data Lake

# COMMAND ----------

Dim_Query = """
(SELECT
Customer_sk AS Customer_sk_Dest,
Customer_Sequence_nk AS Customer_Sequence_nk_Dest,
Customer_ID_nk AS Customer_ID_nk_Dest,
HashKey AS HashKey_Dest,
ETLBatchID_Insert AS ETLBatchID_Insert_Dest
FROM
DW.Dim_Customer) Dim
"""

# COMMAND ----------

Dim_DF = spark.read.jdbc(url=jdbcUrl, table=Dim_Query, properties=connectionProperties)
Dim_DF.cache()
Dim_DF.createOrReplaceTempView("Dim")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) AS DimCnt FROM Dim

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
INNER JOIN Dim ON Staging.Customer_Sequence_nk = Dim.Customer_Sequence_nk_Dest AND Staging.Customer_ID_nk = Dim.Customer_ID_nk_Dest
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
LEFT OUTER JOIN Dim ON Staging.Customer_Sequence_nk = Dim.Customer_Sequence_nk_Dest AND Staging.Customer_ID_nk = Dim.Customer_ID_nk_Dest
WHERE Dim.Customer_sk_Dest IS NULL
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
# MAGIC   "queryCustom"  -> "TRUNCATE TABLE Updates.Dim_Customer_Update"
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
  .jdbc(url=jdbcUrl, table="Updates.Dim_Customer_Update", mode="append", properties=connectionProperties) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation:
# MAGIC Inserting directly to the dimension using JDBC.  Dropping columns that aren't needed.

# COMMAND ----------

#Creating the table.  JDBC creates the table or overwrites it
NewRecordsDF.drop('Customer_sk_Dest', 'Customer_Sequence_nk_Dest', 'Customer_ID_nk_Dest', 'HashKey_Dest', 'ETLBatchID_Insert_Dest').write \
  .jdbc(url=jdbcUrl, table="DW.Dim_Customer", mode="append", properties=connectionProperties) 

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
# MAGIC   "queryCustom"  -> "Exec dbo.usp_Load_Dim_Customer"
# MAGIC ))
# MAGIC 
# MAGIC sqlContext.sqlDBQuery(storedproc)

# COMMAND ----------

