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

# MAGIC %run "/Data Lake Schema/Initialize Connections and Functions" $BulkInsertTableName_Update="" $BulkInsertTableName_Insert=""

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Checking what's in the Data Lake

# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/datalake/staging/master data/product/erp")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation:
# MAGIC Our ETL in this notebook really starts when we source data from the Data Lake.  In this section we're loading our parquet files from the Data Lake into dataframes.  We also enable the dataframes to be refereced as a hive table/view for enabling SQL queries later in the ETL.  

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load the Latest Data Lake Data
# MAGIC Loading the various directories and parquet files from the Data Lake into dataframes and temporary views

# COMMAND ----------

ProductDL = spark.read \
       .parquet("/mnt/datalake/staging/master data/product/erp/SalesLT_Product") \
       .createOrReplaceTempView("vwProduct")

# COMMAND ----------

ProductCategoryDL = spark.read \
       .parquet("/mnt/datalake/staging/master data/product/erp/SalesLT_ProductCategory") \
       .createOrReplaceTempView("vwProductCategory")

# COMMAND ----------

ProductModelDL = spark.read \
       .parquet("/mnt/datalake/staging/master data/product/erp/SalesLT_ProductModel") \
       .createOrReplaceTempView("vwProductModel")

# COMMAND ----------

ProductModelProductDescriptionDL = spark.read \
       .parquet("/mnt/datalake/staging/master data/product/erp/SalesLT_ProductModelProductDescription") \
       .createOrReplaceTempView("vwProductModelProductDescription")

# COMMAND ----------

ProductDescriptionDL = spark.read \
       .parquet("/mnt/datalake/staging/master data/product/erp/SalesLT_ProductDescription") \
       .createOrReplaceTempView("vwProductDescription")

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

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Staging AS
# MAGIC 
# MAGIC WITH DESCLookup AS (
# MAGIC SELECT 
# MAGIC PMPD.ProductModelID,
# MAGIC PMPD.Culture,
# MAGIC PD.Description
# MAGIC FROM 
# MAGIC vwProductModelProductDescription PMPD
# MAGIC LEFT OUTER JOIN vwProductDescription PD ON PMPD.ProductDescriptionID = PD.ProductDescriptionID
# MAGIC )
# MAGIC ,
# MAGIC Source AS (
# MAGIC SELECT 
# MAGIC        P.ProductID
# MAGIC       ,P.ProductNumber AS ProductAlternateKey
# MAGIC       ,P.ProductNumber AS _nk
# MAGIC       ,P.ProductCategoryID AS ProductSubcategoryKey
# MAGIC       ,'' AS WeightUnitMeasureCode
# MAGIC       ,'' AS SizeUnitMeasureCode
# MAGIC       ,P.Name AS EnglishProductName
# MAGIC       ,P.StandardCost AS StandardCost
# MAGIC       ,1 AS FinishedGoodsFlag
# MAGIC       ,P.Color AS Color
# MAGIC       ,'' AS SafetyStockLevel
# MAGIC       ,'' AS ReorderPoint
# MAGIC       ,P.ListPrice AS ListPrice
# MAGIC       ,P.Size AS Size
# MAGIC       ,'' AS SizeRange
# MAGIC       ,P.Weight AS Weight
# MAGIC       ,'' AS DaysToManufacture
# MAGIC       ,'' AS ProductLine
# MAGIC       ,'' AS DealerPrice
# MAGIC       ,'' AS Class
# MAGIC       ,'' AS Style
# MAGIC       ,PM.Name AS ModelName
# MAGIC       ,PDEN.Description AS EnglishDescription
# MAGIC       ,PDFR.Description AS FrenchDescription
# MAGIC       ,PDCH.Description AS ChineseDescription
# MAGIC       ,PDAR.Description AS ArabicDescription
# MAGIC       ,PDHE.Description AS HebrewDescription
# MAGIC       ,PDTH.Description AS ThaiDescription
# MAGIC       ,'' AS GermanDescription
# MAGIC       ,'' AS JapaneseDescription
# MAGIC       ,'' AS TurkishDescription
# MAGIC       ,P.SellStartDate AS StartDate
# MAGIC       ,P.SellEndDate AS EndDate
# MAGIC       ,CASE WHEN P.SellEndDate IS NULL THEN 'Current' ELSE '' END AS Status
# MAGIC FROM 
# MAGIC vwProduct P
# MAGIC LEFT OUTER JOIN vwProductCategory PC ON P.ProductCategoryID = PC.ProductCategoryID
# MAGIC LEFT OUTER JOIN vwProductModel PM ON P.ProductModelID = PM.ProductModelID
# MAGIC LEFT OUTER JOIN (SELECT ProductModelID, Description FROM DESCLookup WHERE TRIM(Culture) = 'en') PDEN ON P.ProductModelID = PDEN.ProductModelID
# MAGIC LEFT OUTER JOIN (SELECT ProductModelID, Description FROM DESCLookup WHERE TRIM(Culture) = 'ar') PDAR ON P.ProductModelID = PDAR.ProductModelID
# MAGIC LEFT OUTER JOIN (SELECT ProductModelID, Description FROM DESCLookup WHERE TRIM(Culture) = 'fr') PDFR ON P.ProductModelID = PDFR.ProductModelID
# MAGIC LEFT OUTER JOIN (SELECT ProductModelID, Description FROM DESCLookup WHERE TRIM(Culture) = 'he') PDHE ON P.ProductModelID = PDHE.ProductModelID
# MAGIC LEFT OUTER JOIN (SELECT ProductModelID, Description FROM DESCLookup WHERE TRIM(Culture) = 'zh-cht') PDCH ON P.ProductModelID = PDCH.ProductModelID
# MAGIC LEFT OUTER JOIN (SELECT ProductModelID, Description FROM DESCLookup WHERE TRIM(Culture) = 'th') PDTH ON P.ProductModelID = PDTH.ProductModelID
# MAGIC )
# MAGIC 
# MAGIC SELECT *,
# MAGIC udfMD5withPython(array(ProductID
# MAGIC            ,ProductSubcategoryKey
# MAGIC            ,WeightUnitMeasureCode
# MAGIC            ,SizeUnitMeasureCode
# MAGIC            ,EnglishProductName
# MAGIC            ,StandardCost
# MAGIC            ,FinishedGoodsFlag
# MAGIC            ,Color
# MAGIC            ,SafetyStockLevel
# MAGIC            ,ReorderPoint
# MAGIC            ,ListPrice
# MAGIC            ,Size
# MAGIC            ,SizeRange
# MAGIC            ,Weight
# MAGIC            ,DaysToManufacture
# MAGIC            ,ProductLine
# MAGIC            ,DealerPrice
# MAGIC            ,Class
# MAGIC            ,Style
# MAGIC            ,ModelName
# MAGIC            ,EnglishDescription
# MAGIC            ,FrenchDescription
# MAGIC            ,ChineseDescription
# MAGIC            ,ArabicDescription
# MAGIC            ,HebrewDescription
# MAGIC            ,ThaiDescription
# MAGIC            ,GermanDescription
# MAGIC            ,JapaneseDescription
# MAGIC            ,TurkishDescription
# MAGIC            ,StartDate
# MAGIC            ,EndDate
# MAGIC            ,Status
# MAGIC   )
# MAGIC   ) AS HashKey
# MAGIC FROM Source

# COMMAND ----------

# %sql
# SELECT * FROM Staging

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Auto-trim strings
# MAGIC 
# MAGIC Many of the DB2 character fields come with extra spaces. These should generally all be removed

# COMMAND ----------

from pyspark.sql.functions import *

autotrimStringsDF = spark.table("Staging")

for columnName, columnType in autotrimStringsDF.dtypes:
  if columnType == "string":
    autotrimStringsDF = autotrimStringsDF.withColumn(columnName, coalesce(trim(col(columnName)), lit("")))
    
autotrimStringsDF.createOrReplaceTempView("Staging")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load SKU dimension for comparison
# MAGIC Load the SKU dimension from the data warehouse for comparison to the source data in the Data Lake

# COMMAND ----------

Dim_Query = """
(SELECT
Product_sk AS _sk_Dest,
ProductAlternateKey AS _nk_Dest,
HashKey AS HashKey_Dest
FROM
DW.Dim_Product) Dim
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
# MAGIC Loading changed records to a bulk update table using JDBC. 

# COMMAND ----------

#Creating the table.  JDBC creates the table or overwrites it
UpdateRecordsDF.write \
  .jdbc(url=jdbcUrl, table="Updates.Dim_Product_Update", mode="overwrite", properties=connectionProperties) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation:
# MAGIC Inserting directly to the dimension using JDBC.  Dropping columns that aren't needed.

# COMMAND ----------

#Creating the table.  JDBC creates the table or overwrites it
NewRecordsDF.drop('_nk', '_sk_Dest', '_nk_Dest', 'HashKey_Dest', 'ETLBatchID_Insert_Dest').write \
  .jdbc(url=jdbcUrl, table="DW.Dim_Product", mode="append", properties=connectionProperties) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation:
# MAGIC Running a Stored Procedure to do a Bulk Update.  Using the Spark SQL DB Connector to run specific commands.  The JDBC driver doesn't support this.

# COMMAND ----------

# MAGIC %scala 
# MAGIC val BulkUpdateQuery = """UPDATE Dim_Product
# MAGIC SET
# MAGIC  Dim_Product.ProductID				= ISNULL(Stg.ProductID			  , Dim_Product.ProductID			 )
# MAGIC ,Dim_Product.ProductAlternateKey	= ISNULL(Stg.ProductAlternateKey  , Dim_Product.ProductAlternateKey	 )
# MAGIC ,Dim_Product.ProductSubcategoryKey	= ISNULL(Stg.ProductSubcategoryKey, Dim_Product.ProductSubcategoryKey)
# MAGIC ,Dim_Product.WeightUnitMeasureCode	= ISNULL(Stg.WeightUnitMeasureCode, Dim_Product.WeightUnitMeasureCode)
# MAGIC ,Dim_Product.SizeUnitMeasureCode	= ISNULL(Stg.SizeUnitMeasureCode  , Dim_Product.SizeUnitMeasureCode	 )
# MAGIC ,Dim_Product.EnglishProductName		= ISNULL(Stg.EnglishProductName	  , Dim_Product.EnglishProductName	 )
# MAGIC ,Dim_Product.StandardCost			= ISNULL(Stg.StandardCost		  , Dim_Product.StandardCost		 )
# MAGIC ,Dim_Product.FinishedGoodsFlag		= ISNULL(Stg.FinishedGoodsFlag	  , Dim_Product.FinishedGoodsFlag	 )
# MAGIC ,Dim_Product.Color					= ISNULL(Stg.Color				  , Dim_Product.Color				 )
# MAGIC ,Dim_Product.SafetyStockLevel		= ISNULL(Stg.SafetyStockLevel	  , Dim_Product.SafetyStockLevel	 )
# MAGIC ,Dim_Product.ReorderPoint			= ISNULL(Stg.ReorderPoint		  , Dim_Product.ReorderPoint		 )
# MAGIC ,Dim_Product.ListPrice				= ISNULL(Stg.ListPrice			  , Dim_Product.ListPrice			 )
# MAGIC ,Dim_Product.Size					= ISNULL(Stg.Size				  , Dim_Product.Size				 )
# MAGIC ,Dim_Product.SizeRange				= ISNULL(Stg.SizeRange			  , Dim_Product.SizeRange			 )
# MAGIC ,Dim_Product.Weight					= ISNULL(Stg.Weight				  , Dim_Product.Weight				 )
# MAGIC ,Dim_Product.DaysToManufacture		= ISNULL(Stg.DaysToManufacture	  , Dim_Product.DaysToManufacture	 )
# MAGIC ,Dim_Product.ProductLine			= ISNULL(Stg.ProductLine		  , Dim_Product.ProductLine			 )
# MAGIC ,Dim_Product.DealerPrice			= ISNULL(Stg.DealerPrice		  , Dim_Product.DealerPrice			 )
# MAGIC ,Dim_Product.Class					= ISNULL(Stg.Class				  , Dim_Product.Class				 )
# MAGIC ,Dim_Product.Style					= ISNULL(Stg.Style				  , Dim_Product.Style				 )
# MAGIC ,Dim_Product.ModelName				= ISNULL(Stg.ModelName			  , Dim_Product.ModelName			 )
# MAGIC ,Dim_Product.EnglishDescription		= ISNULL(Stg.EnglishDescription	  , Dim_Product.EnglishDescription	 )
# MAGIC ,Dim_Product.FrenchDescription		= ISNULL(Stg.FrenchDescription	  , Dim_Product.FrenchDescription	 )
# MAGIC ,Dim_Product.ChineseDescription		= ISNULL(Stg.ChineseDescription	  , Dim_Product.ChineseDescription	 )
# MAGIC ,Dim_Product.ArabicDescription		= ISNULL(Stg.ArabicDescription	  , Dim_Product.ArabicDescription	 )
# MAGIC ,Dim_Product.HebrewDescription		= ISNULL(Stg.HebrewDescription	  , Dim_Product.HebrewDescription	 )
# MAGIC ,Dim_Product.ThaiDescription		= ISNULL(Stg.ThaiDescription	  , Dim_Product.ThaiDescription		 )
# MAGIC ,Dim_Product.GermanDescription		= ISNULL(Stg.GermanDescription	  , Dim_Product.GermanDescription	 )
# MAGIC ,Dim_Product.JapaneseDescription	= ISNULL(Stg.JapaneseDescription  , Dim_Product.JapaneseDescription	 )
# MAGIC ,Dim_Product.TurkishDescription		= ISNULL(Stg.TurkishDescription	  , Dim_Product.TurkishDescription	 )
# MAGIC ,Dim_Product.StartDate				= ISNULL(Stg.StartDate			  , Dim_Product.StartDate			 )
# MAGIC ,Dim_Product.EndDate				= ISNULL(Stg.EndDate			  , Dim_Product.EndDate				 )
# MAGIC ,Dim_Product.Status					= ISNULL(Stg.Status				  , Dim_Product.Status				 )
# MAGIC ,Dim_Product.HashKey				= ISNULL(Stg.HashKey			  , Dim_Product.HashKey			     )
# MAGIC FROM DW.Dim_Product
# MAGIC INNER JOIN Updates.Dim_Product_Update Stg ON Dim_Product.Product_sk = Stg._sk_Dest
# MAGIC WHERE Stg.HashKey <> Dim_Product.HashKey
# MAGIC """

# COMMAND ----------

# MAGIC %scala 
# MAGIC import com.microsoft.azure.sqldb.spark.config.Config
# MAGIC import com.microsoft.azure.sqldb.spark.query._
# MAGIC 
# MAGIC val storedproc = Config(Map(
# MAGIC   "url"          -> dbutils.secrets.get(scope = "key-vault-secrets", key = "DWServerName"),
# MAGIC   "databaseName" -> dbutils.secrets.get(scope = "key-vault-secrets", key = "DWDBName"),
# MAGIC   "user"         -> dbutils.secrets.get(scope = "key-vault-secrets", key = "DWETLAccount"),
# MAGIC   "password"     -> dbutils.secrets.get(scope = "key-vault-secrets", key = "DWETLAccountPassword"),
# MAGIC   "queryCustom"  -> BulkUpdateQuery
# MAGIC ))
# MAGIC 
# MAGIC sqlContext.sqlDBQuery(storedproc)

# COMMAND ----------


