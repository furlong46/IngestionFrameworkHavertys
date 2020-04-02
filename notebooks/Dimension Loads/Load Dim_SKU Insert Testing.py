# Databricks notebook source
#Creates and sets the widgets and variables used later on in the notebook.
dbutils.widgets.text("MasterProcessNumber", "0")

varMasterProcessNumber = dbutils.widgets.get("MasterProcessNumber")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation:
# MAGIC Our notebook starts with setting up connection information to our Data Lake in ADLS, and our Data Warehouse in Azure SQL DB.  

# COMMAND ----------

# MAGIC %md
# MAGIC #### Set the configurations for connecting to ADLS Gen2
# MAGIC Documentation on connecting to ADLS Gen2 is available here: https://docs.databricks.com/spark/latest/data-sources/azure/azure-datalake-gen2.html
# MAGIC All sensitive information is stored using Databricks secrets which can be integrated with Azure Key Vault.  More on secrets here: https://docs.databricks.com/user-guide/secrets/index.html

# COMMAND ----------

varApplicationId = dbutils.secrets.get(scope = "key-vault-secrets", key = "DataAnalyticsQAServicePrincipalClientID") #service principle id
varAuthenticationKey = dbutils.secrets.get(scope = "key-vault-secrets", key = "DataAnalyticsQAServicePrincipalSecret") #service principle key
varTenantId = "804ae74e-8a68-4c11-856b-ccf96b3c8e4e" #the directory id from azure active directory -> properties
varStorageAccountName = "dataanalyticsqaadls21" #storage acccount name
varFileSystemName = "datalake" #ADLS container name

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": varApplicationId,
           "fs.azure.account.oauth2.client.secret": varAuthenticationKey,
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/" + varTenantId + "/oauth2/token"}

# dbutils.fs.mount(
#   source = "abfss://" + varFileSystemName + "@" + varStorageAccountName + ".dfs.core.windows.net/",
#   mount_point = "/mnt/datalake",
#   extra_configs = configs)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Checking what's in the Data Lake

# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/datalake/staging/master data/product/DB203002")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Set the configurations for the Azure SQL Data DB
# MAGIC Documentation on connecting to Azure SQL DB is available here: https://docs.databricks.com/spark/latest/data-sources/sql-databases.html

# COMMAND ----------

jdbcUsername = "ETL" #The dedicated loading user login 
jdbcPassword = dbutils.secrets.get(scope = "key-vault-secrets", key = "HavertysDWETLAccountPassword") #The dediciated loading user login password
jdbcHostname = dbutils.secrets.get(scope = "key-vault-secrets", key = "HavertysDWServerName") #The Azure SQL Server
jdbcDatabase = "HavertysDW_QA" #The Azure SQL Data DB database name
jdbcPort = 1433

jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
connectionProperties = {
  "user" : jdbcUsername,
  "password" : jdbcPassword,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation:
# MAGIC Databricks allows for the creation of user defined funtions which can be used in your code.  Here we're adding a user defined function for creating hash keys.  We'll create a hash key on all of our column values in the dimension table from the source and the destination.  We'll later compare these values to determine if any fields in our row have changed. 

# COMMAND ----------

# MAGIC %md
# MAGIC Add a user defined MD5 Function - used for creating a hash key for all of the descriptive columns

# COMMAND ----------

import hashlib

def udfMD5Python(colList):
  concatenatedString = ''
  for index, stringValue in enumerate(colList):
    concatenatedString += ('' if stringValue is None else stringValue) # Concatenate all the strings together. Check if stringValue is NULL and insert a blank if true
    if index < len(colList)-1: # Check if its not the last element in the list. If true, then append a '|' to uniquely identify column values
      concatenatedString += '|'
  return hashlib.md5(concatenatedString.encode('utf-8')).hexdigest() # Convert the string to a binary string and then hash the binary string and output a hexidecimal value

spark.udf.register("udfMD5withPython", udfMD5Python)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation:
# MAGIC Our ETL in this notebook really starts when we source data from the Data Lake.  In this section we're loading our parquet files from the Data Lake into dataframes.  We also enable the dataframes to be refereced as a hive table/view for enabling SQL queries later in the ETL.  

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load the Latest Data Lake Data
# MAGIC Loading the various directories and parquet files from the Data Lake into dataframes and temporary views

# COMMAND ----------

pcrundata_tbitem_DF = spark.read \
       .parquet("/mnt/datalake/staging/master data/product/DB203002/pcrundata_tbitem") \
       .createOrReplaceTempView("pcrundata_tbitem")

# COMMAND ----------

pcrundata_tbgroup_DF = spark.read \
       .parquet("/mnt/datalake/staging/master data/product/DB203002/pcrundata_tbgroup") \
       .createOrReplaceTempView("pcrundata_tbgroup")

# COMMAND ----------

pcrundata_tbatrmlu_DF = spark.read \
       .parquet("/mnt/datalake/staging/master data/product/DB203002/pcrundata_tbatrmlu") \
       .createOrReplaceTempView("pcrundata_tbatrmlu")

# COMMAND ----------

pcrundata_tbatrmlv_DF = spark.read \
       .parquet("/mnt/datalake/staging/master data/product/DB203002/pcrundata_tbatrmlv") \
       .createOrReplaceTempView("pcrundata_tbatrmlv")

# COMMAND ----------

pcrundata_tbcolnam_DF = spark.read \
       .parquet("/mnt/datalake/staging/master data/product/DB203002/pcrundata_tbcolnam") \
       .createOrReplaceTempView("pcrundata_tbcolnam")

# COMMAND ----------

pcrundata_tbprod_DF = spark.read \
       .parquet("/mnt/datalake/staging/master data/product/DB203002/pcrundata_tbprod") \
       .createOrReplaceTempView("pcrundata_tbprod")

# COMMAND ----------

pcrundata_tbprmitm_DF = spark.read \
       .parquet("/mnt/datalake/staging/master data/product/DB203002/pcrundata_tbprmitm") \
       .createOrReplaceTempView("pcrundata_tbprmitm")

# COMMAND ----------

pcrundata_pcpmcrpcst_DF = spark.read \
       .parquet("/mnt/datalake/staging/master data/product/DB203002/pcrundata_pcpmcrpcst") \
       .createOrReplaceTempView("pcrundata_pcpmcrpcst")

# COMMAND ----------

pcrundata_tbprodml_DF = spark.read \
       .parquet("/mnt/datalake/staging/master data/product/DB203002/pcrundata_tbprodml") \
       .createOrReplaceTempView("pcrundata_tbprodml")

# COMMAND ----------

pcrundata_tbatrgrp_DF = spark.read \
       .parquet("/mnt/datalake/staging/master data/product/DB203002/pcrundata_tbatrgrp") \
       .createOrReplaceTempView("pcrundata_tbatrgrp")

# COMMAND ----------

pcrundata_tbrptcls_DF = spark.read \
       .parquet("/mnt/datalake/staging/master data/product/DB203002/pcrundata_tbrptcls") \
       .createOrReplaceTempView("pcrundata_tbrptcls")

# COMMAND ----------

ipcorpdta_pcpmskustk_DF = spark.read \
       .parquet("/mnt/datalake/staging/master data/product/DB203002/ipcorpdta_pcpmskustk") \
       .createOrReplaceTempView("ipcorpdta_pcpmskustk")

# COMMAND ----------

pcrundata_tborigin_DF = spark.read \
       .parquet("/mnt/datalake/staging/master data/product/DB203002/pcrundata_tborigin") \
       .createOrReplaceTempView("pcrundata_tborigin")

# COMMAND ----------

ipcorpdta_ippmvendor_DF = spark.read \
       .parquet("/mnt/datalake/staging/master data/vendor/DB203002/ipcorpdta_ippmvendor") \
       .createOrReplaceTempView("ipcorpdta_ippmvendor")

# COMMAND ----------

pcrundata_tbfactry_DF = spark.read \
       .parquet("/mnt/datalake/staging/master data/factory/DB203002/pcrundata_tbfactry") \
       .createOrReplaceTempView("pcrundata_tbfactry")

# COMMAND ----------

pcrundata_tbatrprd_DF = spark.read \
       .parquet("/mnt/datalake/staging/master data/product/DB203002/pcrundata_tbatrprd") \
       .createOrReplaceTempView("pcrundata_tbatrprd")

# COMMAND ----------

pcrundata_tblftitm_DF = spark.read \
       .parquet("/mnt/datalake/staging/master data/product/DB203002/pcrundata_tblftitm") \
       .createOrReplaceTempView("pcrundata_tblftitm")

# COMMAND ----------

pcrundata_tborigin_DF = spark.read \
       .parquet("/mnt/datalake/staging/master data/product/DB203002/pcrundata_tborigin") \
       .createOrReplaceTempView("pcrundata_tborigin")

# COMMAND ----------

pcrundata_tbatrrel_DF = spark.read \
       .parquet("/mnt/datalake/staging/master data/product/DB203002/pcrundata_tbatrrel") \
       .createOrReplaceTempView("pcrundata_tbatrrel")

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
# MAGIC WITH SKU AS (
# MAGIC SELECT
# MAGIC   i.item AS SKU_Unformatted_nk,
# MAGIC   left(i.ITEM, 1) || '-' || substring(i.ITEM, 2, 4) || '-' || substring(i.ITEM, 6, 4) AS SKU,
# MAGIC   i.desc AS SKU_Description,
# MAGIC   i.atrid02 AS Vendor_Color_ID,
# MAGIC   vc.value1 AS Vendor_Color,
# MAGIC   '' AS Vendor_Color_Hex_Code,
# MAGIC   i.atrid09 AS Vendor_Finish_ID,
# MAGIC   vf.value1 AS Vendor_Finish,
# MAGIC   i.atrid12 AS Configuration_ID,
# MAGIC   vg.value1 AS Configuration,
# MAGIC   vg.value3 AS Display_Configuration,
# MAGIC   i.atrid03 AS Size_ID,
# MAGIC   us.value1 AS Size,
# MAGIC   i.origvnd AS Original_Vendor_ID,
# MAGIC   0 AS Original_Vendor_sk,
# MAGIC   g.collid AS Collection_ID,
# MAGIC   c.name AS `Collection`,
# MAGIC   i.groupid AS Group_ID,
# MAGIC   g.group AS Group,
# MAGIC   i.prodid AS Product_ID,
# MAGIC   pml.product AS Product,
# MAGIC   i.rptclass AS Reporting_Class_Code,
# MAGIC   rc.desc AS Reporting_Class,
# MAGIC   i.skutype AS SKU_Type,
# MAGIC   i.stocked AS Stocked,
# MAGIC   case when i.rptclass = '10' and i.skutype != 'V' then 'Gold Plan'
# MAGIC        when i.skutype = 'V' then 'Delivery'
# MAGIC        when i.skutype = 'M' then 'Markdown'
# MAGIC        when i.skutype = 'S' then 'Special Order'
# MAGIC        when i.skutype = 'G' then 'Group'
# MAGIC        when i.stocked = 'C' then 'Custom Choice'
# MAGIC        when i.stocked = 'Y' then 'Stocked'
# MAGIC        when i.stocked = 'N' and i.skutype = 'C' then 'Not Stocked'
# MAGIC   else 'Unknown'
# MAGIC   end
# MAGIC   AS Type_of_SKU,
# MAGIC   i.vendor AS Vendor_Code,
# MAGIC   0 AS Vendor_fk,
# MAGIC   g.factoryid AS Factory_Code,
# MAGIC   f.desc AS Factory_Name,
# MAGIC   0 AS Factory_fk,
# MAGIC   g.specordavl AS Special_Order_Available,
# MAGIC   g.vendorlead AS Vendor_Lead_Time,
# MAGIC   g.splordprod AS Product_Lead_Time,
# MAGIC   g.activestr AS Group_Active_Start,
# MAGIC   g.activeend AS Group_Active_End,
# MAGIC   i.flowstr AS SKU_Flow_Start,
# MAGIC   i.flowend AS SKU_Flow_End,
# MAGIC   stE.skustkwhse AS Stocked_at_EDC,
# MAGIC   stE.skustkwhse AS Stocked_at_WDC,
# MAGIC   stE.skustkwhse AS Stocked_at_FDC,
# MAGIC   g.webstatus AS Group_Web_Status,
# MAGIC   i.webstatus AS SKU_Web_Status,
# MAGIC   g.advsignoff AS Merchandising_Approval,
# MAGIC   g.mdssignoff AS Marketing_Approval,
# MAGIC   '' AS Lineup_Status,
# MAGIC   case when current_date between i.flowstr and coalesce(i.flowend, to_date('2099-12-31', 'yyyy-MM-dd')) then 'Y' else 'N' end AS Available_to_Purchase,
# MAGIC   i.shiplength AS Ship_Length,
# MAGIC   i.shipwidth AS Ship_Width,
# MAGIC   i.shipheight AS Ship_Height,
# MAGIC   i.length AS Length,
# MAGIC   i.width AS Width,
# MAGIC   i.height AS Height,
# MAGIC   i.weight AS Weight,
# MAGIC   i.prepcubes AS Prep_Cubes,
# MAGIC   i.cubes AS Cubes,
# MAGIC   p.prepflag AS Prep_Flag,
# MAGIC   p.asmblyrqd AS Assembly_Required,
# MAGIC   p.asmblytime AS Assembly_Time,
# MAGIC   p.leaveinbox AS Leave_in_Box,
# MAGIC   i.mfgid AS Manufacturer_ID,
# MAGIC   i.upccode AS UPC,
# MAGIC   i.htsnumber AS HTS_Number,
# MAGIC   i.reportsku AS Original_SKU,
# MAGIC   p.repitem AS Representative_Item,
# MAGIC   g.ratingcnt AS Group_Rating_Count,
# MAGIC   p.ratingcnt AS Product_Rating_Count,
# MAGIC   g.starrating AS Group_Star_Rating,
# MAGIC   p.starrating AS Product_Star_Rating,
# MAGIC   g.miorigin AS `Made-in_ID`,
# MAGIC   mio.city AS `Made-in_City`,
# MAGIC   mio.state AS `Made-in_State`,
# MAGIC   mio.country AS `Made-in_Country`,
# MAGIC   g.originid AS Origin_ID,
# MAGIC   org.city AS Origin_City,
# MAGIC   org.state AS Origin_State,
# MAGIC   org.country AS Origin_Country,
# MAGIC   p.copy AS Copy,
# MAGIC   g.webdesc AS Web_Description,
# MAGIC   g. prctagdesc AS Price_Tag_Description,
# MAGIC   lft.sendtoloft AS 3D_Model_Required,
# MAGIC   lft.loftmodel AS 3D_Model,
# MAGIC   'https://havertys.scene7.com/is/image/Havertys/' ||
# MAGIC   left(i.ITEM, 1) || '-' || substring(i.ITEM, 2, 4) || '-' || substring(i.ITEM, 6, 4)
# MAGIC   AS Scene_7_Image_URL,
# MAGIC   'https://www.havertys.com/furniture/' || lower(trim(c.name)) || '-' || lower(trim(pml.product)) || '-' || i.item 
# MAGIC   AS `Havertys.com_URL`,
# MAGIC   i.advprice AS Advertised_Price
# MAGIC from pcrundata_tbitem i
# MAGIC LEFT OUTER JOIN pcrundata_tbgroup g on g.groupid = i.groupid
# MAGIC LEFT OUTER JOIN pcrundata_tbcolnam c on c.collid = g.collid
# MAGIC LEFT OUTER JOIN pcrundata_tbprod p on p.groupid = i.groupid and p.prodid = i.prodid
# MAGIC LEFT OUTER JOIN pcrundata_tbprodml pml on pml.prodid = p.prodid
# MAGIC LEFT OUTER JOIN pcrundata_tbatrmlv as vc on i.atrid02 = vc.atrid and vc.typeid = 2
# MAGIC LEFT OUTER JOIN pcrundata_tbatrmlu as us on i.atrid03 = us.atrid and us.typeid = 3
# MAGIC LEFT OUTER JOIN pcrundata_tbatrmlv as vf on i.atrid09 = vf.atrid and vf.typeid = 9
# MAGIC LEFT OUTER JOIN pcrundata_tbatrmlv as vg on i.atrid12 = vg.atrid and vg.typeid = 12
# MAGIC LEFT OUTER JOIN ipcorpdta_pcpmskustk as stE on i.item = stE.skustksku and stE.skustkwhse = 4010
# MAGIC LEFT OUTER JOIN ipcorpdta_pcpmskustk as stF on i.item = stF.skustksku and stF.skustkwhse = 4020
# MAGIC LEFT OUTER JOIN ipcorpdta_pcpmskustk as stW on i.item = stW.skustksku and stW.skustkwhse = 4030
# MAGIC LEFT OUTER JOIN pcrundata_tbrptcls rc on i.rptclass = rc.class
# MAGIC LEFT OUTER JOIN pcrundata_tborigin mio ON g.miorigin = mio.originid
# MAGIC LEFT OUTER JOIN pcrundata_tborigin org ON g.originid = org.originid
# MAGIC LEFT OUTER JOIN pcrundata_tbfactry f on g.factoryid = f.factoryid
# MAGIC LEFT OUTER JOIN pcrundata_tblftitm lft ON i.item = lft.item
# MAGIC )
# MAGIC 
# MAGIC SELECT ROW_NUMBER() OVER (PARTITION BY SKU_Unformatted_nk ORDER BY SKU_Unformatted_nk) AS RowNum, SKU.* FROM SKU
# MAGIC --In MDF, this would be 17 datasets, 1 data flow, In the data flow GUI: 1 Source, 17 Lookups, 17 Selects, 1 Derived Column 

# COMMAND ----------

BaseSKUQuery = """
WITH SKU AS (
SELECT
  i.item AS SKU_Unformatted_nk,
  left(i.ITEM, 1) || '-' || substring(i.ITEM, 2, 4) || '-' || substring(i.ITEM, 6, 4) AS SKU,
  i.desc AS SKU_Description,
  i.atrid02 AS Vendor_Color_ID,
  vc.value1 AS Vendor_Color,
  '' AS Vendor_Color_Hex_Code,
  i.atrid09 AS Vendor_Finish_ID,
  vf.value1 AS Vendor_Finish,
  i.atrid12 AS Configuration_ID,
  vg.value1 AS Configuration,
  vg.value3 AS Display_Configuration,
  i.atrid03 AS Size_ID,
  us.value1 AS Size,
  i.origvnd AS Original_Vendor_ID,
  0 AS Original_Vendor_sk,
  g.collid AS Collection_ID,
  c.name AS `Collection`,
  i.groupid AS Group_ID,
  g.group AS Group,
  i.prodid AS Product_ID,
  pml.product AS Product,
  i.rptclass AS Reporting_Class_Code,
  rc.desc AS Reporting_Class,
  i.skutype AS SKU_Type,
  i.stocked AS Stocked,
  case when i.rptclass = '10' and i.skutype != 'V' then 'Gold Plan'
       when i.skutype = 'V' then 'Delivery'
       when i.skutype = 'M' then 'Markdown'
       when i.skutype = 'S' then 'Special Order'
       when i.skutype = 'G' then 'Group'
       when i.stocked = 'C' then 'Custom Choice'
       when i.stocked = 'Y' then 'Stocked'
       when i.stocked = 'N' and i.skutype = 'C' then 'Not Stocked'
  else 'Unknown'
  end
  AS Type_of_SKU,
  i.vendor AS Vendor_Code,
  0 AS Vendor_fk,
  g.factoryid AS Factory_Code,
  f.desc AS Factory_Name,
  0 AS Factory_fk,
  g.specordavl AS Special_Order_Available,
  g.vendorlead AS Vendor_Lead_Time,
  g.splordprod AS Product_Lead_Time,
  g.activestr AS Group_Active_Start,
  g.activeend AS Group_Active_End,
  i.flowstr AS SKU_Flow_Start,
  i.flowend AS SKU_Flow_End,
  stE.skustkwhse AS Stocked_at_EDC,
  stE.skustkwhse AS Stocked_at_WDC,
  stE.skustkwhse AS Stocked_at_FDC,
  g.webstatus AS Group_Web_Status,
  i.webstatus AS SKU_Web_Status,
  g.advsignoff AS Merchandising_Approval,
  g.mdssignoff AS Marketing_Approval,
  '' AS Lineup_Status,
  case when current_date between i.flowstr and coalesce(i.flowend, to_date('2099-12-31', 'yyyy-MM-dd')) then 'Y' else 'N' end AS Available_to_Purchase,
  i.shiplength AS Ship_Length,
  i.shipwidth AS Ship_Width,
  i.shipheight AS Ship_Height,
  i.length AS Length,
  i.width AS Width,
  i.height AS Height,
  i.weight AS Weight,
  i.prepcubes AS Prep_Cubes,
  i.cubes AS Cubes,
  p.prepflag AS Prep_Flag,
  p.asmblyrqd AS Assembly_Required,
  p.asmblytime AS Assembly_Time,
  p.leaveinbox AS Leave_in_Box,
  i.mfgid AS Manufacturer_ID,
  i.upccode AS UPC,
  i.htsnumber AS HTS_Number,
  i.reportsku AS Original_SKU,
  p.repitem AS Representative_Item,
  g.ratingcnt AS Group_Rating_Count,
  p.ratingcnt AS Product_Rating_Count,
  g.starrating AS Group_Star_Rating,
  p.starrating AS Product_Star_Rating,
  g.miorigin AS `Made-in_ID`,
  mio.city AS `Made-in_City`,
  mio.state AS `Made-in_State`,
  mio.country AS `Made-in_Country`,
  g.originid AS Origin_ID,
  org.city AS Origin_City,
  org.state AS Origin_State,
  org.country AS Origin_Country,
  p.copy AS Copy,
  g.webdesc AS Web_Description,
  g. prctagdesc AS Price_Tag_Description,
  lft.sendtoloft AS 3D_Model_Required,
  lft.loftmodel AS 3D_Model,
  'https://havertys.scene7.com/is/image/Havertys/' ||
  left(i.ITEM, 1) || '-' || substring(i.ITEM, 2, 4) || '-' || substring(i.ITEM, 6, 4)
  AS Scene_7_Image_URL,
  'https://www.havertys.com/furniture/' || lower(trim(c.name)) || '-' || lower(trim(pml.product)) || '-' || i.item 
  AS `Havertys.com_URL`,
  i.advprice AS Advertised_Price
from pcrundata_tbitem i
LEFT OUTER JOIN pcrundata_tbgroup g on g.groupid = i.groupid
LEFT OUTER JOIN pcrundata_tbcolnam c on c.collid = g.collid
LEFT OUTER JOIN pcrundata_tbprod p on p.groupid = i.groupid and p.prodid = i.prodid
LEFT OUTER JOIN pcrundata_tbprodml pml on pml.prodid = p.prodid
LEFT OUTER JOIN pcrundata_tbatrmlv as vc on i.atrid02 = vc.atrid and vc.typeid = 2
LEFT OUTER JOIN pcrundata_tbatrmlu as us on i.atrid03 = us.atrid and us.typeid = 3
LEFT OUTER JOIN pcrundata_tbatrmlv as vf on i.atrid09 = vf.atrid and vf.typeid = 9
LEFT OUTER JOIN pcrundata_tbatrmlv as vg on i.atrid12 = vg.atrid and vg.typeid = 12
LEFT OUTER JOIN ipcorpdta_pcpmskustk as stE on i.item = stE.skustksku and stE.skustkwhse = 4010
LEFT OUTER JOIN ipcorpdta_pcpmskustk as stF on i.item = stF.skustksku and stF.skustkwhse = 4020
LEFT OUTER JOIN ipcorpdta_pcpmskustk as stW on i.item = stW.skustksku and stW.skustkwhse = 4030
LEFT OUTER JOIN pcrundata_tbrptcls rc on i.rptclass = rc.class
LEFT OUTER JOIN pcrundata_tborigin mio ON g.miorigin = mio.originid
LEFT OUTER JOIN pcrundata_tborigin org ON g.originid = org.originid
LEFT OUTER JOIN pcrundata_tbfactry f on g.factoryid = f.factoryid
LEFT OUTER JOIN pcrundata_tblftitm lft ON i.item = lft.item
)

SELECT ROW_NUMBER() OVER (PARTITION BY SKU_Unformatted_nk ORDER BY SKU_Unformatted_nk) AS RowNum, SKU.* FROM SKU
"""

# COMMAND ----------

BaseSKU_DF = spark.sql(BaseSKUQuery) \
              .createOrReplaceTempView("BaseSKU")

# COMMAND ----------

SubcategoryQuery = """
SELECT coalesce(g.item, p.item) AS SKU,
       coalesce(p.prodcatcode, g.groupcatcode) AS SubCategoryCode,
       coalesce(p.prodcatname, g.groupcatname) AS SubCategoryName
FROM
  (SELECT i.item, g.atrid AS groupcatcode, ug.value1 AS groupcatname
   FROM pcrundata_tbitem i
   INNER JOIN pcrundata_tbatrgrp g ON g.groupid = i.groupid AND g.reprptcat = 'Y'
   INNER JOIN pcrundata_tbatrmlu ug ON ug.atrid = g.atrid AND ug.typeid = 14) 
   AS g
FULL OUTER JOIN
  (SELECT i.item, p.atrid AS prodcatcode, up.value1 AS prodcatname
   FROM pcrundata_tbitem i
   INNER JOIN pcrundata_tbatrprd p ON p.groupid = i.groupid AND p.prodid = i.prodid AND p.reprptcat = 'Y'
   INNER JOIN pcrundata_tbatrmlu up ON up.atrid = p.atrid AND up.typeid = 14) 
   AS p ON g.item = p.item
"""

# COMMAND ----------

Subcategory_DF = spark.sql(SubcategoryQuery) \
              .createOrReplaceTempView("Subcategory")

# COMMAND ----------

CategoryHierarchyQuery = """
SELECT DISTINCT subcat.atrid AS SubCategoryCode, subcat.value1 AS SubCategoryName, cat.atrid AS CategoryCode, cat.value1 AS CategoryName, supcat.atrid AS SuperCategoryCode, supcat.value1 AS SuperCategoryName
FROM 
pcrundata_tbatrmlu subcat
INNER JOIN pcrundata_tbatrrel catrel on subcat.atrid = catrel.relatrid 
INNER JOIN pcrundata_tbatrmlu cat on catrel.mainatrid = cat.atrid and cat.typeid = 22
INNER JOIN pcrundata_tbatrrel supcatrel on cat.atrid = supcatrel.relatrid 
INNER JOIN pcrundata_tbatrmlu supcat on supcatrel.mainatrid = supcat.atrid and supcat.typeid = 19
WHERE 
subcat.typeid = 14
"""

# COMMAND ----------

CategoryHierarchy_DF = spark.sql(CategoryHierarchyQuery) \
              .createOrReplaceTempView("CategoryHierarchy")

# COMMAND ----------

SubcategoryLookupQuery = """
SELECT coalesce(g.item, p.item) AS SKU,
       coalesce(p.prodcatcode, g.groupcatcode) AS SubCategoryCode,
       coalesce(p.prodcatname, g.groupcatname) AS SubCategoryName
FROM
  (SELECT i.item, g.atrid AS groupcatcode, ug.value1 AS groupcatname
   FROM pcrundata_tbitem i
   INNER JOIN pcrundata_tbatrgrp g ON g.groupid = i.groupid --AND g.reprptcat = 'Y'
   INNER JOIN pcrundata_tbatrmlu ug ON ug.atrid = g.atrid AND ug.typeid = 14) 
   AS g
FULL OUTER JOIN
  (SELECT i.item, p.atrid AS prodcatcode, up.value1 AS prodcatname
   FROM pcrundata_tbitem i
   INNER JOIN pcrundata_tbatrprd p ON p.groupid = i.groupid AND p.prodid = i.prodid --AND p.reprptcat = 'Y'
   INNER JOIN pcrundata_tbatrmlu up ON up.atrid = p.atrid AND up.typeid = 14) 
   AS p ON g.item = p.item
"""

# COMMAND ----------

SubcategoryLookup_DF = spark.sql(SubcategoryLookupQuery) \
              .createOrReplaceTempView("SubcategoryLookup")

# COMMAND ----------

SKUwithHierarchyQuery = """
SELECT 
ROW_NUMBER() OVER (PARTITION BY BaseSKU.SKU_Unformatted_nk ORDER BY BaseSKU.SKU_Unformatted_nk) AS RowNum2,
--Subcategory.*, 
CategoryHierarchy.*, 
BaseSKU.*
FROM 
BaseSKU 
LEFT OUTER JOIN Subcategory ON BaseSKU.SKU_Unformatted_nk = Subcategory.SKU
LEFT OUTER JOIN CategoryHierarchy ON Subcategory.SubCategoryCode = CategoryHierarchy.SubCategoryCode
WHERE 
BaseSKU.RowNum = 1
"""

# COMMAND ----------

SKUwithHierarchy_DF = spark.sql(SKUwithHierarchyQuery) \
              .createOrReplaceTempView("SKUwithHierarchy")

# COMMAND ----------

StagingQuery = """
SELECT 
CASE WHEN (SELECT MAX(SubCategoryName) FROM SubcategoryLookup WHERE SubCategoryName = 'Motion' AND SubcategoryLookup.SKU = SKUwithHierarchy.SKU_Unformatted_nk) IS NOT NULL THEN 'Y' ELSE 'N' END AS Motion,
COALESCE((SELECT LEFT(MAX(SubCategoryName), 6) FROM SubcategoryLookup WHERE SubCategoryName IN ('Formal Dining','Casual Dining') AND SubcategoryLookup.SKU = SKUwithHierarchy.SKU_Unformatted_nk), '') AS DiningStyle,
COALESCE((SELECT LEFT(MAX(SubCategoryName), 6) FROM SubcategoryLookup WHERE SubCategoryName IN ('Youth Bedrooms','Master Bedrooms') AND SubcategoryLookup.SKU = SKUwithHierarchy.SKU_Unformatted_nk), '') AS BedroomStyle,
CASE WHEN (SELECT MAX(SubCategoryName) FROM SubcategoryLookup WHERE SubCategoryName ='Daybeds, Metal and Upholsterd Beds' AND SubcategoryLookup.SKU = SKUwithHierarchy.SKU_Unformatted_nk) IS NOT NULL THEN 'Y' ELSE 'N' END AS StandaloneBed,
'' AS Primary_Common_Color,
'' AS Primary_Common_Finish,
'' AS Floor_Sample_Bedding,
'' AS Employee_Bedding,
'' AS Accessory_Story,
SKUwithHierarchy.*
FROM
SKUwithHierarchy
WHERE RowNum2 = 1
"""

# COMMAND ----------

Staging_DF = spark.sql(StagingQuery) \
              .createOrReplaceTempView("Staging")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load SKU dimension for comparison
# MAGIC Load the SKU dimension from the data warehouse for comparison to the source data in the Data Lake

# COMMAND ----------

Dim_SKUQuery = """
(SELECT
SKU_sk AS SKU_sk_Dest,
SKU_Unformatted_nk AS SKU_Unformatted_nk_Dest,
HashKey AS HashKey_Dest
FROM
DW.Dim_SKU) Dim_SKU 
"""

# COMMAND ----------

Dim_SKU_DF = (spark.read
    .jdbc(url=jdbcUrl, table=Dim_SKUQuery, properties=connectionProperties)
    .createOrReplaceTempView("Dim_SKU")
)

# COMMAND ----------

NewRecordsDF = spark.sql("""
SELECT 
Staging.*
FROM 
Staging
LEFT OUTER JOIN Dim_SKU ON Staging.SKU_Unformatted_nk = Dim_SKU.SKU_Unformatted_nk_Dest
WHERE Dim_SKU.SKU_sk_Dest IS NULL
""")

# COMMAND ----------

UpdateRecordsDF = spark.sql("""
SELECT 
Dim_SKU.SKU_sk_Dest, Dim_SKU.HashKey_Dest, Dim_SKU.*
FROM 
Staging
INNER JOIN Dim_SKU ON Staging.SKU_Unformatted_nk = Dim_SKU.SKU_Unformatted_nk_Dest
""")

# COMMAND ----------

NewRecordsDF.cache()

# COMMAND ----------

NewRecordsDF.count()

# COMMAND ----------

TableName = "ETL.Dim_SKU_Update5" #The Azure SQL DW dimension table name

NewRecordsTestingDF = NewRecordsDF.limit(0)#.createOrReplaceTempView("NewRecords")

#display(NewRecordsTestingDF)

# working command
# NewRecordsTestingDF.repartition(10).write \
#   .jdbc(url=jdbcUrl, table=TableName, mode="overwrite", properties=connectionProperties)

NewRecordsTestingDF.write \
  .jdbc(url=jdbcUrl, table=TableName, mode="overwrite", properties=connectionProperties) 

# COMMAND ----------

NewRecordsTestingDF.createOrReplaceTempView("NewRecordsLimit")
NewRecordsDF.createOrReplaceTempView("NewRecords")

# COMMAND ----------

# '' AS Primary_Common_Color,
# '' AS Primary_Common_Finish,
# '' AS Floor_Sample_Bedding,
# '' AS Employee_Bedding,
# '' AS Accessory_Story,
# '' AS Logic to Get Special Order Primary Color and Finish

# COMMAND ----------

# MAGIC %scala 
# MAGIC import com.microsoft.azure.sqldb.spark.bulkcopy.BulkCopyMetadata
# MAGIC import com.microsoft.azure.sqldb.spark.config.Config
# MAGIC import com.microsoft.azure.sqldb.spark.connect._
# MAGIC 
# MAGIC val SQLDF = spark.sql("SELECT * FROM NewRecords")
# MAGIC 
# MAGIC val config = Config(Map(
# MAGIC   "url"          -> dbutils.secrets.get(scope = "key-vault-secrets", key = "HavertysDWServerName"),
# MAGIC   "databaseName" -> "HavertysDW_QA",
# MAGIC   "dbTable"      -> "ETL.Dim_SKU_Update2",
# MAGIC   "user"         -> "ETL",
# MAGIC   "password"     -> dbutils.secrets.get(scope = "key-vault-secrets", key = "HavertysDWETLAccountPassword")
# MAGIC ))
# MAGIC 
# MAGIC import org.apache.spark.sql.SaveMode
# MAGIC 
# MAGIC SQLDF.write.mode(SaveMode.Overwrite).sqlDB(config)

# COMMAND ----------

# MAGIC %scala
# MAGIC import com.microsoft.azure.sqldb.spark.bulkcopy.BulkCopyMetadata
# MAGIC import com.microsoft.azure.sqldb.spark.config.Config
# MAGIC import com.microsoft.azure.sqldb.spark.connect._
# MAGIC 
# MAGIC 
# MAGIC //  Add column Metadata.
# MAGIC //  If not specified, metadata will be automatically added
# MAGIC // from the destination table, which may suffer performance.
# MAGIC val SQLDF = spark.sql("SELECT * FROM NewRecords")
# MAGIC //val SQLDF = spark.sql("SELECT SKU_Unformatted_nk, SKU, SKU_Description FROM NewRecords")
# MAGIC //display(SQLDF)
# MAGIC 
# MAGIC var bulkCopyMetadata = new BulkCopyMetadata
# MAGIC bulkCopyMetadata.addColumnMetadata(1, "SKU_Unformatted_nk", java.sql.Types.NVARCHAR, 9, 0)
# MAGIC bulkCopyMetadata.addColumnMetadata(2, "SKU", java.sql.Types.NVARCHAR, 12, 0)
# MAGIC bulkCopyMetadata.addColumnMetadata(3, "SKU_Description", java.sql.Types.NVARCHAR, 30, 0)
# MAGIC 
# MAGIC val bulkCopyConfig = Config(Map(
# MAGIC   "url"          -> dbutils.secrets.get(scope = "key-vault-secrets", key = "HavertysDWServerName"),
# MAGIC   "databaseName" -> "HavertysDW_QA",
# MAGIC   "dbTable"      -> "ETL.Dim_SKU_Update6",
# MAGIC   "user"         -> "ETL",
# MAGIC   "password"     -> dbutils.secrets.get(scope = "key-vault-secrets", key = "HavertysDWETLAccountPassword"),
# MAGIC   "bulkCopyBatchSize" -> "100000",
# MAGIC   "bulkCopyTableLock" -> "true",
# MAGIC   "bulkCopyTimeout"   -> "600"
# MAGIC ))
# MAGIC 
# MAGIC //SQLDF.bulkCopyToSqlDB(bulkCopyConfig, bulkCopyMetadata)
# MAGIC SQLDF.bulkCopyToSqlDB(bulkCopyConfig)

# COMMAND ----------

NewRecordsDF.write \
       .mode("overwrite") \
       .parquet("/mnt/datalake/curated/dw load/dim_sku")

# COMMAND ----------

