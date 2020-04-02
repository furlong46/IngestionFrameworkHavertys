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

# MAGIC %run "./Initialize Connections and Functions" $BulkInsertTableName_Update="Updates.Dim_SKU_Update" $BulkInsertTableName_Insert="Updates.Dim_SKU_Insert"

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

pcrundata_tbitem_DF = spark.read \
       .parquet("/mnt/datalake/staging/master data/product/DB203002/pcrundata_tbitem")

# COMMAND ----------

pcrundata_tbitem_DF.cache()
pcrundata_tbitem_DF.createOrReplaceTempView("pcrundata_tbitem")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM pcrundata_tbitem

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

pcrundata_tbitmurl_DF = spark.read \
       .parquet("/mnt/datalake/staging/master data/product/DB203002/pcrundata_tbitmurl") \
       .createOrReplaceTempView("pcrundata_tbitmurl")

# COMMAND ----------

pcrundata_tbsoitmatr_DF = spark.read \
       .parquet("/mnt/datalake/staging/master data/product/DB203002/pcrundata_tbsoitmatr") \
       .createOrReplaceTempView("pcrundata_tbsoitmatr")

# COMMAND ----------

pcrundata_tbsoatyp_DF = spark.read \
       .parquet("/mnt/datalake/staging/master data/product/DB203002/pcrundata_tbsoatyp") \
       .createOrReplaceTempView("pcrundata_tbsoatyp")

# COMMAND ----------

pcrundata_tbsogdef_DF = spark.read \
       .parquet("/mnt/datalake/staging/master data/product/DB203002/pcrundata_tbsogdef") \
       .createOrReplaceTempView("pcrundata_tbsogdef")

# COMMAND ----------

pcrundata_tbsoadef_DF = spark.read \
       .parquet("/mnt/datalake/staging/master data/product/DB203002/pcrundata_tbsoadef") \
       .createOrReplaceTempView("pcrundata_tbsoadef")

# COMMAND ----------

pcrundata_tbsca_DF = spark.read \
      .parquet("/mnt/datalake/staging/master data/product/DB203002/pcrundata_tbsca") \
      .createOrReplaceTempView("pcrundata_tbsca")

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

csvSchema = StructType([
  StructField("item_formatted", StringType(), False),
  StructField("story", StringType(), False)
])

pcrundata_tbsoadef_DF = spark.read \
       .option("delimiter", ",") \
       .schema(csvSchema) \
       .csv("/mnt/datalake/staging/master data/product/upload/story/") \
       .withColumn('RowNum', row_number().over(Window.partitionBy("item_formatted").orderBy(col("item_formatted")))) \
       .createOrReplaceTempView("Story")

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

BaseSKUQuery = """
WITH BaseSKU AS (
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
  0 AS Original_Vendor_fk,
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
  coalesce(i.itemtype, '') AS Type_of_SKU_Code,
  case when i.itemtype = 'V' then 'Gold Plan'
       when i.itemtype = 'D' then 'Delivery'
       when i.itemtype = 'M' then 'Markdown'
       when i.itemtype = 'S' then 'Special Order'
       when i.itemtype = 'G' then 'Group'
       when i.itemtype = 'C' then 'Custom Choice'
       when i.itemtype = 'Y' then 'Stocked'
       when i.itemtype = 'N' then 'Not Stocked'
       when i.itemtype = 'U' then 'Unknown'
  else ''
  end AS Type_of_SKU,
  i.vendor AS Vendor_ID,
  0 AS Vendor_fk,
  g.factoryid AS Factory_ID,
  0 AS Factory_fk,
  g.specordavl AS Special_Order_Available,
  g.vendorlead AS Vendor_Lead_Time,
  g.splordprod AS Product_Lead_Time,
  g.activestr AS Group_Active_Start,
  g.activeend AS Group_Active_End,
  i.flowstr AS SKU_Flow_StartDate,
  i.flowend AS SKU_Flow_EndDate,
  stE.skustkwhse AS Stocked_at_EDC,
  stW.skustkwhse AS Stocked_at_WDC,
  stF.skustkwhse AS Stocked_at_FDC,
  g.webstatus AS Group_Web_Status,
  i.webstatus AS SKU_Web_Status,
  g.advsignoff AS Merchandising_Approval,
  g.mdssignoff AS Marketing_Approval,
  coalesce(i.lineupstat, '') AS Lineup_Status_Code,
  case when i.lineupstat = 'A' then 'Active MSC'
       when i.lineupstat = 'D' then 'Dropped'
       when i.lineupstat = 'H' then 'Hidden'
       when i.lineupstat = 'K' then 'Unknown Group Web Status'
       when i.lineupstat = 'M' then 'Markdown'
       when i.lineupstat = 'N' then 'Active Not Stocked'
       when i.lineupstat = 'S' then 'Special Order'
       when i.lineupstat = 'U' then 'Upcoming'
       when i.lineupstat = 'W' then 'Active MSC & Web'
       when i.lineupstat = 'C' then 'Watch'
  else ''
  end AS Lineup_Status,
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
  g.miorigin AS Made_in_ID,
  mio.city AS Made_in_City,
  mio.state AS Made_in_State,
  mio.country AS Made_in_Country,
  g.originid AS Origin_ID,
  org.city AS Origin_City,
  org.state AS Origin_State,
  org.country AS Origin_Country,
  p.copy AS Copy,
  g.webdesc AS Web_Description,
  g.prctagdesc AS Price_Tag_Description,
  lft.sendtoloft AS 3D_Model_Required,
  lft.loftmodel AS 3D_Model,
  'https://havertys.scene7.com/is/image/Havertys/' ||
  left(i.ITEM, 1) || '-' || substring(i.ITEM, 2, 4) || '-' || substring(i.ITEM, 6, 4)
  AS Scene_7_Image_URL,
  coalesce(url.URL, '') AS `Havertys.com_URL`,
  i.advprice AS Advertised_Price,
  case when TRIM(g.DIRECTIMP) = 'Y' then 'Direct'
       when TRIM(g.DIRECTIMP) = 'N' then 'Indirect'
  else ''
  end AS IndirectDirect,
  0 AS Supply_Chain_Analyst_fk,
  sca.scaempid AS Supply_Chain_Analyst_nk,
  TRIM(sca.scaname) AS Supply_Chain_Analyst_FullName,
  g.prodfeat as Product_Features,
  case when org.type = 'D' then 'Domestic'
       when org.type = 'F' then 'Import'
  else ''
  end AS Origin
FROM pcrundata_tbitem i
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
LEFT OUTER JOIN pcrundata_tbitmurl url ON i.item = url.item
LEFT OUTER JOIN pcrundata_tbsca sca on sca.scaid = g.analystid
)
,
Subcategory AS (
SELECT coalesce(p.item, g.item) AS SKU,
       coalesce(p.prodcatcode, g.groupcatcode) AS Sub_Category_ID,
       coalesce(p.prodcatname, g.groupcatname) AS Sub_Category
FROM
  (SELECT i.item, g.atrid AS groupcatcode, ug.value1 AS groupcatname
   FROM pcrundata_tbitem i
   INNER JOIN pcrundata_tbatrgrp g ON g.groupid = i.groupid AND g.reprptcat = 'Y'
   INNER JOIN pcrundata_tbatrmlu ug ON ug.atrid = g.atrid AND ug.typeid = 14
   WHERE g.inherit = 'Y') 
   AS g
FULL OUTER JOIN
  (SELECT i.item, p.atrid AS prodcatcode, up.value1 AS prodcatname
   FROM pcrundata_tbitem i
   INNER JOIN pcrundata_tbatrprd p ON p.groupid = i.groupid AND p.prodid = i.prodid AND p.reprptcat = 'Y'
   INNER JOIN pcrundata_tbatrmlu up ON up.atrid = p.atrid AND up.typeid = 14) 
   AS p ON g.item = p.item
)
,
CategoryHierarchy AS (
SELECT DISTINCT subcat.atrid AS Sub_Category_ID, subcat.value1 AS Sub_Category, cat.atrid AS Category_ID, cat.value1 AS Category, supcat.atrid AS Super_Category_ID, supcat.value1 AS Super_Category
FROM 
pcrundata_tbatrmlu subcat
INNER JOIN pcrundata_tbatrrel catrel on subcat.atrid = catrel.relatrid 
INNER JOIN pcrundata_tbatrmlu cat on catrel.mainatrid = cat.atrid and cat.typeid = 22
INNER JOIN pcrundata_tbatrrel supcatrel on cat.atrid = supcatrel.relatrid 
INNER JOIN pcrundata_tbatrmlu supcat on supcatrel.mainatrid = supcat.atrid and supcat.typeid = 19
WHERE 
subcat.typeid = 14
)
,
FOB_Cost AS (
  SELECT a.ccamount, a.ccsku 
  FROM 
  pcrundata_pcpmcrpcst a
  INNER JOIN
  (select ccsku, cctype, max(cceffdate) as cceffdate, max(cccrtdate) as cccrtdate, max(cccrttime) as cccrttime 
    from  pcrundata_pcpmcrpcst
    where cceffdate <= substr(cast(current_date as char(10)),1,4) || substr(cast(current_date as char(10)), 6, 2) || substr(cast(current_date as char(10)), 9, 2) 
    group by ccsku, cctype) b
      on b.ccsku = a.ccsku and b.cctype = a.cctype and b.cceffdate = a.cceffdate and b.cccrtdate = a.cccrtdate and b.cccrttime = a.cccrttime
  where a.cctype = 'F'
)
,
Warehouse_Cost AS (
  SELECT a.ccamount, a.ccsku 
  FROM 
  pcrundata_pcpmcrpcst a
  INNER JOIN
  (select ccsku, cctype, max(cceffdate) as cceffdate, max(cccrtdate) as cccrtdate, max(cccrttime) as cccrttime 
    from  pcrundata_pcpmcrpcst
    where cceffdate <= substr(cast(current_date as char(10)),1,4) || substr(cast(current_date as char(10)), 6, 2) || substr(cast(current_date as char(10)), 9, 2) 
    group by ccsku, cctype) b
      on b.ccsku = a.ccsku and b.cctype = a.cctype and b.cceffdate = a.cceffdate and b.cccrtdate = a.cccrtdate and b.cccrttime = a.cccrttime
  where a.cctype = 'C'
)

SELECT 
--RowNum for eliminating possible duplicates
ROW_NUMBER() OVER (PARTITION BY BaseSKU.SKU_Unformatted_nk ORDER BY BaseSKU.SKU_Unformatted_nk) AS RowNum, 
BaseSKU.*,
CategoryHierarchy.*,
COALESCE(FOB_Cost.ccamount, 0.00) as FOB_Cost,
COALESCE(Warehouse_Cost.ccamount, 0.00) as Warehouse_Cost
FROM 
BaseSKU
LEFT OUTER JOIN Subcategory ON BaseSKU.SKU_Unformatted_nk = Subcategory.SKU
LEFT OUTER JOIN CategoryHierarchy ON Subcategory.Sub_Category_ID = CategoryHierarchy.Sub_Category_ID
LEFT OUTER JOIN FOB_Cost ON TRIM(FOB_Cost.ccsku) = BaseSku.SKU_Unformatted_nk
LEFT OUTER JOIN Warehouse_Cost ON TRIM(Warehouse_Cost.ccsku) = BaseSku.SKU_Unformatted_nk
"""

# COMMAND ----------

BaseSKU_DF = spark.sql(BaseSKUQuery)

# COMMAND ----------

# BaseSKU_DF.cache()
BaseSKU_DF.createOrReplaceTempView("BaseSKU")

# COMMAND ----------

ReportcategoryLookupQuery = """
  SELECT i.item AS SKU, g.atrid AS CategoryCode, ug.value1 AS CategoryName
   FROM pcrundata_tbitem i
   INNER JOIN pcrundata_tbatrgrp g ON g.groupid = i.groupid 
   INNER JOIN pcrundata_tbatrmlu ug ON ug.atrid = g.atrid AND ug.typeid = 14
   WHERE g.inherit = 'Y'
UNION
   SELECT i.item AS SKU, p.atrid AS CategoryCode, up.value1 AS CategoryName
   FROM pcrundata_tbitem i
   INNER JOIN pcrundata_tbatrprd p ON p.groupid = i.groupid AND p.prodid = i.prodid 
   INNER JOIN pcrundata_tbatrmlu up ON up.atrid = p.atrid AND up.typeid = 14
"""

# COMMAND ----------

ReportcategoryLookup_DF = spark.sql(ReportcategoryLookupQuery) \
              .createOrReplaceTempView("ReportcategoryLookup")

# COMMAND ----------

# MAGIC %md
# MAGIC Need to get the primary color and common finish from the tbatrrel where a flag = primary.  This will be implemented in 3/4 weeks (today is 9/9/19)

# COMMAND ----------

StagingQuery = """
WITH SpecialColor AS (
SELECT e.item, e.atrtypid, t.attribute, v.userdesc as uservalue, ROW_NUMBER() OVER (PARTITION BY e.item ORDER BY e.item) AS RowNum
FROM  
pcrundata_tbsoitmatr as e
INNER JOIN pcrundata_tbsoatyp as t on e.atrtypid = t.atrtypid
LEFT OUTER JOIN pcrundata_tbsogdef as g on e.gradeid = g.gradeid
LEFT OUTER JOIN pcrundata_tbsoadef as v on e.atrdefid = v.atrdefid
WHERE v.userdesc IS NOT NULL
AND e.atrtypid IN (255, 291)
)
,
SpecialFinish AS (
SELECT e.item, e.atrtypid, t.attribute, v.userdesc as uservalue, ROW_NUMBER() OVER (PARTITION BY e.item ORDER BY e.item) AS RowNum
FROM  
pcrundata_tbsoitmatr as e
INNER JOIN pcrundata_tbsoatyp as t on e.atrtypid = t.atrtypid
LEFT OUTER JOIN pcrundata_tbsogdef as g on e.gradeid = g.gradeid
LEFT OUTER JOIN pcrundata_tbsoadef as v on e.atrdefid = v.atrdefid
WHERE v.userdesc IS NOT NULL--v.userdesc <> v.vendordesc
AND 
e.atrtypid IN (528, 530, 525, 261)
)
,
CFinish AS (
SELECT v.atrid AS vendorfinishcode, v.value1 AS vendorfinish, u.value1 AS commonfinish
FROM pcrundata_tbatrmlv v 
INNER JOIN pcrundata_tbatrrel r ON r.mainatrid = v.atrid
INNER JOIN pcrundata_tbatrmlu u ON r.relatrid = u.atrid AND u.typeid = 6
WHERE v.value1<>''
GROUP BY vendorfinishcode, vendorfinish, commonfinish
)
, 
CFinishCnt AS (
SELECT vendorfinishcode, vendorfinish, COUNT(1) AS Cnt FROM CFinish GROUP BY vendorfinishcode, vendorfinish
)
,
PrimaryFinish AS (
SELECT CFinish.*, CFinishCnt.Cnt, ROW_NUMBER() OVER (PARTITION BY CFinish.vendorfinishcode ORDER BY CFinish.vendorfinishcode) AS RowNum 
FROM CFinish INNER JOIN CFinishCnt ON CFinish.vendorfinishcode = CFinishCnt.vendorfinishcode
)
,
CColor AS (
SELECT v.atrid AS vendorcolorcode, v.value1 AS vendorcolor, u.value1 AS commoncolor
FROM pcrundata_tbatrmlv v 
INNER JOIN pcrundata_tbatrrel r ON r.mainatrid = v.atrid
INNER JOIN pcrundata_tbatrmlu u ON r.relatrid = u.atrid AND u.typeid = 1
WHERE v.value1<>''
GROUP BY vendorcolorcode, vendorcolor, commoncolor
)
, 
CColorCnt AS (
SELECT vendorcolorcode, vendorcolor, COUNT(1) AS Cnt FROM CColor GROUP BY vendorcolorcode, vendorcolor
)
,
PrimaryColor AS (
SELECT CColor.*, CColorCnt.Cnt, ROW_NUMBER() OVER (PARTITION BY CColor.vendorcolorcode ORDER BY CColor.vendorcolorcode) AS RowNum
FROM CColor INNER JOIN CColorCnt ON CColor.vendorcolorcode = CColorCnt.vendorcolorcode
),
CurrentEDLP AS (
SELECT item, prmkey, price AS Current_EDLP_Price, ROW_NUMBER() OVER (PARTITION BY item ORDER BY prmkey DESC) AS RowNum FROM pcrundata_tbprmitm WHERE prmkey > 20000101 and active = 'A'
)
,
Staging AS (
SELECT
BaseSKU.*,
CASE WHEN (SELECT MAX(CategoryName) FROM ReportcategoryLookup WHERE CategoryName = 'Motion' AND ReportcategoryLookup.SKU = BaseSKU.SKU_Unformatted_nk) IS NOT NULL THEN 'Y' ELSE 'N' END AS Motion,
COALESCE((SELECT LEFT(MAX(CategoryName), 6) FROM ReportcategoryLookup WHERE CategoryName IN ('Formal Dining','Casual Dining') AND ReportcategoryLookup.SKU = BaseSKU.SKU_Unformatted_nk), '') AS Dining_Style,
COALESCE((SELECT LEFT(MAX(CategoryName), 6) FROM ReportcategoryLookup WHERE CategoryName IN ('Youth Bedrooms','Master Bedrooms') AND ReportcategoryLookup.SKU = BaseSKU.SKU_Unformatted_nk), '') AS Bedroom_Style,
CASE WHEN (SELECT MAX(CategoryName) FROM ReportcategoryLookup WHERE CategoryName ='Daybeds, Metal and Upholsterd Beds' AND ReportcategoryLookup.SKU = BaseSKU.SKU_Unformatted_nk) IS NOT NULL THEN 'Y' ELSE 'N' END AS Standalone_Bed,
CASE WHEN COALESCE(PrimaryColor.Cnt, 0) > 1 THEN 'Multiple Colors' ELSE COALESCE(PrimaryColor.commoncolor, '') END AS Primary_Common_Color,
CASE WHEN COALESCE(PrimaryFinish.Cnt, 0) > 1 THEN 'Multiple Finishes' ELSE COALESCE(PrimaryFinish.commonfinish, '') END  AS Primary_Common_Finish,
CASE WHEN COALESCE(instr(Manufacturer_ID, '/FS'), 0) > 0 AND Super_Category ='Bedding' THEN 'Y' ELSE 'N' END AS Floor_Sample_Bedding,
CASE WHEN COALESCE(instr(Manufacturer_ID, 'EP/'), 0) > 0 AND Super_Category ='Bedding' THEN 'Y' ELSE 'N' END AS Employee_Bedding,
Story.story AS Accessory_Story,
SpecialColor.uservalue AS SpecialOrderCommonColor,
SpecialFinish.uservalue AS SpecialOrderCommonFinish,
CurrentEDLP.Current_EDLP_Price
FROM
BaseSKU
LEFT OUTER JOIN SpecialColor ON BaseSKU.SKU_Unformatted_nk = SpecialColor.item AND SpecialColor.RowNum = 1
LEFT OUTER JOIN SpecialFinish ON BaseSKU.SKU_Unformatted_nk = SpecialFinish.item AND SpecialFinish.RowNum = 1
LEFT OUTER JOIN PrimaryColor ON BaseSKU.Vendor_Color_ID = PrimaryColor.vendorcolorcode AND PrimaryColor.RowNum = 1
LEFT OUTER JOIN PrimaryFinish ON BaseSKU.Vendor_Finish_ID = PrimaryFinish.vendorfinishcode AND PrimaryFinish.RowNum = 1
LEFT OUTER JOIN CurrentEDLP ON BaseSKU.SKU_Unformatted_nk = CurrentEDLP.item AND CurrentEDLP.RowNum = 1
LEFT OUTER JOIN Story ON BaseSKU.SKU = Story.item_formatted AND Story.RowNum = 1
WHERE BaseSKU.RowNum = 1
)

SELECT
   TRIM(COALESCE(SKU_Unformatted_nk, '')) AS SKU_Unformatted_nk
  ,TRIM(COALESCE(SKU, '')) AS SKU
  ,TRIM(COALESCE(SKU_Description, '')) AS SKU_Description
  ,COALESCE(Vendor_Color_ID, 0) AS Vendor_Color_ID
  ,TRIM(COALESCE(SpecialOrderCommonColor, Vendor_Color, '')) AS Vendor_Color
  ,TRIM(COALESCE(Vendor_Color_Hex_Code, '')) AS Vendor_Color_Hex_Code
  ,COALESCE(Vendor_Finish_ID, 0) AS Vendor_Finish_ID
  ,TRIM(COALESCE(SpecialOrderCommonFinish, Vendor_Finish, '')) AS Vendor_Finish
  ,COALESCE(Configuration_ID, 0) AS Configuration_ID
  ,TRIM(COALESCE(Configuration, '')) AS Configuration
  ,TRIM(COALESCE(Display_Configuration, '')) AS Display_Configuration
  ,COALESCE(Size_ID, 0) AS Size_ID
  ,TRIM(COALESCE(Size, '')) AS Size
  ,TRIM(COALESCE(Original_Vendor_ID, '')) AS Original_Vendor_ID
  ,Original_Vendor_fk
  ,COALESCE(Collection_ID, 0) AS Collection_ID
  ,TRIM(COALESCE(Collection, '')) AS Collection
  ,COALESCE(Group_ID, 0) AS Group_ID
  ,TRIM(COALESCE(Group, '')) AS Group
  ,COALESCE(Product_ID, 0) AS Product_ID
  ,TRIM(COALESCE(Product, '')) AS Product
  ,TRIM(COALESCE(Reporting_Class_Code, '')) AS Reporting_Class_Code
  ,TRIM(COALESCE(Reporting_Class, '')) AS Reporting_Class
  ,COALESCE(Super_Category_ID, 0) AS Super_Category_ID
  ,TRIM(COALESCE(Super_Category, '')) AS Super_Category
  ,COALESCE(Category_ID, 0) AS Category_ID
  ,TRIM(COALESCE(Category, '')) AS Category
  ,COALESCE(Sub_Category_ID, 0) AS Sub_Category_ID
  ,TRIM(COALESCE(Sub_Category, '')) AS Sub_Category
  ,COALESCE(Current_EDLP_Price, 0) AS Current_EDLP_Price
  ,'' AS Good_Better_Best_Category
  ,'' AS GBB_Price_Point
  ,TRIM(COALESCE(Motion, '')) AS Motion
  ,TRIM(COALESCE(Dining_Style, '')) AS Dining_Style
  ,TRIM(COALESCE(Bedroom_Style, '')) AS Bedroom_Style
  ,TRIM(COALESCE(Standalone_bed, '')) AS Standalone_bed
  ,TRIM(COALESCE(SKU_Type, '')) AS SKU_Type
  ,TRIM(COALESCE(Stocked, '')) AS Stocked
  ,TRIM(COALESCE(Type_of_SKU_Code, '')) AS Type_of_SKU_Code
  ,TRIM(COALESCE(Type_of_SKU, '')) AS Type_of_SKU
  ,TRIM(COALESCE(Vendor_ID, '')) AS Vendor_ID
  ,Vendor_fk
  ,COALESCE(Factory_ID, 0) AS Factory_ID
  ,Factory_fk
  ,TRIM(COALESCE(Special_Order_Available, '')) AS Special_Order_Available
  ,COALESCE(Vendor_Lead_Time, 0) AS Vendor_Lead_Time
  ,COALESCE(Product_Lead_Time, 0) AS Product_Lead_Time
  ,Group_Active_Start
  ,Group_Active_End
  ,SKU_Flow_StartDate
  ,SKU_Flow_EndDate
  ,COALESCE(Stocked_at_EDC, 0) AS Stocked_at_EDC
  ,COALESCE(Stocked_at_WDC, 0) AS Stocked_at_WDC
  ,COALESCE(Stocked_at_FDC, 0) AS Stocked_at_FDC
  ,COALESCE(Group_Web_Status, 0) AS Group_Web_Status
  ,COALESCE(SKU_Web_Status, 0) AS SKU_Web_Status
  ,TRIM(COALESCE(Merchandising_Approval, '')) AS Merchandising_Approval
  ,TRIM(COALESCE(Marketing_Approval, '')) AS Marketing_Approval
  ,TRIM(COALESCE(Lineup_Status_Code, '')) AS Lineup_Status_Code
  ,TRIM(COALESCE(Lineup_Status, '')) AS Lineup_Status
  ,TRIM(COALESCE(Available_to_Purchase, '')) AS Available_to_Purchase
  ,COALESCE(Ship_Length, 0) AS Ship_Length
  ,COALESCE(Ship_Width, 0) AS Ship_Width
  ,COALESCE(Ship_Height, 0) AS Ship_Height
  ,COALESCE(Length, 0) AS Length
  ,COALESCE(Width, 0) AS Width
  ,COALESCE(Height, 0) AS Height
  ,COALESCE(Weight, 0) AS Weight
  ,COALESCE(Prep_Cubes, 0) AS Prep_Cubes
  ,COALESCE(Cubes, 0) AS Cubes
  ,TRIM(COALESCE(Prep_Flag, '')) AS Prep_Flag
  ,TRIM(COALESCE(Assembly_Required, '')) AS Assembly_Required
  ,COALESCE(Assembly_Time, 0) AS Assembly_Time
  ,TRIM(COALESCE(Leave_in_Box, '')) AS Leave_in_Box
  ,TRIM(COALESCE(Manufacturer_ID, '')) AS Manufacturer_ID
  ,COALESCE(UPC, 0) AS UPC
  ,COALESCE(HTS_Number, 0) AS HTS_Number
  ,TRIM(COALESCE(Original_SKU, '')) AS Original_SKU
  ,TRIM(COALESCE(Representative_Item, '')) AS Representative_Item
  ,COALESCE(Group_Rating_Count, 0) AS Group_Rating_Count
  ,COALESCE(Product_Rating_Count, 0) AS Product_Rating_Count
  ,COALESCE(Group_Star_Rating, 0) AS Group_Star_Rating
  ,COALESCE(Product_Star_Rating, 0) AS Product_Star_Rating
  ,COALESCE(Made_in_ID, 0) AS Made_in_ID
  ,TRIM(COALESCE(Made_in_City, '')) AS Made_in_City
  ,TRIM(COALESCE(Made_in_State, '')) AS Made_in_State
  ,TRIM(COALESCE(Made_in_Country, '')) AS Made_in_Country
  ,COALESCE(Origin_ID, 0) AS Origin_ID
  ,TRIM(COALESCE(Origin_City, '')) AS Origin_City
  ,TRIM(COALESCE(Origin_State, '')) AS Origin_State
  ,TRIM(COALESCE(Origin_Country, '')) AS Origin_Country
  ,TRIM(COALESCE(Copy, '')) AS Copy
  ,TRIM(COALESCE(Web_Description, '')) AS Web_Description
  ,TRIM(COALESCE(Price_Tag_Description, '')) AS Price_Tag_Description
  ,TRIM(COALESCE(Primary_Common_Color, '')) AS Primary_Common_Color
  ,TRIM(COALESCE(Primary_Common_Finish, '')) AS Primary_Common_Finish
  ,TRIM(COALESCE(Floor_Sample_Bedding, '')) AS Floor_Sample_Bedding
  ,TRIM(COALESCE(Employee_Bedding, '')) AS Employee_Bedding
  ,TRIM(COALESCE(Accessory_Story, '')) AS Accessory_Story
  ,TRIM(COALESCE(IndirectDirect, '')) AS IndirectDirect
  ,TRIM(COALESCE(3D_Model_Required, '')) AS 3D_Model_Required
  ,TRIM(COALESCE(3D_Model, '')) AS 3D_Model
  ,TRIM(COALESCE(Scene_7_Image_URL, '')) AS Scene_7_Image_URL
  ,TRIM(COALESCE(`Havertys.com_URL`, '')) AS `Havertys.com_URL`
  ,COALESCE(Advertised_Price, 0) AS Advertised_Price
  ,COALESCE(Supply_Chain_Analyst_fk, 0) AS Supply_Chain_Analyst_fk
  ,COALESCE(Supply_Chain_Analyst_nk, 0) AS Supply_Chain_Analyst_nk
  ,COALESCE(Supply_Chain_Analyst_FullName, '') AS Supply_Chain_Analyst_FullName
  ,TRIM(COALESCE(Product_Features, '')) AS Product_Features
  ,COALESCE(Origin, '') AS Origin
  ,COALESCE(FOB_Cost, 0.00) AS FOB_Cost
  ,COALESCE(Warehouse_Cost, 0.00) AS Warehouse_Cost
  ,udfSHA1withPython(array(SKU_Description,Vendor_Color_ID,Vendor_Color,Vendor_Color_Hex_Code,Vendor_Finish_ID,Vendor_Finish,Configuration_ID,Configuration,Display_Configuration,Size_ID,Size,Original_Vendor_ID,Collection_ID,Collection,Group_ID,Group,Product_ID,Product,Reporting_Class_Code,Reporting_Class,Super_Category_ID,Super_Category,Category_ID,Category,Sub_Category_ID,Sub_Category,Motion,Dining_Style,Bedroom_Style,Standalone_bed,SKU_Type,Stocked,Type_of_SKU,Vendor_ID,Factory_ID,Special_Order_Available,Vendor_Lead_Time,Product_Lead_Time,Group_Active_Start,Group_Active_End,SKU_Flow_StartDate,SKU_Flow_EndDate,Stocked_at_EDC,Stocked_at_WDC,Stocked_at_FDC,Group_Web_Status,SKU_Web_Status,Merchandising_Approval,Marketing_Approval,Lineup_Status,Available_to_Purchase,Ship_Length,Ship_Width,Ship_Height,Length,Width,Height,Weight,Prep_Cubes,Cubes,Prep_Flag,Assembly_Required,Assembly_Time,Leave_in_Box,Manufacturer_ID,UPC,HTS_Number,Original_SKU,Representative_Item,Group_Rating_Count,Product_Rating_Count,Group_Star_Rating,Product_Star_Rating,Made_in_ID,Made_in_City,Made_in_State,Made_in_Country,Origin_ID,Origin_City,Origin_State,Origin_Country,Copy,Web_Description,Price_Tag_Description,Primary_Common_Color,Primary_Common_Finish,Floor_Sample_Bedding,Employee_Bedding,Accessory_Story,IndirectDirect,3D_Model_Required,3D_Model,Scene_7_Image_URL,`Havertys.com_URL`,Advertised_Price, Supply_Chain_Analyst_nk, Supply_Chain_Analyst_FullName, Product_Features, Origin, FOB_Cost, Warehouse_Cost)) AS HashKey
  ,0 AS SourceSystem_fk
  ,CAST({0} AS INT) AS ETLBatchID_Insert
  ,CAST({0} AS INT) AS ETLBatchID_Update
FROM Staging
""".format(varMasterProcessNumber)

# COMMAND ----------

Staging_DF = spark.sql(StagingQuery)

# COMMAND ----------

Staging_DF.cache()
Staging_DF.createOrReplaceTempView("Staging")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM Staging

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load SKU dimension for comparison
# MAGIC Load the SKU dimension from the data warehouse for comparison to the source data in the Data Lake

# COMMAND ----------

Dim_SKUQuery = """
(SELECT
SKU_sk AS SKU_sk_Dest,
SKU_Unformatted_nk AS SKU_Unformatted_nk_Dest,
HashKey AS HashKey_Dest,
ETLBatchID_Insert AS ETLBatchID_Insert_Dest
FROM
DW.Dim_SKU) Dim_SKU 
"""

# COMMAND ----------

Dim_SKU_DF = (spark.read
    .jdbc(url=jdbcUrl, table=Dim_SKUQuery, properties=connectionProperties)
)

# COMMAND ----------

Dim_SKU_DF.cache()
Dim_SKU_DF.createOrReplaceTempView("Dim_SKU")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM Dim_SKU

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation:
# MAGIC Here we are checking which of our Staging records are updates by checking which already exist in our dimension.  We compare our hash keys to see if the record has changed in any way.  We store the results to a dataframe called UpdateRecordsDF

# COMMAND ----------

UpdateRecordsDF = spark.sql("""
SELECT 
Staging.*, Dim_SKU.*
FROM 
Staging
INNER JOIN Dim_SKU ON Staging.SKU_Unformatted_nk = Dim_SKU.SKU_Unformatted_nk_Dest
WHERE Staging.HashKey <> Dim_SKU.HashKey_Dest 
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation:
# MAGIC Here we are checking which of our Staging records are inserts by checking which don't exist in our dimension.  We store the results to a dataframe called NewRecordsDF.  

# COMMAND ----------

NewRecordsDF = spark.sql("""
SELECT 
Staging.*, Dim_SKU.*
FROM 
Staging
LEFT OUTER JOIN Dim_SKU ON Staging.SKU_Unformatted_nk = Dim_SKU.SKU_Unformatted_nk_Dest
WHERE Dim_SKU.SKU_sk_Dest IS NULL
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
# MAGIC   "queryCustom"  -> "TRUNCATE TABLE Updates.Dim_SKU_Update"
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
  .jdbc(url=jdbcUrl, table="Updates.Dim_SKU_Update", mode="append", properties=connectionProperties) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation:
# MAGIC Inserting directly to the dimension using JDBC.  Dropping columns that aren't needed.

# COMMAND ----------

#Creating the table.  JDBC creates the table or overwrites it
NewRecordsDF.drop('SKU_sk_Dest', 'SKU_Unformatted_nk_Dest', 'HashKey_Dest', 'ETLBatchID_Insert_Dest').write \
  .jdbc(url=jdbcUrl, table="DW.Dim_SKU", mode="append", properties=connectionProperties) 

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
# MAGIC   "queryCustom"  -> "Exec dbo.usp_Load_Dim_SKU"
# MAGIC ))
# MAGIC 
# MAGIC sqlContext.sqlDBQuery(storedproc)

# COMMAND ----------

