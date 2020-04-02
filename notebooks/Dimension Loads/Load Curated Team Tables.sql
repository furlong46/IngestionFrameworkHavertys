-- Databricks notebook source
-- MAGIC %run "./Initialize Connections and Functions" $BulkInsertTableName_Update="" $BulkInsertTableName_Insert=""

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS DataLakeCurated

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ipcorpdta_tbdwempDF = spark.read \
-- MAGIC        .parquet("/mnt/datalake/staging/master data/employee/DB203002/ipcorpdta_tbdwemp") \
-- MAGIC        .createOrReplaceTempView("ipcorpdta_tbdwemp")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ipcorpdta_tbdwbldDF = spark.read \
-- MAGIC        .parquet("/mnt/datalake/staging/sales/DB203002/ipcorpdta_tbdwbld") \
-- MAGIC        .createOrReplaceTempView("ipcorpdta_tbdwbld")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ipcorpdta_tbdwwrtDF = spark.read \
-- MAGIC        .parquet("/mnt/datalake/staging/sales/DB203002/ipcorpdta_tbdwwrt") \
-- MAGIC        .createOrReplaceTempView("ipcorpdta_tbdwwrt")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC SalesSQL = """
-- MAGIC WITH SalesTeamBaseWritten AS (
-- MAGIC SELECT DISTINCT SALEEMPID, DIVISION * 1000000 + MARKET * 1000 + SALEASC AS SALES_NUMBER, COSALEEMPID, DIVISION * 1000000 + MARKET * 1000 + COSALEASC AS COSALES_NUMBER, BRANCH
-- MAGIC FROM ipcorpdta_tbdwwrt
-- MAGIC )
-- MAGIC ,
-- MAGIC SalesTeamBaseDelivered AS (
-- MAGIC SELECT DISTINCT SALEEMPID, DIVISION * 1000000 + MARKET * 1000 + SALEASC AS SALES_NUMBER, COSALEEMPID, DIVISION * 1000000 + MARKET * 1000 + COSALEASC AS COSALES_NUMBER, BRANCH
-- MAGIC FROM ipcorpdta_tbdwbld
-- MAGIC )
-- MAGIC ,
-- MAGIC SalesTeamW AS (
-- MAGIC SELECT 
-- MAGIC CASE WHEN B.SALEEMPID IS NULL AND B.SALES_NUMBER IS NULL THEN -1 
-- MAGIC      WHEN B.SALEEMPID IS NULL AND B.SALES_NUMBER IS NOT NULL AND empl.EMPLOYEE_NUMBER IS NULL THEN 0 
-- MAGIC      ELSE COALESCE(B.SALEEMPID, CAST(TRIM(empl.EMPLOYEE_NUMBER) AS INT)) END 
-- MAGIC      AS SALEEMPID, 
-- MAGIC CASE WHEN B.COSALEEMPID IS NULL AND B.COSALES_NUMBER IS NULL THEN -1 
-- MAGIC      WHEN B.COSALEEMPID IS NULL AND B.COSALES_NUMBER IS NOT NULL AND coempl.EMPLOYEE_NUMBER IS NULL THEN 0 
-- MAGIC      ELSE COALESCE(B.COSALEEMPID, CAST(TRIM(coempl.EMPLOYEE_NUMBER) AS INT)) END 
-- MAGIC      AS COSALEEMPID,
-- MAGIC ROW_NUMBER() OVER(PARTITION BY B.SALEEMPID, B.SALES_NUMBER, B.COSALEEMPID, B.COSALES_NUMBER ORDER BY B.BRANCH, empl.BRANCH, coempl.BRANCH, CAST(TRIM(empl.EMPLOYEE_NUMBER) AS INT) DESC, CAST(TRIM(coempl.EMPLOYEE_NUMBER) AS INT) DESC) AS RowNum
-- MAGIC FROM
-- MAGIC SalesTeamBaseWritten B
-- MAGIC LEFT OUTER JOIN ipcorpdta_tbdwemp empl ON B.SALES_NUMBER = empl.SALES_NUMBER
-- MAGIC LEFT OUTER JOIN ipcorpdta_tbdwemp coempl ON B.COSALES_NUMBER = coempl.SALES_NUMBER
-- MAGIC )
-- MAGIC ,
-- MAGIC SalesTeamD AS (
-- MAGIC SELECT 
-- MAGIC CASE WHEN B.SALEEMPID IS NULL AND B.SALES_NUMBER IS NULL THEN -1 
-- MAGIC      WHEN B.SALEEMPID IS NULL AND B.SALES_NUMBER IS NOT NULL AND empl.EMPLOYEE_NUMBER IS NULL THEN 0 
-- MAGIC      ELSE COALESCE(B.SALEEMPID, CAST(TRIM(empl.EMPLOYEE_NUMBER) AS INT)) END 
-- MAGIC      AS SALEEMPID, 
-- MAGIC CASE WHEN B.COSALEEMPID IS NULL AND B.COSALES_NUMBER IS NULL THEN -1 
-- MAGIC      WHEN B.COSALEEMPID IS NULL AND B.COSALES_NUMBER IS NOT NULL AND coempl.EMPLOYEE_NUMBER IS NULL THEN 0 
-- MAGIC      ELSE COALESCE(B.COSALEEMPID, CAST(TRIM(coempl.EMPLOYEE_NUMBER) AS INT)) END 
-- MAGIC      AS COSALEEMPID,
-- MAGIC ROW_NUMBER() OVER(PARTITION BY B.SALEEMPID, B.SALES_NUMBER, B.COSALEEMPID, B.COSALES_NUMBER ORDER BY B.BRANCH, empl.BRANCH, coempl.BRANCH, CAST(TRIM(empl.EMPLOYEE_NUMBER) AS INT) DESC, CAST(TRIM(coempl.EMPLOYEE_NUMBER) AS INT) DESC) AS RowNum
-- MAGIC FROM
-- MAGIC SalesTeamBaseDelivered B
-- MAGIC LEFT OUTER JOIN ipcorpdta_tbdwemp empl ON B.SALES_NUMBER = empl.SALES_NUMBER
-- MAGIC LEFT OUTER JOIN ipcorpdta_tbdwemp coempl ON B.COSALES_NUMBER = coempl.SALES_NUMBER
-- MAGIC )
-- MAGIC ,
-- MAGIC SalesTeam AS (
-- MAGIC --The distinct is really not needed as the UNION appears to be removing duplicates
-- MAGIC SELECT DISTINCT * FROM (
-- MAGIC SELECT DISTINCT SALEEMPID, COSALEEMPID FROM SalesTeamW WHERE RowNum = 1
-- MAGIC UNION 
-- MAGIC SELECT DISTINCT SALEEMPID, COSALEEMPID FROM SalesTeamD WHERE RowNum = 1
-- MAGIC ) AS TBL
-- MAGIC )
-- MAGIC ,
-- MAGIC staging_SalesTeam AS (
-- MAGIC SELECT DISTINCT
-- MAGIC COALESCE(GREATEST(SALEEMPID, COSALEEMPID), -1) AS TeamMember1,
-- MAGIC COALESCE(LEAST(SALEEMPID, COSALEEMPID), -1) AS TeamMember2,
-- MAGIC 'Sales' AS TeamType 
-- MAGIC FROM SalesTeam
-- MAGIC )
-- MAGIC 
-- MAGIC SELECT * FROM staging_SalesTeam WHERE NOT(TeamMember1 = 0 OR TeamMember2 = 0)
-- MAGIC """

-- COMMAND ----------

-- MAGIC %python
-- MAGIC salesteamtDF = spark.sql(SalesSQL) \
-- MAGIC        .createOrReplaceTempView("staging_SalesTeam")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DesignerQuery = """
-- MAGIC WITH DesignerTeam AS (
-- MAGIC SELECT DISTINCT COALESCE(DESIGNER, -1) AS DESIGNER, COALESCE(CODESIGNER, -1) AS CODESIGNER
-- MAGIC FROM ipcorpdta_tbdwwrt
-- MAGIC UNION
-- MAGIC SELECT DISTINCT COALESCE(DESIGNER, -1) AS DESIGNER, COALESCE(CODESIGNER, -1) AS CODESIGNER
-- MAGIC FROM ipcorpdta_tbdwbld
-- MAGIC )
-- MAGIC 
-- MAGIC SELECT DISTINCT
-- MAGIC GREATEST(DESIGNER, CODESIGNER) AS TeamMember1,
-- MAGIC LEAST(DESIGNER, CODESIGNER) AS TeamMember2,
-- MAGIC 'Designer' AS TeamType 
-- MAGIC FROM DesignerTeam
-- MAGIC """

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DesignerTeamDF = spark.sql(DesignerQuery) \
-- MAGIC        .createOrReplaceTempView("staging_DesignerTeam")

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS DataLakeCurated.SalesTeam 
USING DELTA
LOCATION '/mnt/datalake/curated/sales/team/DB203002/salesteam/'
AS
SELECT * FROM staging_SalesTeam WHERE 1 = 0

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS DataLakeCurated.DesignerTeam 
USING DELTA
LOCATION '/mnt/datalake/curated/sales/team/DB203002/designerteam/'
AS
SELECT * FROM staging_DesignerTeam WHERE 1 = 0

-- COMMAND ----------

MERGE INTO DataLakeCurated.SalesTeam AS cur
USING staging_SalesTeam AS stage 
ON cur.TeamMember1 = stage.TeamMember1 AND cur.TeamMember2 = stage.TeamMember2
WHEN NOT MATCHED THEN
  INSERT *

-- COMMAND ----------

MERGE INTO DataLakeCurated.DesignerTeam AS cur
USING staging_DesignerTeam AS stage 
ON cur.TeamMember1 = stage.TeamMember1 AND cur.TeamMember2 = stage.TeamMember2
WHEN NOT MATCHED THEN
  INSERT *

-- COMMAND ----------

SELECT TeamMember1, TeamMember2, COUNT(1) FROM DataLakeCurated.SalesTeam GROUP BY TeamMember1, TeamMember2 HAVING COUNT(1)>1

-- COMMAND ----------

SELECT TeamMember1, TeamMember2, COUNT(1) FROM DataLakeCurated.DesignerTeam GROUP BY TeamMember1, TeamMember2 HAVING COUNT(1)>1

-- COMMAND ----------

--OPTIMIZE DataLakeCurated.SalesTeam

-- COMMAND ----------

--OPTIMIZE DataLakeCurated.DesignerTeam

-- COMMAND ----------

