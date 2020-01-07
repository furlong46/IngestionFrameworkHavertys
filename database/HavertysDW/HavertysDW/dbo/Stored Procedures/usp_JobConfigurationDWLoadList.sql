








--dbo.usp_JobConfigurationDWLoadList 269, 'Dim'
CREATE    PROCEDURE [dbo].[usp_JobConfigurationDWLoadList] (
@MasterProcessNumber int,
@DWType varchar(10)
)
AS

--DECLARE @MasterProcessNumber int = 269, @DWType varchar(10) = 'Dim'

DECLARE @max int, @cnt int = 1

IF OBJECT_ID('tempdb..#DWList') IS NOT NULL
    DROP TABLE #DWList;

IF OBJECT_ID('tempdb..#LoadList') IS NOT NULL
    DROP TABLE #LoadList;

IF OBJECT_ID('tempdb..#DWLogStatus') IS NOT NULL
    DROP TABLE #DWLogStatus;

SELECT 
	DW.TableID,
	DW.[SchemaTable]
INTO #DWList
FROM
[ETL].[DWLoadList] DW
WHERE 
DW.DWType = @DWType

SELECT 
	ROW_NUMBER() OVER(ORDER BY DW.TableID) AS RowNum,
	JM.StartTime,
	DW.TableID, 
	DW.[SchemaTable],
	JCon.JobConfiguration 
INTO #LoadList
FROM
[ETL].[JobMaster] JM
INNER JOIN [ETL].[JobConfiguration] JCon ON JCon.TableControlType='DW' 
	AND JM.JobConfiguration = JCon.JobConfiguration 
	AND JM.Status IN ('Running','ReRunning') 
	AND JM.MasterProcessNumber = @MasterProcessNumber
INNER JOIN [ETL].[DWLoadList] DW ON JCon.TableID = DW.TableID AND JCon.ExecuteFlag = CONVERT(bit, 1) AND DW.[Disabled] = CONVERT(bit,  0)
WHERE 
DW.DWType = @DWType
AND NOT EXISTS (SELECT 1 
			FROM [ETL].[JobTableLog] 
			WHERE MasterProcessNumber = JM.MasterProcessNumber 
				AND TableControlType='DW' 
				AND TableID = JCon.TableID
				AND JobConfiguration = JM.JobConfiguration 
				AND [Status] IN ('Running','ReRunning','Success'))

--set max int for looping
SET @max = ISNULL((SELECT COUNT(1) FROM #LoadList), 0)

--create temp table to store DWLogStart results
CREATE TABLE #DWLogStatus 
(
LogStatus varchar(50),
JobTableLogID int,
SchemaTable varchar(50)
)

WHILE @cnt <= @max
BEGIN

DECLARE @SchemaTable varchar(50), @JobConfiguration varchar(50)

SELECT @SchemaTable = SchemaTable, @JobConfiguration = JobConfiguration FROM #LoadList WHERE RowNum = @cnt

INSERT INTO #DWLogStatus
EXEC [dbo].[usp_JobDWLogStart] @MasterProcessNumber, @SchemaTable, @JobConfiguration

SET @cnt = @cnt + 1
END

DECLARE @SQL varchar(max) = 
'WITH DWStatus AS (
SELECT ' +  
(SELECT
SchemaTable = STRING_AGG('ISNULL([' + REPLACE(L.SchemaTable, '.', '') + '_Status' + '], ''Stop-RunningOrSuccess'')' + 'AS [' + REPLACE(L.SchemaTable, '.', '') + '_Status' + ']', ',')
FROM
#DWList L)
+ ' 
FROM (
SELECT 
--L.TableID,
SchemaTable = REPLACE(L.SchemaTable, ''.'', '''') + ''_Status'',
S.LogStatus--,
--S.JobTableLogID
FROM
#DWList L
LEFT OUTER JOIN #DWLogStatus S ON L.SchemaTable = S.SchemaTable
) AS List
PIVOT (MAX(LogStatus)
FOR SchemaTable IN (' + 
(SELECT
SchemaTable = STRING_AGG('[' + REPLACE(L.SchemaTable, '.', '') + '_Status' + ']', ',')
FROM
#DWList L)
+')
) AS PVT
)
,
LogID AS (
SELECT ' + 
(SELECT
SchemaTable = STRING_AGG('ISNULL([' + REPLACE(L.SchemaTable, '.', '') + '_JobTableLogID' + '], 0)' + 'AS [' + REPLACE(L.SchemaTable, '.', '') + '_JobTableLogID' + ']', ',')
FROM
#DWList L)
+'
FROM (
SELECT 
--L.TableID,
SchemaTable = REPLACE(L.SchemaTable, ''.'', '''') + ''_JobTableLogID'',
--S.LogStatus--,
S.JobTableLogID
FROM
#DWList L
LEFT OUTER JOIN #DWLogStatus S ON L.SchemaTable = S.SchemaTable
) AS List
PIVOT (MAX(JobTableLogID)
FOR SchemaTable IN (' + 
(SELECT
SchemaTable = STRING_AGG('[' + REPLACE(L.SchemaTable, '.', '') + '_JobTableLogID' + ']', ',')
FROM
#DWList L)
+')
) AS PVT
)

SELECT * 
FROM DWStatus CROSS JOIN LogID'

EXECUTE(@SQL)