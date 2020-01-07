
--dbo.usp_JobConfigurationDB2MetadataList 1
CREATE   PROCEDURE [dbo].[usp_JobConfigurationDB2MetadataList] (
@MasterProcessNumber int
)
AS

SELECT 
	JM.StartTime,
	DB2IL.[DB2TableID] AS TableID, 
	DB2IL.[SchemaTable], 
	DB2IL.[PKColumnList], 
	DB2IL.[ControlTable], 
	DB2IL.[ControlFilterColumn], 
	--DB2IL.[ControlStartDate], --we should be getting this from DB2
	--DB2IL.[ControlEndDate], --we should be getting this from DB2
	--DB2IL.[ControlFilterValue], --For future use if we're filtering on something other than date
	DB2IL.[ControlFilterType], 
	--DB2IL.[ControlProcessNumber], --we should be getting this from DB2
	DB2IL.[SelectQuery], 
	DB2IL.[DataLakeStagingFolder]
FROM
[ETL].[JobMaster] JM
INNER JOIN [ETL].[JobConfiguration] JCon ON JCon.TableControlType='DB2' 
	AND JM.JobConfiguration = JCon.JobConfiguration 
	AND JM.Status IN ('Running','ReRunning') 
	AND JM.MasterProcessNumber = @MasterProcessNumber
INNER JOIN [ETL].[DB2MetadataIngestionList] DB2IL ON JCon.TableID = DB2IL.DB2TableID AND JCon.ExecuteFlag = CONVERT(bit, 1)
WHERE 
NOT EXISTS (SELECT 1 
			FROM [ETL].[JobTableLog] 
			WHERE MasterProcessNumber = JM.MasterProcessNumber 
				AND TableControlType='DB2' 
				AND TableID = JCon.TableID
				AND JobConfiguration = JM.JobConfiguration 
				AND [Status] IN ('Running','ReRunning','Success'))
