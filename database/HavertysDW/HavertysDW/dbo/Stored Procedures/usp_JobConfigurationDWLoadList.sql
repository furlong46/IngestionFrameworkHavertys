CREATE    PROCEDURE [dbo].[usp_JobConfigurationDWLoadList] (
@MasterProcessNumber int,
@DWType varchar(10) 
)
AS
SELECT 	JM.StartTime, DWL.[TableID] AS TableID, DWL.[SchemaTable], DWL.[DWType], DWL.[DbxNotebookPath]
FROM [ETL].[JobMaster] JM 
INNER JOIN [ETL].[JobConfiguration] JCon ON JCon.TableControlType='DW' AND JM.JobConfiguration = JCon.JobConfiguration AND JM.Status IN ('Running','ReRunning') AND JM.MasterProcessNumber = @MasterProcessNumber
INNER JOIN [ETL].[DWLoadList] DWL ON JCon.TableID = DWL.TableID AND JCon.ExecuteFlag = CONVERT(bit, 1) AND DWL.Disabled = CONVERT(bit, 0) AND DWL.DWType = @DWType
WHERE NOT EXISTS (
	SELECT 1 FROM [ETL].[JobTableLog] 
	WHERE MasterProcessNumber = JM.MasterProcessNumber AND TableControlType='DW' AND TableID = JCon.TableID	AND JobConfiguration = JM.JobConfiguration 
	AND [Status] IN ('Running','ReRunning','Success'))