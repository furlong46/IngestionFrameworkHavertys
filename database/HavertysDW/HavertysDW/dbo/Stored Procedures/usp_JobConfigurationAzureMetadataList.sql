








--dbo.usp_JobConfigurationAzureMetadataList 1
CREATE    PROCEDURE [dbo].[usp_JobConfigurationAzureMetadataList] (
@MasterProcessNumber int
)
AS

DECLARE @CDDCOffset int = (SELECT ConfigValue FROM ETL.Configurations WHERE COnfigKey='CDCOffsetDays')

SELECT 
	JM.StartTime,
	AIL.[AzureTableID] AS TableID, 
	AIL.[SchemaTable], 
	AIL.[PKColumnList], 
	AIL.[FilterColumn], 
	[FilterColumnValue] = ISNULL(CASE WHEN AIL.[FilterColumnValue] IS NOT NULL THEN DATEADD(day, -1 * @CDDCOffset, AIL.[FilterColumnValue]) ELSE AIL.[FilterColumnValue] END, ''), 
	AIL.[FilterColumnDatatype],
	AIL.[SelectQuery], 
	AIL.[DataLakeStagingFolder]
FROM
[ETL].[JobMaster] JM
INNER JOIN [ETL].[JobConfiguration] JCon ON JCon.TableControlType='Azure' 
	AND JM.JobConfiguration = JCon.JobConfiguration 
	AND JM.Status IN ('Running','ReRunning') 
	AND JM.MasterProcessNumber = @MasterProcessNumber
INNER JOIN [ETL].[AzureMetadataIngestionList] AIL ON JCon.TableID = AIL.AzureTableID AND JCon.ExecuteFlag = CONVERT(bit, 1) AND AIL.[Disabled] = CONVERT(bit,  0)
WHERE 
NOT EXISTS (SELECT 1 
			FROM [ETL].[JobTableLog] 
			WHERE MasterProcessNumber = JM.MasterProcessNumber 
				AND TableControlType='Azure' 
				AND TableID = JCon.TableID
				AND JobConfiguration = JM.JobConfiguration 
				AND [Status] IN ('Running','ReRunning','Success'))


