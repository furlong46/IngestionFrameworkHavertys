
--dbo.[usp_AzureMetadataListUpdate] 1
CREATE   PROCEDURE [dbo].[usp_AzureMetadataListUpdate] (
@JobTableLogID int
)
AS

DECLARE @StartTime datetime, @TableID int

SELECT @StartTime = StartTime, @TableID = TableID 
FROM [ETL].[JobTableLog] 
WHERE JobTableLogID = @JobTableLogID

UPDATE [ETL].[AzureMetadataIngestionList] SET FilterColumnValue = @StartTime WHERE AzureTableID = @TableID AND FilterColumn IS NOT NULL

