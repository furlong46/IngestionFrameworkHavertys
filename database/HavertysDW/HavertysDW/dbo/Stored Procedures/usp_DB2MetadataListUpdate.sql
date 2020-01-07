
--dbo.[dbo].[usp_DB2MetadataListUpdate] 1, '2010-07-01 00:00:00.000', '2019-07-18 00:00:00.000', 0
CREATE   PROCEDURE [dbo].[usp_DB2MetadataListUpdate] (
@JobTableLogID int,
@ControlStartDate datetime,
@ControlEndDate datetime,
@ControlProcessNumber int,
@ControlStartDateOver datetime
)
AS

DECLARE @StartTime datetime, @TableID int

SELECT @StartTime = StartTime, @TableID = TableID 
FROM [ETL].[JobTableLog] 
WHERE JobTableLogID = @JobTableLogID

UPDATE [ETL].[DB2MetadataIngestionList] 
SET ControlStartDate = ISNULL(@ControlStartDateOver, @ControlStartDate), ControlEndDate = @ControlEndDate, ControlProcessNumber = @ControlProcessNumber
WHERE DB2TableID = @TableID 

