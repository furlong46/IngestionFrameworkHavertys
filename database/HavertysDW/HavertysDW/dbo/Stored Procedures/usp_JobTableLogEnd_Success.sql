


--dbo.[usp_JobTableLogEnd_Success] 1
CREATE   PROCEDURE [dbo].[usp_JobTableLogEnd_Success] (
@JobTableLogID int
)
AS

DECLARE @Status varchar(50) = 'Success'

UPDATE [ETL].[JobTableLog] SET [Status] = @Status, EndTime = GETDATE() WHERE JobTableLogID = @JobTableLogID 

SELECT 'Stop-' + @Status AS LogStatus
