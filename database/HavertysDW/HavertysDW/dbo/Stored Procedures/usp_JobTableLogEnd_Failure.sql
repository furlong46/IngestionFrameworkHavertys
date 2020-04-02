




--dbo.[usp_JobTableLogEnd_Failure] 1, 'Azure', 1, 'Pcrundata.TbGroup', 'NightlyLoad'
CREATE    PROCEDURE [dbo].[usp_JobTableLogEnd_Failure] (
@JobTableLogID int
)
AS

DECLARE @Status varchar(50) = 'Failure', @MasterProcessNumber int

SET @MasterProcessNumber = (SELECT MasterProcessNumber FROM [ETL].[JobTableLog] WHERE JobTableLogID = @JobTableLogID)

UPDATE [ETL].[JobTableLog] SET [Status] = @Status, EndTime = GETDATE() WHERE JobTableLogID = @JobTableLogID 

--Also fail the master job
Exec [dbo].[usp_MasterJobEnd_Failure] @MasterProcessNumber

SELECT 'Stop-' + @Status AS LogStatus
